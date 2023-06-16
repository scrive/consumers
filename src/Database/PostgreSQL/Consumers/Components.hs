module Database.PostgreSQL.Consumers.Components (
    runConsumer
  , runConsumerWithIdleSignal
  , spawnListener
  , spawnMonitor
  , spawnDispatcher
  ) where

import Control.Applicative
import Control.Concurrent.Lifted
import Control.Concurrent.STM hiding (atomically)
import Control.Exception (AsyncException(ThreadKilled))
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Time
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Data.Function
import Data.Int
import Data.Maybe
import Data.Monoid
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Log
import Prelude
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Thread.Lifted as T
import qualified Data.Foldable as F
import qualified Data.Map.Strict as M

import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.Consumers.Consumer
import Database.PostgreSQL.Consumers.Utils

-- | Run the consumer. The purpose of the returned monadic
-- action is to wait for currently processed jobs and clean up.
-- This function is best used in conjunction with 'finalize' to
-- seamlessly handle the finalization.
runConsumer
  :: ( MonadBaseControl IO m, MonadLog m, MonadMask m, MonadTime m, Eq idx, Show idx
     , FromSQL idx, ToSQL idx )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> m (m ())
runConsumer cc cs = runConsumerWithMaybeIdleSignal cc cs Nothing

runConsumerWithIdleSignal
  :: ( MonadBaseControl IO m, MonadLog m, MonadMask m, MonadTime m, Eq idx, Show idx
     , FromSQL idx, ToSQL idx )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> TMVar Bool
  -> m (m ())
runConsumerWithIdleSignal cc cs idleSignal = runConsumerWithMaybeIdleSignal cc cs (Just idleSignal)

-- | Run the consumer and also signal whenever the consumer is waiting for
-- getNotification or threadDelay.
runConsumerWithMaybeIdleSignal
  :: ( MonadBaseControl IO m, MonadLog m, MonadMask m, MonadTime m, Eq idx, Show idx
     , FromSQL idx, ToSQL idx )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> Maybe (TMVar Bool)
  -> m (m ())
runConsumerWithMaybeIdleSignal cc cs mIdleSignal
  | ccMaxRunningJobs cc < 1 = do
      logInfo_ "ccMaxRunningJobs < 1, not starting the consumer"
      return $ return ()
  | otherwise = do
      semaphore <- newMVar ()
      runningJobsInfo <- liftBase $ newTVarIO M.empty
      runningJobs <- liftBase $ newTVarIO 0

      skipLockedTest :: Either DBException () <-
        try . runDBT cs defaultTransactionSettings $
        runSQL_ "SELECT TRUE FOR UPDATE SKIP LOCKED"
      -- If we can't lock rows using 'skip locked' throw an exception
      either (const $ error "PostgreSQL version with support for SKIP LOCKED is required") pure skipLockedTest

      cid <- registerConsumer cc cs
      localData ["consumer_id" .= show cid] $ do
        listener <- spawnListener cc cs semaphore
        monitor <- localDomain "monitor" $ spawnMonitor cc cs cid
        dispatcher <- localDomain "dispatcher" $ spawnDispatcher cc
          cs cid semaphore runningJobsInfo runningJobs mIdleSignal
        return . localDomain "finalizer" $ do
          stopExecution listener
          stopExecution dispatcher
          waitForRunningJobs runningJobsInfo runningJobs
          stopExecution monitor
          unregisterConsumer cc cs cid
  where
    waitForRunningJobs runningJobsInfo runningJobs = do
      initialJobs <- liftBase $ readTVarIO runningJobsInfo
      (`fix` initialJobs) $ \loop jobsInfo -> do
        -- If jobs are still running, display info about them.
        unless (M.null jobsInfo) $ do
          logInfo "Waiting for running jobs" $ object [
              "job_id" .= showJobsInfo jobsInfo
            ]
        join . atomically $ do
          jobs <- readTVar runningJobs
          if jobs == 0
            then return $ return ()
            else do
              newJobsInfo <- readTVar runningJobsInfo
              -- If jobs info didn't change, wait for it to change.
              -- Otherwise loop so it either displays the new info
              -- or exits if there are no jobs running anymore.
              if newJobsInfo == jobsInfo
                then retry
                else return $ loop newJobsInfo
      where
        showJobsInfo = M.foldr (\idx acc -> show idx : acc) []

-- | Spawn a thread that generates signals for the
-- dispatcher to probe the database for incoming jobs.
spawnListener
  :: (MonadBaseControl IO m, MonadMask m)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> MVar ()
  -> m ThreadId
spawnListener cc cs semaphore =
  forkP "listener" $
  case ccNotificationChannel cc of
    Just chan ->
      runDBT cs noTs . bracket_ (listen chan) (unlisten chan)
      . forever $ do
      -- If there are many notifications, we need to collect them
      -- as soon as possible, because they are stored in memory by
      -- libpq. They are also not squashed, so we perform the
      -- squashing ourselves with the help of MVar ().
      void . getNotification $ ccNotificationTimeout cc
      lift signalDispatcher
    Nothing -> forever $ do
      liftBase . threadDelay $ ccNotificationTimeout cc
      signalDispatcher
  where
    signalDispatcher = do
      liftBase $ tryPutMVar semaphore ()

    noTs = defaultTransactionSettings {
      tsAutoTransaction = False
    }

-- | Spawn a thread that monitors working consumers
-- for activity and periodically updates its own.
spawnMonitor
  :: forall m idx job. (MonadBaseControl IO m, MonadLog m, MonadMask m, MonadTime m,
                        Show idx, FromSQL idx)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> m ThreadId
spawnMonitor ConsumerConfig{..} cs cid = forkP "monitor" . forever $ do
  runDBT cs ts $ do
    now <- currentTime
    -- Update last_activity of the consumer.
    ok <- runSQL01 $ smconcat [
        "UPDATE" <+> raw ccConsumersTable
      , "SET last_activity = " <?> now
      , "WHERE id =" <?> cid
      , "  AND name =" <?> unRawSQL ccJobsTable
      ]
    if ok
      then logInfo_ "Activity of the consumer updated"
      else do
        logInfo_ "Consumer is not registered"
        throwM ThreadKilled
  -- Freeing jobs locked by inactive consumers needs to happen
  -- exactly once, otherwise it's possible to free it twice, after
  -- it was already marked as reserved by other consumer, so let's
  -- run it in serializable transaction.
  (inactiveConsumers, freedJobs) <- runDBT cs tsSerializable $ do
    now <- currentTime
    -- Delete all inactive (assumed dead) consumers and get their ids.
    runSQL_ $ smconcat [
        "DELETE FROM" <+> raw ccConsumersTable
      , "  WHERE last_activity +" <?> iminutes 1 <+> "<= " <?> now
      , "    AND name =" <?> unRawSQL ccJobsTable
      , "  RETURNING id::bigint"
      ]
    inactive :: [Int64] <- fetchMany runIdentity
    -- Reset reserved jobs manually, do not rely on the foreign key constraint
    -- to do its job. We also reset finished_at to correctly bump number of
    -- attempts on the next try.
    freedJobs :: [idx] <- if null inactive
      then return []
      else do
        runSQL_ $ smconcat
          [ "UPDATE" <+> raw ccJobsTable
          , "SET reserved_by = NULL"
          , "  , finished_at = NULL"
          , "WHERE reserved_by = ANY(" <?> Array1 inactive <+> ")"
          , "RETURNING id"
          ]
        fetchMany runIdentity
    return (length inactive, freedJobs)
  when (inactiveConsumers > 0) $ do
    logInfo "Unregistered inactive consumers" $ object [
        "inactive_consumers" .= inactiveConsumers
      ]
  unless (null freedJobs) $ do
    logInfo "Freed locked jobs" $ object [
        "freed_jobs" .= map show freedJobs
      ]
  liftBase . threadDelay $ 30 * 1000000 -- wait 30 seconds
  where
    tsSerializable = ts {
      tsIsolationLevel = Serializable
    }

-- | Spawn a thread that reserves and processes jobs.
spawnDispatcher
  :: forall m idx job. ( MonadBaseControl IO m, MonadLog m, MonadMask m, MonadTime m
                       , Show idx, ToSQL idx )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> MVar ()
  -> TVar (M.Map ThreadId idx)
  -> TVar Int
  -> Maybe (TMVar Bool)
  -> m ThreadId
spawnDispatcher ConsumerConfig{..} cs cid semaphore
  runningJobsInfo runningJobs mIdleSignal =
  forkP "dispatcher" . forever $ do
    void $ takeMVar semaphore
    someJobWasProcessed <- loop 1
    if someJobWasProcessed
      then setIdle False
      else setIdle True
  where
    setIdle :: forall m' . (MonadBaseControl IO m') => Bool -> m' ()
    setIdle isIdle = case mIdleSignal of
      Nothing -> return ()
      Just idleSignal -> atomically $ do
        _ <- tryTakeTMVar idleSignal
        putTMVar idleSignal isIdle

    loop :: Int -> m Bool
    loop limit = do
      (batch, batchSize) <- reserveJobs limit
      when (batchSize > 0) $ do
        logInfo "Processing batch" $ object [
            "batch_size" .= batchSize
          ]
        -- Update runningJobs before forking so that we can
        -- adjust maxBatchSize appropriately later. We also
        -- need to mask asynchronous exceptions here as we
        -- rely on correct value of runningJobs to perform
        -- graceful termination.
        mask $ \restore -> do
          atomically $ modifyTVar' runningJobs (+batchSize)
          let subtractJobs = atomically $ do
                modifyTVar' runningJobs (subtract batchSize)
          void . forkP "batch processor"
            . (`finally` subtractJobs) . restore $
            -- Ensures that we only process one job at a time
            -- when running in @'Duplicating'@ mode.
            case batch of
              Left job -> startJob job >>= joinJob >>= updateJob
              Right jobs -> mapM startJob jobs >>= mapM joinJob >>= updateJobs

        when (batchSize == limit) $ do
          maxBatchSize <- atomically $ do
            jobs <- readTVar runningJobs
            when (jobs >= ccMaxRunningJobs) retry
            return $ ccMaxRunningJobs - jobs
          void $ loop $ min maxBatchSize (2*limit)

      return (batchSize > 0)

    reserveJobs :: Int -> m (Either job [job], Int)
    reserveJobs limit = runDBT cs ts $ do
      now <- currentTime
      n <- runSQL $ smconcat [
          "UPDATE" <+> raw ccJobsTable <+> "SET"
        , "  reserved_by =" <?> cid
        , ", attempts = CASE"
        , "    WHEN finished_at IS NULL THEN attempts + 1"
        , "    ELSE 1"
        , "  END"
        , "WHERE id IN (" <> reservedJobs now <> ")"
        , "RETURNING" <+> mintercalate ", " ccJobSelectors
        ]
      -- Decode lazily as we want the transaction to be as short as possible.
      (, n) . limitJobs . F.toList . fmap ccJobFetcher <$> queryResult
      where
        -- Reserve a single job or a list of jobs depending
        -- on which @'ccMode'@ the consumer is running in.
        limitJobs = case ccMode of
          Standard -> Right
          Duplicating _field -> Left . head
        reservedJobs :: UTCTime -> SQL
        reservedJobs now = case ccMode of
          Standard -> smconcat [
              "SELECT id FROM" <+> raw ccJobsTable
            , "WHERE"
            , "       reserved_by IS NULL"
            , "       AND run_at IS NOT NULL"
            , "       AND run_at <= " <?> now
            , "       ORDER BY run_at"
            , "LIMIT" <?> limit
            , "FOR UPDATE SKIP LOCKED"
            ]
          Duplicating field -> smconcat [
              "WITH latest_for_id AS"
            , "   (SELECT id," <+> raw field <+> "FROM" <+> raw ccJobsTable
            , "   ORDER BY run_at," <+> raw field <> ", id DESC LIMIT 1 FOR UPDATE SKIP LOCKED),"
            , "   lock_all AS"
            , "   (SELECT id," <+> raw field <+> "FROM" <+> raw ccJobsTable
            , "   WHERE" <+> raw field <+> "= (SELECT" <+> raw field <+> "FROM latest_for_id)"
            , "     AND id <= (SELECT id FROM latest_for_id)"
            , "   FOR UPDATE SKIP LOCKED)"
            , "SELECT id FROM lock_all"
            ]

    -- Spawn each job in a separate thread.
    startJob :: job -> m (job, m (T.Result Result))
    startJob job = do
      (_, joinFork) <- mask $ \restore -> T.fork $ do
        tid <- myThreadId
        bracket_ (registerJob tid) (unregisterJob tid) . restore $ do
          ccProcessJob job
      return (job, joinFork)
      where
        registerJob tid = atomically $ do
          modifyTVar' runningJobsInfo . M.insert tid $ ccJobIndex job
        unregisterJob tid = atomically $ do
           modifyTVar' runningJobsInfo $ M.delete tid

    -- Wait for all the jobs and collect their results.
    joinJob :: (job, m (T.Result Result)) -> m (idx, Result)
    joinJob (job, joinFork) = joinFork >>= \eres -> case eres of
      Right result -> return (ccJobIndex job, result)
      Left ex -> do
        action <- ccOnException ex job
        logAttention "Unexpected exception caught while processing job" $
          object [
            "job_id" .= show (ccJobIndex job)
          , "exception" .= show ex
          , "action" .= show action
          ]
        return (ccJobIndex job, Failed action)

    -- Update the status of a job running in @'Duplicating'@ mode.
    updateJob :: (idx, Result) -> m ()
    updateJob (idx, result) = runDBT cs ts $ do
      now <- currentTime
      runSQL_ $ case result of
          Ok Remove -> deleteQuery
          -- TODO: Should we be deduplicating when a job fails with 'Remove' or only
          -- remove the failing job?
          Failed Remove -> deleteQuery
          _ -> retryQuery now (isSuccess result) (getAction result)
      where
        deleteQuery = "DELETE FROM" <+> raw ccJobsTable <+> "WHERE" <+> raw idxRow <+> "<=" <?> idx

        retryQuery now success action = smconcat
          [ "UPDATE" <+> raw ccJobsTable <+> "SET"
          , "  reserved_by = NULL"
          , ", run_at = CASE"
          , "    WHEN FALSE THEN run_at"
          ,      retryToSQL
          , "    ELSE NULL" -- processed
          , "  END"
          , if success then smconcat
              [ ", finished_at = CASE"
              , "    WHEN id =" <?> idx <+> "THEN" <?> now
              , "    ELSE NULL"
              , "  END"
              ]
              else ""
          , "WHERE" <+> raw idxRow <+> "<=" <?> idx
          ]
          where
            retryToSQL = case action of
              RerunAfter int -> "WHEN id =" <?> idx <+> "THEN" <?> now <+> "+" <?> int
              RerunAt time -> "WHEN id =" <?> idx <+> "THEN" <?> time
              MarkProcessed -> ""
              Remove -> error "updateJob: 'Remove' should've been filtered out"

        idxRow = case ccMode of
          Standard -> error $ "'updateJob' should never be called when ccMode = " <> show Standard
          Duplicating field -> field

        isSuccess (Ok _) = True
        isSuccess (Failed _) = False

        getAction (Ok action) = action
        getAction (Failed action) = action

    -- Update the status of jobs running in @'Standard'@ mode.
    updateJobs :: [(idx, Result)] -> m ()
    updateJobs results = runDBT cs ts $ do
      now <- currentTime
      runSQL_ $ smconcat
        [ "WITH removed AS ("
        , "  DELETE FROM" <+> raw ccJobsTable
        , "  WHERE id = ANY (" <?> Array1 deletes <+> ")"
        , ")"
        , "UPDATE" <+> raw ccJobsTable <+> "SET"
        , "  reserved_by = NULL"
        , ", run_at = CASE"
        , "    WHEN FALSE THEN run_at"
        ,      smconcat $ M.foldrWithKey (retryToSQL now) [] retries
        , "    ELSE NULL" -- processed
        , "  END"
        , ", finished_at = CASE"
        , "    WHEN id = ANY(" <?> Array1 successes <+> ") THEN " <?> now
        , "    ELSE NULL"
        , "  END"
        , "WHERE id = ANY (" <?> Array1 (map fst updates) <+> ")"
        ]
      where
        retryToSQL now (Left int) ids =
          ("WHEN id = ANY(" <?> Array1 ids <+> ") THEN " <?> now <> " +" <?> int :)
        retryToSQL _   (Right time) ids =
          ("WHEN id = ANY(" <?> Array1 ids <+> ") THEN" <?> time :)

        retries = foldr (step . f) M.empty updates
          where
            f (idx, result) = case result of
              Ok     action -> (idx, action)
              Failed action -> (idx, action)

            step (idx, action) iretries = case action of
              MarkProcessed  -> iretries
              RerunAfter int -> M.insertWith (++) (Left int) [idx] iretries
              RerunAt time   -> M.insertWith (++) (Right time) [idx] iretries
              Remove         -> error
                "updateJobs: 'Remove' should've been filtered out"

        successes = foldr step [] updates
          where
            step (idx, Ok     _) acc = idx : acc
            step (_,   Failed _) acc =       acc

        (deletes, updates) = foldr step ([], []) results
          where
            step job@(idx, result) (ideletes, iupdates) = case result of
              Ok     Remove -> (idx : ideletes, iupdates)
              Failed Remove -> (idx : ideletes, iupdates)
              _             -> (ideletes, job : iupdates)

----------------------------------------

ts :: TransactionSettings
ts = defaultTransactionSettings {
  -- PostgreSQL doesn't seem to handle very high amount of
  -- concurrent transactions that modify multiple rows in
  -- the same table well (see updateJobs) and sometimes (very
  -- rarely though) ends up in a deadlock. It doesn't matter
  -- much though, we just restart the transaction in such case.
  tsRestartPredicate = Just . RestartPredicate
  $ \e _ -> qeErrorCode e == DeadlockDetected
         || qeErrorCode e == SerializationFailure
}

atomically :: MonadBase IO m => STM a -> m a
atomically = liftBase . STM.atomically
