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
import qualified Control.Exception.Safe as ES
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
runConsumerWithMaybeIdleSignal cc0 cs mIdleSignal
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
    cc = cc0
      { ccOnException = \ex job -> do
          -- Let asynchronous exceptions through (StopExecution in particular).
          ccOnException cc0 ex job `ES.catchAny` \handlerEx -> do
            logAttention "ccOnException threw an exception" $ object
              [ "exception" .= show handlerEx
              ]
            -- Arbitrary delay, but better than letting exceptions from the
            -- handler through and potentially crashlooping the consumer:
            --
            -- 1. A job J fails with an exception, ccOnException is called and
            -- it throws an exception.
            --
            -- 2. The consumer goes down, J is now stuck.
            --
            -- 3. The consumer is restarted, it tries to clean up stuck jobs
            -- (which include J), the cleanup code calls ccOnException on J and
            -- if it throws again, we're back to (2).
            pure . RerunAfter $ idays 1
      }

    waitForRunningJobs runningJobsInfo runningJobs = do
      initialJobs <- liftBase $ readTVarIO runningJobsInfo
      (`fix` initialJobs) $ \loop jobsInfo -> do
        -- If jobs are still running, display info about them.
        when (not $ M.null jobsInfo) $ do
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
              if (newJobsInfo == jobsInfo)
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
                        Show idx, FromSQL idx, ToSQL idx)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> m ThreadId
spawnMonitor ConsumerConfig{..} cs cid = forkP "monitor" . forever $ do
  runDBT cs ts $ do
    now <- currentTime
    -- Update last_activity of the consumer.
    ok <- runPreparedSQL01 (preparedSqlName "setActivity" ccConsumersTable) $ smconcat [
        "UPDATE" <+> raw ccConsumersTable
      , "SET last_activity = " <?> now
      , "WHERE id =" <?> cid
      , "  AND name =" <?> unRawSQL ccJobsTable
      ]
    if ok
      then logInfo_ "Activity of the consumer updated"
      else do
        logInfo_ $ "Consumer is not registered"
        throwM ThreadKilled
  (inactiveConsumers, freedJobs) <- runDBT cs ts $ do
    now <- currentTime
    -- Reserve all inactive (assumed dead) consumers and get their ids. We don't
    -- delete them here, because if the coresponding reserved_by column in the
    -- jobs table has an IMMEDIATE foreign key with the ON DELETE SET NULL
    -- property, we will not be able to determine stuck jobs in the next step.
    runPreparedSQL_ (preparedSqlName "reserveConsumers" ccConsumersTable) $ smconcat
      [ "SELECT id::bigint"
      , "FROM" <+> raw ccConsumersTable
      , "WHERE last_activity +" <?> iminutes 1 <+> "<= " <?> now
      , "  AND name =" <?> unRawSQL ccJobsTable
      , "FOR UPDATE SKIP LOCKED"
      ]
    fetchMany (runIdentity @Int64) >>= \case
      [] -> pure (0, [])
      inactive -> do
        -- Fetch all stuck jobs and run ccOnException on them to determine
        -- actions. This is necessary e.g. to be able to apply exponential
        -- backoff to them correctly.
        runPreparedSQL_ (preparedSqlName "findStuck" ccJobsTable) $ smconcat
          [ "SELECT" <+> mintercalate ", " ccJobSelectors
          , "FROM" <+> raw ccJobsTable
          , "WHERE reserved_by = ANY(" <?> Array1 inactive <+> ")"
          , "FOR UPDATE SKIP LOCKED"
          ]
        stuckJobs <- fetchMany ccJobFetcher
        unless (null stuckJobs) $ do
          results <- forM stuckJobs $ \job -> do
            action <- lift $ ccOnException (toException ThreadKilled) job
            pure (ccJobIndex job, Failed action)
          runPreparedSQL_ (preparedSqlName "updateJobs" ccJobsTable) $ updateJobsQuery ccJobsTable results now
        runPreparedSQL_ (preparedSqlName "removeInactive" ccConsumersTable) $ smconcat
          [ "DELETE FROM" <+> raw ccConsumersTable
          , "WHERE id = ANY(" <?> Array1 inactive <+> ")"
          ]
        pure (length inactive, map ccJobIndex stuckJobs)
  when (inactiveConsumers > 0) $ do
    logInfo "Unregistered inactive consumers" $ object [
        "inactive_consumers" .= inactiveConsumers
      ]
  unless (null freedJobs) $ do
    logInfo "Freed locked jobs" $ object [
        "freed_jobs" .= map show freedJobs
      ]
  liftBase . threadDelay $ 30 * 1000000 -- wait 30 seconds

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
            . (`finally` subtractJobs) . restore $ do
            mapM startJob batch >>= mapM joinJob >>= updateJobs

        when (batchSize == limit) $ do
          maxBatchSize <- atomically $ do
            jobs <- readTVar runningJobs
            when (jobs >= ccMaxRunningJobs) retry
            return $ ccMaxRunningJobs - jobs
          void $ loop $ min maxBatchSize (2*limit)

      return (batchSize > 0)

    reserveJobs :: Int -> m ([job], Int)
    reserveJobs limit = runDBT cs ts $ do
      now <- currentTime
      n <- runPreparedSQL (preparedSqlName "setReservation" ccJobsTable) $ smconcat [
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
      (, n) . F.toList . fmap ccJobFetcher <$> queryResult
      where
        reservedJobs :: UTCTime -> SQL
        reservedJobs now = smconcat [
            "SELECT id FROM" <+> raw ccJobsTable
          , "WHERE"
          , "       reserved_by IS NULL"
          , "       AND run_at IS NOT NULL"
          , "       AND run_at <= " <?> now
          , "       ORDER BY run_at"
          , "LIMIT" <?> limit
          , "FOR UPDATE SKIP LOCKED"
          ]

    -- | Spawn each job in a separate thread.
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

    -- | Wait for all the jobs and collect their results.
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

    -- | Update status of the jobs.
    updateJobs :: [(idx, Result)] -> m ()
    updateJobs results = runDBT cs ts $ do
      now <- currentTime
      runSQL_ $ updateJobsQuery ccJobsTable results now

----------------------------------------

-- | Generate a single SQL query for updating all given jobs.
updateJobsQuery
  :: (Show idx, ToSQL idx)
  => RawSQL ()
  -> [(idx, Result)]
  -> UTCTime
  -> SQL
updateJobsQuery jobsTable results now = smconcat
  [ "WITH removed AS ("
  , "  DELETE FROM" <+> raw jobsTable
  , "  WHERE id = ANY(" <?> Array1 deletes <+> ")"
  , ")"
  , "UPDATE" <+> raw jobsTable <+> "SET"
  , "  reserved_by = NULL"
  , ", run_at = CASE"
  , "    WHEN FALSE THEN run_at"
  ,      smconcat $ M.foldrWithKey retryToSQL [] retries
  , "    ELSE NULL" -- processed
  , "  END"
  , ", finished_at = CASE"
  , "    WHEN id = ANY(" <?> Array1 successes <+> ") THEN " <?> now
  , "    ELSE NULL"
  , "  END"
  , "WHERE id = ANY(" <?> Array1 (map fst updates) <+> ")"
  ]
  where
    retryToSQL (Left int) ids =
      ("WHEN id = ANY(" <?> Array1 ids <+> ") THEN " <?> now <> " +" <?> int :)
    retryToSQL (Right time) ids =
      ("WHEN id = ANY(" <?> Array1 ids <+> ") THEN" <?> time :)

    retries = foldr step M.empty $ map getAction updates
      where
        getAction (idx, result) = case result of
          Ok     action -> (idx, action)
          Failed action -> (idx, action)

        step (idx, action) iretries = case action of
          MarkProcessed  -> iretries
          RerunAfter int -> M.insertWith (++) (Left int) [idx] iretries
          RerunAt time   -> M.insertWith (++) (Right time) [idx] iretries
          Remove         -> error "updateJobs: Remove should've been filtered out"

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
