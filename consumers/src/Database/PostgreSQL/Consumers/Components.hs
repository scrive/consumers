module Database.PostgreSQL.Consumers.Components
  ( runConsumer
  , runConsumerWithIdleSignal
  , spawnListener
  , spawnMonitor
  , spawnDispatcher
  ) where

import Control.Concurrent.Lifted
import Control.Concurrent.STM hiding (atomically)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.Thread.Lifted qualified as T
import Control.Exception (AsyncException (ThreadKilled))
import Control.Exception.Safe qualified as ES
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Time
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Data.Containers.ListUtils (nubOrd)
import Data.Foldable qualified as F
import Data.Function
import Data.Int
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as M
import Data.Maybe (catMaybes, isJust)
import Data.Monoid.Utils
import Data.Text qualified as T
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.Consumers.Consumer
import Database.PostgreSQL.Consumers.Utils
import Database.PostgreSQL.PQTypes
import Log

-- | Run the consumer. The purpose of the returned monadic action is to wait for
-- currently processed jobs and clean up. This function is best used in
-- conjunction with 'finalize' to seamlessly handle the finalization.
--
-- If you want to add metrics, see the
-- [@consumers-metrics-prometheus@](https://hackage.haskell.org/package/consumers-metrics-prometheus)
-- package to seamlessly instrument your consumer.
runConsumer
  :: ( MonadBaseControl IO m
     , MonadLog m
     , MonadMask m
     , MonadTime m
     , Eq idx
     , Show idx
     , FromSQL idx
     , ToSQL idx
     , Ord idx
     , MonadIO m
     )
  => ConsumerConfig m idx job
  -- ^ The consumer.
  -> ConnectionSourceM m
  -> m (m ())
runConsumer cc cs = runConsumerWithMaybeIdleSignal cc cs Nothing

runConsumerWithIdleSignal
  :: ( MonadBaseControl IO m
     , MonadLog m
     , MonadMask m
     , MonadTime m
     , Eq idx
     , Show idx
     , FromSQL idx
     , ToSQL idx
     , Ord idx
     , MonadIO m
     )
  => ConsumerConfig m idx job
  -- ^ The consumer.
  -> ConnectionSourceM m
  -> TMVar Bool
  -> m (m ())
runConsumerWithIdleSignal cc cs idleSignal = runConsumerWithMaybeIdleSignal cc cs (Just idleSignal)

-- | Run the consumer and also signal whenever the consumer is waiting for
-- getNotification or threadDelay.
runConsumerWithMaybeIdleSignal
  :: ( MonadBaseControl IO m
     , MonadLog m
     , MonadMask m
     , MonadTime m
     , Eq idx
     , Show idx
     , FromSQL idx
     , ToSQL idx
     , Ord idx
     , MonadIO m
     )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> Maybe (TMVar Bool)
  -> m (m ())
runConsumerWithMaybeIdleSignal cc0 cs mIdleSignal
  | ccMaxRunningJobs cc < 1 = do
      logInfo_ "ccMaxRunningJobs < 1, not starting the consumer"
      pure $ pure ()
  | otherwise = do
      (triggerNotification, listenNotification) <- mkNotification
      runningJobsInfo <- liftBase $ newTVarIO M.empty
      runningJobs <- liftBase $ newTVarIO 0

      skipLockedTest :: Either DBException () <-
        try . runDBT cs defaultTransactionSettings $
          runSQL_ "SELECT TRUE FOR UPDATE SKIP LOCKED"
      -- If we can't lock rows using 'skip locked' throw an exception
      either (const $ error "PostgreSQL version with support for SKIP LOCKED is required") pure skipLockedTest

      cid <- registerConsumer cc cs
      localData ["consumer_id" .= show cid] $ do
        listener <- spawnListener cc cs triggerNotification
        monitor <- localDomain "monitor" $ spawnMonitor cc cs cid
        dispatcher <-
          localDomain "dispatcher" $
            spawnDispatcher
              cc
              cs
              cid
              listenNotification
              runningJobsInfo
              runningJobs
              mIdleSignal
        pure . localDomain "finalizer" $ do
          stopExecution listener
          stopExecution dispatcher
          waitForRunningJobs runningJobsInfo runningJobs
          stopExecution monitor
          unregisterConsumer cc cs cid
  where
    cc =
      cc0
        { ccOnException = \ex job -> localData (ccJobLogData cc0 job) $ do
            let doOnException = do
                  action <- ccOnException cc0 ex job
                  logInfo "Unexpected exception caught while processing job" $
                    object
                      [ "exception" .= show ex
                      , "action" .= show action
                      ]
                  pure action
            -- Let asynchronous exceptions through (StopExecution in particular).
            doOnException `ES.catchAny` \handlerEx -> do
              -- Arbitrary delay, but better than letting exceptions from the
              -- handler through and potentially crashlooping the consumer:
              --
              -- 1. A job J fails with an exception, ccOnException is called and
              -- it throws an exception.
              --
              -- 2. The consumer goes down, J is now stuck.
              --
              -- 3. The consumer is restarted, it tries to clean up stuck jobs
              -- (which include J), the cleanup code calls ccOnException on J
              -- and if it throws again, we're back to (2).
              let action = RerunAfter $ idays 1
              logAttention "ccOnException threw an exception" $
                object
                  [ "exception" .= show handlerEx
                  , "action" .= show action
                  ]
              pure action
        }

    waitForRunningJobs runningJobsInfo runningJobs = do
      initialJobs <- liftBase $ readTVarIO runningJobsInfo
      (`fix` initialJobs) $ \loop jobsInfo -> do
        -- If jobs are still running, display info about them.
        unless (M.null jobsInfo) $ do
          logInfo "Waiting for running jobs" $
            object
              [ "job_ids" .= showJobsInfo jobsInfo
              ]
        join . atomically $ do
          jobs <- readTVar runningJobs
          if jobs == 0
            then pure $ pure ()
            else do
              newJobsInfo <- readTVar runningJobsInfo
              -- If jobs info didn't change, wait for it to change.  Otherwise
              -- loop so it either displays the new info or exits if there are
              -- no jobs running anymore.
              if newJobsInfo == jobsInfo
                then retry
                else pure $ loop newJobsInfo
      where
        showJobsInfo = M.foldr (\idx acc -> show idx : acc) []

-- | Spawn a thread that generates signals for the dispatcher to probe the
-- database for incoming jobs.
spawnListener
  :: (MonadBaseControl IO m, MonadMask m)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> TriggerNotification m
  -> m ThreadId
spawnListener cc cs outbox =
  forkP "listener" $
    case ccNotificationChannel cc of
      Just chan ->
        runDBT cs noTs
          . bracket_ (listen chan) (unlisten chan)
          . forever
          $ do
            -- If there are many notifications, we need to collect them as soon
            -- as possible, because they are stored in memory by libpq. They are
            -- also not squashed, so we perform the squashing ourselves with the
            -- help of MVar ().
            void . getNotification $ ccNotificationTimeout cc
            lift signalDispatcher
      Nothing -> forever $ do
        liftBase . threadDelay $ ccNotificationTimeout cc
        signalDispatcher
  where
    signalDispatcher = triggerNotification outbox

    noTs =
      defaultTransactionSettings
        { tsAutoTransaction = False
        }

-- | Spawn a thread that monitors working consumers for activity and
-- periodically updates its own.
spawnMonitor
  :: forall m idx job
   . ( MonadBaseControl IO m
     , MonadLog m
     , MonadMask m
     , MonadTime m
     , Show idx
     , FromSQL idx
     , ToSQL idx
     )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> m ThreadId
spawnMonitor ConsumerConfig {..} cs cid = forkP "monitor" . forever $ do
  runDBT cs ts $ do
    now <- currentTime
    -- Update last_activity of the consumer.
    ok <-
      runPreparedSQL01 (preparedSqlName "setActivity" ccConsumersTable) $
        smconcat
          [ "UPDATE" <+> raw ccConsumersTable
          , "SET last_activity = " <?> now
          , "WHERE id =" <?> cid
          , "  AND name =" <?> unRawSQL ccJobsTable
          ]
    if ok
      then logInfo_ "Activity of the consumer updated"
      else do
        logInfo_ "Consumer is not registered"
        throwM ThreadKilled
  (inactiveConsumers, freedJobs) <- runDBT cs ts $ do
    now <- currentTime
    -- Reserve all inactive (assumed dead) consumers and get their ids. We don't
    -- delete them here, because if the coresponding reserved_by column in the
    -- jobs table has an IMMEDIATE foreign key with the ON DELETE SET NULL
    -- property, we will not be able to determine stuck jobs in the next step.
    runPreparedSQL_ (preparedSqlName "reserveConsumers" ccConsumersTable) $
      smconcat
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
        runPreparedSQL_ (preparedSqlName "findStuck" ccJobsTable) $
          smconcat
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
          runSQL_ $ updateJobsQuery ccJobsTable results now
        runPreparedSQL_ (preparedSqlName "removeInactive" ccConsumersTable) $
          smconcat
            [ "DELETE FROM" <+> raw ccConsumersTable
            , "WHERE id = ANY(" <?> Array1 inactive <+> ")"
            ]
        pure (length inactive, map ccJobIndex stuckJobs)
  when (inactiveConsumers > 0) $ do
    logInfo "Unregistered inactive consumers" $
      object
        [ "inactive_consumers" .= inactiveConsumers
        ]
  unless (null freedJobs) $ do
    logInfo "Freed locked jobs" $
      object
        [ "freed_jobs" .= map show freedJobs
        ]
  liftBase . threadDelay $ 30 * 1000000 -- wait 30 seconds

-- | Spawn a thread that reserves and processes jobs.
spawnDispatcher
  :: forall m idx job
   . ( MonadBaseControl IO m
     , MonadLog m
     , MonadMask m
     , MonadTime m
     , Show idx
     , ToSQL idx
     , Ord idx
     , FromSQL idx
     , MonadIO m
     )
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> ListenNotification m
  -> TVar (M.Map ThreadId idx)
  -> TVar Int
  -> Maybe (TMVar Bool)
  -> m ThreadId
spawnDispatcher config@ConsumerConfig {..} cs cid inbox runningJobsInfo runningJobs mIdleSignal = do
  forkP "dispatcher" . forever $ do
    listenNotification inbox
    someJobWasProcessed <- loop 1
    if someJobWasProcessed
      then setIdle False
      else setIdle True
  where
    setIdle :: forall m'. MonadBaseControl IO m' => Bool -> m' ()
    setIdle isIdle = case mIdleSignal of
      Nothing -> pure ()
      Just idleSignal -> atomically $ do
        _ <- tryTakeTMVar idleSignal
        putTMVar idleSignal isIdle

    loop :: Int -> m Bool
    loop limit = do
      -- We process jobs based on reservation. While reserving jobs, we ensure
      -- that we lock the relevant rows, so no 2 consumers access the same rows
      -- concurrently. See PG's `FOR UPDATE SKIP LOCKED`.
      --
      -- There's a batch processing mechanism that makes it so we progressively
      -- process more and more jobs concurrently, if we were able to saturate
      -- job batches.
      (batch, batchSize) <- candidateJobs config cs limit $ \candidates -> do
        reserveJobs config cs cid candidates

      when (batchSize > 0) $ do
        logInfo "Processing batch" $
          object
            [ "batch_size" .= batchSize
            , "limit" .= limit
            ]

        -- Update runningJobs before forking so that we can adjust
        -- maxBatchSize appropriately later. We also need to mask asynchronous
        -- exceptions here as we rely on correct value of runningJobs to
        -- perform graceful termination.
        mask $ \restore -> do
          atomically $ modifyTVar' runningJobs (+ batchSize)
          let subtractJobs = atomically $ do
                modifyTVar' runningJobs (subtract batchSize)
          void
            . forkP "batch processor"
            . (`finally` subtractJobs)
            . restore
            $ do
              mapM startJob batch >>= mapM joinJob >>= updateJobs

        -- TODO: In the presence of mutexes, we can't differentiate between
        -- there not being enough jobs, or that all jobs in the batch were
        -- filtered out. So we assume that we we were able to saturate, and
        -- potentially do an additional redundant iteration.
        when (batchSize == limit || batchSize > 0 && isJust ccMutexColumn) $ do
          maxBatchSize <- atomically $ do
            jobs <- readTVar runningJobs
            when (jobs >= ccMaxRunningJobs) retry
            pure $ ccMaxRunningJobs - jobs
          void . loop $ min maxBatchSize (2 * limit)

      pure (batchSize > 0)

    -- Spawn each job in a separate thread.
    startJob :: job -> m (job, m (T.Result Result))
    startJob job = do
      (_, joinFork) <- mask $ \restore -> T.fork $ do
        tid <- myThreadId
        bracket_ (registerJob tid) (unregisterJob tid) . restore $ do
          localData (ccJobLogData job) $ ccProcessJob job
      pure (job, joinFork)
      where
        registerJob tid = atomically $ do
          modifyTVar' runningJobsInfo . M.insert tid $ ccJobIndex job
        unregisterJob tid = atomically $ do
          modifyTVar' runningJobsInfo $ M.delete tid

    -- Wait for all the jobs and collect their results.
    joinJob :: (job, m (T.Result Result)) -> m (idx, Result)
    joinJob (job, joinFork) =
      joinFork >>= \case
        Right result -> pure (ccJobIndex job, result)
        Left ex -> do
          action <- ccOnException ex job
          pure (ccJobIndex job, Failed action)

    -- Update status of the jobs.
    updateJobs :: [(idx, Result)] -> m ()
    updateJobs results = runDBT cs ts $ do
      now <- currentTime
      runSQL_ $ updateJobsQuery ccJobsTable results now

----------------------------------------

-- | Select (and lock using advisory locks, if relevant) the rows we intend
-- reserving. Ensures that the locks are only held for the duration of the
-- continuation.
candidateJobs
  :: forall m idx job r
   . (MonadThrow m, MonadMask m, MonadBase IO m, MonadTime m, FromSQL idx, Show idx, MonadIO m)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> Int
  -> ([idx] -> m r)
  -> m r
candidateJobs config@ConsumerConfig {..} cs limit action = do
  -- There's two variants of this query, where we do or do not use the mutex
  -- column.
  now <- currentTime
  let preparedName = case ccMutexColumn of
        Just c -> unRawSQL ("candidateJobs" <> c)
        Nothing -> "candidateJobs"

      hashMutexes mutexes = "hashtext(UNNEST(" <?> Array1 mutexes <+> "))"
      unlockAdvisory :: [Maybe T.Text] -> m ()
      unlockAdvisory mutexIds = runDBT cs ts $ do
        let relevantMutexes = nubOrd $ catMaybes mutexIds
        void $ runSQL $ "SELECT pg_advisory_unlock(" <> hashMutexes relevantMutexes <> ")"
      lockAdvisory :: [Maybe T.Text] -> DBT m ()
      lockAdvisory mutexIds = do
        let relevantMutexes = nubOrd $ catMaybes mutexIds
        void $ runSQL $ "SELECT pg_advisory_lock(" <> hashMutexes relevantMutexes <> ")"
      getCandidates :: FromRow t => DBT m [t]
      getCandidates = do
        void $ runPreparedSQL (preparedSqlName preparedName ccJobsTable) $ candidateJobsQuery config limit now
        F.toList <$> queryResult

  case ccMutexColumn of
    Just _ -> do
      let initialize = runDBT cs ts $ do
            result <- getCandidates
            -- In the batch of results, we may have multiple jobs that all want
            -- the same mutex. Ensure we only get one job per mutex-group.
            let firstOfGroup group = case NE.head group of
                  (_, Nothing) -> NE.toList group
                  mutexCandidate -> [mutexCandidate]
                groups = concatMap firstOfGroup $ NE.groupWith snd result
                res@(_, mutexes) = unzip groups
            lockAdvisory mutexes
            pure res
      bracket initialize (unlockAdvisory . snd) (action . fst)
    Nothing -> do
      candidates <- fmap (fmap runIdentity) $ runDBT cs ts getCandidates
      action candidates

candidateJobsQuery :: ConsumerConfig m idx job -> Int -> UTCTime -> SQL
candidateJobsQuery ConsumerConfig {..} limit now =
  case ccMutexColumn of
    Nothing ->
      smconcat
        [ "SELECT"
        , "    id"
        , "FROM" <+> raw ccJobsTable
        , "WHERE"
        , "    reserved_by IS NULL"
        , "    AND run_at IS NOT NULL"
        , "    AND run_at <=" <?> now
        , "ORDER BY run_at"
        , "LIMIT" <?> limit
        , "FOR UPDATE SKIP LOCKED"
        ]
    Just mutexColumn ->
      smconcat
        [ "WITH taken_mutexes AS ("
        , "    SELECT" <+> raw mutexColumn
        , "      AS taken"
        , "    FROM" <+> raw ccJobsTable
        , "    WHERE"
        , "        reserved_by IS NOT NULL"
        , "        AND run_at IS NOT NULL"
        , "        AND run_at <=" <?> now
        , "        AND" <+> raw mutexColumn <+> "IS NOT NULL"
        , "    GROUP BY" <+> raw mutexColumn
        , ")"
        , "SELECT"
        , "    id,"
        , "    " <+> raw mutexColumn
        , "FROM" <+> raw ccJobsTable
        , "LEFT JOIN taken_mutexes on taken=" <+> raw mutexColumn
        , "WHERE"
        , "    reserved_by IS NULL"
        , "    AND run_at IS NOT NULL"
        , "    AND run_at <=" <?> now
        , "    AND taken IS NULL"
        , "ORDER BY run_at"
        , "LIMIT" <?> limit
        , "FOR UPDATE SKIP LOCKED"
        ]

reserveJobs
  :: (MonadBaseControl IO m, MonadMask m, Show idx, ToSQL idx)
  => ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> [idx]
  -> m ([job], Int)
reserveJobs config@ConsumerConfig {..} cs consumerId candidates =
  runDBT cs ts $ do
    n <-
      runPreparedSQL (preparedSqlName "setReservation" ccJobsTable) $
        reserveJobsQuery config consumerId candidates
    -- Decode lazily as we want the transaction to be as short as possible.
    (,n) . F.toList . fmap ccJobFetcher <$> queryResult

reserveJobsQuery :: (Show idx, ToSQL idx) => ConsumerConfig m idx job -> ConsumerID -> [idx] -> SQL
reserveJobsQuery ConsumerConfig {..} consumerId candidates =
  smconcat
    [ "UPDATE" <+> raw ccJobsTable <+> "SET"
    , "  reserved_by =" <?> consumerId
    , ", attempts = CASE"
    , "    WHEN finished_at IS NULL THEN attempts + 1"
    , "    ELSE 1"
    , "  END"
    , "WHERE id = ANY(" <?> Array1 candidates <+> ")"
    , "RETURNING" <+> mintercalate ", " ccJobSelectors
    ]

-- | Generate a single SQL query for updating all given jobs.
--
-- /Note:/ this query can't be run as prepared because it has a variable number
-- of query parameters (see retryToSQL helper).
updateJobsQuery
  :: (Show idx, ToSQL idx)
  => RawSQL ()
  -> [(idx, Result)]
  -> UTCTime
  -> SQL
updateJobsQuery jobsTable results now =
  smconcat
    [ "WITH removed AS ("
    , "  DELETE FROM" <+> raw jobsTable
    , "  WHERE id = ANY(" <?> Array1 deletes <+> ")"
    , ")"
    , "UPDATE" <+> raw jobsTable <+> "SET"
    , "  reserved_by = NULL"
    , ", run_at = CASE"
    , "    WHEN FALSE THEN run_at"
    , smconcat $ M.foldrWithKey retryToSQL [] retries
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

    retries = foldr (step . getAction) M.empty updates
      where
        getAction (idx, result) = case result of
          Ok action -> (idx, action)
          Failed action -> (idx, action)

        step (idx, action) iretries = case action of
          MarkProcessed -> iretries
          RerunAfter int -> M.insertWith (++) (Left int) [idx] iretries
          RerunAt time -> M.insertWith (++) (Right time) [idx] iretries
          Remove -> error "updateJobs: Remove should've been filtered out"

    successes = foldr step [] updates
      where
        step (idx, Ok _) acc = idx : acc
        step (_, Failed _) acc = acc

    (deletes, updates) = foldr step ([], []) results
      where
        step job@(idx, result) (ideletes, iupdates) = case result of
          Ok Remove -> (idx : ideletes, iupdates)
          Failed Remove -> (idx : ideletes, iupdates)
          _ -> (ideletes, job : iupdates)

ts :: TransactionSettings
ts =
  defaultTransactionSettings
    { -- PostgreSQL doesn't seem to handle very high amount of concurrent
      -- transactions that modify multiple rows in the same table well (see
      -- updateJobs) and sometimes (very rarely though) ends up in a
      -- deadlock. It doesn't matter much though, we just restart the
      -- transaction in such case.
      tsRestartPredicate = Just . RestartPredicate $
        \e _ ->
          qeErrorCode e == DeadlockDetected
            || qeErrorCode e == SerializationFailure
    }

atomically :: MonadBase IO m => STM a -> m a
atomically = liftBase . STM.atomically
