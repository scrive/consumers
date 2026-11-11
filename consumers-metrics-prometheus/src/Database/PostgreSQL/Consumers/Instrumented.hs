-- |
-- Provides seamless instrumentation of your existing @consumers@ using Prometheus
-- (see the [`consumers`](https://hackage.haskell.org/package/consumers) library
-- for usage).
module Database.PostgreSQL.Consumers.Instrumented
  ( -- * Instrument
    runInstrumentedConsumer

    -- ** Configuration
  , defaultConsumerMetricsConfig
  , ConsumerMetricsConfig (..)

    -- ** Metrics
  , ConsumerMetrics
  , registerConsumerMetrics
  ) where

import Control.Concurrent.Lifted
import Control.Exception.Safe
import Control.Monad.Base
import Control.Monad.Catch (ExitCase (..))
import Control.Monad.Time
import Control.Monad.Trans.Control
import Database.PostgreSQL.Consumers (Action (..), ConsumerConfig (..), Result (..), runConsumer)
import Database.PostgreSQL.PQTypes
import Log
import Prometheus qualified as Prom

data ConsumerMetricsConfig = ConsumerMetricsConfig
  { collectSeconds :: Int
  -- ^ Collection interval in seconds
  , jobExecutionBuckets :: [Prom.Bucket]
  -- ^ Buckets to use for the 'jobExecution' histogram
  , jobFailedAttemptsBuckets :: [Prom.Bucket]
  -- ^ Buckets to use for the 'jobsFailedAttempts' histogram. Keep a @1@
  -- bucket so that one-off failures are counted apart from repeated ones.
  , collectDegradeThresholdSeconds :: Double
  -- ^ @logAttention@ and graceful degrade if the collection queries take
  -- longer than @x@ seconds in total, or if the whole collection, including
  -- the wait for a pool connection, takes longer than @10 * x@ seconds.
  , collectDegradeSeconds :: Int
  -- ^ Degraded collection interval in seconds
  }

-- | Hopefully sensible defaults that you can use
--
-- @
--  ConsumerMetricsConfig
--    { collectSeconds = 15
--    , jobExecutionBuckets = [0.01, 0.05, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
--    , jobFailedAttemptsBuckets = [1, 2, 3, 5, 10, 20, 50]
--    , collectDegradeThresholdSeconds = 0.1
--    , collectDegradeSeconds = 60
--    }
-- @
defaultConsumerMetricsConfig :: ConsumerMetricsConfig
defaultConsumerMetricsConfig =
  ConsumerMetricsConfig
    { collectSeconds = 15
    , jobExecutionBuckets = [0.01, 0.05, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    , jobFailedAttemptsBuckets = [1, 2, 3, 5, 10, 20, 50]
    , collectDegradeThresholdSeconds = 0.1
    , collectDegradeSeconds = 60
    }

-- | Metrics store for the following Prometheus metrics:
--
-- @
-- # HELP consumers_job_execution_seconds Execution time of jobs in seconds, by job_name, includes the job_result and job_action
-- # TYPE consumers_job_execution_seconds histogram
--
-- # HELP consumers_job_failed_attempts Attempt count at each failed run, by job_name
-- # TYPE consumers_job_failed_attempts histogram
--
-- # HELP consumers_jobs_reserved_total The total number of job reserved, by job_name
-- # TYPE consumers_jobs_reserved_total counter
--
-- # HELP consumers_jobs_overdue The current number of jobs overdue, by job_name
-- # TYPE consumers_jobs_overdue gauge
--
-- # HELP consumers_jobs_scheduled The current number of jobs scheduled to run in the future, by job_name
-- # TYPE consumers_jobs_scheduled gauge
--
-- # HELP consumers_job_info The number of workers registered for a given job_name
-- # TYPE consumers_job_info gauge
-- @
data ConsumerMetrics = ConsumerMetrics
  { collectSeconds :: Int
  , collectDegradeThresholdSeconds :: Double
  , collectDegradeSeconds :: Int
  , jobInfo :: Prom.Vector Prom.Label1 Prom.Gauge
  , jobsOverdue :: Prom.Vector Prom.Label1 Prom.Gauge
  , jobsScheduled :: Prom.Vector Prom.Label1 Prom.Gauge
  , jobsReserved :: Prom.Vector Prom.Label1 Prom.Counter
  , jobsExecution :: Prom.Vector Prom.Label3 Prom.Histogram
  , jobsFailedAttempts :: Prom.Vector Prom.Label1 Prom.Histogram
  }

registerConsumerMetrics :: MonadBaseControl IO m => ConsumerMetricsConfig -> m ConsumerMetrics
registerConsumerMetrics ConsumerMetricsConfig {..} = liftBase $ do
  jobInfo <-
    Prom.register
      . Prom.vector "job_name"
      $ Prom.gauge
        Prom.Info
          { metricName = "consumers_job_info"
          , metricHelp = "The number of workers registered for a given job_name"
          }
  jobsOverdue <-
    Prom.register
      . Prom.vector "job_name"
      $ Prom.gauge
        Prom.Info
          { metricName = "consumers_jobs_overdue"
          , metricHelp = "The current number of jobs overdue, by job_name"
          }
  jobsScheduled <-
    Prom.register
      . Prom.vector "job_name"
      $ Prom.gauge
        Prom.Info
          { metricName = "consumers_jobs_scheduled"
          , metricHelp = "The current number of jobs scheduled to run in the future, by job_name"
          }
  jobsReserved <-
    Prom.register
      . Prom.vector "job_name"
      $ Prom.counter
        Prom.Info
          { metricName = "consumers_jobs_reserved_total"
          , metricHelp = "The total number of job reserved, by job_name"
          }
  jobsExecution <-
    Prom.register
      . Prom.vector ("job_name", "job_result", "job_action")
      $ Prom.histogram
        Prom.Info
          { metricName = "consumers_job_execution_seconds"
          , metricHelp = "Execution time of jobs in seconds, by job_name, includes the job_result and job_action"
          }
        jobExecutionBuckets
  jobsFailedAttempts <-
    Prom.register
      . Prom.vector "job_name"
      $ Prom.histogram
        Prom.Info
          { metricName = "consumers_job_failed_attempts"
          , metricHelp = "Attempt count at each failed run, by job_name"
          }
        jobFailedAttemptsBuckets
  pure $ ConsumerMetrics {..}

-- | Run a 'ConsumerConfig', but with instrumentation added.
--
-- This should be used in place of 'runConsumer'.
-- Use 'registerConsumerMetrics' to create the metrics.
--
-- A thread will spawned to collect "queue" metrics every 'collectSeconds', and
-- an altered @ccProcessJob@ will be run that collects "job" metrics.
--
-- See 'ConsumerMetrics' for more details on the metrics collected.
runInstrumentedConsumer
  :: forall m idx job
   . ( Eq idx
     , Show idx
     , FromSQL idx
     , ToSQL idx
     , MonadBaseControl IO m
     , MonadMask m
     , MonadTime m
     , MonadLog m
     )
  => ConsumerMetrics
  -> ConsumerConfig m idx job
  -> ConnectionSourceM m
  -> m (m ())
runInstrumentedConsumer metrics config connSource = do
  let instrumentedConfig = instrumentConsumerConfig metrics config
  cleanupConsumer <- runConsumer instrumentedConfig connSource
  tidMetricsCollection <- runMetricsCollection connSource metrics config
  pure $ do
    cleanupConsumer
    killThread tidMetricsCollection

-- | Spawns a new thread to run "queue" metrics collection for the given configuration
runMetricsCollection
  :: forall m idx job
   . ( MonadBaseControl IO m
     , MonadMask m
     , MonadTime m
     , MonadLog m
     )
  => ConnectionSourceM m
  -> ConsumerMetrics
  -> ConsumerConfig m idx job
  -> m ThreadId
runMetricsCollection connSource metrics@ConsumerMetrics {..} config = localDomain "metrics-collection" $ fork collectLoop
  where
    collectLoop = do
      seconds <- handleAny handleEx collect
      threadDelay (seconds * 1_000_000)
      collectLoop

    handleEx e = do
      logAttention "Exception while running metrics-collection" $
        object
          [ "exception" .= show e
          , "rerun_seconds" .= collectDegradeSeconds
          ]
      pure collectDegradeSeconds

    collect = do
      logInfo_ "Collecting consumer metrics"
      t1 <- monotonicTime
      queryTime <- collectMetrics connSource metrics config
      t2 <- monotonicTime
      degrade queryTime (t2 - t1)

    -- Graceful degrade if the queries take too long. The runtime also
    -- includes the wait for a pool connection, so it has a looser threshold.
    degrade queryTime runtime
      | queryTime >= collectDegradeThresholdSeconds = do
          logAttention "Consumer metrics collection took long" $ object details
          pure collectDegradeSeconds
      | runtime >= runtimeThreshold = do
          logAttention "Consumer metrics collection waited long" $ object details
          pure collectDegradeSeconds
      | otherwise = pure collectSeconds
      where
        runtimeThreshold = 10 * collectDegradeThresholdSeconds
        details =
          [ "query_time" .= queryTime
          , "runtime" .= runtime
          , "threshold" .= collectDegradeThresholdSeconds
          , "runtime_threshold" .= runtimeThreshold
          , "rerun_seconds" .= collectDegradeSeconds
          ]

-- | Collect and report "queue" metrics for a given configuration.
-- Returns the time spent executing queries, in seconds.
collectMetrics
  :: ( MonadBaseControl IO m
     , MonadMask m
     , MonadThrow m
     )
  => ConnectionSourceM m
  -> ConsumerMetrics
  -> ConsumerConfig m idx job
  -> m Double
collectMetrics connSource ConsumerMetrics {..} ConsumerConfig {ccJobsTable, ccConsumersTable} = runDBT connSource ts $ do
  let jobName = unRawSQL ccJobsTable

  info <- do
    runSQL_ $
      "SELECT count(id)::float8 FROM "
        <> raw ccConsumersTable
        <> " WHERE name =" <?> unRawSQL ccJobsTable
    fetchOne runIdentity
  liftBase $ Prom.withLabel jobInfo jobName (`Prom.setGauge` info)

  -- Both counts are range scans on an index on run_at, which the consumer
  -- needs anyway to reserve jobs. The scheduled count has no reserved_by
  -- condition, because a reserved job always has run_at in the past: the
  -- consumer reserves only jobs with run_at <= now and sets run_at together
  -- with reserved_by = NULL when it releases them. Without a column from the
  -- heap, the planner can use an index-only scan for it.
  (overdue, scheduled) <- do
    runSQL_ $
      "SELECT"
        <> " (SELECT count(*)::float8 FROM "
        <> raw ccJobsTable
        <> " WHERE run_at <= now() AND reserved_by IS NULL)"
        <> ", (SELECT count(*)::float8 FROM "
        <> raw ccJobsTable
        <> " WHERE run_at > now())"
    fetchOne id
  liftBase $ Prom.withLabel jobsOverdue jobName (`Prom.setGauge` overdue)
  liftBase $ Prom.withLabel jobsScheduled jobName (`Prom.setGauge` scheduled)

  statsTime <$> getConnectionStats
  where
    -- The counts do not need one snapshot, so skip the transaction and hold
    -- a connection only while a query runs. This removes two round trips and
    -- frees the connection between queries.
    ts = defaultTransactionSettings {tsConnectionAcquisitionMode = AcquireOnDemand}

-- | Alter a configuration to collect "job" metrics on 'ccProcessJob'
instrumentConsumerConfig
  :: ( MonadBaseControl IO m
     , MonadMask m
     , MonadTime m
     , MonadLog m
     )
  => ConsumerMetrics
  -> ConsumerConfig m idx job
  -> ConsumerConfig m idx job
instrumentConsumerConfig ConsumerMetrics {..} ConsumerConfig {..} =
  ConsumerConfig {ccProcessJob = ccProcessJob', ..}
  where
    jobName = unRawSQL ccJobsTable

    -- Alter the `ccProcessJob` function by adding instrumentation:
    -- First we increment the 'metrics.jobsReserved' counter (while handling
    -- any potential exceptions).  Then we use `generalBracket` to acquire the
    -- start time, process the job, and finally report the job metrics as the
    -- "release" action.  Any exceptions in `ccProcessJob` will be re-raised by
    -- `generalBracket`.  However, we need to handle exceptions in the
    -- "release" action as we don't want those exceptions to propagate upwards
    -- to the consumer's `ccOnException` (and thus potentially change the
    -- result of the job).
    ccProcessJob' job = do
      handleAny handleEx . liftBase $ Prom.withLabel jobsReserved jobName Prom.incCounter
      fst <$> generalBracket monotonicTime (reportJob job) (const $ ccProcessJob job)

    reportJob job t1 jobExit = handleAny handleEx $ do
      t2 <- monotonicTime
      let duration = t2 - t1
          resultLabel = case jobExit of
            ExitCaseSuccess (Ok _) -> "ok"
            ExitCaseSuccess (Failed _) -> "failed"
            ExitCaseException _ -> "exception"
            ExitCaseAbort -> "abort"
          actionLabel = case jobExit of
            ExitCaseSuccess (Ok action) -> actionToLabel action
            ExitCaseSuccess (Failed action) -> actionToLabel action
            -- No action exists when the job throws or aborts.
            ExitCaseException _ -> "none"
            ExitCaseAbort -> "none"
      liftBase $
        Prom.withLabel jobsExecution (jobName, resultLabel, actionLabel) (`Prom.observe` duration)
      -- Only failed runs are recorded. Successful runs would add an "attempt 1"
      -- observation for every job.
      case jobExit of
        ExitCaseSuccess (Ok _) -> pure ()
        _ ->
          liftBase $
            Prom.withLabel jobsFailedAttempts jobName (`Prom.observe` fromIntegral (ccJobAttempts job))

    handleEx e = logAttention "Exception while instrumenting job" $ object ["exception" .= show e]

    actionToLabel action = case action of
      MarkProcessed -> "mark_processed"
      RerunAfter _ -> "rerun_after"
      RerunAt _ -> "rerun_at"
      Remove -> "remove"
