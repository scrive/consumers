{-|
Provides seamless instrumentation of your existing @consumers@ using Prometheus
(see the [`consumers`](https://hackage.haskell.org/package/consumers) library
for usage).
-}
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
import Database.PostgreSQL.Consumers (ConsumerConfig (..), Result (..), runConsumer)
import Database.PostgreSQL.PQTypes
import Log
import Prometheus qualified as Prom

data ConsumerMetricsConfig = ConsumerMetricsConfig
  { collectSeconds :: Int
  -- ^ Collection interval in seconds
  , jobExecutionBuckets :: [Prom.Bucket]
  -- ^ Buckets to use for the 'jobExecution' histogram
  , collectDegradeThresholdSeconds :: Double
  -- ^ @logAttention@ and graceful degrade if collection takes longer than @x@ seconds
  , collectDegradeSeconds :: Int
  -- ^ Degraded collection interval in seconds
  }

-- | Hopefully sensible defaults that you can use
--
-- @
--  ConsumerMetricsConfig
--    { collectSeconds = 15
--    , jobExecutionBuckets = [0.01, 0.05, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
--    , collectDegradeThresholdSeconds = 0.1
--    , collectDegradeSeconds = 60
--    }
-- @
defaultConsumerMetricsConfig :: ConsumerMetricsConfig
defaultConsumerMetricsConfig =
  ConsumerMetricsConfig
    { collectSeconds = 15
    , jobExecutionBuckets = [0.01, 0.05, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    , collectDegradeThresholdSeconds = 0.1
    , collectDegradeSeconds = 60
    }

-- | Metrics store for the following Prometheus metrics:
--
-- @
-- # HELP consumers_job_execution_seconds Execution time of jobs in seconds, by job_name, includes the job_result
-- # TYPE consumers_job_execution_seconds histogram
--
-- # HELP consumers_jobs_reserved_total The total number of job reserved, by job_name
-- # TYPE consumers_jobs_reserved_total counter
--
-- # HELP consumers_jobs_overdue The current number of jobs overdue, by job_name
-- # TYPE consumers_jobs_overdue gauge
--
-- # HELP consumers_job_info The number of workers registered for a given job_name
-- # TYPE consumers_job_info gauge
-- @
--
data ConsumerMetrics = ConsumerMetrics
  { collectSeconds :: Int
  , collectDegradeThresholdSeconds :: Double
  , collectDegradeSeconds :: Int
  , jobInfo :: Prom.Vector Prom.Label1 Prom.Gauge
  , jobsOverdue :: Prom.Vector Prom.Label1 Prom.Gauge
  , jobsReserved :: Prom.Vector Prom.Label1 Prom.Counter
  , jobsExecution :: Prom.Vector Prom.Label2 Prom.Histogram
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
      . Prom.vector ("job_name", "job_result")
      $ Prom.histogram
        Prom.Info
          { metricName = "consumers_job_execution_seconds"
          , metricHelp = "Execution time of jobs in seconds, by job_name, includes the job_result"
          }
        jobExecutionBuckets
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
      collectMetrics connSource metrics config
      t2 <- monotonicTime
      let runtime = t2 - t1
      -- Graceful degrade if things take too long
      if runtime < collectDegradeThresholdSeconds
        then pure collectSeconds
        else do
          logAttention "Consumer metrics collection took long" $
            object
              [ "runtime" .= runtime
              , "threshold" .= collectDegradeThresholdSeconds
              , "rerun_seconds" .= collectDegradeSeconds
              ]
          pure collectDegradeSeconds

-- | Collect and report "queue" metrics for a given configuration
collectMetrics
  :: ( MonadBaseControl IO m
     , MonadMask m
     , MonadThrow m
     )
  => ConnectionSourceM m
  -> ConsumerMetrics
  -> ConsumerConfig m idx job
  -> m ()
collectMetrics connSource ConsumerMetrics {..} ConsumerConfig {ccJobsTable, ccConsumersTable} = runDBT connSource defaultTransactionSettings $ do
  let jobName = unRawSQL ccJobsTable

  info <- do
    runSQL_ $
      "SELECT count(id)::float8 FROM "
        <> raw ccConsumersTable
        <> " WHERE name =" <?> unRawSQL ccJobsTable
    fetchOne runIdentity
  liftBase $ Prom.withLabel jobInfo jobName (`Prom.setGauge` info)

  overdue <- do
    runSQL_ $
      "SELECT count(id)::float8 FROM "
        <> raw ccJobsTable
        <> " WHERE run_at <= now() AND reserved_by IS NULL"
    fetchOne runIdentity
  liftBase $ Prom.withLabel jobsOverdue jobName (`Prom.setGauge` overdue)

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
      fst <$> generalBracket monotonicTime reportJob (const $ ccProcessJob job)

    reportJob t1 jobExit = handleAny handleEx $ do
      t2 <- monotonicTime
      let duration = t2 - t1
          resultLabel = case jobExit of
            ExitCaseSuccess (Ok _) -> "ok"
            ExitCaseSuccess (Failed _) -> "failed"
            ExitCaseException _ -> "exception"
            ExitCaseAbort -> "abort"
      liftBase $ Prom.withLabel jobsExecution (jobName, resultLabel) (`Prom.observe` duration)

    handleEx e = logAttention "Exception while instrumenting job" $ object ["exception" .= show e]
