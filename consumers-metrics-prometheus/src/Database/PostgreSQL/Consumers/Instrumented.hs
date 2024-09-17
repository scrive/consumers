module Database.PostgreSQL.Consumers.Instrumented (
  ConsumerMetricsConfig (..),
  defaultConsumerMetricsConfig,
  ConsumerMetrics,
  registerConsumerMetrics,
  runInstrumentedConsumer,
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
  -- ^ Buckets to use for the 'jobExecution' 'Prom.Histogram'
  , collectDegradeThresholdSeconds :: Double
  -- ^ Log @attention@ and graceful degrade if collection takes longer than @x@ seconds
  , collectDegradeSeconds :: Int
  -- ^ Degraded collection interval in seconds
  }

-- | TODO document
defaultConsumerMetricsConfig :: ConsumerMetricsConfig
defaultConsumerMetricsConfig =
  ConsumerMetricsConfig
    { collectSeconds = 15
    , jobExecutionBuckets = [0.01, 0.05, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    , collectDegradeThresholdSeconds = 0.1
    , collectDegradeSeconds = 60
    }

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
    Prom.register $
      Prom.vector "job_name" $
        Prom.gauge
          Prom.Info
            { metricName = "consumers_job_info"
            , metricHelp = "The number of workers registered for a given job_name"
            }
  jobsOverdue <-
    Prom.register $
      Prom.vector "job_name" $
        Prom.gauge
          Prom.Info
            { metricName = "consumers_jobs_overdue"
            , metricHelp = "The current number of jobs overdue, by job_name"
            }
  jobsReserved <-
    Prom.register $
      Prom.vector "job_name" $
        Prom.counter
          Prom.Info
            { metricName = "consumers_jobs_reserved_total"
            , metricHelp = "The total number of job reserved, by job_name"
            }
  jobsExecution <-
    Prom.register $
      Prom.vector ("job_name", "job_result") $
        Prom.histogram
          Prom.Info
            { metricName = "consumers_job_execution_seconds"
            , metricHelp = "Execution time of jobs in seconds, by job_name, includes the job_result"
            }
          jobExecutionBuckets
  pure $ ConsumerMetrics {..}

-- | Run a 'ConsumerConfig', but with instrumentation added.
--
-- This will spawn a thread to collect "queue" metrics, and alter
-- 'ccProcessJob' to collect "job" metrics.
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
  :: forall m idx job .
   ( MonadBaseControl IO m
   , MonadMask m
   , MonadTime m
   , MonadLog m
   )
  => ConnectionSourceM m
  -> ConsumerMetrics
  -> ConsumerConfig m idx job
  -> m ThreadId
runMetricsCollection connSource metrics config = localDomain "metrics-collection" $ fork collectLoop
  where
    collectLoop = do
      seconds <- handleAny handleEx collect
      threadDelay (seconds * 1_000_000)
      collectLoop

    handleEx e = do
      logAttention "Exception while running metrics-collection" $
        object
          [ "exception" .= show e
          , "rerun_seconds" .= metrics.collectDegradeSeconds
          ]
      pure metrics.collectDegradeSeconds

    collect = do
      logInfo_ "Collecting consumer metrics"
      t1 <- monotonicTime
      collectMetrics connSource metrics config
      t2 <- monotonicTime
      let runtime = t2 - t1
      -- Graceful degrade if things take too long
      if runtime < metrics.collectDegradeThresholdSeconds
        then pure metrics.collectSeconds
        else do
          logAttention "Consumer metrics collection took long" $
            object
              [ "runtime" .= runtime
              , "threshold" .= metrics.collectDegradeThresholdSeconds
              , "rerun_seconds" .= metrics.collectDegradeSeconds
              ]
          pure metrics.collectDegradeSeconds

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
collectMetrics connSource metric config = runDBT connSource defaultTransactionSettings $ do
  let jobName = unRawSQL config.ccJobsTable

  info <- do
    runSQL_ $
      "SELECT count(id)::float8 FROM "
        <> raw config.ccConsumersTable
        <> " WHERE name =" <?> unRawSQL config.ccJobsTable
    fetchOne runIdentity
  liftBase $ Prom.withLabel metric.jobInfo jobName (`Prom.setGauge` info)

  overdue <- do
    runSQL_ $
      "SELECT count(id)::float8 FROM "
        <> raw config.ccJobsTable
        <> " WHERE run_at <= now() AND reserved_by IS NULL"
    fetchOne runIdentity
  liftBase $ Prom.withLabel metric.jobsOverdue jobName (`Prom.setGauge` overdue)

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
instrumentConsumerConfig metrics ConsumerConfig {..} =
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
      handleAny handleEx $ liftBase $ Prom.withLabel metrics.jobsReserved jobName Prom.incCounter
      fst <$> generalBracket monotonicTime reportJob (const $ ccProcessJob job)

    reportJob t1 jobExit = handleAny handleEx $ do
      t2 <- monotonicTime
      let duration = t2 - t1
          resultLabel = case jobExit of
            ExitCaseSuccess (Ok _) -> "ok"
            ExitCaseSuccess (Failed _) -> "failed"
            ExitCaseException _ -> "exception"
            ExitCaseAbort -> "abort"
      liftBase $ Prom.withLabel metrics.jobsExecution (jobName, resultLabel) (`Prom.observe` duration)

    handleEx e = logAttention "Exception while instrumenting job" $ object ["exception" .= show e]
