module Main (main) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Time
import Control.Monad.Trans.Control
import Data.ByteString.Lazy qualified as BSL
import Data.Int
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Time
import Database.PostgreSQL.Consumers
import Database.PostgreSQL.Consumers.Instrumented
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model
import Log
import Log.Backend.StandardOutput
import Prometheus qualified as Prom
import System.Environment
import System.Exit
import Test.HUnit qualified as T
import Text.Read (readMaybe)

-- | 'LogT' has no 'MonadTime' instance, and the instrumented consumer needs
-- one. The system clock is what the tests want.
newtype TestM a = TestM {unTestM :: LogT IO a}
  deriving (Applicative, Functor, Monad, MonadLog, MonadThrow, MonadCatch, MonadMask, MonadIO, MonadBase IO)

instance MonadBaseControl IO TestM where
  type StM TestM a = StM (LogT IO) a
  liftBaseWith f = TestM $ liftBaseWith (\run -> f $ run . unTestM)
  restoreM = TestM . restoreM

instance MonadTime TestM where
  currentTime = liftIO currentTime
  monotonicTime = liftIO monotonicTime

type Job = (Int64, Int32, Int32)

jobsTableName :: T.Text
jobsTableName = "consumers_metrics_test_jobs"

main :: IO ()
main = do
  connString <-
    getArgs >>= \case
      connString : _args -> pure $ T.pack connString
      [] ->
        lookupEnv "GITHUB_ACTIONS" >>= \case
          Just "true" -> pure "host=localhost user=postgres password=postgres dbname=consumers_metrics_test"
          _ -> printUsage >> exitFailure

  let connSettings = defaultConnectionSettings {csConnInfo = connString}
      ConnectionSource connSource = simpleSource connSettings

  -- Collect every second, so that the queue gauges follow the table quickly.
  metrics <-
    registerConsumerMetrics
      defaultConsumerMetricsConfig {collectSeconds = 1}

  withStdOutLogger $ \logger ->
    runLogT "consumers-metrics-test" logger defaultLogLevel . unTestM $ do
      withDB connSource createTables
      jobMetricsTest connSource metrics
      queueMetricsTest connSource metrics
      withDB connSource dropTables
  where
    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

-- | Run four jobs, one per action, and check the execution histogram labels.
jobMetricsTest :: ConnectionSourceM TestM -> ConsumerMetrics -> TestM ()
jobMetricsTest connSource metrics = localDomain "job-metrics" $ do
  withDB connSource $ do
    now <- currentTime
    forM_ [0 .. 3] $ \countdown -> putJob now countdown
    commit
  let processJob :: Job -> TestM Result
      processJob (_, _, countdown) = case countdown of
        0 -> pure $ Ok MarkProcessed
        1 -> pure . Ok $ RerunAfter (ihours 1)
        2 -> do
          now <- currentTime
          pure . Failed $ RerunAt (addUTCTime 3600 now)
        _ -> throwM $ userError "boom"
      handleEx :: SomeException -> Job -> TestM Action
      handleEx _ _ = pure Remove
  withConsumer connSource metrics (mkConsumerConfig 20 processJob handleEx) $ do
    waitFor "four jobs reserved" $ \export ->
      sample export "consumers_jobs_reserved_total" [("job_name", jobsTableName)] == Just 4
    -- The exception job is removed last, so wait for its row to go.
    waitFor "exception job removed" $ \export ->
      executionCount export "exception" "none" == Just 1

  export <- liftIO exportText
  liftIO $ do
    T.assertEqual "ok mark_processed" (Just 1) $ executionCount export "ok" "mark_processed"
    T.assertEqual "ok rerun_after" (Just 1) $ executionCount export "ok" "rerun_after"
    T.assertEqual "failed rerun_at" (Just 1) $ executionCount export "failed" "rerun_at"
    T.assertEqual "exception none" (Just 1) $ executionCount export "exception" "none"
    -- Only the failed run and the exception count as failed attempts.
    T.assertEqual "failed attempts" (Just 2) $
      sample export "consumers_job_failed_attempts_count" [("job_name", jobsTableName)]
  where
    executionCount export result action =
      sample
        export
        "consumers_job_execution_seconds_count"
        [("job_name", jobsTableName), ("job_result", result), ("job_action", action)]

-- | Check the queue gauges with a consumer that holds one job and leaves the
-- rest in the table.
queueMetricsTest :: ConnectionSourceM TestM -> ConsumerMetrics -> TestM ()
queueMetricsTest connSource metrics = localDomain "queue-metrics" $ do
  -- From the previous test: one processed job (run_at NULL), one rerun after
  -- an hour and one rerun at a later time. Both reruns count as scheduled.
  withDB connSource $ do
    now <- currentTime
    forM_ [0 .. 2 :: Int32] $ \_ -> putJob (addUTCTime (-3600) now) 0
    commit
  -- The job holds until the gate opens. The consumer's cleanup waits for
  -- running jobs, so the gate must open before the consumer stops.
  gate <- liftIO newEmptyMVar
  let processJob :: Job -> TestM Result
      processJob _ = liftIO $ readMVar gate >> pure (Ok Remove)
      handleEx :: SomeException -> Job -> TestM Action
      handleEx _ _ = pure Remove
  -- With one running job at most, the consumer reserves one overdue job and
  -- leaves two.
  withConsumer connSource metrics (mkConsumerConfig 1 processJob handleEx) $ do
    waitFor
      "queue gauges"
      ( \export ->
          and
            [ sample export "consumers_jobs_overdue" [("job_name", jobsTableName)] == Just 2
            , sample export "consumers_jobs_scheduled" [("job_name", jobsTableName)] == Just 2
            , sample export "consumers_job_info" [("job_name", jobsTableName)] == Just 1
            ]
      )
      `finally` liftIO (putMVar gate ())

-- | Run an instrumented consumer around an action, and clean it up after.
withConsumer
  :: ConnectionSourceM TestM
  -> ConsumerMetrics
  -> ConsumerConfig TestM Int64 Job
  -> TestM a
  -> TestM a
withConsumer connSource metrics config action = do
  cleanup <- runInstrumentedConsumer metrics config connSource
  action `finally` cleanup

-- | Poll the registry until the predicate holds, or fail after ten seconds.
waitFor :: String -> (T.Text -> Bool) -> TestM ()
waitFor what predicate = go (100 :: Int)
  where
    go 0 = liftIO $ T.assertFailure $ "timed out waiting for " <> what
    go n = do
      export <- liftIO exportText
      if predicate export
        then pure ()
        else liftIO (threadDelay 100_000) >> go (n - 1)

-- | Read one sample from the text export. Labels must be in the order of the
-- metric's vector.
sample :: T.Text -> T.Text -> [(T.Text, T.Text)] -> Maybe Double
sample export name labels = do
  line <- case filter (T.isPrefixOf prefix) (T.lines export) of
    [line] -> Just line
    _ -> Nothing
  readMaybe . T.unpack . T.strip $ T.drop (T.length prefix) line
  where
    prefix =
      name <> "{" <> T.intercalate "," [k <> "=" <> T.pack (show v) | (k, v) <- labels] <> "} "

mkConsumerConfig
  :: Int
  -> (Job -> TestM Result)
  -> (SomeException -> Job -> TestM Action)
  -> ConsumerConfig TestM Int64 Job
mkConsumerConfig maxRunningJobs process onEx =
  ConsumerConfig
    { ccJobsTable = "consumers_metrics_test_jobs"
    , ccConsumersTable = "consumers_metrics_test_consumers"
    , ccJobSelectors = ["id", "attempts", "countdown"]
    , ccJobFetcher = id
    , ccJobIndex = \(i, _, _) -> i
    , ccJobAttempts = \(_, attempts, _) -> attempts
    , ccNotificationChannel = Just "consumers_metrics_test_chan"
    , ccNotificationTimeout = 100 * 1000 -- 100 msec
    , ccMaxRunningJobs = maxRunningJobs
    , ccProcessJob = process
    , ccOnException = onEx
    , ccJobLogData = \(i, _, _) -> ["job_id" .= i]
    }

putJob :: MonadDB m => UTCTime -> Int32 -> m ()
putJob runAt countdown =
  runSQL_ $
    "INSERT INTO consumers_metrics_test_jobs "
      <> "(run_at, finished_at, reserved_by, attempts, countdown) "
      <> "VALUES (" <?> runAt
      <> ", NULL, NULL, 0, " <?> countdown
      <> ")"

withDB :: ConnectionSourceM TestM -> DBT TestM a -> TestM a
withDB connSource = runDBT connSource defaultTransactionSettings

createTables :: (MonadDB m, MonadIO m, MonadLog m, MonadMask m) => m ()
createTables = do
  migrateDatabase defaultExtrasOptions definitions migrations
  checkDatabase defaultExtrasOptions definitions
  where
    definitions = emptyDbDefinitions {dbTables = [consumersTable, jobsTable]}
    migrations =
      [ createTableMigration consumersTable
      , createTableMigration jobsTable
      ]

dropTables :: (MonadDB m, MonadIO m, MonadLog m, MonadMask m) => m ()
dropTables =
  migrateDatabase
    defaultExtrasOptions
    emptyDbDefinitions
    [ dropTableMigration jobsTable
    , dropTableMigration consumersTable
    ]

jobsTable :: Table
jobsTable =
  tblTable
    { tblName = "consumers_metrics_test_jobs"
    , tblVersion = 1
    , tblColumns =
        [ tblColumn {colName = "id", colType = BigSerialT, colNullable = False}
        , tblColumn {colName = "run_at", colType = TimestampWithZoneT, colNullable = True}
        , tblColumn {colName = "finished_at", colType = TimestampWithZoneT, colNullable = True}
        , tblColumn {colName = "reserved_by", colType = BigIntT, colNullable = True}
        , tblColumn {colName = "attempts", colType = IntegerT, colNullable = False}
        , tblColumn {colName = "countdown", colType = IntegerT, colNullable = False}
        ]
    , tblPrimaryKey = pkOnColumn "id"
    , tblForeignKeys =
        [ (fkOnColumn "reserved_by" "consumers_metrics_test_consumers" "id")
            { fkOnDelete = ForeignKeySetNull
            }
        ]
    }

consumersTable :: Table
consumersTable =
  tblTable
    { tblName = "consumers_metrics_test_consumers"
    , tblVersion = 1
    , tblColumns =
        [ tblColumn {colName = "id", colType = BigSerialT, colNullable = False}
        , tblColumn {colName = "name", colType = TextT, colNullable = False}
        , tblColumn {colName = "last_activity", colType = TimestampWithZoneT, colNullable = False}
        ]
    , tblPrimaryKey = pkOnColumn "id"
    }

createTableMigration :: MonadDB m => Table -> Migration m
createTableMigration tbl =
  Migration
    { mgrTableName = tblName tbl
    , mgrFrom = 0
    , mgrAction = StandardMigration $ createTable True tbl
    }

dropTableMigration :: Table -> Migration m
dropTableMigration tbl =
  Migration
    { mgrTableName = tblName tbl
    , mgrFrom = 1
    , mgrAction = DropTableMigration DropTableRestrict
    }

exportText :: IO T.Text
exportText = T.decodeUtf8 . BSL.toStrict <$> Prom.exportMetricsAsText
