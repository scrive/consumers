module Main where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.State.Strict
import Control.Monad.Time
import Control.Monad.Trans.Control
import Data.Int
import Data.Text qualified as T
import Data.Time
import Database.PostgreSQL.Consumers
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model
import Log
import Log.Backend.StandardOutput
import System.Environment
import System.Exit
import Test.HUnit qualified as T

data TestEnvSt = TestEnvSt
  { teCurrentTime :: UTCTime
  , teMonotonicTime :: Double
  }

type InnerTestEnv = StateT TestEnvSt (DBT (LogT IO))

newtype TestEnv a = TestEnv {unTestEnv :: InnerTestEnv a}
  deriving (Applicative, Functor, Monad, MonadLog, MonadDB, MonadThrow, MonadCatch, MonadMask, MonadIO, MonadBase IO, MonadState TestEnvSt)

instance MonadBaseControl IO TestEnv where
  type StM TestEnv a = StM InnerTestEnv a
  liftBaseWith f = TestEnv $ liftBaseWith (\run -> f $ run . unTestEnv)
  restoreM = TestEnv . restoreM

instance MonadTime TestEnv where
  currentTime = gets teCurrentTime
  monotonicTime = gets teMonotonicTime

modifyTestTime :: MonadState TestEnvSt m => (UTCTime -> UTCTime) -> m ()
modifyTestTime modtime = modify (\te -> te {teCurrentTime = modtime . teCurrentTime $ te})

runTestEnv :: ConnectionSourceM (LogT IO) -> Logger -> TestEnv a -> IO a
runTestEnv connSource logger =
  runLogT "consumers-test" logger defaultLogLevel
    . runDBT connSource defaultTransactionSettings
    . (\m' -> fst <$> runStateT m' (TestEnvSt (UTCTime (ModifiedJulianDay 0) 0) 0))
    . unTestEnv

main :: IO ()
main = void . T.runTestTT $ T.TestCase test

test :: IO ()
test = do
  connString <-
    getArgs >>= \case
      connString : _args -> pure $ T.pack connString
      [] ->
        lookupEnv "GITHUB_ACTIONS" >>= \case
          Just "true" -> pure "host=postgres user=postgres password=postgres"
          _ -> printUsage >> exitFailure

  let connSettings =
        defaultConnectionSettings
          { csConnInfo = connString
          }
      ConnectionSource connSource = simpleSource connSettings

  withStdOutLogger $ \logger ->
    runTestEnv connSource logger $ do
      createTables
      idleSignal <- liftIO newEmptyTMVarIO
      putJob 10 >> commit

      forM_ [1 .. 10 :: Int] $ \_ -> do
        -- Move time forward 2hours, because jobs are scheduled 1 hour into future
        modifyTestTime $ addUTCTime (2 * 60 * 60)
        finalize
          ( localDomain "process" $
              runConsumerWithIdleSignal consumerConfig connSource idleSignal
          )
          $ do
            waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

      -- Each job creates 2 new jobs, so there should be 1024 jobs in table.
      runSQL_ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount0 :: Int64 <- fetchOne runIdentity
      -- Move time 2 hours forward
      modifyTestTime $ addUTCTime (2 * 60 * 60)
      finalize
        ( localDomain "process" $
            runConsumerWithIdleSignal consumerConfig connSource idleSignal
        )
        $ do
          waitUntilTrue idleSignal
      -- Jobs are designed to double only 10 times, so there should be no jobs left now.
      runSQL_ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount1 :: Int64 <- fetchOne runIdentity
      liftIO $ T.assertEqual "Number of jobs in table after 10 steps is 1024" 1024 rowcount0
      liftIO $ T.assertEqual "Number of jobs in table after 11 steps is 0" 0 rowcount1

      -- Checking the failing mechanism for a single job.
      putJob 1 >> commit
      do
        -- Move time forward again, since we enqueued new jobs.
        modifyTestTime $ addUTCTime (2 * 60 * 60)
        finalize
          ( localDomain "process" $
              runConsumerWithIdleSignal consumerFailingSingleJobConfig connSource idleSignal
          )
          $ do
            waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

      runSQL_ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount2 :: Int64 <- fetchOne runIdentity
      runSQL_ "SELECT run_at from consumers_test_jobs"
      newRunTime :: UTCTime <- fetchOne runIdentity
      liftIO $ do
        T.assertEqual "The failed job should still be in the table" 1 rowcount2
      areInSixHours [newRunTime]
      -- Clean up.
      runSQL_ "DELETE FROM consumers_test_jobs;"

      -- Checking the failing mechanism for multiple jobs
      let nbOfJobsToFail = 5
      replicateM_ nbOfJobsToFail (putJob 1) >> commit
      do
        -- Move time forward again, since we enqueued new jobs.
        modifyTestTime $ addUTCTime (2 * 60 * 60)
        -- Jobs get processed one at a time, so we need to run this as many times as there are jobs
        replicateM_ nbOfJobsToFail $ do
          finalize
            ( localDomain "process" $
                runConsumerWithIdleSignal consumerFailingAllJobsConfig connSource idleSignal
            )
            $ do
              waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

      runSQL_ "SELECT run_at from consumers_test_jobs"
      newRunTimes :: [UTCTime] <- fetchMany runIdentity
      liftIO $ do
        T.assertEqual "All jobs should fail if the query to fetch them is wrong" 5 (length newRunTimes)
      areInSixHours newRunTimes
      -- Clean up.
      runSQL_ "DELETE FROM consumers_test_jobs"

      -- All done.
      dropTables
  where
    waitUntilTrue tmvar = liftIO . atomically $ do
      takeTMVar tmvar >>= \case
        True -> pure ()
        False -> retry

    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

    areInSixHours toCheck = do
      let inXHours n = addUTCTime (60 * 60 * n) <$> currentTime
      in5Hours <- inXHours 5
      in7Hours <- inXHours 7
      liftIO . T.assertBool "The failed jobs should be planned to run more than 5 hours later" $ all (in5Hours <) toCheck
      liftIO . T.assertBool "The failed jobs should be planned to run less than 7 hours later" $ all (in7Hours >) toCheck

    definitions = emptyDbDefinitions {dbTables = [consumersTable, jobsTable]}
    -- NB: order of migrations is important.
    migrations =
      [ createTableMigration consumersTable
      , createTableMigration jobsTable
      ]

    createTables :: TestEnv ()
    createTables = do
      migrateDatabase
        defaultExtrasOptions
        definitions
        migrations
      checkDatabase
        defaultExtrasOptions
        definitions

    dropTables :: TestEnv ()
    dropTables = do
      migrateDatabase
        defaultExtrasOptions
        emptyDbDefinitions
        [ dropTableMigration jobsTable
        , dropTableMigration consumersTable
        ]

    consumerConfig =
      ConsumerConfig
        { ccJobsTable = "consumers_test_jobs"
        , ccConsumersTable = "consumers_test_consumers"
        , ccJobSelectors = ["id", "countdown"]
        , ccJobFetcher = id
        , ccJobIndex = \(i :: Int64, _ :: Int32) -> i
        , ccNotificationChannel = Just "consumers_test_chan"
        , -- select some small timeout
          ccNotificationTimeout = 100 * 1000 -- 100 msec
        , ccMaxRunningJobs = 20
        , ccProcessJob = processJob
        , ccOnException = handleException
        , ccJobLogData = \(i, _) -> ["job_id" .= i]
        }

    simulatingFailure :: (Int64, Int32) -> (Int64, Int32)
    simulatingFailure _ = error "Simulating row fetch error"

    consumerFailingSingleJobConfig =
      ConsumerConfig
        { ccJobsTable = "consumers_test_jobs"
        , ccConsumersTable = "consumers_test_consumers"
        , ccJobSelectors = ["id", "countdown"]
        , ccJobFetcher = simulatingFailure
        , ccJobIndex = \(i :: Int64, _ :: Int32) -> i
        , ccNotificationChannel = Just "consumers_test_chan"
        , ccNotificationTimeout = 100 * 1000
        , ccMaxRunningJobs = 20
        , ccProcessJob = processJob
        , ccOnException = handleException
        , ccJobLogData = \(i, _) -> ["job_id" .= i]
        }

    consumerFailingAllJobsConfig =
      ConsumerConfig
        { ccJobsTable = "consumers_test_jobs"
        , ccConsumersTable = "consumers_test_consumers"
        , ccJobSelectors = ["id", "countdown::bigint"]
        , ccJobFetcher = id
        , ccJobIndex = \(i :: Int64, _ :: Int32) -> i
        , ccNotificationChannel = Just "consumers_test_chan"
        , ccNotificationTimeout = 100 * 1000
        , ccMaxRunningJobs = 20
        , ccProcessJob = processJob
        , ccOnException = handleException
        , ccJobLogData = \(i, _) -> ["job_id" .= i]
        }

    putJob :: Int32 -> TestEnv ()
    putJob countdown = localDomain "put" $ do
      now <- currentTime
      runSQL_ $
        "INSERT INTO consumers_test_jobs "
          <> "(run_at, finished_at, reserved_by, attempts, countdown) "
          <> "VALUES (" <?> now
          <> " + interval '1 hour', NULL, NULL, 0, " <?> countdown
          <> ")"
      notify "consumers_test_chan" ""

    processJob :: (Int64, Int32) -> TestEnv Result
    processJob (_idx, countdown) = do
      when (countdown > 0) $ do
        putJob (countdown - 1)
        putJob (countdown - 1)
        commit
      pure (Ok Remove)

    handleException :: SomeException -> (Int64, Int32) -> TestEnv Action
    handleException _ _ = pure . RerunAfter $ imicroseconds 500000

jobsTable :: Table
jobsTable =
  tblTable
    { tblName = "consumers_test_jobs"
    , tblVersion = 1
    , tblColumns =
        [ tblColumn
            { colName = "id"
            , colType = BigSerialT
            , colNullable = False
            }
        , tblColumn
            { colName = "run_at"
            , colType = TimestampWithZoneT
            , colNullable = True
            }
        , tblColumn
            { colName = "finished_at"
            , colType = TimestampWithZoneT
            , colNullable = True
            }
        , tblColumn
            { colName = "reserved_by"
            , colType = BigIntT
            , colNullable = True
            }
        , tblColumn
            { colName = "attempts"
            , colType = IntegerT
            , colNullable = False
            }
        , -- The only non-obligatory field:
          tblColumn
            { colName = "countdown"
            , colType = IntegerT
            , colNullable = False
            }
        ]
    , tblPrimaryKey = pkOnColumn "id"
    , tblForeignKeys =
        [ (fkOnColumn "reserved_by" "consumers_test_consumers" "id")
            { fkOnDelete = ForeignKeySetNull
            }
        ]
    }

consumersTable :: Table
consumersTable =
  tblTable
    { tblName = "consumers_test_consumers"
    , tblVersion = 1
    , tblColumns =
        [ tblColumn
            { colName = "id"
            , colType = BigSerialT
            , colNullable = False
            }
        , tblColumn
            { colName = "name"
            , colType = TextT
            , colNullable = False
            }
        , tblColumn
            { colName = "last_activity"
            , colType = TimestampWithZoneT
            , colNullable = False
            }
        ]
    , tblPrimaryKey = pkOnColumn "id"
    }

createTableMigration :: MonadDB m => Table -> Migration m
createTableMigration tbl =
  Migration
    { mgrTableName = tblName tbl
    , mgrFrom = 0
    , mgrAction = StandardMigration $ do
        createTable True tbl
    }

dropTableMigration :: Table -> Migration m
dropTableMigration tbl =
  Migration
    { mgrTableName = tblName tbl
    , mgrFrom = 1
    , mgrAction = DropTableMigration DropTableRestrict
    }
