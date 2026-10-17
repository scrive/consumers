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
import Database.PostgreSQL.Consumers.RetryStrategy
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
main = T.runTestTTAndExit $ T.TestList [T.TestCase retryStrategyTest, T.TestCase test]

retryStrategyTest :: IO ()
retryStrategyTest = do
  let base = imicroseconds 500000
      cap = iminutes 60
  T.assertEqual "first exponential delay" base (exponentialDelay base cap 1)
  T.assertEqual "second exponential delay" (iseconds 1) (exponentialDelay base cap 2)
  forM_ [0, -1, minBound] $ \attempts ->
    T.assertEqual ("exponential delay at " <> show attempts) base (exponentialDelay base cap attempts)
  forM_ [64, 1000, maxBound] $ \attempts ->
    T.assertEqual ("exponential delay at " <> show attempts) (iseconds 3600) (exponentialDelay base cap attempts)
  T.assertEqual "linear delay" (iseconds 3) (linearDelay (iseconds 1) 3)
  T.assertEqual "linear delay at zero" (iseconds 1) (linearDelay (iseconds 1) 0)
  T.assertEqual "retry forever" (RerunAfter base) (retryForever (constantDelay base) maxBound)
  T.assertEqual "retry before limit" (RerunAfter base) (giveUpAfter 3 (constantDelay base) 2)
  T.assertEqual "give up at limit" Remove (giveUpAfter 3 (constantDelay base) 3)
  forM_ [(Nothing, 5, 15), (Just 0.1, 9, 11), (Just 5, 0, 20)] $ \(fraction, low, high) ->
    forM_ [1 .. 20 :: Int] $ \_ -> do
      delay <- jitter fraction $ iseconds 10
      T.assertBool ("jitter " <> show fraction <> " within bounds: " <> show delay) $
        delay >= iseconds low && delay <= iseconds high
  T.assertEqual "no jitter" (iseconds 10) =<< jitter (Just 0) (iseconds 10)

test :: IO ()
test = do
  connString <-
    getArgs >>= \case
      connString : _args -> pure $ T.pack connString
      [] ->
        lookupEnv "GITHUB_ACTIONS" >>= \case
          Just "true" -> pure "host=localhost user=postgres password=postgres dbname=consumers_test"
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
      dropTables
  where
    waitUntilTrue tmvar = liftIO . atomically $ do
      takeTMVar tmvar >>= \case
        True -> pure ()
        False -> retry

    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

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
        , ccJobSelectors = ["id", "attempts", "countdown"]
        , ccJobFetcher = id
        , ccJobIndex = \(i :: Int64, _attempts :: Int32, _countdown :: Int32) -> i
        , ccJobAttempts = \(_i, attempts, _countdown) -> attempts
        , ccNotificationChannel = Just "consumers_test_chan"
        , -- select some small timeout
          ccNotificationTimeout = 100 * 1000 -- 100 msec
        , ccMaxRunningJobs = 20
        , ccProcessJob = processJob
        , ccOnException = handleException
        , ccJobLogData = \(i, _, _) -> ["job_id" .= i]
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

    processJob :: (Int64, Int32, Int32) -> TestEnv Result
    processJob (_idx, _attempts, countdown) = withNewSession $ do
      when (countdown > 0) $ do
        putJob (countdown - 1)
        putJob (countdown - 1)
        commit
      pure (Ok Remove)

    handleException :: SomeException -> (Int64, Int32, Int32) -> TestEnv Action
    handleException _ (_idx, attempts, _countdown) =
      pure $ giveUpAfter 5 (constantDelay $ imicroseconds 500000) attempts

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
