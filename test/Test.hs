{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module Main where

import Data.Monoid.Utils
import Database.PostgreSQL.Consumers
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.State.Strict
import Control.Monad.Time
import Control.Monad.Trans.Control
import Data.Int
import Data.Monoid
import Data.Time
import Log
import Log.Backend.StandardOutput
import Prelude
import System.Environment
import System.Exit
import TextShow

import qualified Data.Text as T
import qualified Test.HUnit as Test

data TestEnvSt = TestEnvSt {
    teCurrentTime :: UTCTime
  , teMonotonicTime :: Double
  }

type InnerTestEnv = StateT TestEnvSt (DBT (LogT IO))

newtype TestEnv a = TestEnv { unTestEnv :: InnerTestEnv a }
  deriving (Applicative, Functor, Monad, MonadLog, MonadDB, MonadThrow, MonadCatch, MonadMask, MonadIO, MonadBase IO, MonadState TestEnvSt)

instance MonadBaseControl IO TestEnv where
  type StM TestEnv a = StM InnerTestEnv a
  liftBaseWith f = TestEnv $ liftBaseWith $ \run -> f $ run . unTestEnv
  restoreM       = TestEnv . restoreM
  {-# INLINE liftBaseWith #-}
  {-# INLINE restoreM #-}

instance MonadTime TestEnv where
  currentTime = gets teCurrentTime
  monotonicTime = gets teMonotonicTime

modifyTestTime :: (MonadState TestEnvSt m) => (UTCTime -> UTCTime) -> m ()
modifyTestTime modtime = modify (\te -> te { teCurrentTime = modtime . teCurrentTime $ te })

runTestEnv :: ConnectionSourceM (LogT IO) -> Logger -> TestEnv a -> IO a
runTestEnv connSource logger =
    runLogT "consumers-test" logger defaultLogLevel
  . runDBT connSource defaultTransactionSettings
  . (\m' -> evalStateT m' (TestEnvSt (UTCTime (ModifiedJulianDay 0) 0) 0))
  . unTestEnv

main :: IO ()
main = do
  connSource <- connectToDB
  void . Test.runTestTT $
    Test.TestList
      [ Test.TestLabel "Test standard consumer config" $ Test.TestCase (test connSource)
      , Test.TestLabel "Test duplicating consumer config" $ Test.TestCase (testDuplicating connSource)
      ]

-- | Connect to the postgres database
connectToDB :: IO (ConnectionSource [MonadBase IO, MonadMask])
connectToDB = do
  connString <- getArgs >>= \case
    connString : _args -> return $ T.pack connString
    [] -> lookupEnv "GITHUB_ACTIONS" >>= \case
      Just "true" -> return "host=postgres user=postgres password=postgres"
      _           -> printUsage >> exitFailure

  pure $ simpleSource defaultConnectionSettings { csConnInfo = connString }
  where
    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

testDuplicating :: ConnectionSource [MonadBase IO, MonadMask] -> IO ()
testDuplicating (ConnectionSource connSource) =
  withSimpleStdOutLogger $ \logger -> runTestEnv connSource logger $ do
    createTables
    idleSignal <- liftIO newEmptyTMVarIO
    let rows = 15
    putJob rows "consumers_test_duplicating_jobs"  "consumers_test_duplicating_chan" *> commit

    -- Move time forward 2hours, because job is scheduled 1 hour into future
    modifyTestTime . addUTCTime $ 2*60*60
    finalize (localDomain "process" $
              runConsumerWithIdleSignal duplicatingConsumerConfig connSource idleSignal) $
      waitUntilTrue idleSignal
    currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

    runSQL_ "SELECT COUNT(*) from person_test"
    rowcount0 :: Int64 <- fetchOne runIdentity

    runSQL_ "SELECT COUNT(*) from consumers_test_duplicating_jobs"
    rowcount1 :: Int64 <- fetchOne runIdentity

    liftIO $ Test.assertEqual "Number of rows in person_test is 2×rows" (2 * rows) (fromIntegral rowcount0)
    liftIO $ Test.assertEqual "Job in consumers_test_duplicating_jobs should be completed" 0 rowcount1

    dropTables
  where

    tables     = [duplicatingConsumersTable, duplicatingJobsTable, personTable]

    migrations = createTableMigration <$> tables

    createTables :: TestEnv ()
    createTables = do
      migrateDatabase defaultExtrasOptions
        {- extensions -} [] {- composites -} [] {- domains -} []
        tables migrations
      checkDatabase defaultExtrasOptions
        {- composites -} [] {- domains -} []
        tables

    dropTables :: TestEnv ()
    dropTables = do
      migrateDatabase defaultExtrasOptions
        {- extensions -} [] {- composites -} [] {- domains -} [] {- tables -} []
        [ dropTableMigration duplicatingJobsTable
        , dropTableMigration duplicatingConsumersTable
        , dropTableMigration personTable
        ]

    duplicatingConsumerConfig = ConsumerConfig
        { ccJobsTable           = "consumers_test_duplicating_jobs"
        , ccConsumersTable      = "consumers_test_duplicating_consumers"
        , ccJobSelectors        = ["id", "countdown"]
        , ccJobFetcher          = id
        , ccJobIndex            = fst
        , ccNotificationChannel = Just "consumers_test_duplicating_chan"
          -- select some small timeout
        , ccNotificationTimeout = 100 * 1000 -- 100 msec
        , ccMaxRunningJobs      = 20
        , ccProcessJob          = insertNRows . snd
        , ccOnException         = \err (idx, _) -> handleException err idx
        , ccMode                = Duplicating "countdown"
        }

insertNRows :: Int32 -> TestEnv Result
insertNRows count = do
  replicateM_ (fromIntegral count) $ do
    runSQL_ "INSERT INTO person_test (name, age) VALUES ('Anna', 20)"
    notify "consumers_test_duplicating_chan" ""
  pure $ Ok Remove

test :: ConnectionSource [MonadBase IO, MonadMask] -> IO ()
test (ConnectionSource connSource) =
  withSimpleStdOutLogger $ \logger ->
    runTestEnv connSource logger $ do
      createTables
      idleSignal <- liftIO newEmptyTMVarIO
      putJob 10 "consumers_test_jobs" "consumers_test_chan" *> commit

      forM_ [1..10::Int] $ \_ -> do
        -- Move time forward 2hours, because jobs are scheduled 1 hour into future
        modifyTestTime $ addUTCTime (2*60*60)
        finalize (localDomain "process" $
                  runConsumerWithIdleSignal consumerConfig connSource idleSignal) $ do
          waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

      -- Each job creates 2 new jobs, so there should be 1024 jobs in table.
      runSQL_ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount0 :: Int64 <- fetchOne runIdentity
      -- Move time 2 hours forward
      modifyTestTime $ addUTCTime (2*60*60)
      finalize (localDomain "process" $
                runConsumerWithIdleSignal consumerConfig connSource idleSignal) $ do
        waitUntilTrue idleSignal
      -- Jobs are designed to double only 10 times, so there should be no jobs left now.
      runSQL_ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount1 :: Int64 <- fetchOne runIdentity
      liftIO $ Test.assertEqual "Number of jobs in table consumers_test_jobs 10 steps is 1024" 1024 rowcount0
      liftIO $ Test.assertEqual "Number of jobs in table consumers_test_jobs 11 steps is 0" 0 rowcount1
      dropTables
    where

      tables     = [consumersTable, jobsTable]
      -- NB: order of migrations is important.
      migrations = createTableMigration <$> tables

      createTables :: TestEnv ()
      createTables = do
        migrateDatabase defaultExtrasOptions
          {- extensions -} [] {- composites -} [] {- domains -} []
          tables migrations
        checkDatabase defaultExtrasOptions
          {- composites -} [] {- domains -} []
          tables

      dropTables :: TestEnv ()
      dropTables = do
        migrateDatabase defaultExtrasOptions
          {- extensions -} [] {- composites -} [] {- domains -} [] {- tables -} []
          [ dropTableMigration jobsTable
          , dropTableMigration consumersTable
          ]

      consumerConfig = ConsumerConfig
        { ccJobsTable           = "consumers_test_jobs"
        , ccConsumersTable      = "consumers_test_consumers"
        , ccJobSelectors        = ["id", "countdown"]
        , ccJobFetcher          = id
        , ccJobIndex            = fst
        , ccNotificationChannel = Just "consumers_test_chan"
          -- select some small timeout
        , ccNotificationTimeout = 100 * 1000 -- 100 msec
        , ccMaxRunningJobs      = 20
        , ccProcessJob          = processJob "consumers_test_jobs" "consumers_test_chan"
        , ccOnException         = \err (idx, _) -> handleException err idx
        , ccMode                = Standard
        }

waitUntilTrue :: MonadIO m => TMVar Bool -> m ()
waitUntilTrue tmvar = whileM_ (not <$> liftIO (atomically $ takeTMVar tmvar)) $ pure ()

putJob :: Int32 -> SQL -> Channel -> TestEnv ()
putJob countdown tableName notifyChan = localDomain "put" $ do
  now <- currentTime
  runSQL_ $ "INSERT INTO" <+> tableName
    <+> "(run_at, finished_at, reserved_by, attempts, countdown)"
    <+> "VALUES (" <?> now <+> "+ interval '1 hour', NULL, NULL, 0, " <?> countdown <> ")"
  notify notifyChan ""

processJob :: SQL -> Channel -> (Int64, Int32) -> TestEnv Result
processJob tableName notifyChan (_idx, countdown) = do
  when (countdown > 0) $ do
    putJob (countdown - 1) tableName notifyChan
    putJob (countdown - 1) tableName notifyChan
    commit
  return (Ok Remove)

handleException :: SomeException -> Int64 -> TestEnv Action
handleException exc idx = do
  logAttention_ $
    "Job #" <> showt idx <> " failed with: " <> showt exc
  return . RerunAfter $ imicroseconds 500000

jobsTable :: Table
jobsTable =
  tblTable
  { tblName = "consumers_test_jobs"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",          colType = BigSerialT
                , colNullable = False }
    , tblColumn { colName = "run_at",      colType = TimestampWithZoneT
                , colNullable = True }
    , tblColumn { colName = "finished_at", colType = TimestampWithZoneT
                , colNullable = True }
    , tblColumn { colName = "reserved_by", colType = BigIntT
                , colNullable = True }
    , tblColumn { colName = "attempts",    colType = IntegerT
                , colNullable = False }

      -- The only non-obligatory field:
    , tblColumn { colName = "countdown",    colType = IntegerT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  , tblForeignKeys = [
    (fkOnColumn "reserved_by" "consumers_test_consumers" "id") {
      fkOnDelete = ForeignKeySetNull
      }
    ]
  }

personTable :: Table
personTable =
  tblTable
  { tblName = "person_test"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",            colType = BigSerialT
                , colNullable = False }
    , tblColumn { colName = "name",          colType = TextT
                , colNullable = False }
    , tblColumn { colName = "age", colType = IntegerT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  }

duplicatingJobsTable :: Table
duplicatingJobsTable =
  tblTable
  { tblName = "consumers_test_duplicating_jobs"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",          colType = BigSerialT
                , colNullable = False }
    , tblColumn { colName = "run_at",      colType = TimestampWithZoneT
                , colNullable = True }
    , tblColumn { colName = "finished_at", colType = TimestampWithZoneT
                , colNullable = True }
    , tblColumn { colName = "reserved_by", colType = BigIntT
                , colNullable = True }
    , tblColumn { colName = "attempts",    colType = IntegerT
                , colNullable = False }

      -- Non-obligatory field "countdown". Really more of a count
      -- and not a countdown, but name is kept to that we can reuse
      -- `putJob` function.
    , tblColumn { colName = "countdown",    colType = IntegerT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  , tblForeignKeys = [
    (fkOnColumn "reserved_by" "consumers_test_duplicating_consumers" "id") {
      fkOnDelete = ForeignKeySetNull
      }
    ]
  }

consumersTable :: Table
consumersTable =
  tblTable
  { tblName = "consumers_test_consumers"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",            colType = BigSerialT
                , colNullable = False }
    , tblColumn { colName = "name",          colType = TextT
                , colNullable = False }
    , tblColumn { colName = "last_activity", colType = TimestampWithZoneT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  }

duplicatingConsumersTable :: Table
duplicatingConsumersTable =
  tblTable
  { tblName = "consumers_test_duplicating_consumers"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",            colType = BigSerialT
                , colNullable = False }
    , tblColumn { colName = "name",          colType = TextT
                , colNullable = False }
    , tblColumn { colName = "last_activity", colType = TimestampWithZoneT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  }

createTableMigration :: (MonadDB m) => Table -> Migration m
createTableMigration tbl = Migration
  { mgrTableName = tblName tbl
  , mgrFrom      = 0
  , mgrAction    = StandardMigration $ do
      createTable True tbl
  }

dropTableMigration :: Table -> Migration m
dropTableMigration tbl = Migration
  { mgrTableName = tblName tbl
  , mgrFrom      = 1
  , mgrAction    = DropTableMigration DropTableRestrict
  }
