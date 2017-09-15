{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module Main where

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
import System.Environment
import TextShow

import qualified Data.Text as T

data TestEnvSt = TestEnvSt {
    teCurrentTime :: UTCTime
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

modifyTestTime :: (MonadState TestEnvSt m) => (UTCTime -> UTCTime) -> m ()
modifyTestTime modtime = modify (\te -> te { teCurrentTime = modtime . teCurrentTime $ te })

runTestEnv :: ConnectionSourceM (LogT IO) -> Logger -> TestEnv a -> IO a
runTestEnv connSource logger m =
    (runLogT "consumers-test" logger)
  . (runDBT connSource {- transactionSettings -} def)
  . (\m' -> fst <$> (runStateT m' $ TestEnvSt $ UTCTime (ModifiedJulianDay 0) 0))
  . unTestEnv
  $ m

main :: IO ()
main = do
  args <- getArgs
  let connString = case args of
        []     -> defaultConnString
        (cs:_) -> cs

  let connSettings                = def { csConnInfo = T.pack connString }
      ConnectionSource connSource = simpleSource connSettings

  withSimpleStdOutLogger $ \logger ->
    runTestEnv connSource logger $ do
      createTables
      idleSignal <- liftIO $ atomically $ newEmptyTMVar
      putJob 10 >> commit

      forM_ [1..10::Int] $ \_ -> do
        -- Move time forward 2hours, because jobs are scheduled 1 hour into future
        modifyTestTime $ addUTCTime (2*60*60)
        finalize (localDomain "process" $
                  runConsumerWithIdleSignal consumerConfig connSource idleSignal) $ do
          waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

      -- Each job creates 2 new jobs, so there should be 1024 jobs in table.
      runSQL_ $ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount0 :: Int64 <- fetchOne runIdentity
      -- Move time 2 hours forward
      modifyTestTime $ addUTCTime (2*60*60)
      finalize (localDomain "process" $
                runConsumerWithIdleSignal consumerConfig connSource idleSignal) $ do
        waitUntilTrue idleSignal
      -- Jobs are designed to double only 10 times, so there should be no jobs left now.
      runSQL_ $ "SELECT COUNT(*) from consumers_test_jobs"
      rowcount1 :: Int64 <- fetchOne runIdentity
      logInfo_ . T.pack . ("Number of jobs in table after 10 steps (expected 1024): "++) . show $ rowcount0
      logInfo_ . T.pack . ("Number of jobs in table after 11 steps (expected 0): "++) . show $ rowcount1
    where
      waitUntilTrue tmvar = whileM_ (not <$> (liftIO $ atomically $ takeTMVar tmvar)) $ return ()
      defaultConnString =
        "postgresql://postgres@localhost/travis_ci_test"

      tables     = [consumersTable, jobsTable]
      -- NB: order of migrations is important.
      migrations = [ createTableMigration consumersTable
                   , createTableMigration jobsTable ]

      createTables :: TestEnv ()
      createTables = do
        migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
          tables migrations
        checkDatabase {- domains -} [] tables

      consumerConfig = ConsumerConfig
        { ccJobsTable           = "consumers_test_jobs"
        , ccConsumersTable      = "consumers_test_consumers"
        , ccJobSelectors        = ["id", "countdown"]
        , ccJobFetcher          = id
        , ccJobIndex            = \(i::Int64, _::Int32) -> i
        , ccNotificationChannel = Just "consumers_test_chan"
          -- select some small timeout
        , ccNotificationTimeout = 100 * 1000 -- 100 msec
        , ccMaxRunningJobs      = 20
        , ccProcessJob          = processJob
        , ccOnException         = handleException
        }

      putJob :: Int32 -> TestEnv ()
      putJob countdown = localDomain "put" $ do
        now <- currentTime
        runSQL_ $ "INSERT INTO consumers_test_jobs "
          <> "(run_at, finished_at, reserved_by, attempts, countdown) "
          <> "VALUES (" <?> now <> " + interval '1 hour', NULL, NULL, 0, " <?> countdown <> ")"

      processJob :: (Int64, Int32) -> TestEnv Result
      processJob (_idx, countdown) = do
        when (countdown > 0) $ do
          putJob (countdown - 1)
          putJob (countdown - 1)
          commit
        return (Ok Remove)

      handleException :: SomeException -> (Int64, Int32) -> TestEnv Action
      handleException exc (idx, _countdown) = do
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

createTableMigration :: (MonadDB m) => Table -> Migration m
createTableMigration tbl = Migration
  { mgrTableName = tblName tbl
  , mgrFrom      = 0
  , mgrAction    = StandardMigration $ do
      createTable True tbl
  }
