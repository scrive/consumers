{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.PostgreSQL.Consumers
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.Int
import Data.Monoid
import Log
import Log.Backend.StandardOutput
import System.Environment
import TextShow

import qualified Data.Text as T

type AppM a = DBT (LogT IO) a

main :: IO ()
main = do
  args <- getArgs
  let connString = case args of
        []     -> defaultConnString
        (cs:_) -> cs

  let connSettings                = def { csConnInfo = T.pack connString }
      ConnectionSource connSource = simpleSource connSettings

  withSimpleStdOutLogger $ \logger ->
    runLogT "consumers-example" logger $
    runDBT connSource {- transactionSettings -} def $ do
        createTables
        finalize (localDomain "process" $
                  runConsumer consumerConfig connSource) $ do
          forM_ [(0::Int)..10] $ \_ -> do
            putJob
            liftIO $ threadDelay (1 * 1000000) -- 1 sec
        dropTables

    where

      defaultConnString =
        "postgresql://postgres@localhost/travis_ci_test"

      tables     = [consumersTable, jobsTable]
      -- NB: order of migrations is important.
      migrations = [ createTableMigration consumersTable
                   , createTableMigration jobsTable ]

      createTables :: AppM ()
      createTables = do
        migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
          tables migrations
        checkDatabase {- domains -} [] tables

      dropTables :: AppM ()
      dropTables = do
        migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
          {- tables -} []
          [ dropTableMigration jobsTable
          , dropTableMigration consumersTable ]

      consumerConfig = ConsumerConfig
        { ccJobsTable           = "consumers_example_jobs"
        , ccConsumersTable      = "consumers_example_consumers"
        , ccJobSelectors        = ["id", "message"]
        , ccJobFetcher          = id
        , ccJobIndex            = \(i::Int64, _msg::T.Text) -> i
        , ccNotificationChannel = Just "consumers_example_chan"
        , ccNotificationTimeout = 10 * 1000000 -- 10 sec
        , ccMaxRunningJobs      = 1
        , ccProcessJob          = processJob
        , ccOnException         = handleException
        }

      putJob :: AppM ()
      putJob = localDomain "put" $ do
        logInfo_ "putJob"
        runSQL_ $ "INSERT INTO consumers_example_jobs "
          <> "(run_at, finished_at, reserved_by, attempts, message) "
          <> "VALUES (NOW(), NULL, NULL, 0, 'hello')"
        commit

      processJob :: (Int64, T.Text) -> AppM Result
      processJob (_idx, msg) = do
        logInfo_ msg
        return (Ok Remove)

      handleException :: SomeException -> (Int64, T.Text) -> AppM Action
      handleException exc (idx, _msg) = do
        logAttention_ $
          "Job #" <> showt idx <> " failed with: " <> showt exc
        return . RerunAfter $ imicroseconds 500000


jobsTable :: Table
jobsTable =
  tblTable
  { tblName = "consumers_example_jobs"
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
    , tblColumn { colName = "message",    colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  , tblForeignKeys = [
    (fkOnColumn "reserved_by" "consumers_example_consumers" "id") {
      fkOnDelete = ForeignKeySetNull
      }
    ]
  }

consumersTable :: Table
consumersTable =
  tblTable
  { tblName = "consumers_example_consumers"
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
