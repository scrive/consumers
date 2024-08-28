{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.Int
import Data.Text qualified as T
import Database.PostgreSQL.Consumers
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model
import Log
import Log.Backend.StandardOutput
import System.Environment
import System.Exit
import TextShow

-- | Main application monad. See the 'log-base' and the 'hpqtypes'
-- packages for documentation on 'DBT' and 'LogT'.
type AppM a = DBT (LogT IO) a

main :: IO ()
main = do
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

  -- Monad stack initialisation.
  withStdOutLogger $ \logger ->
    runLogT "consumers-example" logger defaultLogLevel
      . runDBT connSource defaultTransactionSettings
      $ do
        -- Initialise.
        createTables

        -- Create a consumer, put ten jobs into its queue, and wait
        -- for it to finish. 'runConsumer' returns a finaliser that is
        -- invoked by 'finalize' after the 'putJob' loop.
        finalize
          ( localDomain "process" $
              runConsumer consumerConfig connSource
          )
          . forM_ [(0 :: Int) .. 10]
          $ \_ -> do
            putJob
            liftIO $ threadDelay (1 * 1000000) -- 1 sec

        -- Clean up.
        dropTables
  where
    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

    tables = [consumersTable, jobsTable]
    -- NB: order of migrations is important.
    migrations =
      [ createTableMigration consumersTable
      , createTableMigration jobsTable
      ]

    createTables :: AppM ()
    createTables = do
      migrateDatabase
        defaultExtrasOptions
        [] -- extensions
        [] -- composites
        [] -- domains
        tables
        migrations
      checkDatabase
        defaultExtrasOptions
        [] -- composites
        [] -- domains
        tables

    dropTables :: AppM ()
    dropTables = do
      migrateDatabase
        defaultExtrasOptions
        [] -- extensions
        [] -- composites
        [] -- domains
        [] -- tables
        [ dropTableMigration jobsTable
        , dropTableMigration consumersTable
        ]

    -- Configuration of a consumer. See
    -- 'Database.PostgreSQL.Consumers.Config.ConsumerConfig'.
    consumerConfig =
      ConsumerConfig
        { ccJobsTable = "consumers_example_jobs"
        , ccConsumersTable = "consumers_example_consumers"
        , ccJobSelectors = ["id", "message"]
        , ccJobFetcher = id
        , ccJobIndex = \(i :: Int64, _msg :: T.Text) -> i
        , ccNotificationChannel = Just "consumers_example_chan"
        , ccNotificationTimeout = 10 * 1000000 -- 10 sec
        , ccMaxRunningJobs = 1
        , ccProcessJob = processJob
        , ccOnException = handleException
        }

    -- Add a job to the consumer's queue.
    putJob :: AppM ()
    putJob = localDomain "put" $ do
      logInfo_ "putJob"
      runSQL_ $
        "INSERT INTO consumers_example_jobs "
          <> "(run_at, finished_at, reserved_by, attempts, message) "
          <> "VALUES (NOW(), NULL, NULL, 0, 'hello')"
      notify "consumers_example_chan" ""
      commit

    -- Invoked when a job is ready to be processed.
    processJob :: (Int64, T.Text) -> AppM Result
    processJob (_idx, msg) = do
      logInfo_ msg
      pure (Ok Remove)

    -- Invoked when 'processJob' throws an exception. Can handle
    -- failure in different ways, such as: remove the job from the
    -- queue, mark it as processed, or schedule it for rerun.
    handleException :: SomeException -> (Int64, T.Text) -> AppM Action
    handleException exc (idx, _msg) = do
      logAttention "Job failed" $
        object
          [ "job_id" .= showt idx
          , "exception" .= showt exc
          ]
      pure . RerunAfter $ imicroseconds 500000

-- | Table where jobs are stored. See
-- 'Database.PostgreSQL.Consumers.Config.ConsumerConfig'.
jobsTable :: Table
jobsTable =
  tblTable
    { tblName = "consumers_example_jobs"
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
            { colName = "message"
            , colType = TextT
            , colNullable = False
            }
        ]
    , tblPrimaryKey = pkOnColumn "id"
    , tblForeignKeys =
        [ (fkOnColumn "reserved_by" "consumers_example_consumers" "id")
            { fkOnDelete = ForeignKeySetNull
            }
        ]
    }

-- | Table where registered consumers are stored. See
-- 'Database.PostgreSQL.Consumers.Config.ConsumerConfig'.
consumersTable :: Table
consumersTable =
  tblTable
    { tblName = "consumers_example_consumers"
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
