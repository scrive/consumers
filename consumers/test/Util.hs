{-# LANGUAGE BangPatterns #-}

module Util where

import Control.Applicative ((<|>))
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.State.Strict
import Control.Monad.Time
import Control.Monad.Trans.Control
import Data.Text qualified as T
import Data.Time
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model
import Log
import System.Environment
import System.Exit

getConnectionString :: IO T.Text
getConnectionString = do
  connectionParamsString <- (<|>) <$> paramsFromGithub <*> paramsFromEnvironmentVariables
  allArgs <- getArgs
  case connectionParamsString of
    Just params -> pure (stringFromParams params)
    _ -> case allArgs of
      connString : _args -> pure (T.pack connString)
      [] -> printUsage *> exitFailure
  where
    printUsage = do
      prog <- getProgName
      putStrLn $ "Usage: " <> prog <> " <connection info string>"

    paramsFromGithub =
      lookupEnv "GITHUB_ACTIONS" >>= \case
        Just "true" -> pure $ Just ("postgres", "postgres", "postgres")
        _ -> pure $ Nothing
    paramsFromEnvironmentVariables = do
      variables <-
        sequence
          [ lookupEnv "PGHOST"
          , lookupEnv "PGUSER"
          , lookupEnv "PGDATABASE"
          ]
      case variables of
        [Just host, Just user, Just database] -> pure $ Just (host, user, database)
        _ -> pure $ Nothing
    stringFromParams (host, user, database) =
      (T.pack ("host=" <> host <> " user=" <> user <> " dbname=" <> database))

data TestEnvSt = TestEnvSt
  { teCurrentTime :: UTCTime
  , teMonotonicTime :: Double
  , teJobTableName :: RawSQL ()
  , teConsumerTableName :: RawSQL ()
  , teNotificationChannel :: Channel
  , teAdditionalCols :: [TableColumn]
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

data TestSetup = TestSetup
  { tsTestSuffix :: RawSQL ()
  , tsAdditionalCols :: [TableColumn]
  }

modifyTestTime :: MonadState TestEnvSt m => (UTCTime -> UTCTime) -> m ()
modifyTestTime modtime = modify (\te -> te {teCurrentTime = modtime . teCurrentTime $ te})

runTestEnv :: ConnectionSourceM (LogT IO) -> Logger -> TestSetup -> TestEnv a -> IO a
runTestEnv connSource logger TestSetup {..} test = do
  runLogT "consumers-test" logger defaultLogLevel $
    runDBT connSource defaultTransactionSettings $
      (\m' -> fst <$> runStateT m' testEnvironment) $
        unTestEnv $
          bracket createTables (const dropTables) (const test)
  where
    jobTableName = "jobs_" <> tsTestSuffix
    consumerTableName = "consumers_" <> tsTestSuffix
    notificationChannelName = "notification_" <> tsTestSuffix
    testEnvironment =
      TestEnvSt
        (UTCTime (ModifiedJulianDay 0) 0)
        0
        jobTableName
        consumerTableName
        (Channel notificationChannelName)
        tsAdditionalCols
    jobTable = mkJobsTable jobTableName consumerTableName tsAdditionalCols
    consumerTable = mkConsumersTable consumerTableName
    definitions = emptyDbDefinitions {dbTables = [consumerTable, jobTable]}
    -- NB: order of migrations is important.
    migrations =
      [ createTableMigration consumerTable
      , createTableMigration jobTable
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
        [ dropTableMigration jobTable
        , dropTableMigration consumerTable
        ]

mkJobsTable :: RawSQL () -> RawSQL () -> [TableColumn] -> Table
mkJobsTable tableName consumerTableName additionalCols =
  tblTable
    { tblName = tableName
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
        ]
          ++ additionalCols
    , tblPrimaryKey = pkOnColumn "id"
    , tblForeignKeys =
        [ (fkOnColumn "reserved_by" consumerTableName "id")
            { fkOnDelete = ForeignKeySetNull
            }
        ]
    }

mkConsumersTable :: RawSQL () -> Table
mkConsumersTable tableName =
  tblTable
    { tblName = tableName
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
