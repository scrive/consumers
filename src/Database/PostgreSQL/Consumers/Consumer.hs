module Database.PostgreSQL.Consumers.Consumer
  ( ConsumerID
  , registerConsumer
  , unregisterConsumer
  ) where

import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Time
import Data.Int
import Data.Monoid.Utils
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.Consumers.Utils
import Database.PostgreSQL.PQTypes

-- | ID of a consumer.
newtype ConsumerID = ConsumerID Int64
  deriving (Eq, Ord)

instance PQFormat ConsumerID where
  pqFormat = pqFormat @Int64
instance FromSQL ConsumerID where
  type PQBase ConsumerID = PQBase Int64
  fromSQL mbase = ConsumerID <$> fromSQL mbase
instance ToSQL ConsumerID where
  type PQDest ConsumerID = PQDest Int64
  toSQL (ConsumerID n) = toSQL n

instance Show ConsumerID where
  showsPrec p (ConsumerID n) = showsPrec p n

-- | Register consumer in the consumers table, so that it can reserve jobs using
-- acquired ID.
registerConsumer
  :: (MonadBase IO m, MonadMask m, MonadTime m)
  => ConsumerConfig n idx job
  -> ConnectionSourceM m
  -> m ConsumerID
registerConsumer ConsumerConfig {..} cs = runDBT cs ts $ do
  now <- currentTime
  runPreparedSQL_ (preparedSqlName "registerConsumer" ccConsumersTable) $
    smconcat
      [ "INSERT INTO" <+> raw ccConsumersTable
      , "(name, last_activity) VALUES (" <?> unRawSQL ccJobsTable <> ", " <?> now <> ")"
      , "RETURNING id"
      ]
  fetchOne runIdentity
  where
    ts =
      defaultTransactionSettings
        { tsAutoTransaction = False
        }

-- | Unregister consumer with a given ID.
unregisterConsumer
  :: (MonadBase IO m, MonadMask m)
  => ConsumerConfig n idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> m ()
unregisterConsumer ConsumerConfig {..} cs wid = runDBT cs ts $ do
  -- Free tasks manually in case there is no foreign key constraint on
  -- reserved_by,
  runPreparedSQL_ (preparedSqlName "deregisterJobs" ccJobsTable) $
    smconcat
      [ "UPDATE" <+> raw ccJobsTable
      , "   SET reserved_by = NULL"
      , " WHERE reserved_by =" <?> wid
      ]
  runPreparedSQL_ (preparedSqlName "removeConsumers" ccConsumersTable) $
    smconcat
      [ "DELETE FROM " <+> raw ccConsumersTable
      , "WHERE id =" <?> wid
      , "  AND name =" <?> unRawSQL ccJobsTable
      ]
  where
    ts =
      defaultTransactionSettings
        { tsRestartPredicate = Just . RestartPredicate $
            \e _ -> qeErrorCode e == DeadlockDetected
        }
