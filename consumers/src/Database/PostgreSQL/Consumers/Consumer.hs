-- | Consumer-registry bookkeeping.
--
-- Every running consumer owns a row in the consumers table identified by a
-- 'ConsumerID'. That row serves two purposes: it is the value written into a
-- job's @reserved_by@ column when the consumer claims the job, and it carries
-- a @last_activity@ timestamp that the monitor thread updates as a heartbeat.
-- If a consumer's heartbeat goes stale (more than 60 seconds old) another
-- consumer's monitor will reclaim its reserved jobs and delete the row.
--
-- 'runConsumer' takes care of calling 'registerConsumer' at startup and
-- 'unregisterConsumer' at shutdown; the functions are exposed for callers
-- that want to drive the lifecycle manually.
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
registerConsumer ConsumerConfig {..} cs = runDBT cs defaultTransactionSettings $ do
  now <- currentTime
  runPreparedSQL_ (preparedSqlName "registerConsumer" ccConsumersTable) $
    smconcat
      [ "INSERT INTO" <+> raw ccConsumersTable
      , "(name, last_activity) VALUES (" <?> unRawSQL ccJobsTable <> ", " <?> now <> ")"
      , "RETURNING id"
      ]
  fetchOne runIdentity

-- | Unregister a consumer. Releases any jobs still reserved by it (so they
-- become eligible for processing again) and removes the consumer row from
-- the registry.
unregisterConsumer
  :: (MonadBase IO m, MonadMask m)
  => ConsumerConfig n idx job
  -> ConnectionSourceM m
  -> ConsumerID
  -> m ()
unregisterConsumer ConsumerConfig {..} cs wid = runDBT cs ts $ do
  -- Free tasks manually in case there is no foreign key constraint on
  -- reserved_by.
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
