{-# LANGUAGE ExistentialQuantification #-}
module Database.PostgreSQL.Consumers.Config (
    Action(..)
  , Mode(..)
  , Result(..)
  , ConsumerConfig(..)
  ) where

import Control.Exception (SomeException)
import Data.Time
import Prelude

import Database.PostgreSQL.PQTypes.FromRow
import Database.PostgreSQL.PQTypes.Interval
import Database.PostgreSQL.PQTypes.Notification
import Database.PostgreSQL.PQTypes.SQL
import Database.PostgreSQL.PQTypes.SQL.Raw

-- | Action to take after a job was processed.
data Action
  = MarkProcessed
  | RerunAfter Interval
  | RerunAt UTCTime
  | Remove
  deriving (Eq, Ord, Show)

-- | Result of processing a job.
data Result = Ok Action | Failed Action
  deriving (Eq, Ord, Show)

-- | The mode the consumer will run in.
data Mode = Standard | Duplicating SQL
  deriving (Show)

-- | Config of a consumer.
data ConsumerConfig m idx job = forall row. FromRow row => ConsumerConfig {
-- | Name of the database table where jobs are stored. The table needs to have
-- the following columns in order to be suitable for acting as a job queue:
--
-- * __id__ - represents ID of the job. Needs to be a primary key of a type
-- convertible to text, not nullable.
--
-- * __run_at__ - represents the time at which the job will be
-- processed. Needs to be nullable, of a type comparable with @now()@
-- (TIMESTAMPTZ is recommended).
--
-- Note: a job with run_at set to NULL is never picked for processing. Useful
-- for storing already processed/expired jobs for debugging purposes.
--
-- It's highly recommended to have an index on this column.
--
-- * __finished_at__ - represents the time at which job processing was finished.
-- Needs to be nullable, of a type you can assign @now()@ to (TIMESTAMPTZ is
-- recommended). NULL means that the job was either never processed or that
-- it was started and failed at least once.
--
-- * __reserved_by__ - represents ID of the consumer that currently processes the
-- job. Needs to be nullable, of the type corresponding to id in the table
-- 'ccConsumersTable'. It's recommended (though not neccessary) to make it
-- a foreign key referencing id in 'ccConsumersTable' with ON DELETE SET NULL.
--
-- * __attempts__ - represents number of job processing attempts made so far. Needs
-- to be not nullable, of type INTEGER. Initial value of a fresh job should be
-- 0, therefore it makes sense to make the column default to 0.
--
  ccJobsTable             :: !(RawSQL ())
-- | Name of a database table where registered consumers are stored. The table
-- itself needs to have the following columns:
--
-- * __id__ - represents ID of a consumer. Needs to be a primary key of the type
-- SERIAL or BIGSERIAL (recommended).
--
-- * __name__ - represents jobs table of the consumer. Needs to be not nullable,
-- of type TEXT. Allows for tracking consumers of multiple queues with one
-- table. Set to 'ccJobsTable'.
--
-- * __last_activity__ - represents the last registered activity of the consumer.
-- It's updated periodically by all currently running consumers every 30
-- seconds to prove that they are indeed running. They also check for the
-- registered consumers that didn't update their status for a minute. If any
-- such consumers are found, they are presumed to be not working and all the
-- jobs reserved by them are released. This prevents the situation where
-- a consumer with reserved jobs silently fails (e.g. because of a hard crash)
-- and these jobs stay locked forever, yet are never processed.
--
, ccConsumersTable        :: !(RawSQL ())
-- | Fields needed to be selected from the jobs table in order to
-- assemble a job.
, ccJobSelectors          :: ![SQL]
-- | Function that transforms the list of fields into a job.
, ccJobFetcher            :: !(row -> job)
-- | Selector for taking out job ID from the job object.
, ccJobIndex              :: !(job -> idx)
-- | Notification channel used for listening for incoming jobs.
-- Whenever the consumer receives a notification, it checks the database
-- for any pending jobs (@'run_at <= NOW()'@) and runs them all.
-- If set to 'Nothing', no listening is performed and 'ccNotificationTimeout'
-- should be set to a positive number, otherwise no jobs would be ever run.
-- 'ccNotificationChannel' and 'ccNotificationTimeout' can be combined.
-- The consumer will check for pending jobs either when notification is received or
-- no notification is received for 'ccNotificationTimeout' microseconds since
-- the last check.
, ccNotificationChannel   :: !(Maybe Channel)
-- | Timeout of checking for any pending jobs (@'run_at <= NOW()'@), in
-- microseconds. The consumer checks the database for any pending jobs after
-- 'ccNotificationTimeout' microseconds since the last check was performed,
-- runs them until all pending jobs are processed and after that, the cycle
-- repeats. Note that even if 'ccNotificationChannel' is 'Just', you need to set
-- 'ccNotificationTimeout' to a reasonable number if the jobs you process may fail
-- and are retried later, as there is no way to signal with a notification that
-- a job will need to be performed e.g. in 5 minutes. However, if
-- 'ccNotificationChannel' is 'Just' and jobs are never retried, you can set it
-- to -1, then listening will never timeout. Otherwise it needs to be a positive
-- number.
, ccNotificationTimeout   :: !Int
-- | Maximum amount of jobs that can be processed in parallel.
, ccMaxRunningJobs        :: !Int
-- | Function that processes a job. It's recommended to process each
-- job in a separate DB transaction, otherwise you'll have to remember
-- to commit your changes to the database manually.
, ccProcessJob            :: !(job -> m Result)
-- | Action taken if a job processing function throws an exception.
-- Note that if this action throws an exception, the consumer goes
-- down, so it's best to ensure that it doesn't throw.
, ccOnException           :: !(SomeException -> job -> m Action)
-- | The mode the consumer will use to reserve jobs.
-- In @'Duplicating'@ mode the SQL expression indicates which field
-- in the jobs table (specified by @'ccJobsTable'@) to select for duplication.
, ccMode                  :: Mode
}
