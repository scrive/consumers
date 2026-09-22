module Database.PostgreSQL.Consumers.Config
  ( Action (..)
  , Result (..)
  , Job (..)
  , ConsumerConfig (..)
  , hoistConsumer
  , exponentialBackoff
  ) where

import Control.Exception (SomeException)
import Data.Aeson.Types qualified as A
import Data.Time
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

-- | A job fetched from the queue, together with its queue bookkeeping
-- fields (as opposed to just the caller-supplied 'jobInfo' payload).
data Job idx info = Job
  { jobIndex :: !idx
  , jobRunAt :: !(Maybe UTCTime)
  , jobFinishedAt :: !(Maybe UTCTime)
  , jobAttempts :: !Int
  -- ^ Number of consecutive failed processing attempts, including the
  -- current one. Reset to 1 once a job has succeeded, so this is a streak
  -- of failures rather than a lifetime total.
  , jobInfo :: !info
  }

-- | Config of a consumer.
data ConsumerConfig m idx info = forall row. FromRow row => ConsumerConfig
  { ccJobsTable :: !(RawSQL ())
  -- ^ Name of the database table where jobs are stored. The table needs to have
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
  -- * __finished_at__ - represents the time at which job processing was
  -- finished. Needs to be nullable, of a type you can assign @now()@ to
  -- (TIMESTAMPTZ is recommended). NULL means that the job was either never
  -- processed or that it was started and failed at least once.
  --
  -- * __reserved_by__ - represents ID of the consumer that currently processes
  -- the job. Needs to be nullable, of the type corresponding to id in the table
  -- 'ccConsumersTable'. It's recommended (though not neccessary) to make it a
  -- foreign key referencing id in 'ccConsumersTable' with ON DELETE SET NULL.
  --
  -- * __attempts__ - represents number of job processing attempts made so
  -- far. Needs to be not nullable, of type INTEGER. Initial value of a fresh
  -- job should be 0, therefore it makes sense to make the column default to 0.
  , ccConsumersTable :: !(RawSQL ())
  -- ^ Name of a database table where registered consumers are stored. The table
  -- itself needs to have the following columns:
  --
  -- * __id__ - represents ID of a consumer. Needs to be a primary key of the
  -- type SERIAL or BIGSERIAL (recommended).
  --
  -- * __name__ - represents jobs table of the consumer. Needs to be not
  -- nullable, of type TEXT. Allows for tracking consumers of multiple queues
  -- with one table. Set to 'ccJobsTable'.
  --
  -- * __last_activity__ - represents the last registered activity of the
  -- consumer. It's updated periodically by all currently running consumers
  -- every 30 seconds to prove that they are indeed running. They also check for
  -- the registered consumers that didn't update their status for a minute. If
  -- any such consumers are found, they are presumed to be not working and all
  -- the jobs reserved by them are released. This prevents the situation where a
  -- consumer with reserved jobs silently fails (e.g. because of a hard crash)
  -- and these jobs stay locked forever, yet are never processed.
  , ccJobSelectors :: ![SQL]
  -- ^ Fields needed to be selected from the jobs table in order to assemble a
  -- job. Needs to match what 'ccJobFetcher' expects, and typically includes
  -- @id@, @run_at@, @finished_at@ and @attempts@ alongside whatever columns
  -- make up the job's 'jobInfo' payload.
  , ccJobFetcher :: !(row -> Job idx info)
  -- ^ Function that transforms the list of fields into a job.
  , ccJobIndex :: !(Job idx info -> idx)
  -- ^ Selector for taking out job ID from the job object.
  , ccNotificationChannel :: !(Maybe Channel)
  -- ^ Notification channel used for listening for incoming jobs.  Whenever the
  -- consumer receives a notification, it checks the database for any pending
  -- jobs (@'run_at <= NOW()'@) and runs them all. If set to 'Nothing', no
  -- listening is performed and 'ccNotificationTimeout' should be set to a
  -- positive number, otherwise no jobs would be ever run.
  -- 'ccNotificationChannel' and 'ccNotificationTimeout' can be combined. The
  -- consumer will check for pending jobs either when notification is received
  -- or no notification is received for 'ccNotificationTimeout' microseconds
  -- since the last check.
  , ccNotificationTimeout :: !Int
  -- ^ Timeout of checking for any pending jobs (@'run_at <= NOW()'@), in
  -- microseconds. The consumer checks the database for any pending jobs after
  -- 'ccNotificationTimeout' microseconds since the last check was performed,
  -- runs them until all pending jobs are processed and after that, the cycle
  -- repeats. Note that even if 'ccNotificationChannel' is 'Just', you need to
  -- set 'ccNotificationTimeout' to a reasonable number if the jobs you process
  -- may fail and are retried later, as there is no way to signal with a
  -- notification that a job will need to be performed e.g. in 5
  -- minutes. However, if 'ccNotificationChannel' is 'Just' and jobs are never
  -- retried, you can set it to -1, then listening will never timeout. Otherwise
  -- it needs to be a positive number.
  , ccMaxRunningJobs :: !Int
  -- ^ Maximum amount of jobs that can be processed in parallel.
  , ccProcessJob :: !(Job idx info -> m Result)
  -- ^ Function that processes a job. It's recommended to process each job in a
  -- separate DB transaction, otherwise you'll have to remember to commit your
  -- changes to the database manually.
  , ccOnException :: !(SomeException -> Job idx info -> m Action)
  -- ^ Action taken if a job processing function throws an exception. For
  -- robustness it's best to ensure that it doesn't throw. If it does, the
  -- exception will be logged and the job in question postponed by a day.
  , ccJobLogData :: !(Job idx info -> [A.Pair])
  -- ^ Data to attach to each log message while processing a job.
  }

-- | Change the monad the consumer lives in.
hoistConsumer
  :: (forall r. m r -> n r)
  -> ConsumerConfig m idx info
  -> ConsumerConfig n idx info
hoistConsumer f ConsumerConfig {..} =
  ConsumerConfig
    { ccProcessJob = f . ccProcessJob
    , ccOnException = \ex -> f . ccOnException ex
    , ..
    }

-- | A ready-made 'ccOnException' handler: reruns a job with exponentially
-- increasing delay, then gives up (removes the job) once 'jobAttempts'
-- exceeds @maxAttempts@.
--
-- /Note:/ this mirrors the growth curve from the original proof of concept
-- (delay ^ attempts), so it only grows the delay if @startInterval@ is
-- greater than one second; below that it shrinks towards zero instead.
-- Pick @startInterval@ accordingly, or write a custom handler if you need a
-- more conventional @startInterval * base ^ attempts@ curve.
exponentialBackoff
  :: Monad m
  => Int
  -- ^ Maximum number of attempts before the job is removed.
  -> Interval
  -- ^ Delay before the first retry.
  -> SomeException
  -> Job idx info
  -> m Action
exponentialBackoff maxAttempts startInterval _ Job {..} =
  pure $ case (jobAttempts > maxAttempts, jobAttempts > 2) of
    (True, _) -> Remove
    (False, False) -> RerunAfter startInterval
    (False, True) ->
      RerunAfter . diffTimeToInterval $
        intervalToDiffTime startInterval ^ jobAttempts

intervalToDiffTime :: Interval -> DiffTime
intervalToDiffTime Interval {..} = secondsToDiffTime seconds
  where
    seconds =
      (toInteger intYears * 365 * 24 * 60 * 60)
        + (toInteger intMonths * 30 * 24 * 60 * 60)
        + (toInteger intDays * 24 * 60 * 60)
        + (toInteger intHours * 60 * 60)
        + (toInteger intMinutes * 60)
        + toInteger intSeconds
        + (toInteger intMicroseconds `div` 1000000)

diffTimeToInterval :: DiffTime -> Interval
diffTimeToInterval = imicroseconds . fromInteger . (`div` 1000000) . diffTimeToPicoseconds
