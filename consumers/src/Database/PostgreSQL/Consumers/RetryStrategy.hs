-- | Retry helpers for 'ccOnException', built on 'ccJobAttempts'.
--
-- The delay helpers map the attempt count to an 'Interval':
--
-- @
-- ccOnException = \\_ex job -> pure . RerunAfter $
--   exponentialDelay (iseconds 1) (iminutes 10) (ccJobAttempts config job)
-- @
--
-- 'retryForever' and 'giveUpAfter' turn such a helper into a 'RetryDecision',
-- and 'retryOr' turns that into an 'Action':
--
-- @
-- ccOnException = \\_ex job -> pure . retryOr Remove $
--   giveUpAfter 10 (exponentialDelay (iseconds 1) (iminutes 10)) (ccJobAttempts config job)
-- @
--
-- To act on the decision, e.g. to log when the job is given up, match on the
-- 'RetryDecision' instead of using 'retryOr'.
module Database.PostgreSQL.Consumers.RetryStrategy
  ( -- * Delays
    constantDelay
  , exponentialDelay
  , jitter

    -- * Decisions
  , RetryDecision (..)
  , retryForever
  , giveUpAfter
  , retryOr
  ) where

import Control.Monad.IO.Class
import Data.Int
import Data.Maybe
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.PQTypes.Interval
import System.Random

-- | The same delay before every retry.
constantDelay
  :: Interval
  -- ^ Delay before every retry.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Interval
constantDelay delay _ = delay

-- | @base * 2 ^ (n - 1)@ after attempt @n@, capped at @maxDelay@.
exponentialDelay
  :: Interval
  -- ^ Base delay, used for the first retry.
  -> Interval
  -- ^ Delay cap; the computed delay never exceeds this.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Interval
exponentialDelay base maxDelay attempts =
  microsToInterval . min (intervalToMicros maxDelay) $
    intervalToMicros base * 2 ^ min 62 (max 1 attempts - 1)

-- | Whether to rerun a failed job, and after which delay.
data RetryDecision
  = GiveUp
  | RetryAfter Interval
  deriving (Eq, Show)

-- | Retry the job after the given delay, however many times it fails.
retryForever
  :: (Int32 -> Interval)
  -- ^ Delay for a given attempt count, e.g. 'exponentialDelay' applied to
  -- its first two arguments.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> RetryDecision
retryForever delay = RetryAfter . delay

-- | Like 'retryForever', but give up once the job has failed @maxAttempts@
-- times.
giveUpAfter
  :: Int32
  -- ^ Maximum number of attempts before the job is removed.
  -> (Int32 -> Interval)
  -- ^ Delay for a given attempt count.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> RetryDecision
giveUpAfter maxAttempts delay attempts
  | attempts >= maxAttempts = GiveUp
  | otherwise = retryForever delay attempts

-- | Scale a delay by a random factor between @1 - p@ and @1 + p@, so that jobs
-- which fail together do not retry together. The result can exceed a delay
-- cap by a fraction @p@.
--
-- @p@ is a fraction between 0 and 1, and 0.5 (plus or minus 50%) by default.
-- That spreads retries over one full delay, like the "full jitter" of
-- <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>,
-- but it never gives a delay close to zero.
jitter
  :: MonadIO m
  => Maybe Double
  -- ^ Plus or minus fraction of the delay; 'Nothing' means 0.5.
  -> Interval
  -> m Interval
jitter mFraction delay = do
  factor <- liftIO $ randomRIO (1 - fraction, 1 + fraction)
  pure . microsToInterval . round $ factor * fromInteger (intervalToMicros delay)
  where
    fraction = max 0 . min 1 $ fromMaybe 0.5 mFraction

-- | Turn a 'RetryDecision' into an 'Action', using the given 'Action' on
-- 'GiveUp'.
retryOr :: Action -> RetryDecision -> Action
retryOr onGiveUp = \case
  GiveUp -> onGiveUp
  RetryAfter delay -> RerunAfter delay

----------------------------------------

-- Intervals are compared like PostgreSQL does it: a year is 12 months, a month
-- is 30 days and a day is 24 hours.
intervalToMicros :: Interval -> Integer
intervalToMicros Interval {..} =
  ((((days * 24 + toInteger intHours) * 60 + toInteger intMinutes) * 60 + toInteger intSeconds) * 1000000)
    + toInteger intMicroseconds
  where
    days = (toInteger intYears * 12 + toInteger intMonths) * 30 + toInteger intDays

-- Saturates at the largest interval that 'intSeconds' can hold. 'intMicroseconds'
-- alone would overflow after about 36 minutes.
microsToInterval :: Integer -> Interval
microsToInterval micros = mempty {intSeconds = fromInteger s, intMicroseconds = fromInteger us}
  where
    (s, us) = max 0 (min maxMicros micros) `divMod` 1000000
    maxMicros = toInteger (maxBound :: Int32) * 1000000 + 999999
