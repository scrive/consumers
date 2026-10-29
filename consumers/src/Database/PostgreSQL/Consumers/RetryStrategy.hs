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
--
-- 'jitter' spreads out the retries of jobs that fail together:
--
-- @
-- ccOnException = \\_ex job -> retryOr Remove \<$\>
--   jitter (1, 1.5) (giveUpAfter 10 (exponentialDelay (iseconds 1) (iminutes 10)) (ccJobAttempts config job))
-- @
module Database.PostgreSQL.Consumers.RetryStrategy
  ( -- * Delays
    constantDelay
  , exponentialDelay

    -- * Decisions
  , RetryDecision (..)
  , retryForever
  , giveUpAfter
  , jitter
  , retryOr
  ) where

import Crypto.RNG.Class
import Data.Int
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.PQTypes.Interval

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
    intervalToMicros base * 2 ^ (max 1 attempts - 1)

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

-- | Scale the delay of a 'RetryAfter' by a random factor in the given range,
-- so that jobs which fail together do not retry together. 'GiveUp' stays as
-- it is.
--
-- If the delay is a minimum, e.g. a polling interval that a remote service
-- enforces, use factors of 1 or more, such as @(1, 1.5)@. To never exceed a
-- delay cap, use factors of 1 or less, such as @(0.5, 1)@. Negative factors
-- count as 0.
jitter
  :: CryptoRNG m
  => (Double, Double)
  -- ^ Lowest and highest factor.
  -> RetryDecision
  -> m RetryDecision
jitter _ GiveUp = pure GiveUp
jitter (lo, hi) (RetryAfter delay) = do
  factor <- randomR (max 0 lo, max 0 hi)
  pure . RetryAfter . microsToInterval . round $
    factor * fromInteger (intervalToMicros delay)

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
