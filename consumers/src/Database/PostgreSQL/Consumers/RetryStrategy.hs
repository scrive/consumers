-- | Retry helpers for 'ccOnException', built on 'ccJobAttempts'.
--
-- The delay helpers map the attempt count to an 'Interval':
--
-- @
-- ccOnException = \\_ex job -> pure . RerunAfter $
--   exponentialDelay (iseconds 1) (iminutes 10) (ccJobAttempts config job)
-- @
--
-- 'retryForever' and 'giveUpAfter' turn such a helper into an 'Action':
--
-- @
-- ccOnException = \\_ex job -> pure $
--   giveUpAfter 10 (exponentialDelay (iseconds 1) (iminutes 10)) (ccJobAttempts config job)
-- @
module Database.PostgreSQL.Consumers.RetryStrategy
  ( -- * Delays
    constantDelay
  , linearDelay
  , exponentialDelay
  , jitter

    -- * Actions
  , retryForever
  , giveUpAfter

    -- * Utils
  , RetryDecision (..)
  , withRetryAction
  ) where

import Control.Monad.IO.Class
import Data.Int
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

-- | @n * unit@ after attempt @n@.
linearDelay
  :: Interval
  -- ^ Delay unit; the delay before retry number @n@ is @n * unit@.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Interval
linearDelay unit attempts =
  microsToInterval $ toInteger (max 1 attempts) * intervalToMicros unit

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

-- | Rerun the job after the given delay, however many times it fails.
retryForever
  :: (Int32 -> Interval)
  -- ^ Delay for a given attempt count, e.g. 'exponentialDelay' applied to
  -- its first two arguments.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Action
retryForever delay = RerunAfter . delay

-- | Like 'retryForever', but remove the job once it has failed @maxAttempts@
-- times.
giveUpAfter
  :: Int32
  -- ^ Maximum number of attempts before the job is removed.
  -> (Int32 -> Interval)
  -- ^ Delay for a given attempt count.
  -> Int32
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Action
giveUpAfter maxAttempts delay attempts
  | attempts >= maxAttempts = Remove
  | otherwise = retryForever delay attempts

-- | Scale a delay by a random factor between 0.5 and 1.5, so that jobs which
-- fail together do not retry together.
jitter :: MonadIO m => Interval -> m Interval
jitter delay = do
  factor <- liftIO $ randomRIO (0.5, 1.5 :: Double)
  pure . microsToInterval . round $ factor * fromInteger (intervalToMicros delay)

data RetryDecision
  = GiveUp
  | Retry

-- | Run an action, e.g. logging, based on whether the strategy gave up.
withRetryAction :: Monad m => (RetryDecision -> m ()) -> Action -> m Action
withRetryAction onDecision action = do
  onDecision $ case action of
    RerunAfter _ -> Retry
    RerunAt _ -> Retry
    _ -> GiveUp
  pure action

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
