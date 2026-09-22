-- |
-- Ready-made retry strategies for 'ccOnException', built around
-- 'ccJobAttempts' (the number of consecutive failed processing attempts for
-- a job, including the current one).
--
-- Each strategy takes the maximum number of attempts to allow, some
-- delay-shaping parameters, and the job's current attempt count, and
-- returns the 'Action' to apply: 'Remove' once @maxAttempts@ has been
-- exceeded, otherwise 'RerunAfter' some delay. Wire one into
-- 'ccOnException' using 'ccJobAttempts' to get the attempt count out of the
-- job:
--
-- @
-- ccOnException = \\_ex job -> pure $
--   exponentialBackoff 10 (iseconds 1) (iminutes 10) (ccJobAttempts config job)
-- @
module Database.PostgreSQL.Consumers.RetryStrategy
  ( constantBackoff
  , linearBackoff
  , exponentialBackoff
  , exponentialBackoffWithJitter
  ) where

import Control.Monad.IO.Class
import Data.Time
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.PQTypes.Interval
import System.Random (randomRIO)

-- | Retry after the same fixed delay every time, until @maxAttempts@ is
-- exceeded.
constantBackoff
  :: Int
  -- ^ Maximum number of attempts before the job is removed.
  -> Interval
  -- ^ Delay before every retry.
  -> Int
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Action
constantBackoff maxAttempts delay attempts
  | attempts >= maxAttempts = Remove
  | otherwise = RerunAfter delay

-- | Retry with a delay that grows linearly with the attempt count
-- (@delayUnit * attempts@), until @maxAttempts@ is exceeded.
linearBackoff
  :: Int
  -- ^ Maximum number of attempts before the job is removed.
  -> Interval
  -- ^ Delay unit; the delay before retry number @n@ is @n * delayUnit@.
  -> Int
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Action
linearBackoff maxAttempts delayUnit attempts
  | attempts >= maxAttempts = Remove
  | otherwise = RerunAfter $ scaleInterval attempts delayUnit

-- | Retry with a delay that doubles on every attempt
-- (@baseDelay * 2 ^ (attempts - 1)@), capped at @maxDelay@ so it doesn't
-- grow without bound, until @maxAttempts@ is exceeded.
exponentialBackoff
  :: Int
  -- ^ Maximum number of attempts before the job is removed.
  -> Interval
  -- ^ Base delay, used for the first retry.
  -> Interval
  -- ^ Delay cap; the computed delay never exceeds this.
  -> Int
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> Action
exponentialBackoff maxAttempts baseDelay maxDelay attempts
  | attempts >= maxAttempts = Remove
  | otherwise = RerunAfter $ nextDelay baseDelay maxDelay attempts

-- | Like 'exponentialBackoff', but adds up to +/-50% random jitter to the
-- computed delay.
--
-- Useful when several consumer instances can end up retrying the same kind
-- of job at once, e.g. after a shared dependency (a downstream API, a
-- database) comes back up from an outage: without jitter, every instance
-- backs off on the same schedule and they all retry in lockstep, hitting
-- the recovering dependency again simultaneously.
exponentialBackoffWithJitter
  :: MonadIO m
  => Int
  -- ^ Maximum number of attempts before the job is removed.
  -> Interval
  -- ^ Base delay, used for the first retry (before jitter).
  -> Interval
  -- ^ Delay cap, applied before jitter is added.
  -> Int
  -- ^ The job's current attempt count (see 'ccJobAttempts').
  -> m Action
exponentialBackoffWithJitter maxAttempts baseDelay maxDelay attempts
  | attempts >= maxAttempts = pure Remove
  | otherwise = do
      jitter <- liftIO $ randomRIO (0.5, 1.5 :: Double)
      pure . RerunAfter . scaleIntervalD jitter $ nextDelay baseDelay maxDelay attempts

----------------------------------------

nextDelay :: Interval -> Interval -> Int -> Interval
nextDelay baseDelay maxDelay attempts = min' maxDelay $ scaleInterval (2 ^ (attempts - 1)) baseDelay
  where
    min' a b = if intervalToDiffTime a < intervalToDiffTime b then a else b

-- | Scale an 'Interval' by an integer factor.
scaleInterval :: Int -> Interval -> Interval
scaleInterval factor = diffTimeToInterval . (fromIntegral factor *) . intervalToDiffTime

-- | Scale an 'Interval' by a fractional factor.
scaleIntervalD :: Double -> Interval -> Interval
scaleIntervalD factor = diffTimeToInterval . (realToFrac factor *) . intervalToDiffTime

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
