-- | Retry strategies for 'ccOnException', built on 'ccJobAttempts'.
--
-- A strategy combines a delay function with a decision on whether to retry
-- forever ('retryForever') or give up ('giveUpAfter'):
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

    -- * Strategies
  , retryForever
  , giveUpAfter
  , withJitter

    -- * Utils
  , RetryDecision (..)
  , withRetryAction
  ) where

import Control.Monad.IO.Class
import Data.Int
import Data.Time
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
  scaleInterval attempts unit

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
  min' maxDelay $ scaleInterval (2 ^ (attempts - 1)) base
  where
    min' a b = if intervalToDiffTime a < intervalToDiffTime b then a else b

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

-- | Scale the delay of 'RerunAfter' by a random factor between 0.5 and 1.5,
-- so that jobs which fail together do not retry together.
withJitter :: MonadIO m => Action -> m Action
withJitter = \case
  RerunAfter delay -> do
    factor <- liftIO $ randomRIO (0.5, 1.5 :: Double)
    pure . RerunAfter $ scaleIntervalD factor delay
  action -> pure action

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

-- | Scale an 'Interval' by an integer factor.
scaleInterval :: Int32 -> Interval -> Interval
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
