module Database.PostgreSQL.Consumers.Utils
  ( finalize
  , ThrownFrom (..)
  , stopExecution
  , forkP
  , gforkP
  , preparedSqlName
  ) where

import Control.Concurrent.Lifted
import Control.Concurrent.Thread.Group.Lifted qualified as TG
import Control.Concurrent.Thread.Lifted qualified as T
import Control.Exception.Lifted qualified as E
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Trans.Control
import Data.Maybe
import Data.Text qualified as T
import Database.PostgreSQL.PQTypes.Class
import Database.PostgreSQL.PQTypes.SQL.Raw

-- | Run an action 'm' that returns a finalizer and perform the returned
-- finalizer after the action 'action' completes.
finalize :: (MonadMask m, MonadBase IO m) => m (m ()) -> m a -> m a
finalize m action = do
  finalizer <- newEmptyMVar
  flip finally (tryTakeMVar finalizer >>= fromMaybe (pure ())) $ do
    putMVar finalizer =<< m
    action

----------------------------------------

-- | Exception thrown to a thread to stop its execution.
--
-- All exceptions other than 'StopExecution' thrown to threads spawned by
-- 'forkP' and 'gforkP' are propagated back to the parent thread.
data StopExecution = StopExecution
  deriving (Show)

instance Exception StopExecution where
  toException = E.asyncExceptionToException
  fromException = E.asyncExceptionFromException

-- | Exception thrown from a child thread.
data ThrownFrom = ThrownFrom String SomeException
  deriving (Show)

instance Exception ThrownFrom

-- | Stop execution of a thread.
stopExecution :: MonadBase IO m => ThreadId -> m ()
stopExecution = flip throwTo StopExecution

----------------------------------------

-- | Modified version of 'fork' that propagates thrown exceptions to the parent
-- thread.
forkP :: MonadBaseControl IO m => String -> m () -> m ThreadId
forkP = forkImpl fork

-- | Modified version of 'TG.fork' that propagates thrown exceptions to the
-- parent thread.
gforkP
  :: MonadBaseControl IO m
  => TG.ThreadGroup
  -> String
  -> m ()
  -> m (ThreadId, m (T.Result ()))
gforkP = forkImpl . TG.fork

----------------------------------------

forkImpl
  :: MonadBaseControl IO m
  => (m () -> m a)
  -> String
  -> m ()
  -> m a
forkImpl ffork tname m = E.mask $ \release -> do
  parent <- myThreadId
  ffork $
    release m
      `E.catches` [ E.Handler $ \StopExecution -> pure ()
                  , E.Handler $ throwTo parent . ThrownFrom tname
                  ]

preparedSqlName :: T.Text -> RawSQL () -> QueryName
preparedSqlName baseName tableName = QueryName . T.take 63 $ baseName <> "$" <> unRawSQL tableName
