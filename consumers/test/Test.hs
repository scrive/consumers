module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.RWS
import Control.Monad.Time
import Data.IORef
import Data.Int
import Data.Text qualified as T
import Data.Time
import Database.PostgreSQL.Consumers
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Model
import Log
import Log.Backend.StandardOutput
import Test.HUnit qualified as T
import Test.Tasty
import Test.Tasty.HUnit
import Util

main :: IO ()
main = do
  connectionParamsString <- getConnectionString
  let connectionSettings = defaultConnectionSettings {csConnInfo = connectionParamsString}
  defaultMain (allTests connectionSettings)

allTests :: ConnectionSettings -> TestTree
allTests connectionSource =
  testGroup
    "consumers"
    [ testCase "can grow the number of jobs ran concurrently" (testJobScheduleGrowth connectionSource)
    ]

--------------------

testJobScheduleGrowth :: ConnectionSettings -> IO ()
testJobScheduleGrowth connectionSettings = do
  let ConnectionSource connSource = simpleSource connectionSettings
  withStdOutLogger $ \logger -> do
    let additionalColumns =
          [ tblColumn
              { colName = "countdown"
              , colType = IntegerT
              , colNullable = False
              }
          ]
    runTestEnv connSource logger (TestSetup "test_job_schedule_growth" additionalColumns) $ do
      consumerConfig <- getConsumerConfig
      TestEnvSt {..} <- get
      idleSignal <- liftIO newEmptyTMVarIO
      putJob 10 >> commit

      rowCountGrowth :: [Int64] <- forM [1 .. 10 :: Int] $ \_ -> do
        -- Move time forward 2hours, because jobs are scheduled 1 hour into future
        modifyTestTime $ addUTCTime (2 * 60 * 60)
        finalize
          ( localDomain "process" $
              runConsumerWithIdleSignal consumerConfig connSource idleSignal
          )
          $ waitUntilTrue idleSignal
        currentTime >>= (logInfo_ . T.pack . ("current time: " ++) . show)

        -- Each job creates 2 new jobs, so there should be 1024 jobs in table.
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      -- Move time 2 hours forward
      modifyTestTime $ addUTCTime (2 * 60 * 60)
      finalize
        ( localDomain "process" $
            runConsumerWithIdleSignal consumerConfig connSource idleSignal
        )
        $ waitUntilTrue idleSignal

      -- Jobs are designed to double only 10 times, so there should be no jobs left now.
      runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
      rowcount1 :: Int64 <- fetchOne runIdentity
      liftIO $ T.assertEqual "Number of jobs in table after 10 steps grows exponentially" [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024] rowCountGrowth
      liftIO $ T.assertEqual "Number of jobs in table after 11 steps is 0" 0 rowcount1
  where
    getConsumerConfig :: TestEnv (ConsumerConfig TestEnv Int64 (Int64, Int32))
    getConsumerConfig = do
      TestEnvSt {..} <- get
      pure $
        ConsumerConfig
          { ccJobsTable = teJobTableName
          , ccConsumersTable = teConsumerTableName
          , ccJobSelectors = ["id", "countdown"]
          , ccJobFetcher = id
          , ccJobIndex = \(i :: Int64, _ :: Int32) -> i
          , ccNotificationChannel = Just teNotificationChannel
          , -- select some small timeout
            ccNotificationTimeout = 100 * 1000 -- 100 msec
          , ccMaxRunningJobs = 20
          , ccProcessJob = processJob
          , ccOnException = handleException
          , ccJobLogData = \(i, _) -> ["job_id" .= i]
          , ccMutexColumn = Nothing
          }

    putJob :: Int32 -> TestEnv ()
    putJob countdown = localDomain "put" $ do
      TestEnvSt {..} <- get
      now <- currentTime
      runSQL_ $
        "INSERT INTO "
          <> raw teJobTableName
          <> "(run_at, finished_at, reserved_by, attempts, countdown) "
          <> "VALUES (" <?> now
          <> " + interval '1 hour', NULL, NULL, 0, " <?> countdown
          <> ")"
      notify teNotificationChannel ""

    processJob :: (Int64, Int32) -> TestEnv Result
    processJob (_idx, countdown) = do
      when (countdown > 0) $ do
        putJob (countdown - 1)
        putJob (countdown - 1)
        commit
      pure (Ok Remove)

waitUntilTrue :: MonadIO m => TMVar Bool -> m ()
waitUntilTrue tmvar = liftIO . atomically $ do
  takeTMVar tmvar >>= \case
    True -> pure ()
    False -> retry

handleException :: SomeException -> k -> TestEnv Action
handleException _ _ = pure . RerunAfter $ imicroseconds 500000
