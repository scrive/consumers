module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
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
    , testGroup
        "mutexes"
        [ testCase "force sequential execution" (testMutexesSequential connectionSource)
        , testCase "does not block non-mutexed jobs" (testMutexesConcurrentNonMutexed connectionSource)
        ]
    , testGroup
        "expirations"
        [ testCase "delete expired jobs" (testExpired connectionSource)
        ]
    ]

--------------------

-- | It is possible to require a mutex on executing any job. When multiple of a
-- job all require the same mutex, only 1 will actually be scheduled at any given
-- point in time.
--
-- We test this by inserting a large number of jobs, each individually waiting a
-- small amount of time. If this is consumed sequentially, there should be some
-- number of jobs that haven't finished yet.
testMutexesSequential :: ConnectionSettings -> IO ()
testMutexesSequential connectionSettings = do
  let ConnectionSource connSource = simpleSource connectionSettings
  withStdOutLogger $ \logger -> do
    let additionalColumns =
          [ tblColumn
              { colName = "mutex"
              , colType = TextT
              , colNullable = True
              }
          ]
    runTestEnv connSource logger (TestSetup "test_mutexes_sequential" additionalColumns) $ do
      TestEnvSt {..} <- get
      concurrentAccess <- liftIO $ newIORef (0 :: Int, 0 :: Int)
      let processJob _idx = liftIO $ do
            atomicModifyIORef' concurrentAccess $ \(ca, maxCA) -> ((ca + 1, max (ca + 1) maxCA), ())
            threadDelay 10
            atomicModifyIORef' concurrentAccess $ \(ca, maxCA) -> ((ca - 1, maxCA), ())
            pure (Ok Remove)

      defaultConfig <- defaultConsumerConfig @_ @_ @Int64 processJob ["id"] runIdentity
      let consumerConfig = defaultConfig {ccMutexColumn = Just "mutex"}

      idleSignal <- liftIO newEmptyTMVarIO
      let numberOfJobs = 64
      putJobs numberOfJobs >> commit

      numberOfJobsStart <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      -- Move time forward 2hours, because jobs are scheduled 1 hour into future
      modifyTestTime $ addUTCTime (2 * 60 * 60)
      finalize
        ( localDomain "process" $
            runConsumerWithIdleSignal consumerConfig connSource idleSignal
        )
        $ waitUntilTrue idleSignal

      numberOfJobsEnd <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      (_, maxConcurrentAccess) <- liftIO $ readIORef concurrentAccess

      liftIO $ assertEqual "Should have queued up jobs" numberOfJobsStart (numberOfJobs :: Int64)
      liftIO $ assertEqual "Should have processed all jobs" numberOfJobsEnd (0 :: Int64)
      liftIO $ assertEqual "Should have processed jobs sequentially" maxConcurrentAccess 1
  where
    putJobs nrOfRows = localDomain "put" $ do
      TestEnvSt {..} <- get
      now <- currentTime
      replicateM_ (fromIntegral nrOfRows) $ do
        runSQL_ $
          "INSERT INTO "
            <> raw teJobTableName
            <> "(run_at, finished_at, reserved_by, attempts, mutex) "
            <> "VALUES (" <?> now
            <> " + interval '1 hour', NULL, NULL, 0, 'mut'"
            <> ")"
      notify teNotificationChannel ""

-- | Ensure that other consumers can progress, even if there are wanted mutexes
-- in the set of to be scheduled jobs that are currently held by others.
--
-- Spawn 2 consumers, one that'll get a job with a mutex in one of it's latter
-- batches, that'll continuously block. We subsequently spawn another consumer
-- that can proceed with the other jobs.
testMutexesConcurrentNonMutexed :: ConnectionSettings -> IO ()
testMutexesConcurrentNonMutexed connectionSettings = do
  let ConnectionSource connSource = simpleSource connectionSettings
  withStdOutLogger $ \logger -> do
    let additionalColumns =
          [ tblColumn
              { colName = "mutex"
              , colType = TextT
              , colNullable = True
              }
          , tblColumn
              { colName = "signal"
              , colType = BoolT
              , colNullable = False
              }
          ]
    runTestEnv connSource logger (TestSetup "test_mutexes_concurrent" additionalColumns) $ do
      now <- currentTime
      TestEnvSt {..} <- get
      blockConsumer <- liftIO newEmptyMVar
      waitOnConsumer <- liftIO newEmptyMVar
      let putJobs :: Int -> Int64 -> Maybe T.Text -> Bool -> TestEnv ()
          putJobs timeOffsetHr nrOfRows mutex signal = localDomain "put" $ do
            replicateM_ (fromIntegral nrOfRows) $ do
              runSQL_ $
                "INSERT INTO "
                  <> raw teJobTableName
                  <> "(run_at, finished_at, reserved_by, attempts, mutex, signal) "
                  <> "VALUES (" <?> now
                  <> " + interval '" <?> timeOffsetHr
                  <> " hour', NULL, NULL, 0, " <?> mutex
                  <> ", " <?> signal
                  <> ")"
            notify teNotificationChannel ""

          processJob (_idx, sig) =
            if sig
              then liftIO $ do
                void $ putMVar waitOnConsumer ()
                void $ takeMVar blockConsumer
                pure (Ok Remove)
              else pure (Ok Remove)

      defaultConfig <- defaultConsumerConfig @_ @_ @Int64 processJob ["id", "signal"] fst
      let consumerConfig = defaultConfig {ccMutexColumn = Just "mutex"}

      idleSignalBlocked <- liftIO newEmptyTMVarIO
      idleSignalContinuing <- liftIO newEmptyTMVarIO

      -- We insert 1 + 2 + 3 non-blocking jobs and 1 blocking job.
      putJobs 0 3 Nothing False
        -- Ensure the following are ordered after the above
        >> putJobs 1 3 Nothing False
        >> putJobs 1 1 (Just "mut") True
        >> commit

      numberOfJobsStart <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      -- Move time forward, so we're sure all consumers, when spawned want to
      -- consume all jobs currently in the queue
      shiftTestTimeHours 5

      -- This consumer will get:
      -- [ 1 non-blocking, 2 non-blocking, 2 non-blocking + 1 blocking with mutex ]
      numberBeforeUnblock <- finalize (localDomain "consumer1" $ runConsumerWithIdleSignal consumerConfig connSource idleSignalBlocked) $ do
        liftIO $ takeMVar waitOnConsumer

        putJobs 2 3 Nothing False
          >> putJobs 3 2 (Just "mut") False
          >> putJobs 3 2 Nothing False
          >> commit

        -- This consumer will get
        -- [ 1 non-blocking, 2 non-blocking, 2 non-blocking with mutex + 2 non-blocking ]
        -- Because consumer 1 already has the mutex, this consumer will skip over the 2
        -- mutex containing jobs in the 3rd batch
        finalize (localDomain "consumer2" $ runConsumerWithIdleSignal consumerConfig connSource idleSignalContinuing) $ do
          notify teNotificationChannel ""
          waitUntilTrue idleSignalContinuing

        numberBeforeUnblock <- do
          runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
          fetchOne runIdentity

        void . liftIO $ putMVar blockConsumer ()
        waitUntilTrue idleSignalBlocked
        pure numberBeforeUnblock

      numberOfJobsEnd <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      liftIO $ assertEqual "Should have queued up jobs" numberOfJobsStart (7 :: Int64)
      -- The 6 comes from the 4 blocking mutex and nonblocking jobs in consumer 1
      --  + 2 nonblocking jobs that wanted a mutex in consumer 2
      liftIO $ assertEqual "Should have processed non-mutexed jobs" numberBeforeUnblock (6 :: Int64)
      liftIO $ assertEqual "Should have processed all jobs" numberOfJobsEnd (0 :: Int64)

-- | On execution of a job, we make it possible to cleanup any jobs that are not
-- relevant anymore. This tests that we do indeed correctly delete the selected
-- expired jobs.
testExpired :: ConnectionSettings -> IO ()
testExpired connectionSettings = do
  let ConnectionSource connSource = simpleSource connectionSettings
  withStdOutLogger $ \logger -> do
    let additionalColumns =
          [ tblColumn
              { colName = "expire"
              , colType = TextT
              , colNullable = False
              }
          ]
    runTestEnv connSource logger (TestSetup "test_jobs_expired" additionalColumns) $ do
      TestEnvSt {..} <- get
      nrExecuted <- liftIO $ newIORef (0 :: Int)
      let processJob (Identity _) = do
            liftIO . atomicModifyIORef' nrExecuted $ (\c -> (c + 1, ()))
            pure $ Ok (RemoveExpired (ExpiredSelector "expire" "expire"))

      consumerConfig <- defaultConsumerConfig @_ @_ @Int64 processJob ["id"] runIdentity
      idleSignal <- liftIO newEmptyTMVarIO
      replicateM_ 10 (putJob "expire") >> replicateM_ 10 (putJob "not_expire") >> commit

      (numberOfJobsStart :: Int64) <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      -- Move time forward 2hours, because jobs are scheduled 1 hour into future
      modifyTestTime $ addUTCTime (2 * 60 * 60)
      finalize (localDomain "process" $ runConsumerWithIdleSignal consumerConfig connSource idleSignal) $ do
        waitUntilTrue idleSignal
        notify teNotificationChannel ""

      (numberOfJobsEnd :: Int64) <- do
        runSQL_ ("SELECT COUNT(*) from " <> raw teJobTableName)
        fetchOne runIdentity

      jobsExecuted <- liftIO $ readIORef nrExecuted

      liftIO $ T.assertEqual "Number of jobs in table" 20 numberOfJobsStart
      liftIO $ T.assertEqual "Number of jobs in table after execution" 0 numberOfJobsEnd
      liftIO $ T.assertEqual "Number of jobs executed" 11 jobsExecuted
  where
    putJob :: T.Text -> TestEnv ()
    putJob expired = localDomain "put" $ do
      TestEnvSt {..} <- get
      now <- currentTime
      runSQL_ $
        "INSERT INTO "
          <> raw teJobTableName
          <> "(run_at, finished_at, reserved_by, attempts, expire) "
          <> "VALUES (" <?> now
          <> " + interval '1 hour', NULL, NULL, 0, " <?> expired
          <> ")"
      notify teNotificationChannel ""

-- | Test that when a batch is submitted, it is consumed completely and in an
-- accelerated fashion that grows the batch size exponentially.
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

      rowCountGrowth :: [Int64] <-
        replicateM
          (10 :: Int)
          ( do
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
          )

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
    getConsumerConfig = defaultConsumerConfig processJob ["id", "countdown"] fst

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
waitUntilTrue tmvar = do
  -- Reset the idle signal, before waiting
  liftIO . atomically $ do
    _ <- tryTakeTMVar tmvar
    putTMVar tmvar False
  liftIO . atomically $ do
    takeTMVar tmvar >>= \case
      True -> pure ()
      False -> retry

handleException :: Applicative m => SomeException -> k -> m Action
handleException _ _ = pure . RerunAfter $ imicroseconds 500000

defaultConsumerConfig :: (FromRow job, Applicative m) => (job -> m Result) -> [SQL] -> (job -> idx) -> TestEnv (ConsumerConfig m idx job)
defaultConsumerConfig processJob jobSelectors jobIndex = do
  TestEnvSt {..} <- get
  pure $
    ConsumerConfig
      { ccJobsTable = teJobTableName
      , ccConsumersTable = teConsumerTableName
      , ccJobSelectors = jobSelectors
      , ccJobFetcher = id
      , ccJobIndex = jobIndex
      , ccNotificationChannel = Just teNotificationChannel
      , -- select some small timeout
        ccNotificationTimeout = 100 * 1000 -- 100 msec
      , ccMaxRunningJobs = 20
      , ccProcessJob = processJob
      , ccOnException = handleException
      , ccJobLogData = const []
      , ccMutexColumn = Nothing
      }
