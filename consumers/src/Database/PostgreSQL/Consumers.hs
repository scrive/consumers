-- | A PostgreSQL-backed job queue. Start here.
--
-- A consumer is a worker process that pulls rows out of a jobs table you own,
-- runs your handler on each one, and writes the result back as either a
-- completion, a reschedule, or a delete. Enqueueing a job is just an @INSERT@
-- on the same table, so it composes with the rest of your transactions.
--
-- The two things you actually need are:
--
-- * 'ConsumerConfig' (from "Database.PostgreSQL.Consumers.Config") — describes
--   your jobs table, how to deserialize a job, and what to do with one.
-- * 'runConsumer' — starts the consumer's daemon threads and returns a
--   finalizer you run at shutdown (typically via 'finalize').
--
-- Re-exported submodules:
--
-- * "Database.PostgreSQL.Consumers.Config" — 'ConsumerConfig', 'Action',
--   'Result'.
-- * "Database.PostgreSQL.Consumers.Utils" — supporting machinery: the
--   'finalize' bracket and the 'StopExecution' / 'ThrownFrom' exceptions used
--   by the consumer's internal threads.
--
-- See the package README for an architectural overview and a job-lifecycle
-- diagram.
module Database.PostgreSQL.Consumers
  ( runConsumer
  , runConsumerWithIdleSignal
  , module Database.PostgreSQL.Consumers.Config
  , module Database.PostgreSQL.Consumers.Utils
  ) where

import Database.PostgreSQL.Consumers.Components
import Database.PostgreSQL.Consumers.Config
import Database.PostgreSQL.Consumers.Utils
