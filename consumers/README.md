# consumers

[![Haskell-CI](https://github.com/scrive/consumers/actions/workflows/haskell-ci.yml/badge.svg?branch=master)](https://github.com/scrive/consumers/actions/workflows/haskell-ci.yml)
[![Hackage version](https://img.shields.io/hackage/v/consumers.svg?label=Hackage)](https://hackage.haskell.org/package/consumers)

A PostgreSQL-backed job queue for Haskell. Jobs are rows in tables you own, so
you can enqueue them in the same transaction as the business write that
triggered them: no separate broker, no dual-write problem. Dispatch is driven
by `LISTEN`/`NOTIFY` for low latency and `FOR UPDATE SKIP LOCKED` for
contention-free reservation.

## Features

- **Postgres is the queue.** Jobs live in your own table; enqueue is just an
  `INSERT` in the same transaction as the rest of your write. No Redis, no
  Kafka, no broker to operate.
- **Multiple independent queues.** Each `ConsumerConfig` points at its own jobs
  table paired with its own registry table (conventionally `foo_jobs` and
  `foo_consumers`). The registry schema also permits one table to track
  workers for several queues, distinguished by the `name` column.
- **Low-latency dispatch.** Optional `LISTEN`/`NOTIFY` wakes the consumer the
  instant a job is committed; a configurable polling interval is the fallback
  (and handles delayed/retried jobs).
- **Non-blocking reservation.** Jobs are claimed with
  `SELECT … FOR UPDATE SKIP LOCKED`, so workers never block each other.
- **Scheduled jobs.** Every job has a `run_at` timestamp. One-shot delays,
  retries, and recurring jobs are all just different values of `run_at`;
  recurrence is implemented by having `ccProcessJob` return `RerunAfter` or
  `RerunAt`.
- **At-least-once semantics with a flexible retry hook.** `ccOnException`
  receives the exception and the job and returns the next `Action`
  (`MarkProcessed`, `RerunAfter`, `RerunAt`, or `Remove`), so you can
  implement any backoff policy you like.
- **Dead-consumer reclamation.** Each consumer heartbeats its
  `last_activity` every 30 seconds; any consumer idle for more than 60 seconds
  is presumed dead and its reserved jobs are released back to the queue.
- **Graceful shutdown.** `runConsumer` returns a finalizer that waits for
  in-flight jobs to finish and releases reservations.
- **Bounded concurrency.** `ccMaxRunningJobs` caps how many jobs a single
  consumer process runs in parallel.
- **Structured logging** via [`log-base`](https://hackage.haskell.org/package/log-base);
  per-job context is attached through `ccJobLogData`.
- **Optional Prometheus metrics** via the sibling
  [`consumers-metrics-prometheus`](https://hackage.haskell.org/package/consumers-metrics-prometheus)
  package.

## Quick start

A complete, runnable example lives in
[`example/Example.hs`](example/Example.hs). The shape of an integration is:

1. Create a jobs table with the required columns (`id`, `run_at`, `finished_at`,
   `reserved_by`, `attempts`) plus whatever payload columns you need, and a
   paired registry table (`id`, `name`, `last_activity`). See the Haddock on
   `ccJobsTable` and `ccConsumersTable` for the exact contract.
2. Enqueue a job by `INSERT`ing a row, typically in the same transaction as
   the write that caused it.
3. Build a `ConsumerConfig` describing the tables, how to deserialize a job,
   what to do with it (`ccProcessJob`), and what to do on failure
   (`ccOnException`).
4. Call `runConsumer cfg connSource` to start the worker. It returns an
   `m (m ())`: the outer action starts the daemons, the inner action waits for
   in-flight jobs at shutdown. Wrap the pair with `finalize` to tie shutdown to
   your main loop.

## Architecture

Each call to `runConsumer` spawns three daemon threads inside your process:

- **Listener.** Waits on the `LISTEN`/`NOTIFY` channel (if configured) and/or
  a `ccNotificationTimeout` timer, and pokes the Dispatcher whenever it's time
  to check for due jobs.
- **Dispatcher.** Reserves up to `ccMaxRunningJobs - inFlight` due jobs in a
  single `SELECT … FOR UPDATE SKIP LOCKED` (setting `reserved_by` and bumping
  `attempts`), then forks one worker thread per reserved job. Reserving a
  whole batch in one round-trip amortizes the query cost across all the jobs
  in it. Each worker runs `ccProcessJob` in its own DB transaction; the
  results are folded back into a batched `UPDATE`.
- **Monitor.** Updates this consumer's `last_activity` every 30 seconds and
  scans the registry table for peers that have gone silent for more than
  60 seconds. Any jobs reserved by such a peer are released (with
  `ccOnException` applied), and the dead consumer row is removed.

```
                 ┌──────────────────── Consumer process ────────────────────┐
                 │                                                          │
   NOTIFY  ─────►│  Listener  ──poke──►  Dispatcher  ──fork──►  Worker pool │
                 │                            │                    │        │
                 │      ┌──heartbeat──┐       │                    │        │
                 │      │             ▼       ▼                    ▼        │
                 │  Monitor       SELECT … FOR UPDATE         ccProcessJob  │
                 │      │         SKIP LOCKED;  UPDATE             │        │
                 └──────┼──────────────┼───────────────────────────┼────────┘
                        │              │                           │
                        ▼              ▼                           ▼
                  ┌───────────────────────────────────────────────────────┐
                  │                     PostgreSQL                        │
                  │   consumers table   ◄────►   jobs table               │
                  └───────────────────────────────────────────────────────┘
```

## Job lifecycle

A job's state is encoded implicitly in four columns:
`run_at`, `reserved_by`, `finished_at`, and `attempts`. There is no explicit
status enum; the combination of those columns is the state.

| State          | `run_at`        | `reserved_by` | `finished_at` |
|----------------|-----------------|---------------|---------------|
| Queued         | `> NOW()`       | NULL          | NULL          |
| Ready          | `≤ NOW()`       | NULL          | NULL          |
| Reserved       | `≤ NOW()`       | some consumer | NULL          |
| Completed      | NULL            | NULL          | `NOT NULL`    |
| Rescheduled    | `> NOW()`       | NULL          | NULL          |
| Stuck          | `≤ NOW()`       | dead consumer | NULL          |
| Removed        | (row deleted)                                   |

```
                  INSERT
                    │
                    ▼
              ┌──────────┐   run_at ≤ NOW()    ┌──────────┐
              │  Queued  │ ──────────────────► │  Ready   │ ◄─┐
              └──────────┘                     └────┬─────┘   │
                                                    │         │
                                  Dispatcher reserves         │
                                  reserved_by := me           │
                                  attempts    += 1            │
                                                    │         │
                                                    ▼         │
                                              ┌──────────┐    │
                                              │ Reserved │    │
                                              └────┬─────┘    │
                                                   │          │
                                             ccProcessJob     │
                                                   │          │
                      ┌────────────────────────────┼────────────────────────────┐
                      │                            │                            │
               Ok MarkProcessed           Ok/Failed RerunAfter             Ok/Failed Remove
               Ok Remove                  Ok/Failed RerunAt                (or Ok MarkProcessed)
                      │                            │                            │
                      ▼                            ▼                            ▼
                ┌───────────┐                ┌─────────────┐               ┌──────────┐
                │ Completed │                │ Rescheduled │               │ Removed  │
                └───────────┘                └─────────────┘               └──────────┘
```

If the consumer dies mid-processing the row sits in the Stuck state until the
Monitor on another consumer notices and reclaims it (`ccOnException` is
applied, `reserved_by` is cleared, and the job returns to Ready).

## Observability

`ccJobLogData` attaches a list of structured fields to every log line emitted
while a job is processed; set it to include the job ID and any other
correlation data. For Prometheus metrics, drop in
[`consumers-metrics-prometheus`](https://hackage.haskell.org/package/consumers-metrics-prometheus),
which wraps `runConsumer` and exposes histograms and gauges for running,
overdue, and processed jobs without any changes to your consumer code.
