# consumers-metrics-prometheus-1.1.0.0 (2026-??-??)
* Add `consumers_job_failed_attempts`, a histogram of the attempt count at
  each failed run, by `job_name`. Its `_count` is the number of failed runs,
  not the number of failing jobs. Set its buckets with the new
  `jobFailedAttemptsBuckets` field of `ConsumerMetricsConfig`.
* Add `consumers_jobs_scheduled`, a gauge of the jobs that are not reserved
  and have `run_at` in the future, by `job_name`.
* Add the label `job_action` to `consumers_job_execution_seconds`. Its value
  is `mark_processed`, `rerun_after`, `rerun_at` or `remove`, from the action
  of the job result. If the job throws or aborts, the value is `none`.
* Collect queue metrics without a transaction and hold a pool connection
  only while a query runs. Count the overdue and scheduled jobs in one
  statement. Both counts are range scans on the `run_at` index.
* Compare `collectDegradeThresholdSeconds` against the time spent in the
  collection queries, not the wall-clock time of the collection. The
  wall-clock time, which includes the wait for a pool connection, degrades
  collection only at ten times the threshold, with the log message
  "Consumer metrics collection waited long". Both messages carry
  `query_time` and `runtime`.
* Require `consumers` >= 2.4.0.0.

# consumers-metrics-prometheus-1.0.0.0 (2025-03-03)
* Initial release.
