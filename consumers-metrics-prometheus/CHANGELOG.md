# consumers-metrics-prometheus-1.1.0.0 (2026-??-??)
* Add `consumers_job_failed_attempts`, a histogram of the number of
  processing attempts made so far by jobs whose execution didn't succeed
  (`Failed`, an exception, or an abort), by `job_name`. Distinguishes
  one-off failures from jobs that keep failing. Configurable via the new
  `jobFailedAttemptsBuckets` field on `ConsumerMetricsConfig`.
* Requires `consumers` >= 2.4.0.0, for `ccJobAttempts`.

# consumers-metrics-prometheus-1.0.0.0 (2025-03-03)
* Initial release.
