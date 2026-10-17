# consumers-metrics-prometheus-1.1.0.0 (2026-??-??)
* Add `consumers_job_failed_attempts`, a histogram of the attempt count of
  jobs that did not succeed, by `job_name`. Set its buckets with the new
  `jobFailedAttemptsBuckets` field of `ConsumerMetricsConfig`.
* Require `consumers` >= 2.4.0.0.

# consumers-metrics-prometheus-1.0.0.0 (2025-03-03)
* Initial release.
