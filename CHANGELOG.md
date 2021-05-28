# consumers-2.2.0.4 (2021-06-09)
* Adjust to changes in log-base-0.10.

# consumers-2.2.0.3 (2021-05-28)
* Support GHC 9.0.

# consumers-2.2.0.2 (2020-05-05)
* Support hpqtypes-1.9.0.0 and relax base.

# consumers-2.2.0.1 (2019-05-22)
* Support hpqtypes-1.7.0.0 and hpqtypes-extras-1.9.0.0.

# consumers-2.2.0.0 (2019-02-19)
* When jobs are relased from a dead consumer, reset their finished_at column
* Start the consumer only when ccMaxRunningJobs >= 1

# consumers-2.1.2.0 (2018-07-11)

* Support hpqtypes-1.6.0.0.
* Drop support for GHC < 8.

# consumers-2.1.1.0 (2018-03-18)

* GHC 8.4.1 support.

# consumers-2.1.0.0 (2017-09-18)

* Added a `MonadTime` constraint to `runConsumer`. The `currentTime`
  method from `MonadTime` is now used instead of `now()` in SQL. This
  is mostly useful for testing, to control the flow of time in test
  cases.

* Added a new `runConsumer` variant: `runConsumerWithIdleSignal`.
  When a consumer is idle, this variant signals a TMVar. This is
  mostly useful for testing.

* Added a test suite.

# consumers-2.0.0.1 (2017-07-21)
* Now depends on 'log-base' instead of 'log'.
