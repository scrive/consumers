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
