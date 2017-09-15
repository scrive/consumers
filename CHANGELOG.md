# consumers-2.1.0.0 (2017-09-15)
* Now uses MonadTime instead of IO for current time. Also stop using "now()"
  in SQL. This allows controlling time from outside for testing.
* Add runConsumerWithIdleSignal for signaling outside, that consumer is
  idle. This can be used to stop consumers after they run out of work (testing).
* Now has a test.

# consumers-2.0.0.1 (2017-07-21)
* Now depends on 'log-base' instead of 'log'.
