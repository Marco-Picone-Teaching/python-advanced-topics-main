"""
smart_gateway - the small IoT gateway library used as "code under test" in Lecture 06.

Each module collects, in a testable form, a concept seen in the previous lectures:
  alarm_rules    pure logic (unit conversion, thresholds with hysteresis)       -> Lecture 06, sec. 2-3
  sensors        sensors with injected drivers/clocks, a monitor that publishes -> sec. 4 (test doubles)
  energy_meter   lock-protected shared state                                    -> Lecture 01
  sampler        a periodic polling thread with a stop Event                    -> Lecture 01
  poller         polling many sensors with a ThreadPoolExecutor                 -> Lecture 02
  async_gateway  asyncio reads with timeouts, periodic sampling                 -> Lecture 03
  telemetry      a queue-based aggregation stage                                -> Lecture 04
  event_bus      the thread-based event bus                                     -> Lecture 05
"""
