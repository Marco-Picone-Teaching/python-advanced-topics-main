"""
smart_gateway - the small IoT gateway library used as "code under test" in Section F.

Each module collects, in a testable form, a concept seen in the previous sections:
  alarm_rules    pure logic (unit conversion, thresholds with hysteresis)       -> Sections F.2-F.3
  sensors        sensors with injected drivers/clocks, a monitor that publishes -> Section F.4 (test doubles)
  energy_meter   lock-protected shared state                                    -> Section A
  sampler        a periodic polling thread with a stop Event                    -> Section A
  poller         polling many sensors with a ThreadPoolExecutor                 -> Section B
  async_gateway  asyncio reads with timeouts, periodic sampling                 -> Section C
  telemetry      a queue-based aggregation stage                                -> Section D
  event_bus      the thread-based event bus                                     -> Section E
"""
