"""
Section A - Concurrency Fundamentals & Threading
Section A.7 - Graceful shutdown of long-running polling threads.

Python has no API to "kill" a thread from outside. The idiomatic solution is
cooperative: every worker periodically checks a shared stop Event, and uses
stop_event.wait(period) instead of time.sleep(period) so that it wakes up
IMMEDIATELY when the stop is requested instead of finishing its sleep.
"""

import threading
import time


def periodic_poller(sensor_id: str, period_s: float, stop_event: threading.Event) -> None:
    """Sample a sensor every `period_s` seconds until the stop event is set."""
    samples = 0
    while not stop_event.is_set():
        samples += 1                                  # ...read the sensor here...
        # wait() returns True as soon as the event is set -> no need to finish the full period
        if stop_event.wait(timeout=period_s):
            break
    print(f"{sensor_id}: stopped cleanly after {samples} samples")


def main() -> None:
    stop_event = threading.Event()
    pollers = [
        threading.Thread(target=periodic_poller, args=("temp-01", 0.1, stop_event), name="temp-01"),
        # a slow sensor: with time.sleep(5) the shutdown would take up to 5 seconds
        threading.Thread(target=periodic_poller, args=("co2-01", 5.0, stop_event), name="co2-01"),
    ]
    for p in pollers:
        p.start()

    time.sleep(0.55)                                  # the gateway runs for a while...
    print("Main: shutdown requested")
    t0 = time.perf_counter()
    stop_event.set()                                  # 1. ask every worker to stop
    for p in pollers:
        p.join(timeout=2.0)                           # 2. wait, but never forever
        if p.is_alive():
            print(f"Main: WARNING {p.name} did not stop in time")
    print(f"Main: all pollers stopped in {time.perf_counter() - t0:.3f}s")


if __name__ == "__main__":
    main()
