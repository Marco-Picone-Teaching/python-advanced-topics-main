"""
Section A - Concurrency Fundamentals & Threading
Section A.4.3 - Subclassing Thread and collecting results.

Thread.start() does not return what the target function returns. A clean way to get
results back is to model the worker as a class that stores its own result.
"""

import random
import threading
import time


class SensorPoller(threading.Thread):
    """A thread that reads a sensor `samples` times and keeps the collected values."""

    def __init__(self, sensor_id: str, samples: int, period_s: float) -> None:
        # Always call the parent constructor first: it initialises the thread machinery
        super().__init__(name=f"poller-{sensor_id}")
        self.sensor_id = sensor_id
        self.samples = samples
        self.period_s = period_s
        self.readings: list[float] = []     # filled by run(), read by the main thread after join()
        self._rng = random.Random(sensor_id)

    def run(self) -> None:
        """Code executed in the new thread when start() is called (never call run() directly)."""
        for _ in range(self.samples):
            time.sleep(self.period_s)                       # wait for the next sample
            self.readings.append(round(self._rng.uniform(20.0, 25.0), 2))

    @property
    def average(self) -> float | None:
        """Average of the collected readings (None if nothing has been collected)."""
        return sum(self.readings) / len(self.readings) if self.readings else None


def main() -> None:
    pollers = [
        SensorPoller("temp-01", samples=3, period_s=0.2),
        SensorPoller("temp-02", samples=5, period_s=0.1),
        SensorPoller("temp-03", samples=2, period_s=0.3),
    ]
    for p in pollers:
        p.start()
    for p in pollers:
        p.join()        # after join() it is safe to read p.readings

    for p in pollers:
        print(f"{p.sensor_id}: {p.readings} -> avg {p.average:.2f}")


if __name__ == "__main__":
    main()
