"""
Lecture 01 - Concurrency Fundamentals & Threading
Section 5.1 - A race condition on shared state.

Several smart plugs report their consumed energy to a shared EnergyMeter.
The update is a read-modify-write sequence: read the total, compute, write it back.
If two threads interleave between the read and the write, one update is LOST.

The tiny time.sleep(0) between read and write simulates the processing a real
meter would do (unit conversion, validation, logging...) and makes the problem
visible on every run instead of "once a week in production".
"""

import threading
import time

NUM_PLUGS = 5              # threads
REPORTS_PER_PLUG = 1_000   # updates sent by each thread
ENERGY_PER_REPORT_WH = 1   # each report adds 1 Wh


class EnergyMeter:
    """Accumulates the energy consumed by all the smart plugs of the building (NOT thread-safe)."""

    def __init__(self) -> None:
        self.total_wh = 0

    def add(self, energy_wh: int) -> None:
        current = self.total_wh          # 1. READ the shared value
        time.sleep(0)                    # 2. ...processing: the OS may switch thread here!
        self.total_wh = current + energy_wh   # 3. WRITE back a value computed from a stale read


def smart_plug(meter: EnergyMeter) -> None:
    """Simulate a smart plug that sends many small energy reports."""
    for _ in range(REPORTS_PER_PLUG):
        meter.add(ENERGY_PER_REPORT_WH)


def main() -> None:
    meter = EnergyMeter()
    plugs = [threading.Thread(target=smart_plug, args=(meter,)) for _ in range(NUM_PLUGS)]
    for p in plugs:
        p.start()
    for p in plugs:
        p.join()

    expected = NUM_PLUGS * REPORTS_PER_PLUG * ENERGY_PER_REPORT_WH
    print(f"Expected total: {expected} Wh")
    print(f"Actual total  : {meter.total_wh} Wh  -> lost {expected - meter.total_wh} Wh of readings!")


if __name__ == "__main__":
    main()
