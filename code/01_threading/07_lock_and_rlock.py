"""
Lecture 01 - Concurrency Fundamentals & Threading
Sections 5.2 / 5.3 - Fixing the race with Lock, and when an RLock is needed.
"""

import threading
import time

NUM_PLUGS = 5
REPORTS_PER_PLUG = 1_000


class SafeEnergyMeter:
    """Thread-safe energy meter: the read-modify-write is a critical section."""

    def __init__(self) -> None:
        self.total_wh = 0
        self._lock = threading.Lock()

    def add(self, energy_wh: int) -> None:
        # `with` acquires the lock and ALWAYS releases it, even if an exception is raised
        with self._lock:
            current = self.total_wh
            time.sleep(0)                      # a thread switch here is now harmless
            self.total_wh = current + energy_wh


def smart_plug(meter: SafeEnergyMeter) -> None:
    """Simulate a smart plug that sends many small energy reports."""
    for _ in range(REPORTS_PER_PLUG):
        meter.add(1)


class DeviceRegistry:
    """
    A registry whose public methods call each other while holding the lock.
    With a plain Lock, register_many() -> register() would try to acquire a lock
    the same thread already holds and block forever. An RLock (re-entrant lock)
    can be acquired several times by the thread that owns it.
    """

    def __init__(self) -> None:
        self._devices: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, device_id: str, device_type: str) -> None:
        with self._lock:
            self._devices[device_id] = device_type

    def register_many(self, devices: dict[str, str]) -> None:
        with self._lock:                           # first acquisition
            for device_id, device_type in devices.items():
                self.register(device_id, device_type)   # second acquisition by the SAME thread: OK

    def count(self) -> int:
        with self._lock:
            return len(self._devices)


def main() -> None:
    # --- Lock: no more lost updates ---
    meter = SafeEnergyMeter()
    threads = [threading.Thread(target=smart_plug, args=(meter,)) for _ in range(NUM_PLUGS)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"Expected total: {NUM_PLUGS * REPORTS_PER_PLUG} Wh | Actual total: {meter.total_wh} Wh "
          f"({time.perf_counter() - start:.2f}s)")

    # --- RLock: nested acquisition by the same thread ---
    registry = DeviceRegistry()
    registry.register_many({"temp-01": "TemperatureSensor", "light-01": "SmartLight"})
    print(f"Registered devices: {registry.count()}")


if __name__ == "__main__":
    main()
