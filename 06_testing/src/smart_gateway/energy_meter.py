"""Shared state updated by many threads (Section A.5)."""

import threading
import time


class EnergyMeter:
    """Thread-safe accumulator of the energy reported by smart plugs."""

    def __init__(self) -> None:
        self._total_wh = 0
        self._lock = threading.Lock()

    def add(self, energy_wh: int) -> None:
        if energy_wh < 0:
            raise ValueError("energy cannot be negative")
        with self._lock:
            current = self._total_wh
            time.sleep(0)                     # widen the race window: makes a missing lock obvious
            self._total_wh = current + energy_wh

    @property
    def total_wh(self) -> int:
        with self._lock:
            return self._total_wh
