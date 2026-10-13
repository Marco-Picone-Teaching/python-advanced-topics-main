"""A periodic polling thread with cooperative shutdown (Section A.7)."""

import threading
from collections.abc import Callable


class PeriodicSampler(threading.Thread):
    """Calls `read()` every `period_s` seconds and passes the value to `on_sample()`."""

    def __init__(self, read: Callable[[], float], on_sample: Callable[[float], None],
                 period_s: float, name: str = "sampler") -> None:
        super().__init__(name=name)
        self._read = read
        self._on_sample = on_sample
        self._period_s = period_s
        self._stop_event = threading.Event()
        self.errors = 0

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._on_sample(self._read())
            except Exception:
                self.errors += 1                 # a failing read must not kill the sampler
            self._stop_event.wait(self._period_s)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        self.join(timeout)
