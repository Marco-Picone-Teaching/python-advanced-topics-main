"""
Lecture 01 - Concurrency Fundamentals & Threading
Section 6.4 - threading.Timer: run an action once, after a delay, in a separate thread.

A motion sensor turns the corridor light on; a Timer turns it off automatically after
AUTO_OFF_S seconds. New motion cancels the pending timer and starts a new one.
"""

import threading
import time

AUTO_OFF_S = 0.5


class CorridorLight:
    """Smart light with an automatic switch-off timer."""

    def __init__(self) -> None:
        self.is_on = False
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()
        self._start = time.perf_counter()

    def _log(self, msg: str) -> None:
        print(f"[{time.perf_counter() - self._start:4.2f}s] {msg}")

    def _turn_off(self) -> None:
        """Called by the Timer thread when the delay expires."""
        with self._lock:
            self.is_on = False
            self._log("light OFF (auto-off timer expired)")

    def on_motion_detected(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()             # motion again: postpone the switch-off
            self.is_on = True
            self._log("motion detected -> light ON, auto-off rescheduled")
            self._timer = threading.Timer(AUTO_OFF_S, self._turn_off)
            self._timer.start()


def main() -> None:
    light = CorridorLight()
    light.on_motion_detected()
    time.sleep(0.3)
    light.on_motion_detected()      # before the first timer expires -> it is cancelled
    time.sleep(1.0)                 # no more motion: the second timer fires
    print(f"Final state: {'ON' if light.is_on else 'OFF'}")


if __name__ == "__main__":
    main()
