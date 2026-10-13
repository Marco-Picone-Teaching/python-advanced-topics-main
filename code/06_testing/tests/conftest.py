"""
Shared pytest fixtures. pytest discovers this file automatically: every test in this
directory can request these fixtures just by naming them as parameters.
"""

import threading
import time
from collections.abc import Iterator

import pytest

from smart_gateway.event_bus import EventBus


class FakeClock:
    """A controllable clock: tests decide what 'now' is, instead of depending on real time."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def no_thread_leaks() -> Iterator[None]:
    """Fail the test if it leaves threads running after it has finished."""
    before = set(threading.enumerate())
    yield                                                    # <- the test runs here
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:                       # give stopping threads a moment
        leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
        if not leaked:
            return
        time.sleep(0.01)
    pytest.fail(f"threads leaked by the test: {[t.name for t in leaked]}")


@pytest.fixture
def event_bus(no_thread_leaks: None) -> Iterator[EventBus]:
    """A started EventBus, always stopped at the end of the test (even if the test fails)."""
    bus = EventBus(num_workers=2)
    bus.start()
    yield bus
    bus.stop()
