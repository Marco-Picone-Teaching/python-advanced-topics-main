"""
The thread-based event bus of Lecture 05 (same code as code/05_event_driven/event_bus.py),
copied here so that the test suite is self-contained.
"""

from __future__ import annotations

import fnmatch
import itertools
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("event-bus")


@dataclass(frozen=True)
class Event:
    """An immutable fact: something that happened, with its data."""
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"
    timestamp: float = field(default_factory=time.time)


class EventHandler(ABC):
    """Contract of a callback: every concrete handler implements handle()."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        ...


@dataclass(frozen=True)
class Subscription:
    """Receipt returned by subscribe(): pass it to unsubscribe()."""
    id: int
    pattern: str
    handler: EventHandler


@dataclass
class BusStats:
    published: int = 0
    dispatched: int = 0          # successful handler invocations
    failed: int = 0              # handler invocations that raised


class EventBus:
    """Thread-based publish/subscribe engine."""

    def __init__(self, num_workers: int = 2, max_queue_size: int = 0) -> None:
        self._queue: queue.Queue[Event] = queue.Queue(maxsize=max_queue_size)
        self._subscriptions: dict[int, Subscription] = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()                     # protects _subscriptions and stats
        self._num_workers = num_workers
        self._workers: list[threading.Thread] = []
        self.stats = BusStats()

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self._workers = [
            threading.Thread(target=self._run_worker, name=f"bus-worker-{i}")
            for i in range(self._num_workers)
        ]
        for w in self._workers:
            w.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop accepting events, dispatch the ones already queued, then stop the workers."""
        self._queue.shutdown()                            # graceful: workers drain the queue first
        for w in self._workers:
            w.join(timeout)

    def __enter__(self) -> EventBus:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # ------------------------------------------------------------------ pub / sub
    def publish(self, event: Event) -> None:
        """Enqueue an event; raises queue.ShutDown if the bus has been stopped."""
        self._queue.put(event)
        with self._lock:
            self.stats.published += 1

    def subscribe(self, pattern: str, handler: EventHandler) -> Subscription:
        with self._lock:
            sub = Subscription(next(self._ids), pattern, handler)
            self._subscriptions[sub.id] = sub
            return sub

    def unsubscribe(self, subscription: Subscription) -> None:
        with self._lock:
            self._subscriptions.pop(subscription.id, None)

    def wait_until_idle(self) -> None:
        """Block until every event published so far has been fully dispatched."""
        self._queue.join()

    # ------------------------------------------------------------------ internals
    def _matching_handlers(self, event_type: str) -> list[EventHandler]:
        with self._lock:                                  # snapshot under the lock...
            subs = list(self._subscriptions.values())
        return [s.handler for s in subs if fnmatch.fnmatchcase(event_type, s.pattern)]

    def _run_worker(self) -> None:
        while True:
            try:
                event = self._queue.get()
            except queue.ShutDown:
                return
            try:
                for handler in self._matching_handlers(event.type):   # ...dispatch outside it
                    try:
                        handler.handle(event)
                        with self._lock:
                            self.stats.dispatched += 1
                    except Exception:
                        with self._lock:
                            self.stats.failed += 1
                        logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            finally:
                self._queue.task_done()


class EventBusClient:
    """Facade for application components: write() to publish, read() to subscribe."""

    def __init__(self, bus: EventBus, source: str) -> None:
        self._bus = bus
        self._source = source                             # who is publishing (for tracing)

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        self._bus.publish(Event(event_type, payload or {}, source=self._source))

    def read(self, pattern: str, handler: EventHandler) -> Subscription:
        return self._bus.subscribe(pattern, handler)

    def stop_reading(self, subscription: Subscription) -> None:
        self._bus.unsubscribe(subscription)
