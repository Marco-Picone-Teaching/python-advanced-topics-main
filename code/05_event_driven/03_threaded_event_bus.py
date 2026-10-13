"""
Lecture 05 - Event-Driven Architectures
Section 4 - Step 2: an asynchronous event bus built with threads + queue.Queue.

Three classes with separate responsibilities:
  * EventBus       - the engine: owns the queue, the worker threads and the type -> handlers map.
                     Application components never use it directly.
  * EventHandler   - abstract base class: the CONTRACT of a callback (handle(event)).
  * EventBusClient - the facade given to application components: write() publishes an event,
                     read() subscribes a handler to an event type. Queue and threads stay hidden.

publish() only puts the event in the queue and returns immediately: the publisher is never
slowed down by the handlers, which run in the bus worker threads.
"""

import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-14s] %(message)s")
logger = logging.getLogger("event-bus")


@dataclass(frozen=True)
class Event:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class EventHandler(ABC):
    """Contract of a callback: every concrete handler implements handle()."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        ...


class EventBus:
    """Engine of the event layer: queue + worker pool + dispatch to the registered handlers."""

    def __init__(self, num_workers: int = 2) -> None:
        self._queue: queue.Queue[Event] = queue.Queue()
        self._handlers: dict[str, list[EventHandler]] = {}
        self._lock = threading.Lock()                 # protects _handlers (subscribe vs dispatch)
        self._workers = [
            threading.Thread(target=self._run_worker, name=f"bus-worker-{i}", daemon=True)
            for i in range(num_workers)
        ]

    def start(self) -> None:
        for w in self._workers:
            w.start()

    def publish(self, event: Event) -> None:
        self._queue.put(event)                        # returns immediately

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def wait_until_idle(self) -> None:
        """Block until every published event has been dispatched (useful for demos and tests)."""
        self._queue.join()

    def _run_worker(self) -> None:
        while True:
            event = self._queue.get()
            with self._lock:
                handlers = list(self._handlers.get(event.type, []))   # copy: dispatch outside the lock
            for handler in handlers:
                try:
                    handler.handle(event)
                except Exception:
                    logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            self._queue.task_done()


class EventBusClient:
    """Interface used by application components: only write (publish) and read (subscribe)."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        self._bus.publish(Event(type=event_type, payload=payload or {}))

    def read(self, event_type: str, handler: EventHandler) -> None:
        self._bus.subscribe(event_type, handler)


# --------------------------- application components ---------------------------

class DashboardHandler(EventHandler):
    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s = %s", event.type, event.payload)


class CloudLoggerHandler(EventHandler):
    def handle(self, event: Event) -> None:
        time.sleep(0.3)                               # slow HTTP call: no longer blocks the sensor!
        logger.info("[cloud] stored %s", event.payload)


def main() -> None:
    bus = EventBus(num_workers=2)
    bus.start()
    client = EventBusClient(bus)

    client.read("sensor.temperature", DashboardHandler())
    client.read("sensor.temperature", CloudLoggerHandler())

    start = time.perf_counter()
    for value in (21.0, 21.2, 21.4):
        t0 = time.perf_counter()
        client.write("sensor.temperature", {"sensor": "temp-01", "celsius": value})
        logger.info("sensor: published %.1f, write() took %.1f ms", value, (time.perf_counter() - t0) * 1000)
        time.sleep(0.1)

    bus.wait_until_idle()
    logger.info("all events dispatched in %.2fs", time.perf_counter() - start)


if __name__ == "__main__":
    main()
