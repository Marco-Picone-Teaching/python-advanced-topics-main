"""
Lecture 05 - Event-Driven Architectures
Section 6 - The same event bus with asyncio.Queue and coroutines (module used by 05 and 06).

Same concepts as event_bus.py (Event, handler contract, wildcard subscriptions, graceful
stop), but handlers are COROUTINES and the workers are asyncio tasks in a single thread.
"""

from __future__ import annotations

import asyncio
import fnmatch
import itertools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from event_bus import BusStats, Event            # the Event dataclass is reused unchanged

logger = logging.getLogger("async-event-bus")


class AsyncEventHandler(ABC):
    """Contract of an asynchronous callback."""

    @abstractmethod
    async def handle(self, event: Event) -> None:
        ...


@dataclass(frozen=True)
class AsyncSubscription:
    id: int
    pattern: str
    handler: AsyncEventHandler


class AsyncEventBus:
    def __init__(self, num_workers: int = 2, max_queue_size: int = 0) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._subscriptions: dict[int, AsyncSubscription] = {}
        self._ids = itertools.count(1)
        self._num_workers = num_workers
        self._workers: list[asyncio.Task[None]] = []
        self.stats = BusStats()
        # No locks: everything runs in the event loop thread, and the registry and the stats
        # are never modified across an await.

    async def __aenter__(self) -> AsyncEventBus:
        self._workers = [asyncio.create_task(self._run_worker(), name=f"bus-worker-{i}")
                         for i in range(self._num_workers)]
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._queue.shutdown()                            # graceful: drain, then workers exit
        await asyncio.gather(*self._workers)

    async def publish(self, event: Event) -> None:
        await self._queue.put(event)                      # suspends only if the queue is bounded and full
        self.stats.published += 1

    def subscribe(self, pattern: str, handler: AsyncEventHandler) -> AsyncSubscription:
        sub = AsyncSubscription(next(self._ids), pattern, handler)
        self._subscriptions[sub.id] = sub
        return sub

    def unsubscribe(self, subscription: AsyncSubscription) -> None:
        self._subscriptions.pop(subscription.id, None)

    async def wait_until_idle(self) -> None:
        await self._queue.join()

    async def _run_worker(self) -> None:
        while True:
            try:
                event = await self._queue.get()
            except asyncio.QueueShutDown:
                return
            try:
                handlers = [s.handler for s in list(self._subscriptions.values())
                            if fnmatch.fnmatchcase(event.type, s.pattern)]
                for handler in handlers:
                    try:
                        await handler.handle(event)
                        self.stats.dispatched += 1
                    except Exception:
                        self.stats.failed += 1
                        logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            finally:
                self._queue.task_done()
