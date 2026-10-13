"""
Lecture 05 - Event-Driven Architectures
Section 6.2 - Experiment: the same workload on the thread-based and on the asyncio event bus.

N events are dispatched to M handlers, in three scenarios:
  A) I/O-bound handlers (each one waits 2 ms, e.g. a write to a remote database), 16 workers
  B) the same I/O-bound handlers with 256 workers (high concurrency)
  C) trivial handlers that only update an in-memory counter (pure dispatch overhead)
Both buses always use the same number of workers.
"""

import asyncio
import threading
import time
from collections.abc import Callable

from async_event_bus import AsyncEventBus, AsyncEventHandler
from event_bus import Event, EventBus, EventHandler

N_EVENTS = 1_000
M_HANDLERS = 4
IO_LATENCY_S = 0.002


class ThreadIOHandler(EventHandler):
    def handle(self, event: Event) -> None:
        time.sleep(IO_LATENCY_S)


class ThreadCounterHandler(EventHandler):
    def __init__(self) -> None:
        self.count = 0

    def handle(self, event: Event) -> None:
        self.count += 1               # approximate under threads, fine for a benchmark


class AsyncIOHandler(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        await asyncio.sleep(IO_LATENCY_S)


class AsyncCounterHandler(AsyncEventHandler):
    def __init__(self) -> None:
        self.count = 0

    async def handle(self, event: Event) -> None:
        self.count += 1


def run_threaded(handler_factory: Callable[[], EventHandler], workers: int) -> float:
    start = time.perf_counter()
    with EventBus(num_workers=workers) as bus:
        for _ in range(M_HANDLERS):
            bus.subscribe("sensor.*", handler_factory())
        for i in range(N_EVENTS):
            bus.publish(Event("sensor.temperature", {"seq": i}))
        bus.wait_until_idle()
    return time.perf_counter() - start


async def run_async(handler_factory: Callable[[], AsyncEventHandler], workers: int) -> float:
    start = time.perf_counter()
    async with AsyncEventBus(num_workers=workers) as bus:
        for _ in range(M_HANDLERS):
            bus.subscribe("sensor.*", handler_factory())
        for i in range(N_EVENTS):
            await bus.publish(Event("sensor.temperature", {"seq": i}))
        await bus.wait_until_idle()
    return time.perf_counter() - start


def main() -> None:
    sequential_io = N_EVENTS * M_HANDLERS * IO_LATENCY_S
    print(f"{N_EVENTS} events x {M_HANDLERS} handlers (sequential I/O time would be {sequential_io:.1f}s)")
    scenarios = [
        ("A) I/O-bound,  16 workers", ThreadIOHandler, AsyncIOHandler, 16),
        ("B) I/O-bound, 256 workers", ThreadIOHandler, AsyncIOHandler, 256),
        ("C) trivial,    16 workers", ThreadCounterHandler, AsyncCounterHandler, 16),
    ]
    for label, thread_handler, async_handler, workers in scenarios:
        t_threads = run_threaded(thread_handler, workers)
        t_async = asyncio.run(run_async(async_handler, workers))
        print(f"{label} -> threads: {t_threads:.2f}s | asyncio: {t_async:.2f}s")
    print(f"threads alive after the benchmark: {threading.active_count()}")


if __name__ == "__main__":
    main()
