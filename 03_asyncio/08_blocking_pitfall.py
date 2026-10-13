"""
Section C - Asyncio
Section C.6.1 - The #1 asyncio pitfall: a blocking call inside a coroutine freezes EVERYTHING.

A heartbeat should tick every 100 ms. A "legacy" sensor driver uses time.sleep() (blocking)
instead of asyncio.sleep(): while it runs, the event loop cannot switch to any other task.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable


async def heartbeat(start: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        print(f"  [{time.perf_counter() - start:.2f}s] heartbeat")
        await asyncio.sleep(0.1)


async def read_sensor_blocking() -> float:
    time.sleep(0.5)             # BLOCKING: the whole event loop is stuck here for 0.5 s
    return 20.0


async def read_sensor_non_blocking() -> float:
    await asyncio.sleep(0.5)    # non-blocking: other tasks run during the wait
    return 20.0


async def run(title: str, reader: Callable[[], Awaitable[float]]) -> None:
    print(title)
    start = time.perf_counter()
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(start, stop))
    await asyncio.sleep(0.15)
    t0 = time.perf_counter() - start
    await reader()
    print(f"  sensor read from {t0:.2f}s to {time.perf_counter() - start:.2f}s")
    await asyncio.sleep(0.25)                   # let the heartbeat run a little longer
    stop.set()
    await hb


async def main() -> None:
    await run("--- blocking read inside a coroutine ---", read_sensor_blocking)
    await run("--- non-blocking read ---", read_sensor_non_blocking)


if __name__ == "__main__":
    asyncio.run(main())
