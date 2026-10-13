"""
Section C - Asyncio
Section C.5.1 - Timeouts: asyncio.timeout() (3.11+) and asyncio.wait_for().

Unlike threads, a timed-out coroutine is really CANCELLED: the pending operation stops.
"""

import asyncio
import time


async def read_slow_sensor(sensor_id: str, latency: float) -> float:
    try:
        await asyncio.sleep(latency)
        return 18.2
    finally:
        # runs both on success and on cancellation
        print(f"  {sensor_id}: read operation finished/aborted, bus released")


async def main() -> None:
    start = time.perf_counter()

    # 1. asyncio.timeout(): a context manager, can wrap several awaits
    try:
        async with asyncio.timeout(0.3):
            value = await read_slow_sensor("soil-01", latency=2.0)
            print(f"soil-01 = {value}")
    except TimeoutError:
        print(f"[{time.perf_counter() - start:.2f}s] soil-01: timeout (operation cancelled)")

    # 2. asyncio.wait_for(): the same for a single awaitable
    try:
        value = await asyncio.wait_for(read_slow_sensor("soil-02", latency=0.1), timeout=0.3)
        print(f"[{time.perf_counter() - start:.2f}s] soil-02 = {value}")
    except TimeoutError:
        print("soil-02: timeout")


if __name__ == "__main__":
    asyncio.run(main())
