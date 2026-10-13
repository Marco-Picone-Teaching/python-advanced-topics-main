"""
Lecture 03 - Asyncio
Section 6.2 - Integrating blocking code: asyncio.to_thread() and loop.run_in_executor().

Many real device libraries (serial ports, Modbus, vendor SDKs) are synchronous and blocking.
We cannot rewrite them, but we can run them in a worker thread and AWAIT the result,
so the event loop stays responsive.
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor


def legacy_modbus_read(register: int) -> int:
    """A blocking vendor driver we cannot change."""
    time.sleep(0.5)
    return 1000 + register


async def heartbeat(start: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        print(f"  [{time.perf_counter() - start:.2f}s] heartbeat")
        await asyncio.sleep(0.2)


async def main() -> None:
    start = time.perf_counter()
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(start, stop))

    # 1. asyncio.to_thread(): run the blocking function in the default thread pool
    values = await asyncio.gather(
        asyncio.to_thread(legacy_modbus_read, 1),
        asyncio.to_thread(legacy_modbus_read, 2),
    )
    print(f"[{time.perf_counter() - start:.2f}s] to_thread results: {values}")

    # 2. run_in_executor(): same idea, with an executor chosen by us (e.g. to bound concurrency)
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="modbus") as modbus_executor:
        value = await loop.run_in_executor(modbus_executor, legacy_modbus_read, 3)
    print(f"[{time.perf_counter() - start:.2f}s] run_in_executor result: {value}")

    stop.set()
    await hb


if __name__ == "__main__":
    asyncio.run(main())
