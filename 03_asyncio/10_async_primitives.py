"""
Section C - Asyncio
Section C.7 - asyncio synchronization primitives: Semaphore, Lock, Event.

  * Semaphore: at most 2 concurrent uploads to the cloud API (rate limit)
  * Lock:      a shared counter updated across an await (a race is possible even in asyncio!)
  * Event:     all uploaders wait until the network link is up
"""

import asyncio
import time

upload_slots = asyncio.Semaphore(2)
stats_lock = asyncio.Lock()
network_up = asyncio.Event()
stats = {"uploaded": 0}


async def upload(batch_id: int, start: float) -> None:
    await network_up.wait()                         # wait for connectivity
    async with upload_slots:                        # at most 2 concurrent uploads
        print(f"[{time.perf_counter() - start:.2f}s] uploading batch {batch_id}")
        await asyncio.sleep(0.2)                    # the HTTP request
    async with stats_lock:
        # read-modify-write with an await in the middle: without the lock, another task
        # could run between the read and the write, exactly like with threads
        current = stats["uploaded"]
        await asyncio.sleep(0)                      # e.g. writing an audit log entry
        stats["uploaded"] = current + 1


async def connectivity_monitor(start: float) -> None:
    await asyncio.sleep(0.3)
    print(f"[{time.perf_counter() - start:.2f}s] LTE link is up -> network_up.set()")
    network_up.set()


async def main() -> None:
    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(connectivity_monitor(start))
        for i in range(5):
            tg.create_task(upload(i, start))
    print(f"[{time.perf_counter() - start:.2f}s] uploaded batches: {stats['uploaded']}")


if __name__ == "__main__":
    asyncio.run(main())
