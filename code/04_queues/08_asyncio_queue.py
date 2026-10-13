"""
Lecture 04 - The queue Module
Section 6.2 - asyncio.Queue: the same producer-consumer pattern, with coroutines.

Same API names as queue.Queue (put, get, task_done, join, maxsize, shutdown in 3.13+),
but put()/get() are coroutines that must be AWAITED: they suspend the task, not the thread.
NEVER use queue.Queue inside asyncio code: its blocking get() would freeze the event loop.
"""

import asyncio
import time


async def sensor(sensor_id: str, q: asyncio.Queue[tuple[str, float]], samples: int) -> None:
    for i in range(samples):
        await asyncio.sleep(0.05)
        await q.put((sensor_id, 20.0 + i))          # suspends this task while the queue is full


async def rule_engine(q: asyncio.Queue[tuple[str, float]], start: float) -> None:
    while True:
        sensor_id, value = await q.get()            # suspends until an item is available
        if value >= 22.0:
            print(f"[{time.perf_counter() - start:.2f}s] rule: {sensor_id} = {value} -> ALERT")
        q.task_done()


async def main() -> None:
    q: asyncio.Queue[tuple[str, float]] = asyncio.Queue(maxsize=10)
    start = time.perf_counter()

    consumer = asyncio.create_task(rule_engine(q, start))
    async with asyncio.TaskGroup() as producers:
        producers.create_task(sensor("temp-01", q, samples=3))
        producers.create_task(sensor("temp-02", q, samples=4))

    await q.join()                  # every reading has been evaluated by the rule engine
    consumer.cancel()               # the consumer loops forever: cancel it (or use q.shutdown() in 3.13+)
    print(f"[{time.perf_counter() - start:.2f}s] all readings processed")


if __name__ == "__main__":
    asyncio.run(main())
