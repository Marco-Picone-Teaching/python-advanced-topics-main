"""
Lecture 03 - Asyncio
Section 5.2 - Periodic tasks without drift, using the event loop clock.

A naive `while True: read(); await asyncio.sleep(period)` drifts, because each cycle lasts
`period + read_time`. Scheduling against absolute deadlines keeps a stable sampling rate.
"""

import asyncio


async def read_sensor() -> float:
    await asyncio.sleep(0.03)                 # the read itself takes 30 ms
    return 21.0


async def naive_sampler(samples: int, period: float) -> list[float]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    stamps = []
    for _ in range(samples):
        await read_sensor()
        stamps.append(round(loop.time() - start, 2))
        await asyncio.sleep(period)           # period + read time -> drift
    return stamps


async def drift_free_sampler(samples: int, period: float) -> list[float]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    next_deadline = start
    stamps = []
    for _ in range(samples):
        await read_sensor()
        stamps.append(round(loop.time() - start, 2))
        next_deadline += period
        await asyncio.sleep(max(0.0, next_deadline - loop.time()))   # sleep only what is left
    return stamps


async def main() -> None:
    naive, fixed = await asyncio.gather(naive_sampler(6, 0.2), drift_free_sampler(6, 0.2))
    print(f"naive      sample times: {naive}")
    print(f"drift-free sample times: {fixed}")


if __name__ == "__main__":
    asyncio.run(main())
