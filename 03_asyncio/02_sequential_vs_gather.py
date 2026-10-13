"""
Section C - Asyncio
Section C.3.1 - Awaiting one coroutine at a time vs running them concurrently with gather().
"""

import asyncio
import time

# sensor id -> (latency in seconds, value)
SENSORS = {"temp-01": (0.6, 21.5), "hum-01": (0.3, 47.0), "co2-01": (0.9, 612.0)}


async def read_sensor(sensor_id: str, start: float) -> float:
    latency, value = SENSORS[sensor_id]
    print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: start")
    await asyncio.sleep(latency)                  # non-blocking wait (simulated I/O)
    print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: done -> {value}")
    return value


async def sequential() -> None:
    start = time.perf_counter()
    # Each await waits for the previous read to finish: no concurrency at all
    results = [await read_sensor(s, start) for s in SENSORS]
    print(f"sequential: {results} in {time.perf_counter() - start:.2f}s\n")


async def concurrent() -> None:
    start = time.perf_counter()
    # gather() schedules all the coroutines at once and waits for all of them
    results = await asyncio.gather(*(read_sensor(s, start) for s in SENSORS))
    print(f"gather    : {results} in {time.perf_counter() - start:.2f}s")


async def main() -> None:
    await sequential()
    await concurrent()


if __name__ == "__main__":
    asyncio.run(main())
