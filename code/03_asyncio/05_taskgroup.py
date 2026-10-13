"""
Lecture 03 - Asyncio
Section 4 - Structured concurrency with asyncio.TaskGroup (Python 3.11+).

  * gather() by default propagates the FIRST exception, but the other coroutines keep running
    unsupervised; with return_exceptions=True it returns exceptions mixed with results.
  * TaskGroup: if one task fails, all the others are CANCELLED, and every error is
    collected into an ExceptionGroup, handled with `except*`.
"""

import asyncio


class SensorOfflineError(Exception):
    """The sensor did not answer."""


class SensorCalibrationError(Exception):
    """The sensor answered with an out-of-range value."""


async def read_sensor(sensor_id: str, latency: float) -> float:
    try:
        await asyncio.sleep(latency)
        if sensor_id == "hum-02":
            raise SensorOfflineError(f"{sensor_id} offline")
        if sensor_id == "co2-01":
            raise SensorCalibrationError(f"{sensor_id} returned -40 ppm")
        print(f"  {sensor_id}: ok")
        return 21.0
    except asyncio.CancelledError:
        print(f"  {sensor_id}: cancelled by the TaskGroup")
        raise


async def with_gather() -> None:
    print("--- gather(return_exceptions=True) ---")
    results = await asyncio.gather(
        read_sensor("temp-01", 0.1), read_sensor("hum-02", 0.2), read_sensor("temp-02", 0.3),
        return_exceptions=True,
    )
    for r in results:
        print(f"  result: {r!r}")


async def with_taskgroup() -> None:
    print("--- TaskGroup ---")
    try:
        async with asyncio.TaskGroup() as tg:
            t1 = tg.create_task(read_sensor("temp-01", 0.1))
            tg.create_task(read_sensor("hum-02", 0.2))
            tg.create_task(read_sensor("co2-01", 0.2))
            tg.create_task(read_sensor("temp-02", 1.0))     # still running when the others fail
        # we get here only if ALL tasks succeeded
    except* SensorOfflineError as group:
        for exc in group.exceptions:
            print(f"  handled offline error: {exc}")
    except* SensorCalibrationError as group:
        for exc in group.exceptions:
            print(f"  handled calibration error: {exc}")
    print(f"  temp-01 finished before the failure: {t1.result()}")


async def main() -> None:
    await with_gather()
    await with_taskgroup()


if __name__ == "__main__":
    asyncio.run(main())
