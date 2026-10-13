"""
Lecture 03 - Asyncio
Section 3.2 - Tasks: running a coroutine "in the background" with asyncio.create_task().
"""

import asyncio
import time


async def heartbeat(start: float) -> None:
    """Publish a heartbeat every 0.25 s, forever (until cancelled)."""
    while True:
        print(f"[{time.perf_counter() - start:.2f}s]   heartbeat")
        await asyncio.sleep(0.25)


async def calibrate_sensor(sensor_id: str, start: float) -> str:
    print(f"[{time.perf_counter() - start:.2f}s] calibrating {sensor_id}...")
    await asyncio.sleep(0.6)
    return f"{sensor_id} calibrated"


async def main() -> None:
    start = time.perf_counter()

    # create_task() schedules the coroutine on the event loop IMMEDIATELY and returns a Task
    hb_task = asyncio.create_task(heartbeat(start), name="heartbeat")
    cal_task = asyncio.create_task(calibrate_sensor("temp-01", start), name="calibration")
    print(f"tasks created: {hb_task.get_name()}, {cal_task.get_name()} (main keeps running)")

    result = await cal_task                 # wait only for the calibration
    print(f"[{time.perf_counter() - start:.2f}s] {result}; heartbeat done? {hb_task.done()}")

    hb_task.cancel()                        # stop the background task
    try:
        await hb_task
    except asyncio.CancelledError:
        print(f"[{time.perf_counter() - start:.2f}s] heartbeat cancelled: {hb_task.cancelled()}")


if __name__ == "__main__":
    asyncio.run(main())
