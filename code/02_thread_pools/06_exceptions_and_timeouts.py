"""
Lecture 02 - Thread Pools & Futures
Section 4 - Exceptions and timeouts with futures.

An exception raised inside a worker does NOT crash the program and is NOT printed:
it is stored in the Future and re-raised when you call result().
"""

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed


class SensorOfflineError(Exception):
    """Raised when a sensor does not answer on the field bus."""


VALUES = {"temp-01": 21.5, "hum-02": 48.0, "co2-01": 640.0, "temp-02": 22.8}


def read_sensor(sensor_id: str) -> float:
    time.sleep(0.1)
    if sensor_id == "hum-02":
        raise SensorOfflineError(f"{sensor_id} did not answer")
    if sensor_id == "co2-01":
        time.sleep(1.0)             # a very slow device
    return VALUES[sensor_id]


def main() -> None:
    sensors = ["temp-01", "hum-02", "co2-01", "temp-02"]
    results: dict[str, float] = {}
    errors: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(read_sensor, s): s for s in sensors}

        # 1. Per-future exception handling
        try:
            for future in as_completed(futures, timeout=0.5):   # overall deadline for the cycle
                sensor_id = futures[future]
                try:
                    results[sensor_id] = future.result()
                except SensorOfflineError as exc:
                    errors[sensor_id] = f"offline ({exc})"
                # alternative without try/except: future.exception() returns the exception or None
        except FuturesTimeoutError:
            for future, sensor_id in futures.items():
                if not future.done():
                    errors[sensor_id] = "timeout"

    print(f"results: {results}")
    print(f"errors : {errors}")

    # 2. result(timeout=...) on a single future
    with ThreadPoolExecutor() as executor:
        slow = executor.submit(read_sensor, "co2-01")
        try:
            slow.result(timeout=0.3)
        except FuturesTimeoutError:
            print(f"co2-01: no answer within 0.3 s (the task is still running: {slow.running()})")


if __name__ == "__main__":
    main()
