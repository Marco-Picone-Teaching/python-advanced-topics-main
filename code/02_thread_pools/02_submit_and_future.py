"""
Lecture 02 - Thread Pools & Futures
Section 2.1 - submit() returns a Future: a placeholder for a result that will arrive later.
"""

import time
from concurrent.futures import Future, ThreadPoolExecutor


def read_temperature(sensor_id: str, latency_s: float) -> float:
    """Simulated blocking read of a temperature sensor."""
    time.sleep(latency_s)
    return {"temp-01": 21.5, "temp-02": 23.1}[sensor_id]


def main() -> None:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        # submit() schedules the call and returns IMMEDIATELY with a Future
        future_1: Future[float] = executor.submit(read_temperature, "temp-01", 0.3)
        future_2: Future[float] = executor.submit(read_temperature, "temp-02", 0.6)
        print(f"[{time.perf_counter() - start:.2f}s] submitted, future_1 = {future_1}")

        # The main thread is free to do other work in the meantime...
        time.sleep(0.4)
        print(f"[{time.perf_counter() - start:.2f}s] done? temp-01: {future_1.done()}, "
              f"temp-02: {future_2.done()}")

        # result() returns at once for a finished future...
        value_1 = future_1.result()
        print(f"[{time.perf_counter() - start:.2f}s] temp-01 = {value_1} C")
        # ...and blocks until the value is available otherwise (here: ~0.2 s more)
        value_2 = future_2.result()
        print(f"[{time.perf_counter() - start:.2f}s] temp-02 = {value_2} C, future_2 = {future_2}")


if __name__ == "__main__":
    main()
