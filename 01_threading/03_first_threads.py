"""
Section A - Concurrency Fundamentals & Threading
Section A.4.1 - Creating, starting and joining threads.

One thread per sensor: all the reads wait *at the same time*, so the total time is
roughly the time of the slowest read instead of the sum of all reads.
"""

import random
import threading
import time

READ_LATENCY_S = 0.5
SENSOR_IDS = ["temp-01", "temp-02", "hum-01", "hum-02", "co2-01"]


def poll_sensor(sensor_id: str, start: float) -> None:
    """Read one sensor and print the value together with the name of the running thread."""
    time.sleep(READ_LATENCY_S)  # simulated blocking I/O
    value = round(random.Random(sensor_id).uniform(18.0, 30.0), 1)
    thread_name = threading.current_thread().name
    print(f"[{time.perf_counter() - start:4.2f}s] {thread_name:>14}: {sensor_id} -> {value}")


def main() -> None:
    start = time.perf_counter()

    # 1. Create the Thread objects: nothing runs yet
    threads = [
        threading.Thread(target=poll_sensor, args=(sensor_id, start), name=f"poller-{sensor_id}")
        for sensor_id in SENSOR_IDS
    ]

    # 2. start() asks the OS to schedule each thread: they now run concurrently
    for t in threads:
        t.start()
    print(f"Main thread: started {len(threads)} threads, active threads = {threading.active_count()}")

    # 3. join() blocks the main thread until each worker has finished
    for t in threads:
        t.join()

    print(f"All sensors polled in {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
