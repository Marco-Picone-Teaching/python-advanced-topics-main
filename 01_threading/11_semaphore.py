"""
Section A - Concurrency Fundamentals & Threading
Section A.6.3 - threading.Semaphore: limit how many threads use a resource at the same time.

Six sensor threads share an I2C bus controller that can serve at most 2 transactions
at once. A Semaphore(2) acts as a counter of free "slots".
"""

import threading
import time

MAX_CONCURRENT_BUS_ACCESSES = 2
bus_slots = threading.BoundedSemaphore(MAX_CONCURRENT_BUS_ACCESSES)

active = 0                      # how many threads are on the bus right now (for the demo)
active_lock = threading.Lock()


def read_over_i2c(sensor_id: str, start: float) -> None:
    global active
    with bus_slots:                             # blocks if 2 threads already hold a slot
        with active_lock:
            active += 1
            print(f"[{time.perf_counter() - start:4.2f}s] {sensor_id} on the bus (active = {active})")
        time.sleep(0.3)                         # the I2C transaction
        with active_lock:
            active -= 1
    # leaving the `with` releases the slot and lets a waiting thread in


def main() -> None:
    start = time.perf_counter()
    sensors = [f"sensor-{i:02d}" for i in range(1, 7)]
    threads = [threading.Thread(target=read_over_i2c, args=(s, start)) for s in sensors]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"6 reads with 2 bus slots took {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
