"""
Lecture 04 - The queue Module
Section 4 - Producer-consumer with backpressure.

Two sensor threads (producers) generate readings at 20 Hz each; one uploader thread
(consumer) sends them over a slow cellular link that can handle only ~12 readings/s.
With maxsize=5 the queue applies BACKPRESSURE: when it is full, producers block in put()
instead of filling the gateway's memory.
"""

import queue
import threading
import time
from dataclasses import dataclass

READINGS_PER_SENSOR = 10


@dataclass
class Reading:
    sensor_id: str
    seq: int
    value: float


def sensor_producer(sensor_id: str, q: queue.Queue[Reading], start: float) -> None:
    for seq in range(READINGS_PER_SENSOR):
        time.sleep(0.05)                                   # 20 Hz sampling
        t_before = time.perf_counter()
        q.put(Reading(sensor_id, seq, 20.0 + seq / 10))    # blocks while the queue is full
        waited = time.perf_counter() - t_before
        if waited > 0.01:
            print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: blocked {waited * 1000:.0f} ms "
                  f"on put() (queue full)")


def uploader(q: queue.Queue[Reading], total: int, start: float) -> None:
    for _ in range(total):
        q.get()                                            # take the next reading...
        time.sleep(0.08)                                   # ...and send it: slow uplink, ~12 readings/s
        q.task_done()
    print(f"[{time.perf_counter() - start:.2f}s] uploader: {total} readings sent")


def main() -> None:
    q: queue.Queue[Reading] = queue.Queue(maxsize=5)
    start = time.perf_counter()
    total = 2 * READINGS_PER_SENSOR
    threads = [
        threading.Thread(target=sensor_producer, args=("temp-01", q, start)),
        threading.Thread(target=sensor_producer, args=("hum-01", q, start)),
        threading.Thread(target=uploader, args=(q, total, start)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
