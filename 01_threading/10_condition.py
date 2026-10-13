"""
Section A - Concurrency Fundamentals & Threading
Section A.6.2 - threading.Condition: wait until a *condition on shared state* becomes true.

A sensor thread appends readings to a shared buffer; an uploader thread must wait
until at least BATCH_SIZE readings are available before sending a batch to the cloud.
The Condition couples a lock (protecting the buffer) with a wait/notify mechanism.

Note: this is exactly the kind of code that queue.Queue will hide from us (Section D).
"""

import threading
import time

BATCH_SIZE = 3
TOTAL_READINGS = 9

buffer: list[float] = []
condition = threading.Condition()   # contains its own Lock


def sensor_producer() -> None:
    """Produce one temperature reading every 0.1 s."""
    for i in range(TOTAL_READINGS):
        time.sleep(0.1)
        with condition:                     # acquire the underlying lock
            buffer.append(20.0 + i * 0.1)
            print(f"[sensor]   buffered reading #{i + 1} (buffer size = {len(buffer)})")
            condition.notify()              # wake up one waiting thread


def cloud_uploader() -> None:
    """Upload readings in batches of BATCH_SIZE."""
    uploaded = 0
    while uploaded < TOTAL_READINGS:
        with condition:
            # wait_for() releases the lock while sleeping and re-checks the predicate on wake-up
            condition.wait_for(lambda: len(buffer) >= BATCH_SIZE)
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]
        # the (slow) upload happens OUTSIDE the lock, so the producer is not blocked
        print(f"[uploader] uploading batch {[round(v, 1) for v in batch]}")
        uploaded += len(batch)


def main() -> None:
    threads = [threading.Thread(target=cloud_uploader), threading.Thread(target=sensor_producer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
