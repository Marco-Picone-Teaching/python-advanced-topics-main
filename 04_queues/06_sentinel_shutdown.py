"""
Section D - The queue Module
Section D.5.1 - Clean shutdown with a sentinel ("poison pill"). Works on every Python version.

Workers loop forever on get(). To stop them, the producer puts one special sentinel
object per worker: a worker that receives it exits its loop.
"""

import queue
import threading
import time

SENTINEL = object()        # a unique object: it can never be confused with a real reading
NUM_WORKERS = 3


def storage_worker(q: queue.Queue[object]) -> None:
    """Persist readings to the local time-series database until the sentinel arrives."""
    name = threading.current_thread().name
    stored = 0
    while True:
        item = q.get()
        try:
            if item is SENTINEL:                   # identity check: `is`, not `==`
                print(f"  {name}: sentinel received, exiting after {stored} readings")
                return
            time.sleep(0.02)                       # simulated DB write
            stored += 1
        finally:
            q.task_done()


def main() -> None:
    q: queue.Queue[object] = queue.Queue()
    workers = [threading.Thread(target=storage_worker, args=(q,), name=f"db-writer-{i}")
               for i in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    for i in range(30):
        q.put({"sensor": "temp-01", "seq": i})

    for _ in workers:                              # ONE sentinel per worker
        q.put(SENTINEL)
    for w in workers:
        w.join()
    print(f"all workers stopped, items left in the queue: {q.qsize()}")


if __name__ == "__main__":
    main()
