"""
Lecture 04 - The queue Module
Section 5.2 - Queue.shutdown() and queue.ShutDown (Python 3.13+).

  * shutdown(immediate=False): no more put(); get() keeps returning the remaining items and
    raises ShutDown only when the queue is EMPTY -> graceful drain.
  * shutdown(immediate=True): the queue is emptied at once, every blocked or future get()
    raises ShutDown -> fast stop, pending items are discarded.
"""

import queue
import sys
import threading
import time

if sys.version_info < (3, 13):
    sys.exit("This example requires Python 3.13+ (use the sentinel pattern of 06 instead)")


def uploader(q: queue.Queue[int], results: list[int]) -> None:
    while True:
        try:
            batch = q.get()                         # raises queue.ShutDown after shutdown()
        except queue.ShutDown:
            print(f"  {threading.current_thread().name}: queue shut down, exiting")
            return
        time.sleep(0.05)                            # simulated upload
        results.append(batch)
        q.task_done()


def run(immediate: bool) -> None:
    q: queue.Queue[int] = queue.Queue()
    results: list[int] = []
    workers = [threading.Thread(target=uploader, args=(q, results), name=f"uploader-{i}") for i in range(2)]
    for w in workers:
        w.start()
    for batch in range(10):
        q.put(batch)

    time.sleep(0.12)                                # the service is asked to stop...
    q.shutdown(immediate=immediate)
    try:
        q.put(99)
    except queue.ShutDown:
        print("  put() after shutdown -> queue.ShutDown")
    for w in workers:
        w.join()
    print(f"shutdown(immediate={immediate}): {len(results)}/10 batches uploaded\n")


def main() -> None:
    run(immediate=False)
    run(immediate=True)


if __name__ == "__main__":
    main()
