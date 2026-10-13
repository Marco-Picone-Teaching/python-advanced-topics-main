"""
Section D - The queue Module
Section D.3.2 - task_done() and join(): waiting until every item has been PROCESSED.

queue.join() does not wait for the queue to be empty: it waits until task_done() has been
called once for every item that was put(). An item taken with get() but still being
processed keeps join() blocked.
"""

import queue
import threading
import time


def config_pusher(jobs: queue.Queue[str]) -> None:
    """Worker: push a new configuration to each device."""
    while True:
        device_id = jobs.get()
        try:
            time.sleep(0.2)                            # simulated configuration upload
            print(f"  [{threading.current_thread().name}] configuration pushed to {device_id}")
        finally:
            jobs.task_done()                           # ALWAYS, even if the processing fails


def main() -> None:
    jobs: queue.Queue[str] = queue.Queue()
    for i in range(3):
        threading.Thread(target=config_pusher, args=(jobs,), name=f"pusher-{i}", daemon=True).start()

    start = time.perf_counter()
    for device in ["plug-01", "plug-02", "plug-03", "plug-04", "plug-05", "plug-06"]:
        jobs.put(device)

    jobs.join()                                        # blocks until 6 task_done() calls
    print(f"all devices configured in {time.perf_counter() - start:.2f}s "
          f"(unfinished tasks: {jobs.unfinished_tasks})")


if __name__ == "__main__":
    main()
