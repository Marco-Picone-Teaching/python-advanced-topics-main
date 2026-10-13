"""
Lecture 02 - Thread Pools & Futures
Section 5 - Executor lifecycle: context manager, shutdown(), cancel_futures, max_workers.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor


def upload_batch(batch_id: int) -> int:
    time.sleep(0.2)                 # simulated upload of a batch of readings to the cloud
    return batch_id


def main() -> None:
    # Default number of workers (Python 3.13+): min(32, os.process_cpu_count() + 4)
    cpus = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    with ThreadPoolExecutor() as executor:
        # _max_workers is a private attribute: read here only to show the default value
        print(f"CPUs usable by this process: {cpus} -> default max_workers = {executor._max_workers}")

    # 1. The context manager calls shutdown(wait=True) on exit: all 6 uploads complete
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(upload_batch, i) for i in range(6)]
    print(f"with-block: {sum(f.done() for f in futures)}/6 uploads completed "
          f"in {time.perf_counter() - start:.2f}s")

    # 2. Explicit shutdown with cancel_futures=True: queued jobs are dropped (e.g. on SIGTERM)
    start = time.perf_counter()
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(upload_batch, i) for i in range(6)]
    time.sleep(0.05)                                     # the first 2 jobs are now running
    executor.shutdown(wait=True, cancel_futures=True)
    completed = sum(f.done() and not f.cancelled() for f in futures)
    cancelled = sum(f.cancelled() for f in futures)
    print(f"shutdown(cancel_futures=True): {completed} completed, {cancelled} cancelled "
          f"in {time.perf_counter() - start:.2f}s")

    # 3. After shutdown the executor refuses new work
    try:
        executor.submit(upload_batch, 99)
    except RuntimeError as exc:
        print(f"submit after shutdown -> RuntimeError: {exc}")


if __name__ == "__main__":
    main()
