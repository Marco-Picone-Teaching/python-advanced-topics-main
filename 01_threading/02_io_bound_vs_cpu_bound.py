"""
Section A - Concurrency Fundamentals & Threading
Section A.3.2 - The GIL in action: threads help I/O-bound work, not CPU-bound work.

We run the same amount of work twice, sequentially and with 4 threads:
  * I/O-bound  -> waiting for 4 sensors (time.sleep releases the GIL)
  * CPU-bound  -> computing a moving-average filter over a long signal (pure Python code
                  that holds the GIL)
"""

import sys
import threading
import time
from collections.abc import Callable

NUM_TASKS = 4


def io_bound_task() -> None:
    """Wait for a (simulated) sensor answer: the thread sleeps and releases the GIL."""
    time.sleep(0.5)


def cpu_bound_task() -> None:
    """Apply a moving-average filter to a synthetic signal: pure computation."""
    signal = [float(i % 100) for i in range(300_000)]
    window = 5
    filtered = []
    for i in range(len(signal) - window):
        filtered.append(sum(signal[i:i + window]) / window)


def run_sequential(task: Callable[[], None]) -> float:
    """Run NUM_TASKS copies of the task one after the other; return elapsed seconds."""
    start = time.perf_counter()
    for _ in range(NUM_TASKS):
        task()
    return time.perf_counter() - start


def run_threaded(task: Callable[[], None]) -> float:
    """Run NUM_TASKS copies of the task in parallel threads; return elapsed seconds."""
    start = time.perf_counter()
    threads = [threading.Thread(target=task) for _ in range(NUM_TASKS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - start


def main() -> None:
    # sys._is_gil_enabled() exists since Python 3.13; older versions always have the GIL
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"Python {sys.version.split()[0]} - GIL enabled: {gil_enabled}")

    for name, task in [("I/O-bound", io_bound_task), ("CPU-bound", cpu_bound_task)]:
        seq = run_sequential(task)
        thr = run_threaded(task)
        print(f"{name:10} sequential: {seq:5.2f}s | {NUM_TASKS} threads: {thr:5.2f}s "
              f"| speed-up: x{seq / thr:.1f}")


if __name__ == "__main__":
    main()
