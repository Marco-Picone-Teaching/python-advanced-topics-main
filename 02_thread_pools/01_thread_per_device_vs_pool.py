"""
Section B - Thread Pools & Futures
Section B.1 - Why "one thread per device" does not scale, and what a pool changes.

Experiment A - 500 devices, 50 ms read latency each (I/O-bound):
    one brand-new thread per device vs. pools of 50 and 10 reusable workers.
    We record the elapsed time and the PEAK number of threads alive in the process.

Experiment B - 20,000 tiny jobs (e.g. validating a reading, ~no I/O):
    here the cost of creating/destroying a thread per job dominates.
"""

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

NUM_DEVICES = 500
READ_LATENCY_S = 0.05
NUM_TINY_JOBS = 20_000

peak_threads = 0
peak_lock = threading.Lock()


def poll_device(device_id: int) -> int:
    """Simulated device read that also tracks the peak number of live threads."""
    global peak_threads
    with peak_lock:
        peak_threads = max(peak_threads, threading.active_count())
    time.sleep(READ_LATENCY_S)
    return device_id


def validate_reading(value: int) -> bool:
    """A tiny job: check that a raw reading is inside the sensor range."""
    return 0 <= value <= 4095


def one_thread_per_job(job: Callable[[int], object], n: int) -> None:
    threads = [threading.Thread(target=job, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def thread_pool(job: Callable[[int], object], n: int, workers: int) -> None:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(job, range(n)))


def measure(label: str, func: Callable[..., None], *args: object) -> None:
    global peak_threads
    peak_threads = 0
    start = time.perf_counter()
    func(*args)
    peak = f" | peak live threads: {peak_threads}" if peak_threads else ""
    print(f"  {label:26} time: {time.perf_counter() - start:5.2f}s{peak}")


def main() -> None:
    print(f"A) {NUM_DEVICES} I/O-bound device reads")
    measure("one thread per device", one_thread_per_job, poll_device, NUM_DEVICES)
    measure("ThreadPoolExecutor(50)", thread_pool, poll_device, NUM_DEVICES, 50)
    measure("ThreadPoolExecutor(10)", thread_pool, poll_device, NUM_DEVICES, 10)

    print(f"B) {NUM_TINY_JOBS} tiny validation jobs")
    measure("one thread per job", one_thread_per_job, validate_reading, NUM_TINY_JOBS)
    measure("ThreadPoolExecutor(8)", thread_pool, validate_reading, NUM_TINY_JOBS, 8)


if __name__ == "__main__":
    main()
