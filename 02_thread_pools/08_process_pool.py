"""
Section B - Thread Pools & Futures
Section B.6 - ProcessPoolExecutor for CPU-bound analytics.

Every night the gateway analyses the day of data of each sensor looking for anomalies
(readings more than 5 standard deviations away from a rolling mean). This is pure
Python computation: threads cannot run it in parallel because of the GIL, processes can.

IMPORTANT: with processes the `if __name__ == "__main__":` guard is MANDATORY.
On Windows and macOS (and on Linux from Python 3.14) child processes are started with
"spawn"/"forkserver": each child re-imports this module, and without the guard it would
try to create its own pool again.
"""

import math
import random
import time
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor

SENSORS = ["temp-01", "temp-02", "hum-01", "hum-02"]
SAMPLES_PER_SENSOR = 1_000_000
WINDOW = 50


def generate_day_of_data(sensor_id: str) -> list[float]:
    """Synthetic signal with a few injected spikes (deterministic per sensor)."""
    rng = random.Random(sensor_id)
    data = [20.0 + math.sin(i / 500) + rng.gauss(0, 0.1) for i in range(SAMPLES_PER_SENSOR)]
    for i in range(1000, SAMPLES_PER_SENSOR, 100_000):
        data[i] += 5.0                                  # anomaly
    return data


def detect_anomalies(sensor_id: str) -> tuple[str, int]:
    """CPU-bound: rolling mean / std over a sliding window, count points beyond 5 sigma."""
    data = generate_day_of_data(sensor_id)
    anomalies = 0
    window_sum = sum(data[:WINDOW])
    window_sq = sum(x * x for x in data[:WINDOW])
    for i in range(WINDOW, len(data)):
        mean = window_sum / WINDOW
        std = math.sqrt(max(window_sq / WINDOW - mean * mean, 1e-12))
        if abs(data[i] - mean) > 5 * std:
            anomalies += 1
        # slide the window: add the new point, remove the oldest one
        window_sum += data[i] - data[i - WINDOW]
        window_sq += data[i] * data[i] - data[i - WINDOW] * data[i - WINDOW]
    return sensor_id, anomalies


def run_with(executor_factory: Callable[[], Executor] | None) -> tuple[float, dict[str, int]]:
    start = time.perf_counter()
    if executor_factory is None:
        results = dict(map(detect_anomalies, SENSORS))            # plain sequential
    else:
        with executor_factory() as executor:
            results = dict(executor.map(detect_anomalies, SENSORS))
    return time.perf_counter() - start, results


def main() -> None:
    seq_time, results = run_with(None)
    thr_time, _ = run_with(lambda: ThreadPoolExecutor(max_workers=4))
    proc_time, _ = run_with(lambda: ProcessPoolExecutor(max_workers=4))

    print(f"anomalies found: {results}")
    print(f"sequential          : {seq_time:5.2f}s")
    print(f"ThreadPoolExecutor  : {thr_time:5.2f}s (x{seq_time / thr_time:.1f})")
    print(f"ProcessPoolExecutor : {proc_time:5.2f}s (x{seq_time / proc_time:.1f})")


if __name__ == "__main__":
    main()
