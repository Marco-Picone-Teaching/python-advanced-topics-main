"""Polling many sensors concurrently with a thread pool (Section B)."""

from collections.abc import Callable, Mapping
from concurrent.futures import Executor, Future, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass, field


@dataclass
class CycleReport:
    readings: dict[str, float] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


def poll_all(readers: Mapping[str, Callable[[], float]], executor: Executor,
             deadline_s: float) -> CycleReport:
    """
    Run every reader concurrently on `executor` and collect the results within `deadline_s`.
    Failed readers are reported as "error: <message>", readers that miss the deadline as "timeout".
    """
    report = CycleReport()
    futures: dict[Future[float], str] = {executor.submit(read): sid for sid, read in readers.items()}
    try:
        for future in as_completed(futures, timeout=deadline_s):
            sensor_id = futures[future]
            try:
                report.readings[sensor_id] = future.result()
            except Exception as exc:
                report.errors[sensor_id] = f"error: {exc}"
    except FuturesTimeoutError:
        for future, sensor_id in futures.items():
            if not future.done():
                future.cancel()
                report.errors[sensor_id] = "timeout"
    return report
