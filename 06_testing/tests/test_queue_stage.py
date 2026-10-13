"""
Section F.7.1: testing a queue-based pipeline stage (Section D).

A stage that reads from an input queue and writes to an output queue is easy to test in
isolation: the test plays the role of BOTH the upstream and the downstream stage.
"""

import queue
import threading

import pytest

from smart_gateway.telemetry import aggregation_stage

TIMEOUT_S = 2.0


def drain(q: queue.Queue[float]) -> list[float]:
    """Read everything from a queue until it is shut down (each get() with a timeout)."""
    items = []
    while True:
        try:
            items.append(q.get(timeout=TIMEOUT_S))
        except queue.ShutDown:
            return items


def run_stage(values: list[float], window: int) -> list[float]:
    in_q: queue.Queue[float] = queue.Queue()
    out_q: queue.Queue[float] = queue.Queue()
    worker = threading.Thread(target=aggregation_stage, args=(in_q, out_q, window))
    worker.start()
    for v in values:
        in_q.put(v)
    in_q.shutdown()                                   # "no more data": the stage must flush and stop
    result = drain(out_q)
    worker.join(TIMEOUT_S)
    assert not worker.is_alive()
    return result


def test_averages_every_window(no_thread_leaks: None) -> None:
    assert run_stage([20.0, 22.0, 24.0, 26.0], window=2) == [21.0, 25.0]


def test_flushes_partial_window_on_shutdown(no_thread_leaks: None) -> None:
    assert run_stage([20.0, 22.0, 30.0], window=2) == [21.0, 30.0]


def test_no_input_produces_no_output(no_thread_leaks: None) -> None:
    assert run_stage([], window=3) == []


def test_invalid_window_is_rejected() -> None:
    with pytest.raises(ValueError):
        aggregation_stage(queue.Queue(), queue.Queue(), window=0)


def test_join_confirms_all_inputs_processed(no_thread_leaks: None) -> None:
    in_q: queue.Queue[float] = queue.Queue()
    out_q: queue.Queue[float] = queue.Queue()
    worker = threading.Thread(target=aggregation_stage, args=(in_q, out_q, 3))
    worker.start()
    for v in (1.0, 2.0, 3.0):
        in_q.put(v)
    in_q.join()                                       # returns when task_done() was called 3 times
    assert out_q.get(timeout=TIMEOUT_S) == 2.0
    in_q.shutdown()
    worker.join(TIMEOUT_S)
