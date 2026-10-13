"""A queue-based processing stage (Section D): readings in, window averages out."""

import queue


def aggregation_stage(in_q: queue.Queue[float], out_q: queue.Queue[float], window: int) -> None:
    """
    Consume readings from in_q and put one average every `window` readings on out_q.
    When in_q is shut down (Python 3.13+), flush the partial window and shut down out_q.
    """
    if window <= 0:
        raise ValueError("window must be positive")
    buffer: list[float] = []
    while True:
        try:
            value = in_q.get()
        except queue.ShutDown:
            break
        buffer.append(value)
        if len(buffer) == window:
            out_q.put(sum(buffer) / window)
            buffer.clear()
        in_q.task_done()
    if buffer:
        out_q.put(sum(buffer) / len(buffer))
    out_q.shutdown()
