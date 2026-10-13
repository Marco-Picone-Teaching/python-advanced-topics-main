"""
Section D - The queue Module
Section D.6.3 - multiprocessing.Queue: exchanging data between PROCESSES.

queue.Queue lives in the memory of one process and cannot be shared with another process.
multiprocessing.Queue has a similar API but transfers items through a pipe (items are pickled).
Here a separate process runs a CPU-heavy feature extraction on batches of vibration samples.
"""

import math
import multiprocessing as mp
import os


def feature_extractor(inbox: "mp.Queue[list[float] | None]", outbox: "mp.Queue[tuple[int, float]]") -> None:
    """Runs in a child process: compute the RMS of each batch until the None sentinel arrives."""
    while (batch := inbox.get()) is not None:
        rms = math.sqrt(sum(x * x for x in batch) / len(batch))
        outbox.put((os.getpid(), round(rms, 3)))


def main() -> None:
    inbox: mp.Queue = mp.Queue()
    outbox: mp.Queue = mp.Queue()
    worker = mp.Process(target=feature_extractor, args=(inbox, outbox))
    worker.start()

    for amplitude in (0.5, 1.0, 2.0):             # three batches from a vibration sensor
        inbox.put([amplitude * math.sin(i / 10) for i in range(10_000)])
    inbox.put(None)                               # sentinel

    for _ in range(3):
        pid, rms = outbox.get()
        print(f"main pid {os.getpid()} <- child pid {pid}: RMS = {rms}")
    worker.join()


if __name__ == "__main__":                        # mandatory with multiprocessing
    main()
