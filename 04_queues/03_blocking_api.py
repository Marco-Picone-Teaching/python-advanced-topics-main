"""
Section D - The queue Module
Section D.3.1 - Blocking, timeouts and non-blocking variants: put/get, *_nowait, Empty/Full.

A bounded buffer (maxsize=2) between a sensor and the uplink.
"""

import queue
import threading
import time


def main() -> None:
    buffer: queue.Queue[float] = queue.Queue(maxsize=2)
    start = time.perf_counter()

    def log(msg: str) -> None:
        print(f"[{time.perf_counter() - start:.2f}s] {msg}")

    # --- non-blocking variants raise immediately ---
    try:
        buffer.get_nowait()
    except queue.Empty:
        log("get_nowait() on an empty queue -> queue.Empty")

    buffer.put(21.5)
    buffer.put(21.6)
    try:
        buffer.put_nowait(21.7)
    except queue.Full:
        log(f"put_nowait() on a full queue (size {buffer.qsize()}/{buffer.maxsize}) -> queue.Full")

    # --- timeout: wait at most N seconds, then raise ---
    try:
        buffer.put(21.7, timeout=0.3)
    except queue.Full:
        log("put(timeout=0.3) -> queue.Full after waiting 0.3 s")

    # --- blocking put: waits until a consumer frees a slot ---
    def slow_uplink() -> None:
        time.sleep(0.5)
        log(f"uplink: took {buffer.get()} from the queue")

    threading.Thread(target=slow_uplink).start()
    log("put(21.7) blocks until there is room...")
    buffer.put(21.7)
    log(f"put(21.7) completed, queue size = {buffer.qsize()}")


if __name__ == "__main__":
    main()
