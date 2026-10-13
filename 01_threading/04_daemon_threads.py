"""
Section A - Concurrency Fundamentals & Threading
Section A.4.2 - Daemon vs non-daemon threads.

A *heartbeat* thread that publishes "gateway alive" forever is a typical daemon:
we do not want it to keep the program alive once the main work is done.
"""

import threading
import time


def heartbeat(period_s: float) -> None:
    """Infinite loop: announce that the gateway is alive every `period_s` seconds."""
    beat = 0
    while True:
        beat += 1
        print(f"  [heartbeat] gateway alive (beat #{beat})")
        time.sleep(period_s)


def main() -> None:
    # daemon=True -> the interpreter will NOT wait for this thread at exit
    hb = threading.Thread(target=heartbeat, args=(0.3,), name="heartbeat", daemon=True)
    hb.start()

    print("Main: doing the real work for ~1 second...")
    time.sleep(1.0)
    print(f"Main: work done, heartbeat still alive? {hb.is_alive()}")
    print("Main: exiting (the daemon thread is killed abruptly)")
    # With daemon=False this program would never terminate!


if __name__ == "__main__":
    main()
