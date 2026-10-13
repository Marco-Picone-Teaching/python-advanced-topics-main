"""
Lecture 01 - Concurrency Fundamentals & Threading
Section 5.4 - Deadlock: two threads, two locks, opposite order.

A "scene" controller needs exclusive access to BOTH the HVAC and the window actuator
(never open the window while the HVAC is cooling). Two scenes lock them in the
opposite order -> each thread holds one lock and waits forever for the other.

To keep the demo from hanging we use acquire(timeout=...) to DETECT the deadlock;
then we fix it with a global lock ordering.
"""

import threading
import time

hvac_lock = threading.Lock()
window_lock = threading.Lock()


def scene(name: str, first: threading.Lock, second: threading.Lock,
          first_name: str, second_name: str) -> None:
    """Acquire two actuator locks in the given order and apply a scene."""
    with first:
        print(f"{name}: locked {first_name}")
        time.sleep(0.1)                           # give the other thread time to grab its first lock
        if second.acquire(timeout=1.0):           # a plain `with second:` would block forever
            try:
                print(f"{name}: locked {second_name} -> scene applied")
            finally:
                second.release()
        else:
            print(f"{name}: DEADLOCK detected, could not lock {second_name} (giving up)")


def run(title: str, targets: list[tuple]) -> None:
    print(f"--- {title} ---")
    threads = [threading.Thread(target=scene, args=args) for args in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main() -> None:
    # Opposite order -> deadlock
    run("Opposite lock order", [
        ("night-scene  ", hvac_lock, window_lock, "HVAC", "window"),
        ("morning-scene", window_lock, hvac_lock, "window", "HVAC"),
    ])
    # Fix: every thread acquires the locks in the SAME global order (HVAC first, then window)
    run("Same lock order", [
        ("night-scene  ", hvac_lock, window_lock, "HVAC", "window"),
        ("morning-scene", hvac_lock, window_lock, "HVAC", "window"),
    ])


if __name__ == "__main__":
    main()
