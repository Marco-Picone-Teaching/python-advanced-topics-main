"""
Lecture 01 - Concurrency Fundamentals & Threading
Section 6.5 - threading.local: data that is global in the code but private to each thread.

Each worker keeps its own (simulated) connection to the field bus. The same attribute
name `ctx.connection` refers to a DIFFERENT object in every thread.
"""

import threading

ctx = threading.local()


def get_connection() -> str:
    """Lazily create one connection per thread and reuse it afterwards."""
    if not hasattr(ctx, "connection"):
        ctx.connection = f"modbus-conn<{threading.current_thread().name}>"
        print(f"{threading.current_thread().name}: opened {ctx.connection}")
    return ctx.connection


def worker(requests: int) -> None:
    for _ in range(requests):
        conn = get_connection()         # opened only on the first call of each thread
    print(f"{threading.current_thread().name}: served {requests} requests with {conn}")


def main() -> None:
    threads = [threading.Thread(target=worker, args=(3,), name=f"worker-{i}") for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
