"""
Section B - Thread Pools & Futures
Section B.3.2 - concurrent.futures.wait() with FIRST_COMPLETED and ALL_COMPLETED.

Redundancy: a critical room has 3 replicated temperature sensors. We ask all of them
and use the FIRST answer (lowest latency), cancelling the others if possible.
Then we wait for a group of actuator commands with an overall deadline.
"""

import time
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, ThreadPoolExecutor, wait

REPLICAS = {"temp-A": (0.40, 21.4), "temp-B": (0.15, 21.6), "temp-C": (0.30, 21.5)}


def read_replica(sensor_id: str) -> tuple[str, float]:
    latency, value = REPLICAS[sensor_id]
    time.sleep(latency)
    return sensor_id, value


def send_command(actuator_id: str, latency: float) -> str:
    time.sleep(latency)
    return f"{actuator_id}: ACK"


def main() -> None:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(read_replica, s) for s in REPLICAS]
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        sensor_id, value = next(iter(done)).result()
        print(f"[{time.perf_counter() - start:.2f}s] first answer: {sensor_id} = {value} C "
              f"({len(not_done)} replicas still running)")
        for f in not_done:
            f.cancel()          # returns False: they are already running, we just ignore them

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        commands = {"light-01": 0.1, "blind-01": 0.3, "hvac-01": 2.0}   # hvac-01 is unresponsive
        futures = [executor.submit(send_command, a, lat) for a, lat in commands.items()]
        done, not_done = wait(futures, timeout=0.5, return_when=ALL_COMPLETED)
        print(f"[{time.perf_counter() - start:.2f}s] deadline reached: {len(done)} done, "
              f"{len(not_done)} still pending")
        for f in done:
            print(f"   {f.result()}")
        # leaving the `with` block still waits for hvac-01 (see Section B.5 on shutdown)
    print(f"[{time.perf_counter() - start:.2f}s] executor closed")


if __name__ == "__main__":
    main()
