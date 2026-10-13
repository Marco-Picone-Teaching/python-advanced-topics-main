"""
Section A - Concurrency Fundamentals & Threading
Section A.1.1 - The problem: a gateway that polls its sensors one after the other.

Each simulated sensor read takes READ_LATENCY_S seconds (think of a slow serial bus,
a Modbus request, or an HTTP call to a remote device). The gateway spends almost all
of its time *waiting*, not computing.
"""

import random
import time

# Simulated I/O latency of a single sensor read (seconds)
READ_LATENCY_S = 0.5

# The sensors installed in a room of our Smart Building
SENSOR_IDS = ["temp-01", "temp-02", "hum-01", "hum-02", "co2-01"]


def read_sensor(sensor_id: str) -> float:
    """Simulate a blocking read of a sensor and return its measurement."""
    time.sleep(READ_LATENCY_S)          # the gateway is idle while waiting for the device
    rng = random.Random(sensor_id)      # deterministic value per sensor (reproducible output)
    return round(rng.uniform(18.0, 30.0), 1)


def main() -> None:
    start = time.perf_counter()

    readings: dict[str, float] = {}
    for sensor_id in SENSOR_IDS:
        readings[sensor_id] = read_sensor(sensor_id)
        print(f"[{time.perf_counter() - start:4.2f}s] {sensor_id} -> {readings[sensor_id]}")

    elapsed = time.perf_counter() - start
    print(f"Polled {len(readings)} sensors in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
