"""
Section B - Thread Pools & Futures
Section B.8 - Let's See How It Works: polling a whole building with a thread pool.

Every polling cycle the gateway:
  1. submits one read job per sensor to a bounded ThreadPoolExecutor;
  2. collects results in completion order, with a deadline for the whole cycle;
  3. classifies each failure (offline sensor, timeout) without stopping the cycle;
  4. submits actuator commands for the rooms that are too warm and waits for their ACKs.
"""

import logging
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-10s] %(message)s")
logger = logging.getLogger("building")

CYCLE_DEADLINE_S = 0.6
COOLING_THRESHOLD_C = 25.0


class SensorOfflineError(Exception):
    """The sensor did not answer on the field bus."""


@dataclass
class TemperatureSensor:
    """Simulated room sensor with configurable latency and failure behaviour."""
    sensor_id: str
    room: str
    latency_s: float
    offline: bool = False

    def read(self) -> float:
        time.sleep(self.latency_s)
        if self.offline:
            raise SensorOfflineError(f"{self.sensor_id} is offline")
        return round(random.Random(self.sensor_id).uniform(20.0, 28.0), 1)


@dataclass
class CycleReport:
    """Outcome of one polling cycle."""
    readings: dict[str, float] = field(default_factory=dict)   # room -> temperature
    errors: dict[str, str] = field(default_factory=dict)       # sensor id -> reason


def build_sensors() -> list[TemperatureSensor]:
    sensors = [TemperatureSensor(f"temp-{i:02d}", f"room-{100 + i}", latency_s=0.05 + 0.03 * i)
               for i in range(1, 11)]
    sensors[3].offline = True           # temp-04 is broken
    sensors[9].latency_s = 1.0          # temp-10 is extremely slow
    return sensors


def poll_building(executor: ThreadPoolExecutor, sensors: list[TemperatureSensor]) -> CycleReport:
    report = CycleReport()
    futures: dict[Future[float], TemperatureSensor] = {executor.submit(s.read): s for s in sensors}
    try:
        for future in as_completed(futures, timeout=CYCLE_DEADLINE_S):
            sensor = futures[future]
            try:
                report.readings[sensor.room] = future.result()
            except SensorOfflineError as exc:
                report.errors[sensor.sensor_id] = str(exc)
                logger.warning("%s", exc)
    except FuturesTimeoutError:
        for future, sensor in futures.items():
            if not future.done():
                future.cancel()        # only succeeds if the job has not started yet
                report.errors[sensor.sensor_id] = f"no answer within {CYCLE_DEADLINE_S}s"
                logger.warning("%s: timeout", sensor.sensor_id)
    return report


def set_hvac_mode(room: str, mode: str) -> str:
    time.sleep(0.1)                    # simulated command to the room's HVAC unit
    logger.info("hvac of %s -> %s", room, mode)
    return room


def main() -> None:
    sensors = build_sensors()
    start = time.perf_counter()
    # A bounded pool: never more than 8 concurrent requests on the building network
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="worker") as executor:
        report = poll_building(executor, sensors)
        logger.info("cycle: %d readings, %d errors in %.2fs",
                    len(report.readings), len(report.errors), time.perf_counter() - start)

        warm_rooms = [room for room, t in report.readings.items() if t > COOLING_THRESHOLD_C]
        commands = [executor.submit(set_hvac_mode, room, "COOLING") for room in warm_rooms]
        acked = [f.result() for f in commands]
        logger.info("cooling enabled in %s", sorted(acked))

        # Leaving the with-block calls shutdown(wait=True): it waits for the temp-10 read,
        # which is still RUNNING. A running job cannot be interrupted from outside: this is
        # why real device drivers must enforce their own I/O timeout (e.g. socket timeouts).

    for room, temperature in sorted(report.readings.items()):
        logger.info("%s: %.1f C", room, temperature)
    logger.info("errors: %s", report.errors)


if __name__ == "__main__":
    main()
