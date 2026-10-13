"""
Lecture 04 - The queue Module
Section 7 - Let's See How It Works: a multi-stage telemetry pipeline (Python 3.13+).

    [acquisition threads] --raw_q--> [processing thread] --uplink_q (priority)--> [uplink thread]

  * Acquisition: one thread per sensor, produces raw Readings.
  * Processing : drops invalid readings, emits an ALARM immediately when a threshold is
                 crossed, and aggregates the others into 1 average every WINDOW readings.
  * Uplink     : sends messages to the cloud, ALARMs first (PriorityQueue).
  * Shutdown   : cascades stage by stage with Queue.shutdown(): each stage drains its input,
                 flushes what it holds, and shuts down its output queue.
"""

import itertools
import logging
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-11s] %(message)s")
logger = logging.getLogger("pipeline")

WINDOW = 5                                   # readings per aggregate
VALID_RANGE = (-40.0, 125.0)                 # physical range of the temperature sensor
ALARM_THRESHOLD_C = 30.0


@dataclass
class Reading:
    sensor_id: str
    value: float


class Priority(IntEnum):
    ALARM = 0
    AGGREGATE = 1


_seq = itertools.count()


@dataclass(order=True)
class UplinkMessage:
    priority: Priority
    seq: int = field(default_factory=lambda: next(_seq))
    body: str = field(default="", compare=False)


def acquisition(sensor_id: str, samples: list[float], raw_q: queue.Queue[Reading]) -> None:
    """Stage 1: emit the (simulated) samples of one sensor at 50 Hz."""
    for value in samples:
        time.sleep(0.02)
        raw_q.put(Reading(sensor_id, value))
    logger.info("done, %d samples produced", len(samples))


def processing(raw_q: queue.Queue[Reading], uplink_q: queue.PriorityQueue[UplinkMessage]) -> None:
    """Stage 2: validate, raise alarms, aggregate."""
    windows: dict[str, list[float]] = {}
    dropped = 0
    while True:
        try:
            reading = raw_q.get()
        except queue.ShutDown:
            break
        lo, hi = VALID_RANGE
        if not lo <= reading.value <= hi:
            dropped += 1                                          # e.g. a disconnected probe
        elif reading.value > ALARM_THRESHOLD_C:
            uplink_q.put(UplinkMessage(Priority.ALARM, body=f"ALARM {reading.sensor_id}={reading.value}"))
        else:
            window = windows.setdefault(reading.sensor_id, [])
            window.append(reading.value)
            if len(window) == WINDOW:
                avg = sum(window) / WINDOW
                uplink_q.put(UplinkMessage(Priority.AGGREGATE, body=f"avg {reading.sensor_id}={avg:.2f}"))
                window.clear()
        raw_q.task_done()

    # input exhausted: flush partial windows, then propagate the shutdown downstream
    for sensor_id, window in windows.items():
        if window:
            avg = sum(window) / len(window)
            uplink_q.put(UplinkMessage(Priority.AGGREGATE, body=f"avg {sensor_id}={avg:.2f} (partial)"))
    logger.info("input closed, %d invalid readings dropped", dropped)
    uplink_q.shutdown()                                           # graceful: uplink drains the rest


def uplink(uplink_q: queue.PriorityQueue[UplinkMessage], sent: list[str]) -> None:
    """Stage 3: send messages over a slow link, most urgent first."""
    while True:
        try:
            msg = uplink_q.get()
        except queue.ShutDown:
            break
        time.sleep(0.08)                                          # slow cellular link
        sent.append(msg.body)
        logger.info("sent %-9s %s", msg.priority.name, msg.body)
        uplink_q.task_done()
    logger.info("queue closed, %d messages sent", len(sent))


def make_samples(sensor_id: str, n: int) -> list[float]:
    rng = random.Random(sensor_id)
    samples = [round(rng.uniform(21.0, 23.0), 1) for _ in range(n)]
    samples[2] = -127.0                                       # an invalid reading (probe error)...
    samples[11] = 85.0 if sensor_id == "temp-02" else 31.5   # ...and a late spike -> ALARM
    return samples


def main() -> None:
    raw_q: queue.Queue[Reading] = queue.Queue(maxsize=20)
    uplink_q: queue.PriorityQueue[UplinkMessage] = queue.PriorityQueue()
    sent: list[str] = []

    producers = [threading.Thread(target=acquisition, args=(s, make_samples(s, 14), raw_q), name=s)
                 for s in ("temp-01", "temp-02")]
    processor = threading.Thread(target=processing, args=(raw_q, uplink_q), name="processing")
    sender = threading.Thread(target=uplink, args=(uplink_q, sent), name="uplink")

    for t in (*producers, processor, sender):
        t.start()

    for p in producers:                  # 1. wait for the sources to finish...
        p.join()
    raw_q.shutdown()                     # 2. ...then close the first queue: the shutdown cascades
    processor.join()
    sender.join()
    logger.info("pipeline stopped: %d messages delivered", len(sent))


if __name__ == "__main__":
    main()
