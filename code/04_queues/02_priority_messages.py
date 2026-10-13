"""
Lecture 04 - The queue Module
Section 2.2 - PriorityQueue with dataclasses: stable ordering and non-comparable payloads.

Problem: with (priority, payload) tuples, two items with the SAME priority are compared on
the payload. If the payload is a dict or a custom object, Python raises TypeError.
Solution: a dataclass(order=True) with a monotonic sequence number as tie-breaker and the
payload excluded from comparisons.
"""

import itertools
import queue
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class Priority(IntEnum):
    """Lower value = more urgent (PriorityQueue extracts the smallest item first)."""
    ALARM = 0
    COMMAND = 1
    TELEMETRY = 2


_sequence = itertools.count()          # thread-safe enough for a counter (next() is atomic in CPython)


@dataclass(order=True)
class GatewayMessage:
    priority: Priority
    seq: int = field(default_factory=lambda: next(_sequence))   # FIFO order among equal priorities
    payload: dict[str, Any] = field(default_factory=dict, compare=False)


def main() -> None:
    # 1. The problem: tuples with equal priority compare the payloads
    broken: queue.PriorityQueue[tuple[int, dict[str, Any]]] = queue.PriorityQueue()
    broken.put((2, {"sensor": "temp-01", "value": 21.5}))
    try:
        broken.put((2, {"sensor": "temp-02", "value": 22.1}))
    except TypeError as exc:
        print(f"tuple with dict payload -> TypeError: {exc}")

    # 2. The solution
    q: queue.PriorityQueue[GatewayMessage] = queue.PriorityQueue()
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "temp-01", "value": 21.5}))
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "temp-02", "value": 22.1}))
    q.put(GatewayMessage(Priority.COMMAND, payload={"actuator": "hvac-01", "mode": "COOLING"}))
    q.put(GatewayMessage(Priority.ALARM, payload={"sensor": "smoke-04", "room": "room-104"}))
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "hum-01", "value": 48.0}))

    while not q.empty():
        msg = q.get()
        print(f"{msg.priority.name:9} seq={msg.seq} {msg.payload}")


if __name__ == "__main__":
    main()
