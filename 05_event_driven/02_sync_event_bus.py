"""
Section E - Event-Driven Architectures
Section E.3 - Step 1: a minimal synchronous, in-memory event bus.

Compared with the Observer pattern, publishers and subscribers no longer know each other:
both only know the BUS and the name of the event TYPE (the topic).
"""

import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Event:
    """An immutable fact that happened in the system."""
    type: str                                       # e.g. "sensor.temperature"
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


Handler = Callable[[Event], None]


class SyncEventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        # still synchronous: every handler runs in the publisher's thread, now
        for handler in self._handlers.get(event.type, []):
            handler(event)


# --- three independent components, each knows only the bus ---

def dashboard(event: Event) -> None:
    print(f"[dashboard] {event.type}: {event.payload}")


def co2_rule(event: Event) -> None:
    if event.payload["ppm"] > 1000:
        print(f"[co2-rule]  {event.payload['room']}: CO2 too high -> ventilation needed")


def main() -> None:
    bus = SyncEventBus()
    bus.subscribe("sensor.temperature", dashboard)
    bus.subscribe("sensor.co2", dashboard)
    bus.subscribe("sensor.co2", co2_rule)

    bus.publish(Event("sensor.temperature", {"room": "room-101", "celsius": 22.4}))
    bus.publish(Event("sensor.co2", {"room": "room-101", "ppm": 1180}))
    bus.publish(Event("sensor.humidity", {"room": "room-101", "percent": 51}))   # nobody listens


if __name__ == "__main__":
    main()
