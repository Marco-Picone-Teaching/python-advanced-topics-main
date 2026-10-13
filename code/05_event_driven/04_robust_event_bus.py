"""
Lecture 05 - Event-Driven Architectures
Section 5 - Using the robust event bus defined in event_bus.py:
wildcards, unsubscribe, failing handlers, graceful stop.
"""

import logging

from event_bus import Event, EventBus, EventBusClient, EventHandler

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-12s] %(message)s")
logger = logging.getLogger("demo")


class AuditLog(EventHandler):
    """Subscribed to '*': records every event of the system."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def handle(self, event: Event) -> None:
        self.entries.append(f"{event.source} -> {event.type}")


class SensorDashboard(EventHandler):
    """Subscribed to 'sensor.*': shows every sensor reading."""

    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s %s", event.type, event.payload)


class BuggyPlugin(EventHandler):
    """A third-party plugin with a bug: it crashes on CO2 events."""

    def handle(self, event: Event) -> None:
        if event.type == "sensor.co2":
            raise KeyError("celsius")


def main() -> None:
    audit = AuditLog()
    with EventBus(num_workers=2) as bus:                  # start() ... stop()
        sensors = EventBusClient(bus, source="room-101-gateway")
        dashboard_sub = sensors.read("sensor.*", SensorDashboard())
        sensors.read("*", audit)
        sensors.read("sensor.co2", BuggyPlugin())

        sensors.write("sensor.temperature", {"celsius": 22.1})
        sensors.write("sensor.co2", {"ppm": 850})         # BuggyPlugin fails, the others are fine
        sensors.write("actuator.window.command", {"state": "OPEN"})
        bus.wait_until_idle()

        sensors.stop_reading(dashboard_sub)               # the dashboard is closed
        sensors.write("sensor.temperature", {"celsius": 22.3})   # not shown anymore
    # leaving the with-block: queued events are dispatched, workers stopped

    logger.info("audit log: %s", audit.entries)
    logger.info("stats: %s", bus.stats)


if __name__ == "__main__":
    main()
