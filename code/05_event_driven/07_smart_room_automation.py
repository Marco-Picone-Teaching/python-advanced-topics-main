"""
Lecture 05 - Event-Driven Architectures
Section 8 - Let's See How It Works: an event-driven Smart Room.

    sensors ──sensor.*──► RuleEngine ──actuator.<name>.command──► actuators ──actuator.<name>.state──► Dashboard
                              └────────alarm.*──────────────────► NotificationService

No component references another one: they only share the bus and the event names.
"""

import logging
import threading
import time
from typing import Any

from event_bus import Event, EventBus, EventBusClient, EventHandler

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-12s] %(message)s")
logger = logging.getLogger("smart-room")

ROOM = "room-101"


# ------------------------------------------------------------------ sensors (publishers)
def sensor_publisher(client: EventBusClient, event_type: str, key: str,
                     values: list[Any], period_s: float) -> None:
    """Publish a scripted sequence of readings, one every period_s seconds."""
    for value in values:
        client.write(event_type, {"room": ROOM, key: value})
        time.sleep(period_s)


# ------------------------------------------------------------------ rule engine
class RuleEngine(EventHandler):
    """Turns sensor events into actuator commands and alarms."""

    TEMP_HIGH_C = 25.0
    CO2_HIGH_PPM = 1000
    CO2_ALARM_PPM = 1200

    def __init__(self, client: EventBusClient) -> None:
        self._client = client
        self._lock = threading.Lock()          # handle() runs concurrently in several bus workers
        self._co2_alarm_active = False

    def handle(self, event: Event) -> None:
        match event.type:
            case "sensor.temperature":
                mode = "COOLING" if event.payload["celsius"] > self.TEMP_HIGH_C else "OFF"
                self._client.write("actuator.hvac.command", {"room": ROOM, "mode": mode})
            case "sensor.co2":
                ppm = event.payload["ppm"]
                state = "OPEN" if ppm > self.CO2_HIGH_PPM else "CLOSED"
                self._client.write("actuator.window.command", {"room": ROOM, "state": state})
                with self._lock:                   # check-then-act on shared state: needs the lock
                    raise_alarm = ppm > self.CO2_ALARM_PPM and not self._co2_alarm_active
                    if raise_alarm:
                        self._co2_alarm_active = True
                if raise_alarm:
                    self._client.write("alarm.co2", {"room": ROOM, "ppm": ppm})
            case "sensor.presence":
                state = "ON" if event.payload["present"] else "OFF"
                self._client.write("actuator.light.command", {"room": ROOM, "state": state})


# ------------------------------------------------------------------ actuators (subscribers + publishers)
class Actuator(EventHandler):
    """Executes commands and publishes its new state only when it actually changes."""

    def __init__(self, name: str, state_key: str, initial: str, client: EventBusClient) -> None:
        self.name = name
        self.state_key = state_key
        self.state = initial
        self._client = client
        self._lock = threading.Lock()

    def handle(self, event: Event) -> None:
        requested = event.payload[self.state_key]
        with self._lock:
            if requested == self.state:
                return                                         # idempotent: nothing to do
            time.sleep(0.02)                                   # command sent to the device
            old, self.state = self.state, requested
        self._client.write(f"actuator.{self.name}.state", {"room": ROOM, "from": old, "to": requested})


# ------------------------------------------------------------------ observers
class Dashboard(EventHandler):
    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %-22s %s -> %s", event.type, event.payload["from"], event.payload["to"])


class NotificationService(EventHandler):
    def handle(self, event: Event) -> None:
        logger.warning("[notify] %s in %s: %s  (sending push notification to the facility manager)",
                       event.type, event.payload["room"], event.payload)


def main() -> None:
    with EventBus(num_workers=3) as bus:
        # each component gets its own client, labelled with its name (event.source)
        rules = RuleEngine(EventBusClient(bus, "rule-engine"))
        hvac = Actuator("hvac", "mode", "OFF", EventBusClient(bus, "hvac-01"))
        window = Actuator("window", "state", "CLOSED", EventBusClient(bus, "window-01"))
        light = Actuator("light", "state", "OFF", EventBusClient(bus, "light-01"))

        wiring = EventBusClient(bus, "bootstrap")
        wiring.read("sensor.*", rules)
        wiring.read("actuator.hvac.command", hvac)
        wiring.read("actuator.window.command", window)
        wiring.read("actuator.light.command", light)
        wiring.read("actuator.*.state", Dashboard())
        wiring.read("alarm.*", NotificationService())

        publishers = [
            threading.Thread(target=sensor_publisher, name="temp-01", args=(
                EventBusClient(bus, "temp-01"), "sensor.temperature", "celsius",
                [24.2, 24.8, 25.3, 25.6, 25.1, 24.7], 0.1)),
            threading.Thread(target=sensor_publisher, name="co2-01", args=(
                EventBusClient(bus, "co2-01"), "sensor.co2", "ppm",
                [820, 950, 1080, 1250, 1150, 980], 0.1)),
            threading.Thread(target=sensor_publisher, name="pir-01", args=(
                EventBusClient(bus, "pir-01"), "sensor.presence", "present",
                [True, True, True, False, False, False], 0.1)),
        ]
        for p in publishers:
            p.start()
        for p in publishers:
            p.join()
        bus.wait_until_idle()

    logger.info("final state: hvac=%s window=%s light=%s", hvac.state, window.state, light.state)
    logger.info("bus stats: %s", bus.stats)


if __name__ == "__main__":
    main()
