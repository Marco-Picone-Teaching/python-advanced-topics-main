"""
Section A - Concurrency Fundamentals & Threading
Section A.9 - Let's See How It Works: a multithreaded Smart Room gateway.

Putting everything together:
  * one polling thread per sensor (Thread subclass), each with its own sampling period;
  * a shared, lock-protected RoomState holding the latest reading of every sensor;
  * a controller thread that evaluates a simple rule and drives an HVAC actuator;
  * an Event used as the shared stop signal for a graceful shutdown;
  * logging with thread names instead of print().
"""

import logging
import random
import threading
import time
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(relativeCreated)6.0fms [%(threadName)-12s] %(message)s",
)
logger = logging.getLogger("gateway")


@dataclass
class Reading:
    """A single measurement produced by a sensor."""
    sensor_id: str
    value: float
    unit: str
    timestamp: float


class RoomState:
    """Latest reading of every sensor of the room. Thread-safe."""

    def __init__(self) -> None:
        self._latest: dict[str, Reading] = {}
        self._lock = threading.Lock()

    def update(self, reading: Reading) -> None:
        with self._lock:
            self._latest[reading.sensor_id] = reading

    def snapshot(self) -> dict[str, Reading]:
        """Return a COPY, so callers can iterate without holding the lock."""
        with self._lock:
            return dict(self._latest)


class SimulatedSensor:
    """A sensor whose read() blocks for a while, like a real field-bus request."""

    def __init__(self, sensor_id: str, unit: str, base: float, drift: float) -> None:
        self.sensor_id = sensor_id
        self.unit = unit
        self._value = base
        self._drift = drift
        self._rng = random.Random(sensor_id)

    def read(self) -> Reading:
        time.sleep(0.05)                                     # simulated I/O latency
        self._value += self._drift + self._rng.uniform(-0.1, 0.1)
        return Reading(self.sensor_id, round(self._value, 2), self.unit, time.time())


class HVACActuator:
    """A (simulated) HVAC unit that can be switched between modes."""

    def __init__(self, actuator_id: str) -> None:
        self.actuator_id = actuator_id
        self.mode = "OFF"

    def set_mode(self, mode: str) -> None:
        if mode != self.mode:
            logger.info("%s: %s -> %s", self.actuator_id, self.mode, mode)
            self.mode = mode


class SensorPoller(threading.Thread):
    """Periodically reads one sensor and stores the value in the shared RoomState."""

    def __init__(self, sensor: SimulatedSensor, period_s: float,
                 state: RoomState, stop_event: threading.Event) -> None:
        super().__init__(name=sensor.sensor_id)
        self.sensor = sensor
        self.period_s = period_s
        self.state = state
        self.stop_event = stop_event

    def run(self) -> None:
        while not self.stop_event.is_set():
            reading = self.sensor.read()
            self.state.update(reading)
            logger.info("%s = %.2f %s", reading.sensor_id, reading.value, reading.unit)
            self.stop_event.wait(self.period_s)
        logger.info("poller stopped")


class ClimateController(threading.Thread):
    """Reads the room state and applies the rule: temperature > 24 C -> COOLING."""

    def __init__(self, state: RoomState, hvac: HVACActuator,
                 stop_event: threading.Event, threshold_c: float = 24.0) -> None:
        super().__init__(name="controller")
        self.state = state
        self.hvac = hvac
        self.stop_event = stop_event
        self.threshold_c = threshold_c

    def run(self) -> None:
        while not self.stop_event.wait(0.25):     # evaluate the rule every 250 ms
            temperature = self.state.snapshot().get("temp-01")
            if temperature is not None:
                self.hvac.set_mode("COOLING" if temperature.value > self.threshold_c else "OFF")
        logger.info("controller stopped")


def main() -> None:
    state = RoomState()
    stop_event = threading.Event()
    hvac = HVACActuator("hvac-01")

    threads: list[threading.Thread] = [
        SensorPoller(SimulatedSensor("temp-01", "C", base=23.5, drift=0.2), 0.2, state, stop_event),
        SensorPoller(SimulatedSensor("hum-01", "%", base=45.0, drift=0.5), 0.4, state, stop_event),
        ClimateController(state, hvac, stop_event),
    ]
    for t in threads:
        t.start()

    time.sleep(1.5)                     # let the gateway run for a while
    logger.info("shutdown requested")
    stop_event.set()
    for t in threads:
        t.join(timeout=2.0)

    for sensor_id, reading in state.snapshot().items():
        logger.info("final %s = %.2f %s", sensor_id, reading.value, reading.unit)
    logger.info("final HVAC mode = %s", hvac.mode)


if __name__ == "__main__":
    main()
