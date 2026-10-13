"""
Sensors whose dependencies are INJECTED (driver, clock, bus client), so that tests can
replace them with test doubles instead of real hardware, real time, or a real event bus.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from smart_gateway.alarm_rules import ThresholdRule, adc_to_celsius


class SensorReadError(Exception):
    """The driver could not read the sensor (bus error, CRC error, timeout...)."""


class SensorDriver(Protocol):
    """Low-level access to the hardware: returns the raw ADC value."""

    def read_raw(self) -> int: ...


class Publisher(Protocol):
    """Anything that can publish an event, e.g. the EventBusClient of Section E."""

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None: ...


@dataclass(frozen=True)
class Reading:
    sensor_id: str
    celsius: float
    timestamp: float


class TemperatureSensor:
    """Reads a temperature through a driver, retrying transient errors."""

    def __init__(self, sensor_id: str, driver: SensorDriver,
                 clock: Callable[[], float] | None = None, retries: int = 2) -> None:
        self.sensor_id = sensor_id
        self._driver = driver
        # NOT `clock=time.time` as default argument: defaults are evaluated once, when the
        # function is defined, so a later patch of time.time would never be seen.
        self._clock = clock if clock is not None else time.time
        self._retries = retries

    def read(self) -> Reading:
        last_error: SensorReadError | None = None
        for _ in range(1 + self._retries):
            try:
                raw = self._driver.read_raw()
            except SensorReadError as exc:
                last_error = exc                     # transient error: try again
                continue
            return Reading(self.sensor_id, round(adc_to_celsius(raw), 2), self._clock())
        raise SensorReadError(f"{self.sensor_id}: failed after {1 + self._retries} attempts") from last_error


class TemperatureMonitor:
    """Reads a sensor, evaluates a threshold rule and publishes alarm events on state changes."""

    def __init__(self, sensor: TemperatureSensor, rule: ThresholdRule, publisher: Publisher) -> None:
        self._sensor = sensor
        self._rule = rule
        self._publisher = publisher

    def check(self) -> Reading:
        reading = self._sensor.read()
        was_active = self._rule.active
        is_active = self._rule.evaluate(reading.celsius)
        if is_active and not was_active:
            self._publisher.write("alarm.temperature.raised",
                                  {"sensor": reading.sensor_id, "celsius": reading.celsius})
        elif was_active and not is_active:
            self._publisher.write("alarm.temperature.cleared", {"sensor": reading.sensor_id})
        return reading
