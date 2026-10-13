"""
Section E - Event-Driven Architectures
Section E.2 - Recap of the Observer pattern (OOP lecture) and its limits.

The sensor (Subject) notifies its observers SYNCHRONOUSLY: notify() calls every update()
in the sensor's own thread. A single slow observer (a cloud logger doing an HTTP call)
delays the sensor's sampling loop, and an observer that raises breaks the loop entirely.
"""

import time
from typing import Protocol


class Observer(Protocol):
    """Anything with an update(sensor_id, value) method can observe a sensor (duck typing)."""

    def update(self, sensor_id: str, value: float) -> None: ...


class TemperatureSensor:
    """Subject: keeps a list of observers and notifies them on every new reading."""

    def __init__(self, sensor_id: str) -> None:
        self.sensor_id = sensor_id
        self._observers: list[Observer] = []

    def add_observer(self, observer: Observer) -> None:
        self._observers.append(observer)

    def publish_reading(self, value: float) -> None:
        for observer in self._observers:          # synchronous: one after the other
            observer.update(self.sensor_id, value)


class Dashboard:
    def update(self, sensor_id: str, value: float) -> None:
        print(f"    [dashboard] {sensor_id} = {value}")


class CloudLogger:
    def update(self, sensor_id: str, value: float) -> None:
        time.sleep(0.3)                           # slow HTTP request to the cloud
        print(f"    [cloud] {sensor_id} = {value} stored")


def main() -> None:
    sensor = TemperatureSensor("temp-01")
    sensor.add_observer(Dashboard())
    sensor.add_observer(CloudLogger())

    start = time.perf_counter()
    for value in (21.0, 21.2, 21.4):              # the sensor wants to sample every 0.1 s...
        t0 = time.perf_counter()
        sensor.publish_reading(value)
        print(f"[{time.perf_counter() - start:.2f}s] publish_reading() blocked the sensor "
              f"for {time.perf_counter() - t0:.2f}s")


if __name__ == "__main__":
    main()
