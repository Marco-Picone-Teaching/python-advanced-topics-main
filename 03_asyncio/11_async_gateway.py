"""
Section C - Asyncio
Section C.9 - Let's See How It Works: an asyncio Smart Room gateway.

The same Smart Room of Section A, rewritten with coroutines:
  * one sampling task per sensor (drift-free periodic loop, per-read timeout);
  * a rule engine task: CO2 > 1000 ppm -> open the window, temperature > 25 C -> HVAC COOLING;
  * a cloud uploader task limited by a Semaphore;
  * everything supervised by a TaskGroup and stopped by an asyncio.Event;
  * logging with %(taskName)s (Python 3.12+) to see which task produced each line.
"""

import asyncio
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(taskName)-12s] %(message)s")
logger = logging.getLogger("async-gateway")


@dataclass
class SimulatedSensor:
    """A sensor with a non-blocking read (e.g. over an async MQTT/HTTP client)."""
    sensor_id: str
    unit: str
    value: float
    drift: float
    latency_s: float = 0.05

    async def read(self) -> float:
        await asyncio.sleep(self.latency_s)
        self.value += self.drift                          # a slowly rising signal
        return round(self.value, 1)


class Actuator:
    """A simulated actuator whose commands take some time to be acknowledged."""

    def __init__(self, actuator_id: str, state: str) -> None:
        self.actuator_id = actuator_id
        self.state = state

    async def set_state(self, state: str) -> None:
        if state != self.state:
            await asyncio.sleep(0.05)                     # command round-trip
            logger.info("%s: %s -> %s", self.actuator_id, self.state, state)
            self.state = state


class AsyncGateway:
    def __init__(self, sensors: list[SimulatedSensor], period_s: float) -> None:
        self.sensors = sensors
        self.period_s = period_s
        self.latest: dict[str, float] = {}          # no lock needed: single thread, no await inside updates
        self.window = Actuator("window-01", "CLOSED")
        self.hvac = Actuator("hvac-01", "OFF")
        self.stop = asyncio.Event()
        self.upload_slots = asyncio.Semaphore(2)
        self.uploads = 0

    async def sample(self, sensor: SimulatedSensor) -> None:
        """Drift-free periodic sampling of one sensor, with a timeout on every read."""
        loop = asyncio.get_running_loop()
        deadline = loop.time()
        while not self.stop.is_set():
            try:
                async with asyncio.timeout(0.2):
                    self.latest[sensor.sensor_id] = await sensor.read()
            except TimeoutError:
                logger.warning("%s: read timeout", sensor.sensor_id)
            deadline += self.period_s
            # wait until the next deadline OR until stop is set, whichever comes first
            try:
                async with asyncio.timeout(max(0.0, deadline - loop.time())):
                    await self.stop.wait()
            except TimeoutError:
                pass
        logger.info("sampling stopped")

    async def rule_engine(self) -> None:
        while not self.stop.is_set():
            co2 = self.latest.get("co2-01")
            temp = self.latest.get("temp-01")
            if co2 is not None:
                await self.window.set_state("OPEN" if co2 > 1000 else "CLOSED")
            if temp is not None:
                await self.hvac.set_state("COOLING" if temp > 25.0 else "OFF")
            await asyncio.sleep(0.1)
        logger.info("rule engine stopped")

    async def upload(self, snapshot: dict[str, float]) -> None:
        async with self.upload_slots:
            await asyncio.sleep(0.15)                      # HTTP POST to the cloud
            self.uploads += 1

    async def cloud_uploader(self) -> None:
        async with asyncio.TaskGroup() as uploads:
            while not self.stop.is_set():
                await asyncio.sleep(0.3)
                uploads.create_task(self.upload(dict(self.latest)), name="upload")
        logger.info("uploader stopped after %d uploads", self.uploads)

    async def run(self, duration_s: float) -> None:
        async with asyncio.TaskGroup() as tg:
            for s in self.sensors:
                tg.create_task(self.sample(s), name=s.sensor_id)
            tg.create_task(self.rule_engine(), name="rules")
            tg.create_task(self.cloud_uploader(), name="uploader")
            await asyncio.sleep(duration_s)
            logger.info("shutdown requested")
            self.stop.set()
        # the TaskGroup exits only when every task has finished
        logger.info("final readings: %s", self.latest)
        logger.info("final actuators: window=%s hvac=%s", self.window.state, self.hvac.state)


async def main() -> None:
    sensors = [
        SimulatedSensor("temp-01", "C", value=24.4, drift=0.08),
        SimulatedSensor("hum-01", "%", value=45.0, drift=0.3),
        SimulatedSensor("co2-01", "ppm", value=900.0, drift=15.0),
    ]
    await AsyncGateway(sensors, period_s=0.1).run(duration_s=1.5)


if __name__ == "__main__":
    asyncio.run(main())
