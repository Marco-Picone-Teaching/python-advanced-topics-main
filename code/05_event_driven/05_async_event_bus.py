"""
Lecture 05 - Event-Driven Architectures
Section 6 - Using the asyncio event bus: the example of 03 rewritten with coroutines.
"""

import asyncio
import logging
import time

from async_event_bus import AsyncEventBus, AsyncEventHandler
from event_bus import Event

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(taskName)-12s] %(message)s")
logger = logging.getLogger("demo")


class Dashboard(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s = %s", event.type, event.payload)


class CloudLogger(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        await asyncio.sleep(0.3)                  # async HTTP call: the loop keeps running
        logger.info("[cloud] stored %s", event.payload)


async def main() -> None:
    start = time.perf_counter()
    async with AsyncEventBus(num_workers=4) as bus:
        bus.subscribe("sensor.*", Dashboard())
        bus.subscribe("sensor.temperature", CloudLogger())
        for value in (21.0, 21.2, 21.4):
            await bus.publish(Event("sensor.temperature", {"sensor": "temp-01", "celsius": value}))
            logger.info("sensor: published %.1f", value)
            await asyncio.sleep(0.1)
        await bus.wait_until_idle()
    logger.info("all events dispatched in %.2fs, stats: %s", time.perf_counter() - start, bus.stats)


if __name__ == "__main__":
    asyncio.run(main())
