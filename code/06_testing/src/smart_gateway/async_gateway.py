"""asyncio helpers of the gateway (Lecture 03)."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping

AsyncReader = Callable[[], Awaitable[float]]


async def read_with_timeout(read: AsyncReader, timeout_s: float) -> float | None:
    """Return the value read, or None if the sensor does not answer within timeout_s."""
    try:
        async with asyncio.timeout(timeout_s):
            return await read()
    except TimeoutError:
        return None


async def read_all(readers: Mapping[str, AsyncReader], timeout_s: float) -> dict[str, float | None]:
    """Read every sensor concurrently; a slow sensor yields None instead of delaying the others."""
    async with asyncio.TaskGroup() as tg:
        tasks = {sid: tg.create_task(read_with_timeout(read, timeout_s)) for sid, read in readers.items()}
    return {sid: task.result() for sid, task in tasks.items()}


async def sample_forever(read: AsyncReader, publish: Callable[[float], Awaitable[None]],
                         period_s: float) -> None:
    """Read and publish every period_s seconds, until the task is cancelled."""
    while True:
        await publish(await read())
        await asyncio.sleep(period_s)
