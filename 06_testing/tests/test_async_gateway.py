"""
Section F.6: testing asyncio code with pytest-asyncio.

  * @pytest.mark.asyncio runs the test coroutine in a fresh event loop;
  * AsyncMock creates awaitable mocks (await mock() returns return_value / applies side_effect);
  * timeouts are tested with asyncio.Event-controlled fakes, not with long sleeps.
"""

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from smart_gateway.async_gateway import read_all, read_with_timeout, sample_forever


async def never_answers() -> float:
    """A sensor that hangs forever (until cancelled)."""
    await asyncio.Event().wait()
    return 0.0


@pytest.mark.asyncio
async def test_read_with_timeout_returns_value() -> None:
    read = AsyncMock(return_value=21.5)
    assert await read_with_timeout(read, timeout_s=1.0) == 21.5
    read.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_with_timeout_returns_none_on_timeout() -> None:
    assert await read_with_timeout(never_answers, timeout_s=0.01) is None


@pytest.mark.asyncio
async def test_timed_out_read_is_really_cancelled() -> None:
    cancelled = asyncio.Event()

    async def slow_sensor() -> float:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()                  # the sensor must be able to clean up
            raise
        return 0.0

    assert await read_with_timeout(slow_sensor, timeout_s=0.01) is None
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_read_all_slow_sensor_does_not_delay_the_others() -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    result = await read_all(
        {"temp-01": AsyncMock(return_value=21.5), "hum-01": AsyncMock(return_value=48.0),
         "co2-01": never_answers},
        timeout_s=0.05,
    )
    assert result == {"temp-01": 21.5, "hum-01": 48.0, "co2-01": None}
    assert loop.time() - start < 0.5          # bounded by the timeout, not by the slow sensor


@pytest.mark.asyncio
async def test_read_all_propagates_unexpected_errors() -> None:
    failing = AsyncMock(side_effect=ConnectionError("broker unreachable"))
    with pytest.raises(ExceptionGroup) as exc_info:           # raised by the TaskGroup
        await read_all({"temp-01": failing, "co2-01": never_answers}, timeout_s=1.0)
    assert exc_info.group_contains(ConnectionError)


@pytest.mark.asyncio
async def test_sample_forever_publishes_until_cancelled() -> None:
    received: asyncio.Queue[float] = asyncio.Queue()
    read = AsyncMock(side_effect=[20.0, 20.5, 21.0, 21.5])

    task = asyncio.create_task(sample_forever(read, publish=received.put, period_s=0))
    samples = [await asyncio.wait_for(received.get(), timeout=1.0) for _ in range(3)]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert samples == [20.0, 20.5, 21.0]


@pytest_asyncio.fixture          # async fixtures need pytest_asyncio.fixture in "strict" mode
async def running_sampler() -> AsyncIterator[list[float]]:
    """An ASYNC fixture: starts a background sampling task and cancels it at teardown."""
    published: list[float] = []

    async def publish(value: float) -> None:
        published.append(value)

    task = asyncio.create_task(sample_forever(AsyncMock(return_value=22.0), publish, period_s=0.001))
    yield published
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_async_fixture(running_sampler: list[float]) -> None:
    async with asyncio.timeout(1.0):
        while len(running_sampler) < 3:
            await asyncio.sleep(0.001)
    assert set(running_sampler) == {22.0}
