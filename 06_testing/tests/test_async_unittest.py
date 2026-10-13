"""
Section F.6.1: asyncio tests with the standard library only
(unittest.IsolatedAsyncioTestCase, no third-party plugin needed).
"""

import asyncio
import unittest
from unittest.mock import AsyncMock

from smart_gateway.async_gateway import read_all, read_with_timeout


class TestAsyncGateway(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        # runs inside the test's event loop, before every test
        self.fast_sensor = AsyncMock(return_value=21.5)

    async def test_value_is_returned(self) -> None:
        self.assertEqual(await read_with_timeout(self.fast_sensor, timeout_s=1.0), 21.5)

    async def test_timeout_returns_none(self) -> None:
        async def hanging() -> float:
            await asyncio.sleep(10)
            return 0.0

        self.assertIsNone(await read_with_timeout(hanging, timeout_s=0.01))

    async def test_read_all(self) -> None:
        result = await read_all({"temp-01": self.fast_sensor}, timeout_s=1.0)
        self.assertDictEqual(result, {"temp-01": 21.5})
        self.fast_sensor.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
