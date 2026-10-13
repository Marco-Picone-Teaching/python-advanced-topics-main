"""
Section C - Asyncio
Section C.2 - Coroutine functions, coroutine objects and asyncio.run().

Calling an `async def` function does NOT run its body: it creates a coroutine object.
The body runs only when the coroutine is awaited (or wrapped in a Task) inside an event loop.
"""

import asyncio


async def read_temperature(sensor_id: str) -> float:
    """Coroutine: simulate a non-blocking read of a temperature sensor."""
    print(f"  {sensor_id}: request sent, waiting for the answer...")
    await asyncio.sleep(0.5)           # suspends THIS coroutine, the event loop is free
    print(f"  {sensor_id}: answer received")
    return 21.7


async def main() -> None:
    # 1. Calling the coroutine function only creates a coroutine object
    coro = read_temperature("temp-01")
    print(f"read_temperature('temp-01') returned: {coro!r}")

    # 2. Awaiting it runs the body and gives back the return value
    value = await coro
    print(f"awaited value: {value}")


if __name__ == "__main__":
    # asyncio.run() creates the event loop, runs main() until it completes, then closes the loop
    asyncio.run(main())
