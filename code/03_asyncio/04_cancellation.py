"""
Lecture 03 - Asyncio
Section 3.3 - Cooperative cancellation: CancelledError is raised INSIDE the coroutine,
at the await where it is suspended, so it can clean up (try/finally).
"""

import asyncio


class Valve:
    """A water valve actuator that must never be left open."""

    def __init__(self) -> None:
        self.is_open = False

    async def open(self) -> None:
        await asyncio.sleep(0.05)
        self.is_open = True
        print("valve: OPEN")

    async def close(self) -> None:
        await asyncio.sleep(0.05)
        self.is_open = False
        print("valve: CLOSED")


async def irrigation_cycle(valve: Valve, duration_s: float) -> None:
    await valve.open()
    try:
        print(f"irrigating for {duration_s}s...")
        await asyncio.sleep(duration_s)          # <- CancelledError is raised HERE
        print("irrigation completed")
    except asyncio.CancelledError:
        print("irrigation cancelled (rain detected!)")
        raise                                     # always re-raise CancelledError
    finally:
        await valve.close()                       # cleanup ALWAYS runs


async def main() -> None:
    valve = Valve()
    task = asyncio.create_task(irrigation_cycle(valve, duration_s=10))
    await asyncio.sleep(0.3)                      # after a while the rain sensor fires...
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    print(f"task cancelled: {task.cancelled()}, valve open: {valve.is_open}")


if __name__ == "__main__":
    asyncio.run(main())
