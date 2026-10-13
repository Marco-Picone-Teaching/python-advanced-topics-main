<!-- omit in toc -->
# Asyncio

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - Asynchronous Programming with `asyncio`                 |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **Prerequisites:** [01 - Concurrency Fundamentals & Threading](01_concurrency_and_threading.md),
> [02 - Thread Pools & Futures](02_thread_pools_and_futures.md).
> **Runnable code:** [`code/03_asyncio/`](code/03_asyncio/).

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 From Threads to Coroutines](#1-from-threads-to-coroutines)
  - [1.1 Why Another Concurrency Model?](#11-why-another-concurrency-model)
  - [1.2 The Event Loop and Cooperative Scheduling](#12-the-event-loop-and-cooperative-scheduling)
- [2 Coroutines Step by Step](#2-coroutines-step-by-step)
  - [2.1 `async def`, `await` and `asyncio.run()`](#21-async-def-await-and-asynciorun)
- [3 Running Coroutines Concurrently](#3-running-coroutines-concurrently)
  - [3.1 Sequential `await` vs. `asyncio.gather()`](#31-sequential-await-vs-asynciogather)
  - [3.2 Tasks: Coroutines Running in the Background](#32-tasks-coroutines-running-in-the-background)
  - [3.3 Cancellation Is Cooperative (and Actually Works)](#33-cancellation-is-cooperative-and-actually-works)
- [4 Structured Concurrency with `TaskGroup`](#4-structured-concurrency-with-taskgroup)
  - [4.1 The Problem with Loose Tasks and `gather()`](#41-the-problem-with-loose-tasks-and-gather)
  - [4.2 `asyncio.TaskGroup` and `except*`](#42-asynciotaskgroup-and-except)
- [5 Timeouts and Periodic Tasks](#5-timeouts-and-periodic-tasks)
  - [5.1 `asyncio.timeout()` and `asyncio.wait_for()`](#51-asynciotimeout-and-asynciowait_for)
  - [5.2 Periodic Sampling Without Drift](#52-periodic-sampling-without-drift)
- [6 Blocking Code: the Number One Pitfall](#6-blocking-code-the-number-one-pitfall)
  - [6.1 A Blocking Call Freezes the Whole Loop](#61-a-blocking-call-freezes-the-whole-loop)
  - [6.2 Bridging Blocking Code: `asyncio.to_thread()` and `run_in_executor()`](#62-bridging-blocking-code-asyncioto_thread-and-run_in_executor)
- [7 Synchronization Primitives](#7-synchronization-primitives)
- [8 Choosing the Right Tool](#8-choosing-the-right-tool)
- [9 Let's See How It Works: Putting It All Together](#9-lets-see-how-it-works-putting-it-all-together)
- [10 References](#10-references)
<!-- /TOC -->

---

# 1 From Threads to Coroutines

## 1.1 Why Another Concurrency Model?

Threads work well for I/O-bound code, but the previous two lectures also showed their costs:

- every thread is an OS resource, so **thousands of concurrent operations** means thousands of threads
  or a bounded pool that makes requests wait;
- the OS can **preempt** a thread anywhere, so every piece of shared data needs a lock, and forgetting
  one produces race conditions that appear only occasionally;
- a running thread **cannot be stopped** from outside: we can only stop *waiting* for it.

`asyncio` takes a different approach: **a single thread** runs many tasks, and the tasks
**cooperate**, explicitly giving control back whenever they are about to wait for I/O.

## 1.2 The Event Loop and Cooperative Scheduling

The heart of `asyncio` is the **event loop**: a scheduler that runs in one thread and keeps a list of
tasks that are ready to run and tasks waiting for something (a socket to become readable, a timer to
expire...).

```
                ┌──────────────────────── Event loop (1 thread) ─────────────────────┐
                │                                                                    │
  ready tasks:  │  run task A until its next `await` ──► A is now waiting for I/O    │
                │  run task B until its next `await` ──► B is now waiting for a timer│
                │  nothing ready? ask the OS: "wake me up when any I/O is ready"     │
                │  I/O for A is ready ──► A is ready again ──► resume A after `await`│
                └────────────────────────────────────────────────────────────────────┘
```

Two consequences are fundamental:

- **Switching happens only at `await`.** Between two `await`s a task runs without interruption,
  so a whole class of race conditions simply disappears (but not all of them, see
  [Section 7](#7-synchronization-primitives)).
- **A task that never awaits blocks everybody.** If a task performs a long computation or calls a
  blocking function, the loop cannot switch to anything else ([Section 6](#6-blocking-code-the-number-one-pitfall)).

---

# 2 Coroutines Step by Step

## 2.1 `async def`, `await` and `asyncio.run()`

- A function defined with **`async def`** is a *coroutine function*.
- Inside it, **`await x`** suspends the coroutine until the *awaitable* `x` (another coroutine, a
  task, a future) has completed, letting the event loop run other tasks in the meantime.
- **`asyncio.run(main())`** is the standard entry point: it creates an event loop, runs the `main()`
  coroutine until it completes, and closes the loop.

The key difference with a normal function is what happens when you **call** it:

| | Normal function | Coroutine function (`async def`) |
|---|---|---|
| Calling `f()` | Runs the body immediately | Runs **nothing**: returns a **coroutine object** |
| Actual execution | Immediate | When the coroutine is **awaited** or wrapped in a **Task** |
| Can it suspend? | No | Yes, at every `await` (e.g. while waiting for I/O) |
| Concurrency | None (it blocks the thread) | Cooperative: other coroutines run while it is suspended |

**IoT example:** a coroutine that simulates a non-blocking read of a temperature sensor.


`code/03_asyncio/01_first_coroutine.py`
```python
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
```

**Output:**
```
read_temperature('temp-01') returned: <coroutine object read_temperature at 0x7a2151197060>
  temp-01: request sent, waiting for the answer...
  temp-01: answer received
awaited value: 21.7
```

**Key Points:**
- The first `print` inside `read_temperature` appears only **after** the coroutine object has been
  created and printed: calling the function did not execute it.
- `await` can only be used **inside** an `async def` function. The bridge between normal code and
  asynchronous code is `asyncio.run()`.
- `asyncio.sleep()` is the non-blocking equivalent of `time.sleep()`: it suspends the coroutine and
  lets the loop do other work. In real code this place is taken by an async network call (an async
  MQTT client, an HTTP request with `aiohttp`/`httpx`, a socket read...).

> **Note:** if you create a coroutine object and never await it, Python emits a
> `RuntimeWarning: coroutine '...' was never awaited`. It almost always means a forgotten `await`.

---

# 3 Running Coroutines Concurrently

## 3.1 Sequential `await` vs. `asyncio.gather()`

Awaiting coroutines one after the other is **still sequential**: `await` waits for the result
before moving on. To run them concurrently we must hand them all to the event loop at once.
`asyncio.gather(*aws)` does exactly that, and returns the list of results **in the order of the
arguments**:

**IoT example:** reading three sensors with different latencies, first with sequential `await`s and then with
`gather()`.


`code/03_asyncio/02_sequential_vs_gather.py`
```python
import asyncio
import time

# sensor id -> (latency in seconds, value)
SENSORS = {"temp-01": (0.6, 21.5), "hum-01": (0.3, 47.0), "co2-01": (0.9, 612.0)}


async def read_sensor(sensor_id: str, start: float) -> float:
    latency, value = SENSORS[sensor_id]
    print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: start")
    await asyncio.sleep(latency)                  # non-blocking wait (simulated I/O)
    print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: done -> {value}")
    return value


async def sequential() -> None:
    start = time.perf_counter()
    # Each await waits for the previous read to finish: no concurrency at all
    results = [await read_sensor(s, start) for s in SENSORS]
    print(f"sequential: {results} in {time.perf_counter() - start:.2f}s\n")


async def concurrent() -> None:
    start = time.perf_counter()
    # gather() schedules all the coroutines at once and waits for all of them
    results = await asyncio.gather(*(read_sensor(s, start) for s in SENSORS))
    print(f"gather    : {results} in {time.perf_counter() - start:.2f}s")


async def main() -> None:
    await sequential()
    await concurrent()


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
[0.00s] temp-01: start
[0.60s] temp-01: done -> 21.5
[0.60s] hum-01: start
[0.90s] hum-01: done -> 47.0
[0.90s] co2-01: start
[1.80s] co2-01: done -> 612.0
sequential: [21.5, 47.0, 612.0] in 1.80s

[0.00s] temp-01: start
[0.00s] hum-01: start
[0.00s] co2-01: start
[0.30s] hum-01: done -> 47.0
[0.60s] temp-01: done -> 21.5
[0.90s] co2-01: done -> 612.0
gather    : [21.5, 47.0, 612.0] in 0.90s
```

**Key Points:**
- With `gather()` the three reads **start together**, finish in order of latency, and the total time
  is the **slowest** latency (0.9 s), exactly like the threaded version of Lecture 01, but with **a
  single thread**.
- The returned list follows the **argument order**, not the completion order (like
  `executor.map()` in Lecture 02).

## 3.2 Tasks: Coroutines Running in the Background

`asyncio.create_task(coro)` wraps a coroutine in a **`Task`** and schedules it on the loop
**immediately**. The caller does not wait: it gets a `Task` object (a subclass of `Future`) and can
continue with other work, awaiting the task later, or never.

**IoT example:** a heartbeat that runs in the background while a sensor is being calibrated.


`code/03_asyncio/03_create_task.py`
```python
import asyncio
import time


async def heartbeat(start: float) -> None:
    """Publish a heartbeat every 0.25 s, forever (until cancelled)."""
    while True:
        print(f"[{time.perf_counter() - start:.2f}s]   heartbeat")
        await asyncio.sleep(0.25)


async def calibrate_sensor(sensor_id: str, start: float) -> str:
    print(f"[{time.perf_counter() - start:.2f}s] calibrating {sensor_id}...")
    await asyncio.sleep(0.6)
    return f"{sensor_id} calibrated"


async def main() -> None:
    start = time.perf_counter()

    # create_task() schedules the coroutine on the event loop IMMEDIATELY and returns a Task
    hb_task = asyncio.create_task(heartbeat(start), name="heartbeat")
    cal_task = asyncio.create_task(calibrate_sensor("temp-01", start), name="calibration")
    print(f"tasks created: {hb_task.get_name()}, {cal_task.get_name()} (main keeps running)")

    result = await cal_task                 # wait only for the calibration
    print(f"[{time.perf_counter() - start:.2f}s] {result}; heartbeat done? {hb_task.done()}")

    hb_task.cancel()                        # stop the background task
    try:
        await hb_task
    except asyncio.CancelledError:
        print(f"[{time.perf_counter() - start:.2f}s] heartbeat cancelled: {hb_task.cancelled()}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
tasks created: heartbeat, calibration (main keeps running)
[0.00s]   heartbeat
[0.00s] calibrating temp-01...
[0.25s]   heartbeat
[0.50s]   heartbeat
[0.60s] temp-01 calibrated; heartbeat done? False
[0.60s] heartbeat cancelled: True
```

**Key Points:**
- The tasks start running as soon as `main()` reaches its first `await`, which gives control back to
  the loop.
- A `Task` has the same interface as the futures of Lecture 02: `done()`, `result()`, `cancel()`,
  `cancelled()`, `add_done_callback()`, plus a `name` for debugging.
- **Keep a reference to every task you create.** The event loop holds only a *weak* reference to
  tasks: a task that nobody references can be garbage-collected before it finishes. Storing tasks in
  a variable, a set, or (better) creating them in a `TaskGroup` avoids the problem.

## 3.3 Cancellation Is Cooperative (and Actually Works)

Remember the limit of threads and executors: a running job **cannot be stopped**. With asyncio,
`task.cancel()` **really stops** the task: it raises `asyncio.CancelledError` **inside** the
coroutine, at the `await` where it is currently suspended. The coroutine can react with
`try/except/finally` to clean up.

**IoT example:** an irrigation cycle that opens a water valve and must **always** close it, even when the cycle
is cancelled because rain has been detected.


`code/03_asyncio/04_cancellation.py`
```python
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
```

**Output:**
```
valve: OPEN
irrigating for 10s...
irrigation cancelled (rain detected!)
valve: CLOSED
task cancelled: True, valve open: False
```

**Key Points:**
- The 10-second irrigation was stopped after 0.3 s, and the `finally` block **closed the valve**:
  cancellation is both **effective** and **safe**.
- **Always re-raise `CancelledError`** after handling it. Swallowing it breaks the cancellation
  machinery (timeouts, `TaskGroup`, shutdown).
- In general, any long operation (a download, a batch job, a slow request) can be interrupted at any
  time without leaving resources in an inconsistent state. In an IoT system this means that an
  irrigation cycle, a firmware update or a slow poll never leaves an actuator in an unsafe state.

---

# 4 Structured Concurrency with `TaskGroup`

## 4.1 The Problem with Loose Tasks and `gather()`

When one of several concurrent reads fails, what should happen to the others?

- `gather()` (default) propagates the **first** exception to the caller, but the other coroutines
  **keep running** without anybody waiting for them;
- `gather(..., return_exceptions=True)` returns exceptions **mixed with the results** and leaves it
  to you to check each element;
- tasks created with `create_task()` and forgotten can fail **silently**.

## 4.2 `asyncio.TaskGroup` and `except*`

**`asyncio.TaskGroup`** (Python 3.11+) implements *structured concurrency*: tasks are created inside
an `async with` block, and the block **does not exit until all of them are finished**. If any task
fails:

1. all the other tasks of the group are **cancelled**;
2. once everything has stopped, all the errors are raised together as an **`ExceptionGroup`**;
3. the caller handles them with the **`except*`** syntax, which matches exceptions *inside* the
   group by type.

**IoT example:** four sensors are read concurrently; two of them fail with different errors while a third is
still running.


`code/03_asyncio/05_taskgroup.py`
```python
import asyncio


class SensorOfflineError(Exception):
    """The sensor did not answer."""


class SensorCalibrationError(Exception):
    """The sensor answered with an out-of-range value."""


async def read_sensor(sensor_id: str, latency: float) -> float:
    try:
        await asyncio.sleep(latency)
        if sensor_id == "hum-02":
            raise SensorOfflineError(f"{sensor_id} offline")
        if sensor_id == "co2-01":
            raise SensorCalibrationError(f"{sensor_id} returned -40 ppm")
        print(f"  {sensor_id}: ok")
        return 21.0
    except asyncio.CancelledError:
        print(f"  {sensor_id}: cancelled by the TaskGroup")
        raise


async def with_gather() -> None:
    print("--- gather(return_exceptions=True) ---")
    results = await asyncio.gather(
        read_sensor("temp-01", 0.1), read_sensor("hum-02", 0.2), read_sensor("temp-02", 0.3),
        return_exceptions=True,
    )
    for r in results:
        print(f"  result: {r!r}")


async def with_taskgroup() -> None:
    print("--- TaskGroup ---")
    try:
        async with asyncio.TaskGroup() as tg:
            t1 = tg.create_task(read_sensor("temp-01", 0.1))
            tg.create_task(read_sensor("hum-02", 0.2))
            tg.create_task(read_sensor("co2-01", 0.2))
            tg.create_task(read_sensor("temp-02", 1.0))     # still running when the others fail
        # we get here only if ALL tasks succeeded
    except* SensorOfflineError as group:
        for exc in group.exceptions:
            print(f"  handled offline error: {exc}")
    except* SensorCalibrationError as group:
        for exc in group.exceptions:
            print(f"  handled calibration error: {exc}")
    print(f"  temp-01 finished before the failure: {t1.result()}")


async def main() -> None:
    await with_gather()
    await with_taskgroup()


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
--- gather(return_exceptions=True) ---
  temp-01: ok
  temp-02: ok
  result: 21.0
  result: SensorOfflineError('hum-02 offline')
  result: 21.0
--- TaskGroup ---
  temp-01: ok
  temp-02: cancelled by the TaskGroup
  handled offline error: hum-02 offline
  handled calibration error: co2-01 returned -40 ppm
  temp-01 finished before the failure: 21.0
```

**Key Points:**
- With the `TaskGroup`, as soon as `hum-02` and `co2-01` fail, the still-running `temp-02` is
  **cancelled**: no orphan tasks keep running after the error.
- **Both** errors are reported (they happened in the same iteration of the loop), each one handled by
  its own `except*` clause. With plain `except` you would see only one of them.
- **When to use what:** `TaskGroup` when the tasks form a unit of work that must succeed or fail
  together; `gather(return_exceptions=True)` when failures are expected and independent (e.g. poll
  100 endpoints, here sensors, and report which ones are offline).

---

# 5 Timeouts and Periodic Tasks

## 5.1 `asyncio.timeout()` and `asyncio.wait_for()`

A program must never wait forever for an external resource (a server, a database, a device). asyncio
offers two ways to bound a wait:

- `async with asyncio.timeout(seconds):` (Python 3.11+), a context manager that can wrap **any
  number of awaits**;
- `await asyncio.wait_for(aw, timeout=seconds)`, the same for a single awaitable.

Both raise the built-in `TimeoutError` and, unlike the thread-based timeouts of Lecture 02,
**cancel** the operation that was in progress.

**IoT example:** two soil-moisture sensors, one of which is much too slow.


`code/03_asyncio/06_timeouts.py`
```python
import asyncio
import time


async def read_slow_sensor(sensor_id: str, latency: float) -> float:
    try:
        await asyncio.sleep(latency)
        return 18.2
    finally:
        # runs both on success and on cancellation
        print(f"  {sensor_id}: read operation finished/aborted, bus released")


async def main() -> None:
    start = time.perf_counter()

    # 1. asyncio.timeout(): a context manager, can wrap several awaits
    try:
        async with asyncio.timeout(0.3):
            value = await read_slow_sensor("soil-01", latency=2.0)
            print(f"soil-01 = {value}")
    except TimeoutError:
        print(f"[{time.perf_counter() - start:.2f}s] soil-01: timeout (operation cancelled)")

    # 2. asyncio.wait_for(): the same for a single awaitable
    try:
        value = await asyncio.wait_for(read_slow_sensor("soil-02", latency=0.1), timeout=0.3)
        print(f"[{time.perf_counter() - start:.2f}s] soil-02 = {value}")
    except TimeoutError:
        print("soil-02: timeout")


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
  soil-01: read operation finished/aborted, bus released
[0.30s] soil-01: timeout (operation cancelled)
  soil-02: read operation finished/aborted, bus released
[0.40s] soil-02 = 18.2
```

> **Note:** the total run time is 0.4 s, not 2.1 s: the 2-second read of `soil-01` was really
> **aborted** at 0.3 s, and its `finally` block released the bus. Compare this with Lecture 02, where
> the program had to wait for the slow `temp-10` read even after the timeout.

## 5.2 Periodic Sampling Without Drift

Many programs must do something at a fixed rate (polling an API, sending a heartbeat, sampling a
sensor). The obvious loop,
`while True: await read(); await asyncio.sleep(period)`, **drifts**: every cycle lasts
`period + read_time`. The fix is to schedule against **absolute deadlines**, using the event loop
clock `loop.time()`:

**IoT example:** a sensor sampled every 200 ms, whose read takes 30 ms.


`code/03_asyncio/07_periodic_sampling.py`
```python
import asyncio


async def read_sensor() -> float:
    await asyncio.sleep(0.03)                 # the read itself takes 30 ms
    return 21.0


async def naive_sampler(samples: int, period: float) -> list[float]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    stamps = []
    for _ in range(samples):
        await read_sensor()
        stamps.append(round(loop.time() - start, 2))
        await asyncio.sleep(period)           # period + read time -> drift
    return stamps


async def drift_free_sampler(samples: int, period: float) -> list[float]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    next_deadline = start
    stamps = []
    for _ in range(samples):
        await read_sensor()
        stamps.append(round(loop.time() - start, 2))
        next_deadline += period
        await asyncio.sleep(max(0.0, next_deadline - loop.time()))   # sleep only what is left
    return stamps


async def main() -> None:
    naive, fixed = await asyncio.gather(naive_sampler(6, 0.2), drift_free_sampler(6, 0.2))
    print(f"naive      sample times: {naive}")
    print(f"drift-free sample times: {fixed}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
naive      sample times: [0.03, 0.26, 0.49, 0.72, 0.95, 1.18]
drift-free sample times: [0.03, 0.23, 0.43, 0.63, 0.83, 1.03]
```

The naive sampler accumulates 30 ms of error at every cycle: after one hour at 5 Hz it would have
lost more than 8 minutes of samples. The drift-free version keeps an exact 200 ms period.

---

# 6 Blocking Code: the Number One Pitfall

## 6.1 A Blocking Call Freezes the Whole Loop

Since all the tasks share **one thread**, a single blocking call (`time.sleep()`, `requests.get()`,
a synchronous serial read, a heavy computation) stops **every** task until it returns:

**IoT example:** a heartbeat that should tick every 100 ms, and a "legacy" sensor read that uses
`time.sleep()` instead of `asyncio.sleep()`.


`code/03_asyncio/08_blocking_pitfall.py`
```python
import asyncio
import time
from collections.abc import Awaitable, Callable


async def heartbeat(start: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        print(f"  [{time.perf_counter() - start:.2f}s] heartbeat")
        await asyncio.sleep(0.1)


async def read_sensor_blocking() -> float:
    time.sleep(0.5)             # BLOCKING: the whole event loop is stuck here for 0.5 s
    return 20.0


async def read_sensor_non_blocking() -> float:
    await asyncio.sleep(0.5)    # non-blocking: other tasks run during the wait
    return 20.0


async def run(title: str, reader: Callable[[], Awaitable[float]]) -> None:
    print(title)
    start = time.perf_counter()
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(start, stop))
    await asyncio.sleep(0.15)
    t0 = time.perf_counter() - start
    await reader()
    print(f"  sensor read from {t0:.2f}s to {time.perf_counter() - start:.2f}s")
    await asyncio.sleep(0.25)                   # let the heartbeat run a little longer
    stop.set()
    await hb


async def main() -> None:
    await run("--- blocking read inside a coroutine ---", read_sensor_blocking)
    await run("--- non-blocking read ---", read_sensor_non_blocking)


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
--- blocking read inside a coroutine ---
  [0.00s] heartbeat
  [0.10s] heartbeat
  sensor read from 0.15s to 0.65s
  [0.65s] heartbeat
  [0.75s] heartbeat
  [0.85s] heartbeat
--- non-blocking read ---
  [0.00s] heartbeat
  [0.10s] heartbeat
  [0.20s] heartbeat
  [0.30s] heartbeat
  [0.40s] heartbeat
  [0.50s] heartbeat
  [0.60s] heartbeat
  sensor read from 0.15s to 0.65s
  [0.70s] heartbeat
  [0.80s] heartbeat
```

In the blocking case the heartbeat **disappears for half a second** (from 0.10 s to 0.65 s). In a real
application that means missed keep-alive messages, dropped connections, requests that time out (in the
IoT gateway: a delayed alarm).

> **Note:** being inside an `async def` does not make a function non-blocking. Only `await`ing
> something that is *actually asynchronous* gives control back to the loop. `asyncio.run(main(),
> debug=True)` logs a warning for every callback that blocks the loop for more than 100 ms: a useful
> tool to find these bugs.

## 6.2 Bridging Blocking Code: `asyncio.to_thread()` and `run_in_executor()`

Many libraries (database drivers, HTTP clients, vendor SDKs, serial-port and Modbus libraries) exist
only in a synchronous, blocking
version. We cannot rewrite them, but we can run them **in a worker thread** and **await** the result:

- `await asyncio.to_thread(func, *args)` (Python 3.9+) runs `func` in the loop's default thread pool;
- `await loop.run_in_executor(executor, func, *args)` does the same with an executor of your choice:
  a `ThreadPoolExecutor` to bound concurrency, or a `ProcessPoolExecutor` for CPU-bound work.

This is where Lecture 02 and this lecture meet: **asyncio for coordination, executors for blocking
or CPU-heavy work.**

**IoT example:** a blocking vendor driver for Modbus registers, called from asyncio code.


`code/03_asyncio/09_to_thread.py`
```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor


def legacy_modbus_read(register: int) -> int:
    """A blocking vendor driver we cannot change."""
    time.sleep(0.5)
    return 1000 + register


async def heartbeat(start: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        print(f"  [{time.perf_counter() - start:.2f}s] heartbeat")
        await asyncio.sleep(0.2)


async def main() -> None:
    start = time.perf_counter()
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(start, stop))

    # 1. asyncio.to_thread(): run the blocking function in the default thread pool
    values = await asyncio.gather(
        asyncio.to_thread(legacy_modbus_read, 1),
        asyncio.to_thread(legacy_modbus_read, 2),
    )
    print(f"[{time.perf_counter() - start:.2f}s] to_thread results: {values}")

    # 2. run_in_executor(): same idea, with an executor chosen by us (e.g. to bound concurrency)
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="modbus") as modbus_executor:
        value = await loop.run_in_executor(modbus_executor, legacy_modbus_read, 3)
    print(f"[{time.perf_counter() - start:.2f}s] run_in_executor result: {value}")

    stop.set()
    await hb


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
  [0.00s] heartbeat
  [0.20s] heartbeat
  [0.40s] heartbeat
[0.50s] to_thread results: [1001, 1002]
  [0.60s] heartbeat
  [0.80s] heartbeat
[1.00s] run_in_executor result: 1003
```

**Key Points:**
- The heartbeat never stops: the blocking reads run in other threads while the loop stays free.
- The two `to_thread` reads run **concurrently** (both finish at 0.5 s).
- The usual caveat applies: a blocking call wrapped in a thread **cannot be cancelled**. If the
  surrounding task is cancelled, the loop stops waiting but the thread finishes its job anyway.

---

# 7 Synchronization Primitives

asyncio has its own versions of the primitives of Lecture 01: `asyncio.Lock`, `Event`, `Condition`,
`Semaphore`, `BoundedSemaphore`, and `Barrier` (3.11+). They have the same meaning, but they are
**awaited** instead of blocking the thread (`async with lock:`, `await event.wait()`).

> **Note:** never use `threading` primitives inside coroutines (a `threading.Lock.acquire()` would
> block the whole loop), and never use asyncio primitives from other threads: they are **not
> thread-safe**.

"Single thread" does not mean "no race conditions". A race is still possible whenever a
read-modify-write sequence **contains an `await`**, because another task can run in between:

**IoT example:** 5 batches of readings must be uploaded to a cloud API that accepts at most 2 concurrent
requests (`Semaphore`), only once the cellular link is up (`Event`), updating a shared counter (`Lock`).


`code/03_asyncio/10_async_primitives.py`
```python
import asyncio
import time

upload_slots = asyncio.Semaphore(2)
stats_lock = asyncio.Lock()
network_up = asyncio.Event()
stats = {"uploaded": 0}


async def upload(batch_id: int, start: float) -> None:
    await network_up.wait()                         # wait for connectivity
    async with upload_slots:                        # at most 2 concurrent uploads
        print(f"[{time.perf_counter() - start:.2f}s] uploading batch {batch_id}")
        await asyncio.sleep(0.2)                    # the HTTP request
    async with stats_lock:
        # read-modify-write with an await in the middle: without the lock, another task
        # could run between the read and the write, exactly like with threads
        current = stats["uploaded"]
        await asyncio.sleep(0)                      # e.g. writing an audit log entry
        stats["uploaded"] = current + 1


async def connectivity_monitor(start: float) -> None:
    await asyncio.sleep(0.3)
    print(f"[{time.perf_counter() - start:.2f}s] LTE link is up -> network_up.set()")
    network_up.set()


async def main() -> None:
    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(connectivity_monitor(start))
        for i in range(5):
            tg.create_task(upload(i, start))
    print(f"[{time.perf_counter() - start:.2f}s] uploaded batches: {stats['uploaded']}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
[0.30s] LTE link is up -> network_up.set()
[0.30s] uploading batch 0
[0.30s] uploading batch 1
[0.50s] uploading batch 2
[0.50s] uploading batch 3
[0.70s] uploading batch 4
[0.90s] uploaded batches: 5
```

**Key Points:**
- All 5 uploads wait for the `Event` and start only when the link is up (0.30 s).
- The `Semaphore(2)` lets them through in waves of two, respecting the API's concurrency limit.
- Remove the `async with stats_lock:` and the final count becomes wrong: the `await asyncio.sleep(0)`
  lets other tasks interleave between the read and the write, exactly like `time.sleep(0)` did
  with threads in Lecture 01. **Rule of thumb:** code without `await` between reading and writing
  shared state is atomic in asyncio; code with an `await` in between needs a lock.

---

# 8 Choosing the Right Tool

| Workload | Recommended tool | Why |
|---|---|---|
| Few blocking I/O operations, simple scripts | `threading` / `ThreadPoolExecutor` | Works with any (blocking) library, little code to change |
| Many concurrent I/O operations (hundreds or thousands of connections, sockets, HTTP/MQTT clients, devices) | `asyncio` | One thread, tiny per-task cost, real cancellation and timeouts |
| CPU-bound work (numerical analytics, image processing, ML) | `ProcessPoolExecutor` / `multiprocessing` | Bypasses the GIL with one interpreter per core |
| asyncio application that must call blocking libraries | `asyncio` + `asyncio.to_thread()` / `run_in_executor()` | Keeps the loop responsive |
| asyncio application with a CPU-heavy step | `asyncio` + `run_in_executor(ProcessPoolExecutor)` | Coordination in the loop, computation in processes |

A few further considerations:

- **Ecosystem:** asyncio requires **async libraries** end to end (e.g. `httpx`/`aiohttp`, `aiomqtt`,
  `asyncpg`). One blocking call in the chain breaks the model.
- **"Function coloring":** an `async` function can be awaited only from another `async` function;
  moving to asyncio usually affects the whole call chain, not a single function.
- **Readability:** asyncio code reads almost like sequential code, and the explicit `await`s make
  the points where other tasks can run visible in the source.

---

# 9 Let's See How It Works: Putting It All Together

A complete asyncio application is typically a set of long-running tasks (periodic workers, a
decision loop, background senders) supervised by a `TaskGroup` and stopped by a shared `Event`.

**IoT example:** the Smart Room gateway of Lecture 01, rewritten with asyncio. It combines almost everything seen in
this lecture:

- one **sampling task per sensor**, with a drift-free period and a **timeout on every read**;
- a **rule engine** task (CO2 > 1000 ppm → open the window; temperature > 25 °C → HVAC COOLING);
- a **cloud uploader** that creates one upload task every 0.3 s, limited by a **`Semaphore`**;
- everything supervised by a **`TaskGroup`** and stopped by an **`asyncio.Event`**;
- logging with `%(taskName)s` (Python 3.12+), the asyncio counterpart of `%(threadName)s`.

`code/03_asyncio/11_async_gateway.py`
```python
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
```

**Output:**
```
  767ms [rules       ] window-01: CLOSED -> OPEN
  918ms [rules       ] hvac-01: OFF -> COOLING
 1514ms [Task-1      ] shutdown requested
 1514ms [temp-01     ] sampling stopped
 1514ms [hum-01      ] sampling stopped
 1514ms [co2-01      ] sampling stopped
 1525ms [rules       ] rule engine stopped
 1666ms [uploader    ] uploader stopped after 5 uploads
 1666ms [Task-1      ] final readings: {'temp-01': 25.6, 'hum-01': 49.5, 'co2-01': 1125.0}
 1666ms [Task-1      ] final actuators: window=OPEN hvac=COOLING
```

**Key Points:**
- **Everything runs in a single thread**, yet the periodic workers (here, sensors) are sampled concurrently, actuators are
  commanded, and uploads proceed in parallel, with no `threading.Lock` anywhere.
- The shutdown is **structured**: `stop.set()` makes every loop exit, and the outer `TaskGroup`
  guarantees that `run()` returns only after all tasks (including the nested upload tasks) are done.
  The uploader stops 150 ms after the others because it lets its last upload finish.
- Waiting on `stop.wait()` with a timeout instead of `asyncio.sleep()` makes each sampler react to
  the stop **immediately**, the asyncio version of the `stop_event.wait(period)` trick of Lecture 01.
- The main task is named `Task-1` by default; every other task has an explicit, meaningful name.

**Limits of this design** (addressed next):
- The decision loop (the rule engine) still **polls** shared state every 100 ms. With an
  `asyncio.Queue` ([Lecture 04](04_queues.md)) producers can **push** data, and consumers react only
  when something arrives. With an event bus ([Lecture 05](05_event_driven_architecture.md)) producers
  and consumers do not even need to know about each other.
- How do we write **automated tests** for coroutines, timeouts and cancellation? →
  [Lecture 06](06_testing.md).

---

# 10 References

- Python documentation - [`asyncio` - Asynchronous I/O](https://docs.python.org/3/library/asyncio.html)
- Python documentation - [Coroutines and Tasks](https://docs.python.org/3/library/asyncio-task.html)
- Python documentation - [Event Loop](https://docs.python.org/3/library/asyncio-eventloop.html)
- Python documentation - [Developing with asyncio](https://docs.python.org/3/library/asyncio-dev.html) (debug mode, blocking calls)
- [PEP 654 - Exception Groups and `except*`](https://peps.python.org/pep-0654/)
- Real Python - [Async IO in Python: A Complete Walkthrough](https://realpython.com/async-io-python/)
