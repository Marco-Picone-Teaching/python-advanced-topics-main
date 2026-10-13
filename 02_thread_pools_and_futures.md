<!-- omit in toc -->
# Thread Pools & Futures

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - Thread Pools & Futures (`concurrent.futures`)           |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **Prerequisites:** [01 - Concurrency Fundamentals & Threading](01_concurrency_and_threading.md).
> **Runnable code:** [`code/02_thread_pools/`](code/02_thread_pools/).

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 From One Thread per Task to a Pool of Workers](#1-from-one-thread-per-task-to-a-pool-of-workers)
  - [1.1 The Limits of Manual Threads](#11-the-limits-of-manual-threads)
  - [1.2 The Thread Pool Idea](#12-the-thread-pool-idea)
- [2 `ThreadPoolExecutor` and `Future`](#2-threadpoolexecutor-and-future)
  - [2.1 `submit()` and the `Future` Object](#21-submit-and-the-future-object)
  - [2.2 Callbacks and Cancellation](#22-callbacks-and-cancellation)
- [3 Collecting Results](#3-collecting-results)
  - [3.1 `executor.map()` vs. `as_completed()`](#31-executormap-vs-as_completed)
  - [3.2 `wait()`: First Completed, All Completed, With a Deadline](#32-wait-first-completed-all-completed-with-a-deadline)
- [4 Exceptions and Timeouts](#4-exceptions-and-timeouts)
  - [4.1 Exceptions Are Stored in the Future](#41-exceptions-are-stored-in-the-future)
  - [4.2 Let's See How It Works: a Failing Job and a Slow Job](#42-lets-see-how-it-works-a-failing-job-and-a-slow-job)
- [5 Executor Lifecycle and Shutdown](#5-executor-lifecycle-and-shutdown)
  - [5.1 Context Manager, `shutdown()`, `cancel_futures`](#51-context-manager-shutdown-cancel_futures)
  - [5.2 How Many Workers?](#52-how-many-workers)
- [6 `ProcessPoolExecutor` for CPU-Bound Work](#6-processpoolexecutor-for-cpu-bound-work)
  - [6.1 Same API, Different Workers](#61-same-api-different-workers)
  - [6.2 Let's See How It Works: Threads vs. Processes on CPU-Bound Jobs](#62-lets-see-how-it-works-threads-vs-processes-on-cpu-bound-jobs)
  - [6.3 Threads or Processes?](#63-threads-or-processes)
- [7 Under the Hood: a Pool Is a Queue Plus Workers](#7-under-the-hood-a-pool-is-a-queue-plus-workers)
- [8 Let's See How It Works: Putting It All Together](#8-lets-see-how-it-works-putting-it-all-together)
- [9 References](#9-references)
<!-- /TOC -->

---

# 1 From One Thread per Task to a Pool of Workers

## 1.1 The Limits of Manual Threads

In the previous lecture every task had its own dedicated thread. This works for a handful of tasks,
but real programs often have **thousands** of independent jobs: requests to serve, files to process,
remote endpoints to query. Creating one thread per job has several problems:

- **Unbounded resources:** every thread has its own stack (reserved virtual memory, 8 MB by default
  on Linux) and a kernel data structure. The OS limits how many threads a process can create.
- **Creation cost:** creating and destroying a thread takes time. For many short jobs this overhead
  can be much larger than the job itself.
- **Unbounded load on external systems:** 1,000 simultaneous requests can saturate the network, a
  database, or a remote API with rate limits.
- **Plumbing code:** results, exceptions and timeouts must be handled manually (shared lists,
  `Thread` subclasses, locks...).

## 1.2 The Thread Pool Idea

A **thread pool** creates a **fixed number of worker threads once**, and **reuses** them to execute
many jobs. Jobs that cannot run immediately wait in an internal **queue**:

```
  submit(job) ──►  ┌────────── work queue ──────────┐      ┌─ worker-0 ─┐
  submit(job) ──►  │ job6  job5  job4  job3         │ ───► ├─ worker-1 ─┤ ──► results (Futures)
  submit(job) ──►  └────────────────────────────────┘      └─ worker-2 ─┘
                     waiting jobs                            max_workers = 3
```

**IoT example:** a building gateway that must read 500 devices (50 ms of I/O latency each), and a
second experiment with 20,000 tiny jobs (validating raw readings). The script compares one thread per
job with a pool:

`code/02_thread_pools/01_thread_per_device_vs_pool.py`
```python
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

NUM_DEVICES = 500
READ_LATENCY_S = 0.05
NUM_TINY_JOBS = 20_000

peak_threads = 0
peak_lock = threading.Lock()


def poll_device(device_id: int) -> int:
    """Simulated device read that also tracks the peak number of live threads."""
    global peak_threads
    with peak_lock:
        peak_threads = max(peak_threads, threading.active_count())
    time.sleep(READ_LATENCY_S)
    return device_id


def validate_reading(value: int) -> bool:
    """A tiny job: check that a raw reading is inside the sensor range."""
    return 0 <= value <= 4095


def one_thread_per_job(job: Callable[[int], object], n: int) -> None:
    threads = [threading.Thread(target=job, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def thread_pool(job: Callable[[int], object], n: int, workers: int) -> None:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(job, range(n)))


def measure(label: str, func: Callable[..., None], *args: object) -> None:
    global peak_threads
    peak_threads = 0
    start = time.perf_counter()
    func(*args)
    peak = f" | peak live threads: {peak_threads}" if peak_threads else ""
    print(f"  {label:26} time: {time.perf_counter() - start:5.2f}s{peak}")


def main() -> None:
    print(f"A) {NUM_DEVICES} I/O-bound device reads")
    measure("one thread per device", one_thread_per_job, poll_device, NUM_DEVICES)
    measure("ThreadPoolExecutor(50)", thread_pool, poll_device, NUM_DEVICES, 50)
    measure("ThreadPoolExecutor(10)", thread_pool, poll_device, NUM_DEVICES, 10)

    print(f"B) {NUM_TINY_JOBS} tiny validation jobs")
    measure("one thread per job", one_thread_per_job, validate_reading, NUM_TINY_JOBS)
    measure("ThreadPoolExecutor(8)", thread_pool, validate_reading, NUM_TINY_JOBS, 8)


if __name__ == "__main__":
    main()
```

**Output:**
```
A) 500 I/O-bound device reads
  one thread per device      time:  0.08s | peak live threads: 501
  ThreadPoolExecutor(50)     time:  0.51s | peak live threads: 51
  ThreadPoolExecutor(10)     time:  2.51s | peak live threads: 11
B) 20000 tiny validation jobs
  one thread per job         time:  0.97s
  ThreadPoolExecutor(8)      time:  0.06s
```

**Key Points** (read the numbers carefully, they are more interesting than they look):
- In experiment **A**, one thread per job is the *fastest*: 500 threads all wait at the same
  time. But it needed **501 live threads**. With 50,000 jobs it would try to create 50,000
  threads, and the OS, the network or the remote systems would give up long before.
- The pool **bounds** the concurrency: with `N` workers, 500 jobs of 50 ms take about
  `500 / N × 0.05 s`. `max_workers` is a **tuning knob** between speed and resource usage, and it
  also protects external systems (here, the devices) from being flooded.
- In experiment **B** the jobs are tiny, so the **cost of creating a thread** dominates: reusing 8
  threads is ~15× faster than creating 20,000.

The rest of this lecture uses the standard library module that implements this idea:
**`concurrent.futures`**.

---

# 2 `ThreadPoolExecutor` and `Future`

`concurrent.futures` provides a high-level interface with two central concepts:

- an **Executor** (`ThreadPoolExecutor` or `ProcessPoolExecutor`) accepts callables and runs them on
  its pool of workers;
- a **Future** is the object you get back immediately when you submit a job: a **placeholder for a
  result that will be available later**.

## 2.1 `submit()` and the `Future` Object

`executor.submit(fn, *args, **kwargs)` schedules `fn(*args, **kwargs)` and returns a `Future`
**without waiting**:

| `Future` method | Behaviour |
|---|---|
| `result(timeout=None)` | Return the value; **block** until it is available (or raise `TimeoutError`). If the job raised an exception, **re-raise it** here |
| `exception(timeout=None)` | Return the exception raised by the job, or `None` |
| `done()` | `True` if the job has finished (successfully, with an error, or cancelled) |
| `running()` | `True` if a worker is currently executing it |
| `cancel()` | Try to cancel the job: succeeds **only if it has not started yet** |
| `cancelled()` | `True` if the job was cancelled |
| `add_done_callback(fn)` | Call `fn(future)` when the future completes |

**IoT example:** two temperature sensors with different latencies are read through a pool, while the
main thread keeps working:

`code/02_thread_pools/02_submit_and_future.py`
```python
import time
from concurrent.futures import Future, ThreadPoolExecutor


def read_temperature(sensor_id: str, latency_s: float) -> float:
    """Simulated blocking read of a temperature sensor."""
    time.sleep(latency_s)
    return {"temp-01": 21.5, "temp-02": 23.1}[sensor_id]


def main() -> None:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        # submit() schedules the call and returns IMMEDIATELY with a Future
        future_1: Future[float] = executor.submit(read_temperature, "temp-01", 0.3)
        future_2: Future[float] = executor.submit(read_temperature, "temp-02", 0.6)
        print(f"[{time.perf_counter() - start:.2f}s] submitted, future_1 = {future_1}")

        # The main thread is free to do other work in the meantime...
        time.sleep(0.4)
        print(f"[{time.perf_counter() - start:.2f}s] done? temp-01: {future_1.done()}, "
              f"temp-02: {future_2.done()}")

        # result() returns at once for a finished future...
        value_1 = future_1.result()
        print(f"[{time.perf_counter() - start:.2f}s] temp-01 = {value_1} C")
        # ...and blocks until the value is available otherwise (here: ~0.2 s more)
        value_2 = future_2.result()
        print(f"[{time.perf_counter() - start:.2f}s] temp-02 = {value_2} C, future_2 = {future_2}")


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.00s] submitted, future_1 = <Future at 0x71b886904c20 state=running>
[0.40s] done? temp-01: True, temp-02: False
[0.40s] temp-01 = 21.5 C
[0.60s] temp-02 = 23.1 C, future_2 = <Future at 0x71b886abae90 state=finished returned float>
```

**Key Points:**
- Compared with the `SensorPoller(Thread)` class of Lecture 01, we got **return values for free**:
  no shared lists, no `join()`, no subclassing.
- The `repr` of a future shows its **state**: `pending` (queued), `running`, `finished`.
- The main thread did useful work (the `sleep(0.4)`) while the reads were in progress, and then
  blocked in `result()` only for as long as needed.
- `Future[float]` is a generic type hint: it documents the type that `result()` will return.

## 2.2 Callbacks and Cancellation

Instead of blocking on `result()`, you can register a **callback** that is invoked when the future
completes. And a job that is still waiting in the queue can be **cancelled**.

**IoT example:** a pool with a single worker receives 4 over-the-air (OTA) firmware-update jobs for
four cameras; the maintenance window closes and the pending jobs are cancelled:

`code/02_thread_pools/03_callbacks_and_cancel.py`
```python
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor


def update_firmware(device_id: str) -> str:
    time.sleep(0.2)                                   # simulated OTA update
    return f"{device_id} updated to v2.1.0"


def on_update_done(device_id: str, future: Future[str]) -> None:
    """Called when the future finishes OR is cancelled."""
    where = threading.current_thread().name
    if future.cancelled():
        print(f"  [callback in {where}] {device_id}: job cancelled")
    else:
        print(f"  [callback in {where}] {future.result()}")


def main() -> None:
    devices = ["cam-01", "cam-02", "cam-03", "cam-04"]
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="ota") as executor:
        futures = {dev: executor.submit(update_firmware, dev) for dev in devices}
        for dev, f in futures.items():
            # add_done_callback() passes only the future: a lambda binds the device id too
            f.add_done_callback(lambda fut, dev=dev: on_update_done(dev, fut))

        # The maintenance window is closing: cancel what has not started yet
        for dev, f in futures.items():
            cancelled = f.cancel()        # a successful cancel runs the callback right now
            print(f"cancel({dev}) -> {cancelled}")


if __name__ == "__main__":
    main()
```

**Output:**
```
cancel(cam-01) -> False
  [callback in MainThread] cam-02: job cancelled
cancel(cam-02) -> True
  [callback in MainThread] cam-03: job cancelled
cancel(cam-03) -> True
  [callback in MainThread] cam-04: job cancelled
cancel(cam-04) -> True
  [callback in ota_0] cam-01 updated to v2.1.0
```

**Key Points:**
- With a single worker, `cam-01` is **already running** and cannot be cancelled (`False`); the other
  three are still queued and are cancelled (`True`). **A running job cannot be interrupted.**
- Callbacks run in the thread that **completes** the future: the worker thread (`ota_0`) for a
  finished job, the thread calling `cancel()` (`MainThread`) for a cancelled one. They must therefore
  be **short and thread-safe**.
- `thread_name_prefix` gives the workers meaningful names in logs (`ota_0`, `ota_1`, ...).

---

# 3 Collecting Results

## 3.1 `executor.map()` vs. `as_completed()`

When you submit many jobs, there are two main ways to consume their results:

- `executor.map(fn, iterable)`: like the built-in `map()`, but concurrent. Returns results **in
  the same order as the inputs**.
- `as_completed(futures)`: an iterator that yields each future **as soon as it completes**.

**IoT example:** querying the firmware version of 4 smart plugs whose response times differ a lot:

`code/02_thread_pools/04_map_vs_as_completed.py`
```python
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# device id -> simulated response time (s)
DEVICES = {"plug-01": 0.8, "plug-02": 0.2, "plug-03": 0.5, "plug-04": 0.1}


def get_firmware_version(device_id: str) -> tuple[str, str]:
    time.sleep(DEVICES[device_id])                 # simulated HTTP request to the device
    return device_id, "v1.4.2"


def main() -> None:
    print("--- executor.map(): submission order ---")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        for device_id, version in executor.map(get_firmware_version, DEVICES):
            print(f"[{time.perf_counter() - start:.2f}s] {device_id}: {version}")

    print("--- as_completed(): completion order ---")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        # keep a future -> device map, as_completed() yields only the futures
        futures = {executor.submit(get_firmware_version, dev): dev for dev in DEVICES}
        for future in as_completed(futures):
            device_id, version = future.result()
            print(f"[{time.perf_counter() - start:.2f}s] {device_id}: {version}")


if __name__ == "__main__":
    main()
```

**Output:**
```
--- executor.map(): submission order ---
[0.80s] plug-01: v1.4.2
[0.80s] plug-02: v1.4.2
[0.80s] plug-03: v1.4.2
[0.80s] plug-04: v1.4.2
--- as_completed(): completion order ---
[0.10s] plug-04: v1.4.2
[0.20s] plug-02: v1.4.2
[0.50s] plug-03: v1.4.2
[0.80s] plug-01: v1.4.2
```

**Key Points:**
- With `map()`, the slow `plug-01` (first in the list) **holds back** all the others, even though
  they were ready long before. Total time is the same, but the **latency of each result** is not.
- With `as_completed()`, each answer is handled **the moment it arrives**: ideal for updating a
  dashboard or reacting to alarms.
- The idiom `futures = {executor.submit(fn, x): x for x in items}` keeps track of which input each
  future belongs to, since `as_completed()` gives you back only the futures.

| | `executor.map()` | `as_completed()` |
|---|---|---|
| Result order | Input order | Completion order |
| Returns | Values directly | Futures |
| Exceptions | Raised when *iterating* to that element | Raised by `future.result()`, one at a time |
| Best for | Ordered batch processing | Reacting to results as soon as possible |

## 3.2 `wait()`: First Completed, All Completed, With a Deadline

`concurrent.futures.wait(futures, timeout=None, return_when=...)` blocks until a condition is met
and returns two sets, `(done, not_done)`:

| `return_when` | Returns when... |
|---|---|
| `ALL_COMPLETED` (default) | all futures have finished (or the timeout expires) |
| `FIRST_COMPLETED` | at least one future has finished |
| `FIRST_EXCEPTION` | a future raised an exception (or all completed) |

A typical use of `FIRST_COMPLETED` is **redundancy**: the same request is sent to several replicas
(servers, mirrors, services) and the first answer wins. `ALL_COMPLETED` with a `timeout` gives a
**deadline** for a group of jobs.

**IoT example:** a critical room has three replicated temperature sensors and we want the fastest
answer; then a group of actuator commands must be acknowledged within 0.5 s:

`code/02_thread_pools/05_wait.py`
```python
import time
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, ThreadPoolExecutor, wait

REPLICAS = {"temp-A": (0.40, 21.4), "temp-B": (0.15, 21.6), "temp-C": (0.30, 21.5)}


def read_replica(sensor_id: str) -> tuple[str, float]:
    latency, value = REPLICAS[sensor_id]
    time.sleep(latency)
    return sensor_id, value


def send_command(actuator_id: str, latency: float) -> str:
    time.sleep(latency)
    return f"{actuator_id}: ACK"


def main() -> None:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(read_replica, s) for s in REPLICAS]
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        sensor_id, value = next(iter(done)).result()
        print(f"[{time.perf_counter() - start:.2f}s] first answer: {sensor_id} = {value} C "
              f"({len(not_done)} replicas still running)")
        for f in not_done:
            f.cancel()          # returns False: they are already running, we just ignore them

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        commands = {"light-01": 0.1, "blind-01": 0.3, "hvac-01": 2.0}   # hvac-01 is unresponsive
        futures = [executor.submit(send_command, a, lat) for a, lat in commands.items()]
        done, not_done = wait(futures, timeout=0.5, return_when=ALL_COMPLETED)
        print(f"[{time.perf_counter() - start:.2f}s] deadline reached: {len(done)} done, "
              f"{len(not_done)} still pending")
        for f in done:
            print(f"   {f.result()}")
        # leaving the `with` block still waits for hvac-01 (see Section 5 on shutdown)
    print(f"[{time.perf_counter() - start:.2f}s] executor closed")


if __name__ == "__main__":
    main()
```

**Output** (`done` is a set, so the order of the ACK lines may vary):
```
[0.15s] first answer: temp-B = 21.6 C (2 replicas still running)
[0.50s] deadline reached: 2 done, 1 still pending
   blind-01: ACK
   light-01: ACK
[2.00s] executor closed
```

> **Note:** look at the last line: the `wait()` returned after 0.5 s, but leaving the `with` block
> took until 2.0 s, because the executor **waits for the running jobs** before closing. We will see
> why, and how to control it, in [Section 5](#5-executor-lifecycle-and-shutdown).

---

# 4 Exceptions and Timeouts

## 4.1 Exceptions Are Stored in the Future

An exception raised inside a worker **does not crash the program and is not printed**. It is
captured and **stored in the future**, and re-raised in the caller's thread when you call
`result()`. This is very convenient, but it also means that **if you never call `result()` (or
`exception()`), errors are silently lost**.

## 4.2 Let's See How It Works: a Failing Job and a Slow Job

**IoT example:** four sensors are read concurrently; one is offline (its read raises an exception)
and one is very slow (it misses the deadline).

`code/02_thread_pools/06_exceptions_and_timeouts.py`
```python
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed


class SensorOfflineError(Exception):
    """Raised when a sensor does not answer on the field bus."""


VALUES = {"temp-01": 21.5, "hum-02": 48.0, "co2-01": 640.0, "temp-02": 22.8}


def read_sensor(sensor_id: str) -> float:
    time.sleep(0.1)
    if sensor_id == "hum-02":
        raise SensorOfflineError(f"{sensor_id} did not answer")
    if sensor_id == "co2-01":
        time.sleep(1.0)             # a very slow device
    return VALUES[sensor_id]


def main() -> None:
    sensors = ["temp-01", "hum-02", "co2-01", "temp-02"]
    results: dict[str, float] = {}
    errors: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(read_sensor, s): s for s in sensors}

        # 1. Per-future exception handling
        try:
            for future in as_completed(futures, timeout=0.5):   # overall deadline for the cycle
                sensor_id = futures[future]
                try:
                    results[sensor_id] = future.result()
                except SensorOfflineError as exc:
                    errors[sensor_id] = f"offline ({exc})"
                # alternative without try/except: future.exception() returns the exception or None
        except FuturesTimeoutError:
            for future, sensor_id in futures.items():
                if not future.done():
                    errors[sensor_id] = "timeout"

    print(f"results: {results}")
    print(f"errors : {errors}")

    # 2. result(timeout=...) on a single future
    with ThreadPoolExecutor() as executor:
        slow = executor.submit(read_sensor, "co2-01")
        try:
            slow.result(timeout=0.3)
        except FuturesTimeoutError:
            print(f"co2-01: no answer within 0.3 s (the task is still running: {slow.running()})")


if __name__ == "__main__":
    main()
```

**Output:**
```
results: {'temp-01': 21.5, 'temp-02': 22.8}
errors : {'hum-02': 'offline (hum-02 did not answer)', 'co2-01': 'timeout'}
co2-01: no answer within 0.3 s (the task is still running: True)
```

**Key Points:**
- The `try/except` goes **around `future.result()`**, not around `submit()`: `submit()` never raises
  the job's exception.
- A failing job does not stop the others: each future is handled independently, and errors are
  **collected** and reported, which is what robust batch code must do (here: an offline sensor does not
  stop the polling cycle).
- There are two kinds of timeout: `as_completed(..., timeout=...)` is a deadline for the **whole
  group**, `result(timeout=...)` is a deadline for **one** future.
- A timeout means "*I stop waiting*", **not** "*the job is stopped*": `running()` is still `True`.
  The thread keeps working until the job returns.
- Since Python 3.11, `concurrent.futures.TimeoutError` is an alias of the built-in `TimeoutError`.
  Importing it with an explicit alias keeps the code readable and compatible with older versions.

---

# 5 Executor Lifecycle and Shutdown

## 5.1 Context Manager, `shutdown()`, `cancel_futures`

An executor owns threads, so it must be **shut down** when no longer needed:

- `executor.shutdown(wait=True, cancel_futures=False)`: stop accepting new jobs; with `wait=True`
  block until **all pending and running jobs** are done; with `cancel_futures=True` (Python 3.9+)
  first **cancel every job that has not started yet**;
- using the executor in a **`with` block** calls `shutdown(wait=True)` automatically on exit, even if
  an exception is raised. This is the recommended style.

## 5.2 How Many Workers?

If you do not pass `max_workers`, `ThreadPoolExecutor` uses `min(32, cpu_count + 4)`. Since
**Python 3.13**, `cpu_count` is `os.process_cpu_count()`, i.e. the CPUs **this process is allowed to
use** (it respects CPU affinity, for example in a container), instead of all the CPUs of the machine.

This default is designed for I/O-bound work. For I/O-bound jobs, however, the right value usually
depends on **what is on the other side**, not on the CPU: how many concurrent requests can the database,
the remote API (or, in the IoT example, the field bus and the device firmware) tolerate? Choose it
explicitly and make it
configurable.

**IoT example:** batches of readings uploaded to the cloud, with a normal shutdown and a fast one:

`code/02_thread_pools/07_shutdown.py`
```python
import os
import time
from concurrent.futures import ThreadPoolExecutor


def upload_batch(batch_id: int) -> int:
    time.sleep(0.2)                 # simulated upload of a batch of readings to the cloud
    return batch_id


def main() -> None:
    # Default number of workers (Python 3.13+): min(32, os.process_cpu_count() + 4)
    cpus = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    with ThreadPoolExecutor() as executor:
        # _max_workers is a private attribute: read here only to show the default value
        print(f"CPUs usable by this process: {cpus} -> default max_workers = {executor._max_workers}")

    # 1. The context manager calls shutdown(wait=True) on exit: all 6 uploads complete
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(upload_batch, i) for i in range(6)]
    print(f"with-block: {sum(f.done() for f in futures)}/6 uploads completed "
          f"in {time.perf_counter() - start:.2f}s")

    # 2. Explicit shutdown with cancel_futures=True: queued jobs are dropped (e.g. on SIGTERM)
    start = time.perf_counter()
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(upload_batch, i) for i in range(6)]
    time.sleep(0.05)                                     # the first 2 jobs are now running
    executor.shutdown(wait=True, cancel_futures=True)
    completed = sum(f.done() and not f.cancelled() for f in futures)
    cancelled = sum(f.cancelled() for f in futures)
    print(f"shutdown(cancel_futures=True): {completed} completed, {cancelled} cancelled "
          f"in {time.perf_counter() - start:.2f}s")

    # 3. After shutdown the executor refuses new work
    try:
        executor.submit(upload_batch, 99)
    except RuntimeError as exc:
        print(f"submit after shutdown -> RuntimeError: {exc}")


if __name__ == "__main__":
    main()
```

**Output** (the first line depends on your machine):
```
CPUs usable by this process: 16 -> default max_workers = 20
with-block: 6/6 uploads completed in 0.60s
shutdown(cancel_futures=True): 2 completed, 4 cancelled in 0.20s
submit after shutdown -> RuntimeError: cannot schedule new futures after shutdown
```

**Key Points:**
- With 2 workers, 6 uploads of 0.2 s take 0.6 s (3 waves), and the `with` block waits for all of them.
- `cancel_futures=True` is what you want on a fast shutdown: the 2 **running** uploads finish, the
  4 **queued** ones are dropped.
- **Running jobs are never interrupted**, not even by shutdown. Long-running jobs should therefore
  check a stop `Event` (as in Lecture 01) or enforce their own I/O timeouts.

---

# 6 `ProcessPoolExecutor` for CPU-Bound Work

## 6.1 Same API, Different Workers

`ProcessPoolExecutor` has **exactly the same interface** as `ThreadPoolExecutor` (`submit`, `map`,
futures, `shutdown`...), but its workers are separate **processes**. Each process has its own
interpreter and **its own GIL**, so CPU-bound Python code can run **in parallel** on multiple cores.

The price to pay:

- **Startup cost:** creating a process is much more expensive than creating a thread.
- **No shared memory:** arguments and return values are **serialized with `pickle`** and sent
  between processes. They must be picklable (module-level functions, simple data, dataclasses:
  yes; lambdas, open sockets, locks: no), and sending large data is expensive.
- **The `if __name__ == "__main__":` guard is mandatory:** depending on the platform, child
  processes are started with *spawn* or *forkserver* (the default on Windows and macOS, and on Linux
  since Python 3.14), which **re-imports your main module** in every child. Without the guard each
  child would try to create its own pool.

## 6.2 Let's See How It Works: Threads vs. Processes on CPU-Bound Jobs

**IoT example:** every night the gateway analyses a day of data of each sensor, looking for anomalous
readings with a sliding-window statistic. This is pure Python computation, the perfect case for
processes. The script runs it sequentially, with a thread pool and with a process pool:

`code/02_thread_pools/08_process_pool.py`
```python
import math
import random
import time
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor

SENSORS = ["temp-01", "temp-02", "hum-01", "hum-02"]
SAMPLES_PER_SENSOR = 1_000_000
WINDOW = 50


def generate_day_of_data(sensor_id: str) -> list[float]:
    """Synthetic signal with a few injected spikes (deterministic per sensor)."""
    rng = random.Random(sensor_id)
    data = [20.0 + math.sin(i / 500) + rng.gauss(0, 0.1) for i in range(SAMPLES_PER_SENSOR)]
    for i in range(1000, SAMPLES_PER_SENSOR, 100_000):
        data[i] += 5.0                                  # anomaly
    return data


def detect_anomalies(sensor_id: str) -> tuple[str, int]:
    """CPU-bound: rolling mean / std over a sliding window, count points beyond 5 sigma."""
    data = generate_day_of_data(sensor_id)
    anomalies = 0
    window_sum = sum(data[:WINDOW])
    window_sq = sum(x * x for x in data[:WINDOW])
    for i in range(WINDOW, len(data)):
        mean = window_sum / WINDOW
        std = math.sqrt(max(window_sq / WINDOW - mean * mean, 1e-12))
        if abs(data[i] - mean) > 5 * std:
            anomalies += 1
        # slide the window: add the new point, remove the oldest one
        window_sum += data[i] - data[i - WINDOW]
        window_sq += data[i] * data[i] - data[i - WINDOW] * data[i - WINDOW]
    return sensor_id, anomalies


def run_with(executor_factory: Callable[[], Executor] | None) -> tuple[float, dict[str, int]]:
    start = time.perf_counter()
    if executor_factory is None:
        results = dict(map(detect_anomalies, SENSORS))            # plain sequential
    else:
        with executor_factory() as executor:
            results = dict(executor.map(detect_anomalies, SENSORS))
    return time.perf_counter() - start, results


def main() -> None:
    seq_time, results = run_with(None)
    thr_time, _ = run_with(lambda: ThreadPoolExecutor(max_workers=4))
    proc_time, _ = run_with(lambda: ProcessPoolExecutor(max_workers=4))

    print(f"anomalies found: {results}")
    print(f"sequential          : {seq_time:5.2f}s")
    print(f"ThreadPoolExecutor  : {thr_time:5.2f}s (x{seq_time / thr_time:.1f})")
    print(f"ProcessPoolExecutor : {proc_time:5.2f}s (x{seq_time / proc_time:.1f})")


if __name__ == "__main__":
    main()
```

**Output** (on a multi-core machine):
```
anomalies found: {'temp-01': 26, 'temp-02': 27, 'hum-01': 25, 'hum-02': 22}
sequential          : 1.36s
ThreadPoolExecutor  : 1.60s (x0.9)
ProcessPoolExecutor : 0.41s (x3.3)
```

**Key Points:**
- Switching from threads to processes required changing **one class name**: this is the power of
  the common `Executor` interface.
- Threads are even **slower** than sequential code here (GIL + switching overhead); processes give
  an x3.3 speed-up on 4 workers (not x4, because of process startup and pickling).
- Each worker receives only a short string (the **sensor id**) and returns a small tuple: the heavy
  data is generated *inside* the worker. Designing jobs so that little data crosses the process
  boundary is key to getting good speed-ups.

## 6.3 Threads or Processes?

| | `ThreadPoolExecutor` | `ProcessPoolExecutor` |
|---|---|---|
| Best for | I/O-bound jobs (network, HTTP, DB, devices) | CPU-bound jobs (analytics, image processing, ML) |
| Parallel Python execution | No (with the GIL) | Yes |
| Startup cost | Low | High |
| Data exchange | Shared memory (free, but needs locks) | Pickling (costly, but isolated) |
| Job requirements | Any callable | Picklable function and arguments, `__main__` guard |

---

# 7 Under the Hood: a Pool Is a Queue Plus Workers

There is no magic inside `ThreadPoolExecutor`. Conceptually (and very close to the real CPython
implementation) it is made of:

1. a **thread-safe queue** of *work items* (a function, its arguments, and the `Future` to fill);
2. `max_workers` **threads**, each running a loop: *take an item from the queue → run it → set the
   result (or the exception) on the future*;
3. a **sentinel** value put in the queue at shutdown, to tell the workers to exit.

`submit()` simply creates a `Future`, wraps it with the call in a work item, puts it in the queue and
returns the future. This is the **producer-consumer pattern**, which we will build ourselves with
the `queue` module in [Lecture 04](04_queues.md), and which is also the backbone of the event bus in
[Lecture 05](05_event_driven_architecture.md).

---

# 8 Let's See How It Works: Putting It All Together

The final example uses a bounded pool to run a batch of I/O-bound jobs with a **deadline**, and
handles all the situations seen in this lecture: results in completion order, a job that fails, a job
that misses the deadline, and a second batch of jobs submitted to the same pool based on the results
of the first.

**IoT example: polling a whole building.** 10 rooms are polled; one sensor is offline, one misses the
cycle deadline, and actuator commands are submitted to the same pool for the rooms that are
too warm.

`code/02_thread_pools/09_building_poller.py`
```python
import logging
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-10s] %(message)s")
logger = logging.getLogger("building")

CYCLE_DEADLINE_S = 0.6
COOLING_THRESHOLD_C = 25.0


class SensorOfflineError(Exception):
    """The sensor did not answer on the field bus."""


@dataclass
class TemperatureSensor:
    """Simulated room sensor with configurable latency and failure behaviour."""
    sensor_id: str
    room: str
    latency_s: float
    offline: bool = False

    def read(self) -> float:
        time.sleep(self.latency_s)
        if self.offline:
            raise SensorOfflineError(f"{self.sensor_id} is offline")
        return round(random.Random(self.sensor_id).uniform(20.0, 28.0), 1)


@dataclass
class CycleReport:
    """Outcome of one polling cycle."""
    readings: dict[str, float] = field(default_factory=dict)   # room -> temperature
    errors: dict[str, str] = field(default_factory=dict)       # sensor id -> reason


def build_sensors() -> list[TemperatureSensor]:
    sensors = [TemperatureSensor(f"temp-{i:02d}", f"room-{100 + i}", latency_s=0.05 + 0.03 * i)
               for i in range(1, 11)]
    sensors[3].offline = True           # temp-04 is broken
    sensors[9].latency_s = 1.0          # temp-10 is extremely slow
    return sensors


def poll_building(executor: ThreadPoolExecutor, sensors: list[TemperatureSensor]) -> CycleReport:
    report = CycleReport()
    futures: dict[Future[float], TemperatureSensor] = {executor.submit(s.read): s for s in sensors}
    try:
        for future in as_completed(futures, timeout=CYCLE_DEADLINE_S):
            sensor = futures[future]
            try:
                report.readings[sensor.room] = future.result()
            except SensorOfflineError as exc:
                report.errors[sensor.sensor_id] = str(exc)
                logger.warning("%s", exc)
    except FuturesTimeoutError:
        for future, sensor in futures.items():
            if not future.done():
                future.cancel()        # only succeeds if the job has not started yet
                report.errors[sensor.sensor_id] = f"no answer within {CYCLE_DEADLINE_S}s"
                logger.warning("%s: timeout", sensor.sensor_id)
    return report


def set_hvac_mode(room: str, mode: str) -> str:
    time.sleep(0.1)                    # simulated command to the room's HVAC unit
    logger.info("hvac of %s -> %s", room, mode)
    return room


def main() -> None:
    sensors = build_sensors()
    start = time.perf_counter()
    # A bounded pool: never more than 8 concurrent requests on the building network
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="worker") as executor:
        report = poll_building(executor, sensors)
        logger.info("cycle: %d readings, %d errors in %.2fs",
                    len(report.readings), len(report.errors), time.perf_counter() - start)

        warm_rooms = [room for room, t in report.readings.items() if t > COOLING_THRESHOLD_C]
        commands = [executor.submit(set_hvac_mode, room, "COOLING") for room in warm_rooms]
        acked = [f.result() for f in commands]
        logger.info("cooling enabled in %s", sorted(acked))

        # Leaving the with-block calls shutdown(wait=True): it waits for the temp-10 read,
        # which is still RUNNING. A running job cannot be interrupted from outside: this is
        # why real device drivers must enforce their own I/O timeout (e.g. socket timeouts).

    for room, temperature in sorted(report.readings.items()):
        logger.info("%s: %.1f C", room, temperature)
    logger.info("errors: %s", report.errors)


if __name__ == "__main__":
    main()
```

**Output:**
```
  173ms [MainThread] temp-04 is offline
  604ms [MainThread] temp-10: timeout
  604ms [MainThread] cycle: 8 readings, 2 errors in 0.60s
  704ms [worker_2  ] hvac of room-107 -> COOLING
  705ms [worker_3  ] hvac of room-108 -> COOLING
  705ms [MainThread] cooling enabled in ['room-107', 'room-108']
 1114ms [MainThread] room-101: 20.6 C
 1114ms [MainThread] room-102: 23.6 C
 1114ms [MainThread] room-103: 20.4 C
 1114ms [MainThread] room-105: 21.9 C
 1114ms [MainThread] room-106: 21.9 C
 1114ms [MainThread] room-107: 26.3 C
 1114ms [MainThread] room-108: 26.3 C
 1115ms [MainThread] room-109: 21.4 C
 1115ms [MainThread] errors: {'temp-04': 'temp-04 is offline', 'temp-10': 'no answer within 0.6s'}
```

**Key Points:**
- The batch has a **predictable duration** (0.6 s) no matter how badly a single job behaves: slow or
  failing jobs (here, devices) are reported, not waited for.
- Only 8 of the 10 reads run at first (`max_workers=8`): the other 2 wait in the pool queue. The pool
  also protects the network from bursts.
- The same pool executes both sensor reads and actuator commands: an executor is a general-purpose
  "*run this concurrently*" service.
- The last log lines appear at ~1.1 s and not at 0.6 s: the `with` block waited for the still-running
  `temp-10` read. **You can stop waiting for a job, but you cannot kill it.** Keep this limit in mind:
  cooperative cancellation is one of the things that `asyncio` handles much better
  ([Lecture 03](03_asyncio.md)).

---

# 9 References

- Python documentation - [`concurrent.futures` - Launching parallel tasks](https://docs.python.org/3/library/concurrent.futures.html)
- Python documentation - [What's New in Python 3.13](https://docs.python.org/3/whatsnew/3.13.html) (`os.process_cpu_count()` and the executor default)
- Python documentation - [`multiprocessing` - Contexts and start methods](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods)
- Real Python - [Speed Up Your Python Program With Concurrency](https://realpython.com/python-concurrency/)
