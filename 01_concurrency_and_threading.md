<!-- omit in toc -->
# Concurrency Fundamentals & Threading

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - Concurrency Fundamentals & Threading                    |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **Runnable code:** every example of this lecture is available, complete and ready to run, in
> [`code/01_threading/`](code/01_threading/). The file name is shown above each snippet.

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 Why Concurrency Matters in Software](#1-why-concurrency-matters-in-software)
  - [1.1 The Problem: Programs That Wait](#11-the-problem-programs-that-wait)
  - [1.2 What We Want to Achieve](#12-what-we-want-to-achieve)
- [2 Core Concepts](#2-core-concepts)
  - [2.1 Processes and Threads](#21-processes-and-threads)
  - [2.2 Concurrency vs. Parallelism](#22-concurrency-vs-parallelism)
  - [2.3 I/O-Bound vs. CPU-Bound Work](#23-io-bound-vs-cpu-bound-work)
  - [2.4 Preemptive vs. Cooperative Multitasking](#24-preemptive-vs-cooperative-multitasking)
- [3 The Global Interpreter Lock (GIL)](#3-the-global-interpreter-lock-gil)
  - [3.1 What the GIL Is and Why It Exists](#31-what-the-gil-is-and-why-it-exists)
  - [3.2 Let's See How It Works: Threads vs. I/O-Bound and CPU-Bound Tasks](#32-lets-see-how-it-works-threads-vs-io-bound-and-cpu-bound-tasks)
- [4 The `threading` Module](#4-the-threading-module)
  - [4.1 Creating, Starting and Joining Threads](#41-creating-starting-and-joining-threads)
  - [4.2 Daemon Threads](#42-daemon-threads)
  - [4.3 Subclassing `Thread` and Collecting Results](#43-subclassing-thread-and-collecting-results)
- [5 Race Conditions and Locks](#5-race-conditions-and-locks)
  - [5.1 The Problem: a Race Condition on Shared State](#51-the-problem-a-race-condition-on-shared-state)
  - [5.2 Mutual Exclusion with `Lock`](#52-mutual-exclusion-with-lock)
  - [5.3 Re-entrant Locks with `RLock`](#53-re-entrant-locks-with-rlock)
  - [5.4 Deadlock](#54-deadlock)
- [6 Coordination Primitives](#6-coordination-primitives)
  - [6.1 `Event`: Signaling Between Threads](#61-event-signaling-between-threads)
  - [6.2 `Condition`: Waiting for a State Change](#62-condition-waiting-for-a-state-change)
  - [6.3 `Semaphore`: Limiting Concurrent Access](#63-semaphore-limiting-concurrent-access)
  - [6.4 `Timer`: Delayed Actions](#64-timer-delayed-actions)
  - [6.5 `threading.local`: Per-Thread Data](#65-threadinglocal-per-thread-data)
  - [6.6 Summary of the Primitives](#66-summary-of-the-primitives)
- [7 Graceful Shutdown of Worker Threads](#7-graceful-shutdown-of-worker-threads)
- [8 Python Versions and Free-Threading](#8-python-versions-and-free-threading)
- [9 Let's See How It Works: Putting It All Together](#9-lets-see-how-it-works-putting-it-all-together)
- [10 References](#10-references)
<!-- /TOC -->

---

# 1 Why Concurrency Matters in Software

## 1.1 The Problem: Programs That Wait

Most real programs spend a large part of their time **not computing**, but **waiting** for something
outside the CPU: a network request, a database query, a file on disk, a serial port, a remote API.
All these operations are **slow compared to the CPU** (microseconds or milliseconds, against the
nanoseconds of an instruction), and while one of them is in progress a sequential program has nothing
to do but wait for the answer.

If a program has to perform many such operations and does them **one after the other**, its total
running time is the **sum** of all the waits, even though the CPU is idle almost all the time.
Concurrency is the set of techniques that let a program make progress on **several tasks at once**,
so that these waits overlap.

**IoT example: a gateway that polls its sensors.** Throughout this lecture series the examples come
from an IoT scenario: a **gateway** that reads sensors (temperature, humidity, CO2...), drives
actuators, and talks to a cloud platform. Reading a sensor over a field bus is a typical slow I/O
operation. Let's start from the simplest possible gateway: a loop that polls the sensors of a room one
after the other. Each read is simulated with `time.sleep(0.5)`, which stands for the I/O latency of the
device.

`code/01_threading/01_sequential_polling.py`
```python
import random
import time

# Simulated I/O latency of a single sensor read (seconds)
READ_LATENCY_S = 0.5

# The sensors installed in a room of our Smart Building
SENSOR_IDS = ["temp-01", "temp-02", "hum-01", "hum-02", "co2-01"]


def read_sensor(sensor_id: str) -> float:
    """Simulate a blocking read of a sensor and return its measurement."""
    time.sleep(READ_LATENCY_S)          # the gateway is idle while waiting for the device
    rng = random.Random(sensor_id)      # deterministic value per sensor (reproducible output)
    return round(rng.uniform(18.0, 30.0), 1)


def main() -> None:
    start = time.perf_counter()

    readings: dict[str, float] = {}
    for sensor_id in SENSOR_IDS:
        readings[sensor_id] = read_sensor(sensor_id)
        print(f"[{time.perf_counter() - start:4.2f}s] {sensor_id} -> {readings[sensor_id]}")

    elapsed = time.perf_counter() - start
    print(f"Polled {len(readings)} sensors in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.50s] temp-01 -> 18.9
[1.00s] temp-02 -> 23.4
[1.50s] hum-01 -> 20.0
[2.00s] hum-02 -> 18.4
[2.50s] co2-01 -> 23.1
Polled 5 sensors in 2.50s
```

The total time is the **sum** of all the latencies: 5 sensors × 0.5 s = 2.5 s. With 100 sensors the
polling cycle would take 50 seconds, and a CO2 alarm could be detected almost a minute late. Yet the
CPU is idle for almost the whole time.

## 1.2 What We Want to Achieve

In general, what we want is to **overlap the waiting**: start all the independent operations, and
collect the results as soon as they arrive. For the gateway example, the timeline we have and the one we
want look like this:

```
Sequential (what we have)                 Concurrent (what we want)
time ─────────────────────────────►       time ──────►
temp-01 ████                              temp-01 ████
temp-02     ████                          temp-02 ████
hum-01          ████                      hum-01  ████
hum-02              ████                  hum-02  ████
co2-01                  ████              co2-01  ████
        total = sum of latencies                  total ≈ slowest latency
```

Python offers three main tools to get there, and this lecture series covers all of them:

| Tool | Module | Lecture |
|---|---|---|
| Threads | `threading` | this lecture |
| Pools of threads / processes | `concurrent.futures` | [02 - Thread Pools & Futures](02_thread_pools_and_futures.md) |
| Coroutines and an event loop | `asyncio` | [03 - Asyncio](03_asyncio.md) |

Before writing any concurrent code, however, we need a few precise concepts.

---

# 2 Core Concepts

## 2.1 Processes and Threads

- A **process** is a running program: it has its **own memory space**, its own resources (open
  files, sockets) and at least one thread. Two processes do not share memory: to exchange data they
  need explicit inter-process communication (pipes, sockets, queues).
- A **thread** is an independent flow of execution **inside a process**. All the threads of the
  same process **share the same memory**: global variables, objects, open connections.

```
┌──────────────────────── Process: app.py ────────────────────────────┐
│  Shared memory: global variables, objects, open sockets, files      │
│                                                                     │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│   │ MainThread  │    │ worker-1    │    │ worker-2    │             │
│   │ own stack   │    │ own stack   │    │ own stack   │             │
│   └─────────────┘    └─────────────┘    └─────────────┘             │
└─────────────────────────────────────────────────────────────────────┘
```

Sharing memory is both the **strength** of threads (exchanging data costs nothing: they just read
the same object) and their **main danger** (two threads modifying the same object at the same time
can corrupt it, see [Section 5](#5-race-conditions-and-locks)).

## 2.2 Concurrency vs. Parallelism

These two words are often used as synonyms, but they describe different things:

- **Concurrency** is about *structure*: the program is organized as several tasks that are **in
  progress at the same time**, and their execution can be interleaved. A single CPU core can run
  concurrent tasks by quickly switching between them.
- **Parallelism** is about *execution*: several tasks are **literally running at the same instant**
  on different CPU cores.

```
Concurrency on 1 core (interleaving)       Parallelism on 2 cores
core 0: A A B B A B A A B                  core 0: A A A A A
                                           core 1: B B B B B
```

A useful analogy: a single barista serving several customers by alternating between the espresso
machine and the milk steamer is **concurrent**; two baristas working side by side are **parallel**.

For programs whose tasks spend most of their time **waiting** (network servers, clients of remote
APIs, programs that talk to databases or devices), **concurrency is usually what we need**:
interleaving the tasks on one core is enough to overlap the waits. Parallelism matters when the tasks
are busy **computing**.

## 2.3 I/O-Bound vs. CPU-Bound Work

The single most important question to ask before choosing a concurrency tool is: *what is my task
limited by?*

| | I/O-bound | CPU-bound |
|---|---|---|
| Limited by | Waiting for external resources | Processor speed |
| Typical examples | Network requests, database queries, file reads/writes, waiting for user input or for a device | Numerical computation, image/video processing, compression, encryption, ML inference |
| IoT examples | Reading sensors, sending commands to actuators, HTTP/MQTT calls | Signal filtering (FFT, moving averages on large windows), processing camera frames |
| CPU usage while running | Low (mostly idle) | 100% of a core |
| What helps | Overlapping the waits: threads, `asyncio` | More cores: multiple processes |

## 2.4 Preemptive vs. Cooperative Multitasking

When several tasks share a CPU, *someone* decides when to switch from one to the other:

- **Preemptive multitasking** (threads): the **operating system** decides, and can interrupt a thread
  at almost any point, even in the middle of an operation, to give the CPU to another one. The code
  has no control over when this happens.
- **Cooperative multitasking** (`asyncio` coroutines): each task **voluntarily gives control back** at
  explicit points (`await`). Between two `await`s a task is never interrupted.

Preemption is convenient (a task stuck in a loop cannot freeze the others) but it is also the source
of *race conditions*: since a thread can be interrupted anywhere, any access to shared data must be
protected. We will come back to the cooperative model in [Lecture 03](03_asyncio.md).

---

# 3 The Global Interpreter Lock (GIL)

## 3.1 What the GIL Is and Why It Exists

**CPython**, the reference implementation of Python (the one you download from python.org), has a
**Global Interpreter Lock**: a mutex that allows **only one thread at a time to execute Python
bytecode**, even on a machine with many cores.

It exists for historical and practical reasons:

- CPython manages memory with **reference counting**: every object keeps a counter of how many
  references point to it. Updating these counters from several threads at once without protection
  would corrupt them, causing memory leaks or crashes.
- A single global lock is simple, makes single-threaded code fast (no fine-grained locking cost), and
  makes it easy to write C extensions that are safe by default.

The crucial detail is **when the GIL is released**:

- a thread that performs a **blocking I/O operation** (reading a socket, a file, a serial port,
  `time.sleep()`) **releases the GIL** while it waits, so other threads can run;
- a thread running pure Python computation holds the GIL and releases it only periodically (every
  few milliseconds) to let others take turns, but **only one of them computes at any instant**.

> **Consequence:** in standard CPython, threads give **concurrency** for I/O-bound work, but **no
> parallelism** for CPU-bound Python code.

## 3.2 Let's See How It Works: Threads vs. I/O-Bound and CPU-Bound Tasks

The following script runs the same amount of work sequentially and with 4 threads, first for an
I/O-bound task and then for a CPU-bound task.

**IoT example:** the I/O-bound task waits for a sensor answer; the CPU-bound task applies a
moving-average filter to a long signal.

`code/01_threading/02_io_bound_vs_cpu_bound.py`
```python
import sys
import threading
import time
from collections.abc import Callable

NUM_TASKS = 4


def io_bound_task() -> None:
    """Wait for a (simulated) sensor answer: the thread sleeps and releases the GIL."""
    time.sleep(0.5)


def cpu_bound_task() -> None:
    """Apply a moving-average filter to a synthetic signal: pure computation."""
    signal = [float(i % 100) for i in range(300_000)]
    window = 5
    filtered = []
    for i in range(len(signal) - window):
        filtered.append(sum(signal[i:i + window]) / window)


def run_sequential(task: Callable[[], None]) -> float:
    """Run NUM_TASKS copies of the task one after the other; return elapsed seconds."""
    start = time.perf_counter()
    for _ in range(NUM_TASKS):
        task()
    return time.perf_counter() - start


def run_threaded(task: Callable[[], None]) -> float:
    """Run NUM_TASKS copies of the task in parallel threads; return elapsed seconds."""
    start = time.perf_counter()
    threads = [threading.Thread(target=task) for _ in range(NUM_TASKS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - start


def main() -> None:
    # sys._is_gil_enabled() exists since Python 3.13; older versions always have the GIL
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"Python {sys.version.split()[0]} - GIL enabled: {gil_enabled}")

    for name, task in [("I/O-bound", io_bound_task), ("CPU-bound", cpu_bound_task)]:
        seq = run_sequential(task)
        thr = run_threaded(task)
        print(f"{name:10} sequential: {seq:5.2f}s | {NUM_TASKS} threads: {thr:5.2f}s "
              f"| speed-up: x{seq / thr:.1f}")


if __name__ == "__main__":
    main()
```

**Output** (exact times depend on your machine):
```
Python 3.14.4 - GIL enabled: True
I/O-bound  sequential:  2.00s | 4 threads:  0.50s | speed-up: x4.0
CPU-bound  sequential:  0.20s | 4 threads:  0.21s | speed-up: x1.0
```

**Key Points:**
- The **I/O-bound** workload gets a perfect x4 speed-up: the 4 threads wait *at the same time*.
- The **CPU-bound** workload gets **no speed-up at all** (it can even be slightly slower, because of
  the overhead of switching between threads): the GIL lets only one thread compute at a time.
- For CPU-bound work you need **multiple processes**, each with its own interpreter and its own GIL:
  see `ProcessPoolExecutor` in [Lecture 02](02_thread_pools_and_futures.md).

> **Note:** the GIL is a property of **CPython**, not of the Python language. Starting with Python
> 3.13 there is also an optional *free-threaded* build without the GIL: see
> [Section 8](#8-python-versions-and-free-threading).

---

# 4 The `threading` Module

## 4.1 Creating, Starting and Joining Threads

The `threading.Thread` class represents a thread. The minimum you need is:

| Step | Code | What happens |
|---|---|---|
| Create | `t = threading.Thread(target=func, args=(a, b), name="...")` | A thread *object* is created: **nothing runs yet** |
| Start | `t.start()` | The OS schedules a new thread that calls `func(a, b)`; `start()` returns immediately |
| Wait | `t.join()` | The *calling* thread blocks until `t` has finished |

**IoT example:** the polling loop of Section 1.1, rewritten with one thread per sensor:

`code/01_threading/03_first_threads.py`
```python
import random
import threading
import time

READ_LATENCY_S = 0.5
SENSOR_IDS = ["temp-01", "temp-02", "hum-01", "hum-02", "co2-01"]


def poll_sensor(sensor_id: str, start: float) -> None:
    """Read one sensor and print the value together with the name of the running thread."""
    time.sleep(READ_LATENCY_S)  # simulated blocking I/O
    value = round(random.Random(sensor_id).uniform(18.0, 30.0), 1)
    thread_name = threading.current_thread().name
    print(f"[{time.perf_counter() - start:4.2f}s] {thread_name:>14}: {sensor_id} -> {value}")


def main() -> None:
    start = time.perf_counter()

    # 1. Create the Thread objects: nothing runs yet
    threads = [
        threading.Thread(target=poll_sensor, args=(sensor_id, start), name=f"poller-{sensor_id}")
        for sensor_id in SENSOR_IDS
    ]

    # 2. start() asks the OS to schedule each thread: they now run concurrently
    for t in threads:
        t.start()
    print(f"Main thread: started {len(threads)} threads, active threads = {threading.active_count()}")

    # 3. join() blocks the main thread until each worker has finished
    for t in threads:
        t.join()

    print(f"All sensors polled in {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
```

**Output** (the order of the sensor lines changes from run to run):
```
Main thread: started 5 threads, active threads = 6
[0.50s] poller-temp-01: temp-01 -> 18.9
[0.50s]  poller-hum-02: hum-02 -> 18.4
[0.50s]  poller-hum-01: hum-01 -> 20.0
[0.50s]  poller-co2-01: co2-01 -> 23.1
[0.50s] poller-temp-02: temp-02 -> 23.4
All sensors polled in 0.50s
```

**Key Points:**
- The total time dropped from 2.5 s to **0.5 s**: the latency of a single read.
- `active_count()` is 6: the 5 pollers **plus the main thread**, which is itself a thread
  (`MainThread`).
- The **completion order is not deterministic**: it depends on how the OS schedules the threads.
  Never write code that relies on the order in which threads finish.
- `args` must be a **tuple**: for a single argument write `args=(sensor_id,)`, with the trailing comma.
- Giving threads a meaningful `name` costs nothing and makes logs and debugging much easier.

> **Note:** calling `t.run()` instead of `t.start()` does *not* create a new thread: the function
> runs in the caller's thread, sequentially. Always use `start()`.

## 4.2 Daemon Threads

When the main thread reaches the end of the program, the interpreter **waits for all the non-daemon
threads to finish** before exiting. A **daemon** thread, instead, is abruptly terminated when the
last non-daemon thread exits.

Daemon threads are meant for **background services that run forever** and whose interruption
does not matter (a heartbeat, a cache refresher, a metrics reporter).

**IoT example:** a gateway heartbeat:

`code/01_threading/04_daemon_threads.py`
```python
import threading
import time


def heartbeat(period_s: float) -> None:
    """Infinite loop: announce that the gateway is alive every `period_s` seconds."""
    beat = 0
    while True:
        beat += 1
        print(f"  [heartbeat] gateway alive (beat #{beat})")
        time.sleep(period_s)


def main() -> None:
    # daemon=True -> the interpreter will NOT wait for this thread at exit
    hb = threading.Thread(target=heartbeat, args=(0.3,), name="heartbeat", daemon=True)
    hb.start()

    print("Main: doing the real work for ~1 second...")
    time.sleep(1.0)
    print(f"Main: work done, heartbeat still alive? {hb.is_alive()}")
    print("Main: exiting (the daemon thread is killed abruptly)")
    # With daemon=False this program would never terminate!


if __name__ == "__main__":
    main()
```

**Output:**
```
  [heartbeat] gateway alive (beat #1)
Main: doing the real work for ~1 second...
  [heartbeat] gateway alive (beat #2)
  [heartbeat] gateway alive (beat #3)
  [heartbeat] gateway alive (beat #4)
Main: work done, heartbeat still alive? True
Main: exiting (the daemon thread is killed abruptly)
```

> **Note:** a daemon thread is killed **without any cleanup**: open files are not flushed, a
> half-written record or a half-sent message is never completed (in the IoT example: a command to an
> actuator). Never use daemon threads for work that must
> finish correctly: use a stop signal instead ([Section 7](#7-graceful-shutdown-of-worker-threads)).

## 4.3 Subclassing `Thread` and Collecting Results

`start()` returns `None`: there is no direct way to get the return value of the target function.
A clean, object-oriented solution is to **subclass `Thread`**, override `run()`, and store the
results as attributes of the thread object itself.

**IoT example:** a `SensorPoller` thread that reads a sensor a number of times and keeps the values:

`code/01_threading/05_thread_subclass.py`
```python
import random
import threading
import time


class SensorPoller(threading.Thread):
    """A thread that reads a sensor `samples` times and keeps the collected values."""

    def __init__(self, sensor_id: str, samples: int, period_s: float) -> None:
        # Always call the parent constructor first: it initialises the thread machinery
        super().__init__(name=f"poller-{sensor_id}")
        self.sensor_id = sensor_id
        self.samples = samples
        self.period_s = period_s
        self.readings: list[float] = []     # filled by run(), read by the main thread after join()
        self._rng = random.Random(sensor_id)

    def run(self) -> None:
        """Code executed in the new thread when start() is called (never call run() directly)."""
        for _ in range(self.samples):
            time.sleep(self.period_s)                       # wait for the next sample
            self.readings.append(round(self._rng.uniform(20.0, 25.0), 2))

    @property
    def average(self) -> float | None:
        """Average of the collected readings (None if nothing has been collected)."""
        return sum(self.readings) / len(self.readings) if self.readings else None


def main() -> None:
    pollers = [
        SensorPoller("temp-01", samples=3, period_s=0.2),
        SensorPoller("temp-02", samples=5, period_s=0.1),
        SensorPoller("temp-03", samples=2, period_s=0.3),
    ]
    for p in pollers:
        p.start()
    for p in pollers:
        p.join()        # after join() it is safe to read p.readings

    for p in pollers:
        print(f"{p.sensor_id}: {p.readings} -> avg {p.average:.2f}")


if __name__ == "__main__":
    main()
```

**Output:**
```
temp-01: [20.38, 21.67, 23.56] -> avg 21.87
temp-02: [22.25, 20.95, 21.17, 22.11, 24.18] -> avg 22.13
temp-03: [20.25, 21.07] -> avg 20.66
```

**Key Points:**
- Override **only** `__init__()` (calling `super().__init__()` first) and `run()`.
- Each thread writes **only its own** `readings` list, and the main thread reads them **only after
  `join()`**: there is no concurrent access to the same data, so no lock is needed.
- This pattern fits the OOP model of the previous lecture well: the worker *is a* thread with a
  clear responsibility. In [Lecture 02](02_thread_pools_and_futures.md) we will see that
  `concurrent.futures` gives us return values (and exceptions) for free.

---

# 5 Race Conditions and Locks

## 5.1 The Problem: a Race Condition on Shared State

A **race condition** happens when the result of a program depends on the unpredictable timing of
several threads accessing the **same data**, and at least one of them **modifies** it.

The classic case is a **shared counter or accumulator** updated by several threads. Updating it is a
**read-modify-write** sequence: *read* the current value, *compute* the new one, *write* it back. If
two threads interleave between the read and the write, one update is lost.

**IoT example:** an `EnergyMeter` that accumulates the energy reported by several smart plugs, each
one running in its own thread.

`code/01_threading/06_race_condition.py`
```python
import threading
import time

NUM_PLUGS = 5              # threads
REPORTS_PER_PLUG = 1_000   # updates sent by each thread
ENERGY_PER_REPORT_WH = 1   # each report adds 1 Wh


class EnergyMeter:
    """Accumulates the energy consumed by all the smart plugs of the building (NOT thread-safe)."""

    def __init__(self) -> None:
        self.total_wh = 0

    def add(self, energy_wh: int) -> None:
        current = self.total_wh          # 1. READ the shared value
        time.sleep(0)                    # 2. ...processing: the OS may switch thread here!
        self.total_wh = current + energy_wh   # 3. WRITE back a value computed from a stale read


def smart_plug(meter: EnergyMeter) -> None:
    """Simulate a smart plug that sends many small energy reports."""
    for _ in range(REPORTS_PER_PLUG):
        meter.add(ENERGY_PER_REPORT_WH)


def main() -> None:
    meter = EnergyMeter()
    plugs = [threading.Thread(target=smart_plug, args=(meter,)) for _ in range(NUM_PLUGS)]
    for p in plugs:
        p.start()
    for p in plugs:
        p.join()

    expected = NUM_PLUGS * REPORTS_PER_PLUG * ENERGY_PER_REPORT_WH
    print(f"Expected total: {expected} Wh")
    print(f"Actual total  : {meter.total_wh} Wh  -> lost {expected - meter.total_wh} Wh of readings!")


if __name__ == "__main__":
    main()
```

**Output** (the numbers change at every run):
```
Expected total: 5000 Wh
Actual total  : 1002 Wh  -> lost 3998 Wh of readings!
```

What went wrong? Here is one possible interleaving of two threads:

```
            Thread A (plug-1)              Thread B (plug-2)            total_wh
step 1      current = total_wh  (=10)                                   10
step 2      ── switch ──►                  current = total_wh  (=10)    10
step 3                                     total_wh = 10 + 1            11
step 4      ◄── switch ──
step 5      total_wh = 10 + 1                                           11   ← B's update is LOST
```

Both threads read `10`, both write `11`: two reports were received, but the total grew by one.

> **Note:** the `time.sleep(0)` only makes the problem **visible on every run**, by giving the OS an
> explicit chance to switch thread. Without it the bug is still there, it just shows up rarely:
> even `self.total_wh += energy_wh` is **not atomic** (it compiles to several bytecode instructions:
> load, add, store). A bug that appears "once a week in production" is much worse than one that
> appears on every run. **The GIL does not protect you from race conditions**: it protects the
> interpreter's internal data structures, not the consistency of *your* data.

## 5.2 Mutual Exclusion with `Lock`

A **lock** (or *mutex*, from *mutual exclusion*) guarantees that only one thread at a time executes
a given block of code, called the **critical section**:

- `lock.acquire()`: if the lock is free, take it; otherwise **block** until it is released;
- `lock.release()`: release the lock, waking up one of the waiting threads.

The Pythonic way to use it is the `with` statement, which **always** releases the lock, even when
an exception is raised inside the block.

**IoT example:** the energy meter of Section 5.1, now thread-safe:

`code/01_threading/07_lock_and_rlock.py` (part 1)
```python
import threading
import time

NUM_PLUGS = 5
REPORTS_PER_PLUG = 1_000


class SafeEnergyMeter:
    """Thread-safe energy meter: the read-modify-write is a critical section."""

    def __init__(self) -> None:
        self.total_wh = 0
        self._lock = threading.Lock()

    def add(self, energy_wh: int) -> None:
        # `with` acquires the lock and ALWAYS releases it, even if an exception is raised
        with self._lock:
            current = self.total_wh
            time.sleep(0)                      # a thread switch here is now harmless
            self.total_wh = current + energy_wh


def smart_plug(meter: SafeEnergyMeter) -> None:
    """Simulate a smart plug that sends many small energy reports."""
    for _ in range(REPORTS_PER_PLUG):
        meter.add(1)
```

**Output:**
```
Expected total: 5000 Wh | Actual total: 5000 Wh (0.29s)
```

**Key Points:**
- The lock is an **attribute of the object** that owns the shared data: the class is responsible for
  its own thread-safety, and the callers do not need to know about it (**encapsulation**).
- A thread switch inside the critical section is now harmless: the other threads cannot enter it
  until the lock is released.
- Keep critical sections **as short as possible**: while a thread holds the lock, all the others
  that need it are blocked. Never perform slow I/O (e.g. an HTTP upload) while holding a lock
  unless it is really necessary.
- Prefer `with lock:` over manual `acquire()`/`release()`: forgetting a `release()` (for example
  because of an exception) blocks every other thread forever.

## 5.3 Re-entrant Locks with `RLock`

A plain `Lock` does not remember *who* acquired it. If the thread that holds it tries to acquire it
**again**, it blocks forever, waiting for itself. This happens naturally when a method that holds the
lock calls another method of the same class that also takes the lock.

A **re-entrant lock** (`threading.RLock`) can be acquired several times **by the thread that owns
it**; it is released when the owner has called `release()` the same number of times.

**IoT example:** a device registry whose `register_many()` method calls `register()` while holding the
lock:

`code/01_threading/07_lock_and_rlock.py` (part 2)
```python
class DeviceRegistry:
    """
    A registry whose public methods call each other while holding the lock.
    With a plain Lock, register_many() -> register() would try to acquire a lock
    the same thread already holds and block forever. An RLock (re-entrant lock)
    can be acquired several times by the thread that owns it.
    """

    def __init__(self) -> None:
        self._devices: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, device_id: str, device_type: str) -> None:
        with self._lock:
            self._devices[device_id] = device_type

    def register_many(self, devices: dict[str, str]) -> None:
        with self._lock:                           # first acquisition
            for device_id, device_type in devices.items():
                self.register(device_id, device_type)   # second acquisition by the SAME thread: OK

    def count(self) -> int:
        with self._lock:
            return len(self._devices)
```

**Output:**
```
Registered devices: 2
```

> **Note:** use `Lock` by default and switch to `RLock` only when nested acquisition by the same
> thread is really part of the design. Replacing `RLock` with `Lock` in the class above makes
> `register_many()` hang forever: try it.

## 5.4 Deadlock

Locks solve race conditions but introduce a new risk: the **deadlock**, a situation where two (or
more) threads are each waiting for a lock held by the other, so none of them can proceed.

The classic scenario: two threads both need **two locks**, A and B, and they acquire them in the
**opposite order**. Thread 1 takes A and waits for B; thread 2 takes B and waits for A. Neither can
ever continue.

**IoT example:** a *scene controller* must lock **both** the HVAC and the window actuator (to never
open the window while the HVAC is cooling), and two scenes take the two locks in opposite order. To keep
the demo from hanging, we use `acquire(timeout=...)` to *detect* the deadlock:

`code/01_threading/08_deadlock.py`
```python
import threading
import time

hvac_lock = threading.Lock()
window_lock = threading.Lock()


def scene(name: str, first: threading.Lock, second: threading.Lock,
          first_name: str, second_name: str) -> None:
    """Acquire two actuator locks in the given order and apply a scene."""
    with first:
        print(f"{name}: locked {first_name}")
        time.sleep(0.1)                           # give the other thread time to grab its first lock
        if second.acquire(timeout=1.0):           # a plain `with second:` would block forever
            try:
                print(f"{name}: locked {second_name} -> scene applied")
            finally:
                second.release()
        else:
            print(f"{name}: DEADLOCK detected, could not lock {second_name} (giving up)")


def run(title: str, targets: list[tuple]) -> None:
    print(f"--- {title} ---")
    threads = [threading.Thread(target=scene, args=args) for args in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main() -> None:
    # Opposite order -> deadlock
    run("Opposite lock order", [
        ("night-scene  ", hvac_lock, window_lock, "HVAC", "window"),
        ("morning-scene", window_lock, hvac_lock, "window", "HVAC"),
    ])
    # Fix: every thread acquires the locks in the SAME global order (HVAC first, then window)
    run("Same lock order", [
        ("night-scene  ", hvac_lock, window_lock, "HVAC", "window"),
        ("morning-scene", hvac_lock, window_lock, "HVAC", "window"),
    ])


if __name__ == "__main__":
    main()
```

**Output:**
```
--- Opposite lock order ---
night-scene  : locked HVAC
morning-scene: locked window
night-scene  : DEADLOCK detected, could not lock window (giving up)
morning-scene: locked HVAC -> scene applied
--- Same lock order ---
night-scene  : locked HVAC
night-scene  : locked window -> scene applied
morning-scene: locked HVAC
morning-scene: locked window -> scene applied
```

**Key Points - how to avoid deadlocks:**
- **Lock ordering:** define a global order for the locks and always acquire them in that order.
- **Hold as few locks as possible**, for as short a time as possible; avoid calling unknown code
  (callbacks, handlers) while holding a lock.
- **Timeouts:** `acquire(timeout=...)` turns a silent freeze into a detectable, loggable error.
- **Avoid shared state altogether** when you can: passing messages through a thread-safe queue
  ([Lecture 04](04_queues.md)) removes most of the need for explicit locks.

---

# 6 Coordination Primitives

Locks protect data. Often, however, threads also need to **coordinate**: wait for something to
happen, wait for a condition to become true, limit access to a resource, or run an action later.
The `threading` module offers a primitive for each of these needs.

## 6.1 `Event`: Signaling Between Threads

An `Event` is a thread-safe **boolean flag**:

| Method | Effect |
|---|---|
| `set()` | Set the flag to `True` and wake up **all** the threads waiting on it |
| `clear()` | Reset the flag to `False` |
| `is_set()` | Return the current value of the flag |
| `wait(timeout=None)` | Block until the flag is `True` (or the timeout expires); return the flag value |

The typical use is "*one thread signals, one or more threads react*", without the waiting threads
having to poll a variable in a loop.

**IoT example:** a CO2 monitor raises an alarm, and a window actuator reacts to it:

`code/01_threading/09_event.py`
```python
import threading
import time

CO2_THRESHOLD_PPM = 1000
# Simulated CO2 readings coming from the sensor, one every 0.2 s
CO2_SAMPLES_PPM = [620, 700, 810, 930, 1050, 1120]


def co2_monitor(alarm: threading.Event) -> None:
    """Read the CO2 sensor and raise the alarm when the threshold is exceeded."""
    for ppm in CO2_SAMPLES_PPM:
        time.sleep(0.2)
        print(f"[co2-monitor] CO2 = {ppm} ppm")
        if ppm > CO2_THRESHOLD_PPM:
            print("[co2-monitor] threshold exceeded -> alarm.set()")
            alarm.set()          # wakes up EVERY thread waiting on this event
            return


def window_actuator(alarm: threading.Event) -> None:
    """Block until the alarm is raised, then open the window."""
    print("[window] waiting for the CO2 alarm...")
    alarm.wait()                 # sleeps without consuming CPU until set() is called
    print("[window] alarm received -> opening the window")


def main() -> None:
    alarm = threading.Event()    # internal flag, initially False
    threads = [
        threading.Thread(target=window_actuator, args=(alarm,)),
        threading.Thread(target=co2_monitor, args=(alarm,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"Alarm flag is set: {alarm.is_set()}")


if __name__ == "__main__":
    main()
```

**Output:**
```
[window] waiting for the CO2 alarm...
[co2-monitor] CO2 = 620 ppm
[co2-monitor] CO2 = 700 ppm
[co2-monitor] CO2 = 810 ppm
[co2-monitor] CO2 = 930 ppm
[co2-monitor] CO2 = 1050 ppm
[co2-monitor] threshold exceeded -> alarm.set()
[window] alarm received -> opening the window
Alarm flag is set: True
```

**Key Points:**
- `wait()` puts the thread to sleep **without consuming CPU**: compare it with a *busy-wait* loop like
  `while not alarm_flag: pass`, which burns a whole core doing nothing.
- An `Event` is the standard way to implement a **stop signal** for worker threads
  ([Section 7](#7-graceful-shutdown-of-worker-threads)), and it is also one of the most useful tools
  to write **deterministic tests** for threaded code ([Lecture 06](06_testing.md)).

## 6.2 `Condition`: Waiting for a State Change

An `Event` signals *that something happened*. Sometimes a thread must instead wait until a
**condition on shared data** becomes true, e.g. "the buffer contains at least 3 items". A
`Condition` combines a **lock** (protecting the shared data) with a **wait/notify** mechanism:

- `wait()` / `wait_for(predicate)`: **release the lock** and sleep until notified; on wake-up the lock
  is re-acquired (and with `wait_for` the predicate is re-checked automatically);
- `notify()` / `notify_all()`: wake up one / all the waiting threads (must be called while holding
  the lock).

**IoT example:** a sensor thread appends readings to a shared buffer; an uploader thread must wait
until at least 3 readings are available before sending a batch to the cloud:

`code/01_threading/10_condition.py`
```python
import threading
import time

BATCH_SIZE = 3
TOTAL_READINGS = 9

buffer: list[float] = []
condition = threading.Condition()   # contains its own Lock


def sensor_producer() -> None:
    """Produce one temperature reading every 0.1 s."""
    for i in range(TOTAL_READINGS):
        time.sleep(0.1)
        with condition:                     # acquire the underlying lock
            buffer.append(20.0 + i * 0.1)
            print(f"[sensor]   buffered reading #{i + 1} (buffer size = {len(buffer)})")
            condition.notify()              # wake up one waiting thread


def cloud_uploader() -> None:
    """Upload readings in batches of BATCH_SIZE."""
    uploaded = 0
    while uploaded < TOTAL_READINGS:
        with condition:
            # wait_for() releases the lock while sleeping and re-checks the predicate on wake-up
            condition.wait_for(lambda: len(buffer) >= BATCH_SIZE)
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]
        # the (slow) upload happens OUTSIDE the lock, so the producer is not blocked
        print(f"[uploader] uploading batch {[round(v, 1) for v in batch]}")
        uploaded += len(batch)


def main() -> None:
    threads = [threading.Thread(target=cloud_uploader), threading.Thread(target=sensor_producer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
```

**Output:**
```
[sensor]   buffered reading #1 (buffer size = 1)
[sensor]   buffered reading #2 (buffer size = 2)
[sensor]   buffered reading #3 (buffer size = 3)
[uploader] uploading batch [20.0, 20.1, 20.2]
[sensor]   buffered reading #4 (buffer size = 1)
[sensor]   buffered reading #5 (buffer size = 2)
[sensor]   buffered reading #6 (buffer size = 3)
[uploader] uploading batch [20.3, 20.4, 20.5]
[sensor]   buffered reading #7 (buffer size = 1)
[sensor]   buffered reading #8 (buffer size = 2)
[sensor]   buffered reading #9 (buffer size = 3)
[uploader] uploading batch [20.6, 20.7, 20.8]
```

> **Note:** always wait on a condition in a loop (or with `wait_for`, which contains the loop): a
> thread may be woken up and find that the condition is *no longer* true because another thread got
> there first. This producer/consumer code is correct but verbose and easy to get wrong. The
> `queue.Queue` class ([Lecture 04](04_queues.md)) is essentially this same mechanism, already
> implemented and tested for you.

## 6.3 `Semaphore`: Limiting Concurrent Access

A **semaphore** is a counter of available "slots":

- `acquire()` decrements the counter; if it is already 0, the thread **blocks** until a slot is freed;
- `release()` increments it, waking up a waiting thread.

It is the right tool when a resource can be used by **at most N threads at a time**: a pool of
database connections, a remote API with a limit on concurrent requests, a hardware controller.

**IoT example:** six sensor threads share an I2C bus controller that can serve at most 2 transactions
at once.

`code/01_threading/11_semaphore.py`
```python
import threading
import time

MAX_CONCURRENT_BUS_ACCESSES = 2
bus_slots = threading.BoundedSemaphore(MAX_CONCURRENT_BUS_ACCESSES)

active = 0                      # how many threads are on the bus right now (for the demo)
active_lock = threading.Lock()


def read_over_i2c(sensor_id: str, start: float) -> None:
    global active
    with bus_slots:                             # blocks if 2 threads already hold a slot
        with active_lock:
            active += 1
            print(f"[{time.perf_counter() - start:4.2f}s] {sensor_id} on the bus (active = {active})")
        time.sleep(0.3)                         # the I2C transaction
        with active_lock:
            active -= 1
    # leaving the `with` releases the slot and lets a waiting thread in


def main() -> None:
    start = time.perf_counter()
    sensors = [f"sensor-{i:02d}" for i in range(1, 7)]
    threads = [threading.Thread(target=read_over_i2c, args=(s, start)) for s in sensors]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"6 reads with 2 bus slots took {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.00s] sensor-01 on the bus (active = 1)
[0.00s] sensor-02 on the bus (active = 2)
[0.30s] sensor-03 on the bus (active = 2)
[0.30s] sensor-04 on the bus (active = 2)
[0.60s] sensor-05 on the bus (active = 2)
[0.60s] sensor-06 on the bus (active = 2)
6 reads with 2 bus slots took 0.90s
```

**Key Points:**
- The 6 reads proceed in waves of 2: the semaphore never lets more than 2 threads on the bus.
- `BoundedSemaphore` behaves like `Semaphore` but raises `ValueError` if `release()` is called more
  times than `acquire()`: a cheap safety net against bugs. Prefer it.
- A `Lock` is conceptually a semaphore with a single slot.

## 6.4 `Timer`: Delayed Actions

A `threading.Timer` is a thread that runs a function **once, after a delay**, and can be
**cancelled** before it fires. Typical uses: timeouts, delayed retries, "*do X unless something
happens first*" (debouncing).

**IoT example:** switch off a corridor light automatically, a few seconds after the last detected
motion; new motion cancels the pending timer and starts a new one.

`code/01_threading/12_timer.py`
```python
import threading
import time

AUTO_OFF_S = 0.5


class CorridorLight:
    """Smart light with an automatic switch-off timer."""

    def __init__(self) -> None:
        self.is_on = False
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()
        self._start = time.perf_counter()

    def _log(self, msg: str) -> None:
        print(f"[{time.perf_counter() - self._start:4.2f}s] {msg}")

    def _turn_off(self) -> None:
        """Called by the Timer thread when the delay expires."""
        with self._lock:
            self.is_on = False
            self._log("light OFF (auto-off timer expired)")

    def on_motion_detected(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()             # motion again: postpone the switch-off
            self.is_on = True
            self._log("motion detected -> light ON, auto-off rescheduled")
            self._timer = threading.Timer(AUTO_OFF_S, self._turn_off)
            self._timer.start()


def main() -> None:
    light = CorridorLight()
    light.on_motion_detected()
    time.sleep(0.3)
    light.on_motion_detected()      # before the first timer expires -> it is cancelled
    time.sleep(1.0)                 # no more motion: the second timer fires
    print(f"Final state: {'ON' if light.is_on else 'OFF'}")


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.00s] motion detected -> light ON, auto-off rescheduled
[0.30s] motion detected -> light ON, auto-off rescheduled
[0.80s] light OFF (auto-off timer expired)
Final state: OFF
```

> **Note:** `_turn_off()` runs in the **Timer's own thread**, while `on_motion_detected()` runs in the
> caller's thread: both touch `is_on`, so both take the lock.

## 6.5 `threading.local`: Per-Thread Data

Sometimes each thread needs its **own copy** of a resource that the code accesses through a single
global name, for example a connection (to a database, to a remote service) that is not safe to share
between threads. A `threading.local()` object has attributes whose values are **different in every
thread**.

**IoT example:** each worker keeps its own connection to a Modbus field bus:

`code/01_threading/13_thread_local.py`
```python
import threading

ctx = threading.local()


def get_connection() -> str:
    """Lazily create one connection per thread and reuse it afterwards."""
    if not hasattr(ctx, "connection"):
        ctx.connection = f"modbus-conn<{threading.current_thread().name}>"
        print(f"{threading.current_thread().name}: opened {ctx.connection}")
    return ctx.connection


def worker(requests: int) -> None:
    for _ in range(requests):
        conn = get_connection()         # opened only on the first call of each thread
    print(f"{threading.current_thread().name}: served {requests} requests with {conn}")


def main() -> None:
    threads = [threading.Thread(target=worker, args=(3,), name=f"worker-{i}") for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
```

**Output:**
```
worker-0: opened modbus-conn<worker-0>
worker-0: served 3 requests with modbus-conn<worker-0>
worker-1: opened modbus-conn<worker-1>
worker-1: served 3 requests with modbus-conn<worker-1>
```

## 6.6 Summary of the Primitives

| Primitive | Question it answers | Typical use | IoT example in this lecture |
|---|---|---|---|
| `Lock` | "Only one thread at a time may touch this data" | Shared counters, caches, registries | Shared energy counter |
| `RLock` | "...and the same thread may re-enter" | Methods of a class calling each other | Device registry |
| `Event` | "Has something happened?" | Stop signals, "ready" notifications | CO2 alarm |
| `Condition` | "Wait until this state is true" | Bounded buffers, batching | Batch of readings ready for upload |
| `Semaphore` | "At most N threads at a time" | Connection pools, rate limits | Shared I2C bus |
| `Timer` | "Do this later, unless cancelled" | Timeouts, debouncing, delayed retries | Auto-off light |
| `local` | "Each thread has its own copy" | Per-thread connections or context | Per-thread Modbus connection |

---

# 7 Graceful Shutdown of Worker Threads

Long-running programs (servers, daemons, background services) often run worker threads **forever**,
until someone stops them (a `Ctrl+C`, a configuration reload, a service restart). Python deliberately has **no API to kill a thread from outside**: a
thread killed in the middle of an operation could leave a lock acquired or a command half-sent.

The idiomatic solution is **cooperative**:

1. every worker loops **while a shared stop `Event` is not set**;
2. instead of `time.sleep(period)`, it waits with **`stop_event.wait(period)`**, which returns
   immediately when the event is set;
3. the main thread sets the event and **joins each worker with a timeout**.

**IoT example:** two periodic sensor pollers, one of them with a long (5 s) sampling period:

`code/01_threading/14_graceful_shutdown.py`
```python
import threading
import time


def periodic_poller(sensor_id: str, period_s: float, stop_event: threading.Event) -> None:
    """Sample a sensor every `period_s` seconds until the stop event is set."""
    samples = 0
    while not stop_event.is_set():
        samples += 1                                  # ...read the sensor here...
        # wait() returns True as soon as the event is set -> no need to finish the full period
        if stop_event.wait(timeout=period_s):
            break
    print(f"{sensor_id}: stopped cleanly after {samples} samples")


def main() -> None:
    stop_event = threading.Event()
    pollers = [
        threading.Thread(target=periodic_poller, args=("temp-01", 0.1, stop_event), name="temp-01"),
        # a slow sensor: with time.sleep(5) the shutdown would take up to 5 seconds
        threading.Thread(target=periodic_poller, args=("co2-01", 5.0, stop_event), name="co2-01"),
    ]
    for p in pollers:
        p.start()

    time.sleep(0.55)                                  # the gateway runs for a while...
    print("Main: shutdown requested")
    t0 = time.perf_counter()
    stop_event.set()                                  # 1. ask every worker to stop
    for p in pollers:
        p.join(timeout=2.0)                           # 2. wait, but never forever
        if p.is_alive():
            print(f"Main: WARNING {p.name} did not stop in time")
    print(f"Main: all pollers stopped in {time.perf_counter() - t0:.3f}s")


if __name__ == "__main__":
    main()
```

**Output:**
```
Main: shutdown requested
temp-01: stopped cleanly after 6 samples
co2-01: stopped cleanly after 1 samples
Main: all pollers stopped in 0.000s
```

**Key Points:**
- The CO2 poller has a 5-second period, yet it stops **instantly**: `stop_event.wait(5.0)` is
  interrupted by `set()`. With `time.sleep(5.0)` the shutdown would take up to 5 seconds.
- `join(timeout=...)` guarantees that the main thread never hangs because of a misbehaving worker;
  checking `is_alive()` afterwards tells you whether the timeout expired.
- The same pattern (stop signal + join with timeout) will come back with queues
  ([Lecture 04](04_queues.md)), where the stop signal can also be a special *sentinel* message.

---

# 8 Python Versions and Free-Threading

The GIL has been part of CPython for over 30 years, but things are changing:

- **Python 3.13 (October 2024)** introduced an **experimental free-threaded build** of CPython
  ([PEP 703](https://peps.python.org/pep-0703/)): a separate interpreter binary, usually called
  `python3.13t`, compiled **without the GIL**. In this build, threads running Python code can
  truly execute **in parallel** on multiple cores.
- **Python 3.14 (October 2025)** moved the free-threaded build from *experimental* to **officially
  supported** ([PEP 779](https://peps.python.org/pep-0779/)), with a much smaller single-thread
  performance penalty. It is still an **optional** build: the default `python3` keeps the GIL.

You can check which interpreter you are running:

`code/01_threading/15_free_threading_check.py`
```python
import sys
import sysconfig


def main() -> None:
    # 1 if the interpreter was COMPILED with free-threading support, 0/None otherwise
    free_threaded_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    # Even a free-threaded build can re-enable the GIL at runtime (PYTHON_GIL=1, or an
    # extension module that does not declare itself free-threading safe)
    gil_enabled_now = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True

    print(f"Python version       : {sys.version.split()[0]}")
    print(f"Free-threaded build  : {free_threaded_build}")
    print(f"GIL currently enabled: {gil_enabled_now}")


if __name__ == "__main__":
    main()
```

**Output** (standard build):
```
Python version       : 3.14.4
Free-threaded build  : False
GIL currently enabled: True
```

With `uv` you can try the free-threaded build without touching your system Python:
`uv run --python 3.14t 02_io_bound_vs_cpu_bound.py`. On a multi-core machine the CPU-bound
speed-up of [Section 3.2](#32-lets-see-how-it-works-threads-vs-io-bound-and-cpu-bound-tasks) will
no longer be x1.0.

**What free-threading does and does not change:**

| | Changes? |
|---|---|
| CPU-bound Python code in threads can use multiple cores | **Yes** |
| I/O-bound code in threads | No, it already worked well with the GIL |
| Need for locks around shared data | **No, if anything it becomes more important**: race conditions like the one in Section 5.1 happen *more* often |
| Every third-party C extension works | Not yet: extensions must be rebuilt/declared free-threading safe, otherwise the GIL is re-enabled |

> **Note:** everything you learn in this lecture about locks, events and shutdown applies
> unchanged to the free-threaded build. Correct concurrent code does not rely on the GIL.

Other concurrency-related changes in recent versions, covered in the next lectures:

| Version | Change | Lecture |
|---|---|---|
| 3.11 | `asyncio.TaskGroup`, `asyncio.timeout()`, `ExceptionGroup` / `except*` | [03](03_asyncio.md) |
| 3.13 | `queue.Queue.shutdown()` and the `queue.ShutDown` exception | [04](04_queues.md) |
| 3.13 | `ThreadPoolExecutor` default `max_workers` uses `os.process_cpu_count()` | [02](02_thread_pools_and_futures.md) |

---

# 9 Let's See How It Works: Putting It All Together

This section combines the concepts of the lecture into one small but complete multithreaded
application, organised the way real ones usually are: several **producer threads** that each do their
own periodic work, one **shared object** that encapsulates its lock, one **consumer thread** that makes
decisions, a single **stop signal**, and **logging** that shows which thread did what.

**IoT example: a Smart Room gateway.**

- one `SensorPoller` thread per sensor, each with **its own sampling period**;
- a shared, **lock-protected** `RoomState` with the latest reading of every sensor;
- a `ClimateController` thread that applies a rule (*temperature > 24 °C → HVAC in COOLING mode*);
- a single **stop `Event`** for a graceful shutdown;
- **`logging`** with thread names instead of `print()` (as recommended in the Best Practices lecture).

```
 temp-01 ─► SensorPoller ──update()──┐
                                     ▼
                          ┌─────────────────────┐              ┌───────────────────┐
                          │ RoomState (+ Lock)  │──snapshot()─►│ ClimateController │──set_mode()──► hvac-01
                          └─────────────────────┘              └───────────────────┘
                                     ▲
 hum-01  ─► SensorPoller ──update()──┘

          every thread stops as soon as stop_event.set() is called
```

`code/01_threading/16_threaded_gateway.py`
```python
import logging
import random
import threading
import time
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(relativeCreated)6.0fms [%(threadName)-12s] %(message)s",
)
logger = logging.getLogger("gateway")


@dataclass
class Reading:
    """A single measurement produced by a sensor."""
    sensor_id: str
    value: float
    unit: str
    timestamp: float


class RoomState:
    """Latest reading of every sensor of the room. Thread-safe."""

    def __init__(self) -> None:
        self._latest: dict[str, Reading] = {}
        self._lock = threading.Lock()

    def update(self, reading: Reading) -> None:
        with self._lock:
            self._latest[reading.sensor_id] = reading

    def snapshot(self) -> dict[str, Reading]:
        """Return a COPY, so callers can iterate without holding the lock."""
        with self._lock:
            return dict(self._latest)


class SimulatedSensor:
    """A sensor whose read() blocks for a while, like a real field-bus request."""

    def __init__(self, sensor_id: str, unit: str, base: float, drift: float) -> None:
        self.sensor_id = sensor_id
        self.unit = unit
        self._value = base
        self._drift = drift
        self._rng = random.Random(sensor_id)

    def read(self) -> Reading:
        time.sleep(0.05)                                     # simulated I/O latency
        self._value += self._drift + self._rng.uniform(-0.1, 0.1)
        return Reading(self.sensor_id, round(self._value, 2), self.unit, time.time())


class HVACActuator:
    """A (simulated) HVAC unit that can be switched between modes."""

    def __init__(self, actuator_id: str) -> None:
        self.actuator_id = actuator_id
        self.mode = "OFF"

    def set_mode(self, mode: str) -> None:
        if mode != self.mode:
            logger.info("%s: %s -> %s", self.actuator_id, self.mode, mode)
            self.mode = mode


class SensorPoller(threading.Thread):
    """Periodically reads one sensor and stores the value in the shared RoomState."""

    def __init__(self, sensor: SimulatedSensor, period_s: float,
                 state: RoomState, stop_event: threading.Event) -> None:
        super().__init__(name=sensor.sensor_id)
        self.sensor = sensor
        self.period_s = period_s
        self.state = state
        self.stop_event = stop_event

    def run(self) -> None:
        while not self.stop_event.is_set():
            reading = self.sensor.read()
            self.state.update(reading)
            logger.info("%s = %.2f %s", reading.sensor_id, reading.value, reading.unit)
            self.stop_event.wait(self.period_s)
        logger.info("poller stopped")


class ClimateController(threading.Thread):
    """Reads the room state and applies the rule: temperature > 24 C -> COOLING."""

    def __init__(self, state: RoomState, hvac: HVACActuator,
                 stop_event: threading.Event, threshold_c: float = 24.0) -> None:
        super().__init__(name="controller")
        self.state = state
        self.hvac = hvac
        self.stop_event = stop_event
        self.threshold_c = threshold_c

    def run(self) -> None:
        while not self.stop_event.wait(0.25):     # evaluate the rule every 250 ms
            temperature = self.state.snapshot().get("temp-01")
            if temperature is not None:
                self.hvac.set_mode("COOLING" if temperature.value > self.threshold_c else "OFF")
        logger.info("controller stopped")


def main() -> None:
    state = RoomState()
    stop_event = threading.Event()
    hvac = HVACActuator("hvac-01")

    threads: list[threading.Thread] = [
        SensorPoller(SimulatedSensor("temp-01", "C", base=23.5, drift=0.2), 0.2, state, stop_event),
        SensorPoller(SimulatedSensor("hum-01", "%", base=45.0, drift=0.5), 0.4, state, stop_event),
        ClimateController(state, hvac, stop_event),
    ]
    for t in threads:
        t.start()

    time.sleep(1.5)                     # let the gateway run for a while
    logger.info("shutdown requested")
    stop_event.set()
    for t in threads:
        t.join(timeout=2.0)

    for sensor_id, reading in state.snapshot().items():
        logger.info("final %s = %.2f %s", sensor_id, reading.value, reading.unit)
    logger.info("final HVAC mode = %s", hvac.mode)


if __name__ == "__main__":
    main()
```

**Output** (timings vary slightly between runs):
```
    52ms [temp-01     ] temp-01 = 23.62 C
    52ms [hum-01      ] hum-01 = 45.43 %
   302ms [temp-01     ] temp-01 = 23.78 C
   502ms [hum-01      ] hum-01 = 46.03 %
   553ms [temp-01     ] temp-01 = 24.02 C
   752ms [controller  ] hvac-01: OFF -> COOLING
   803ms [temp-01     ] temp-01 = 24.13 C
   952ms [hum-01      ] hum-01 = 46.55 %
  1054ms [temp-01     ] temp-01 = 24.33 C
  1304ms [temp-01     ] temp-01 = 24.62 C
  1403ms [hum-01      ] hum-01 = 47.07 %
  1502ms [MainThread  ] shutdown requested
  1502ms [controller  ] controller stopped
  1502ms [hum-01      ] poller stopped
  1502ms [temp-01     ] poller stopped
  1503ms [MainThread  ] final temp-01 = 24.62 C
  1503ms [MainThread  ] final hum-01 = 47.07 %
  1503ms [MainThread  ] final HVAC mode = COOLING
```

**Key Points:**
- Every class has **one responsibility**: sensors read, pollers schedule, `RoomState` stores, the
  controller decides, the actuator acts, exactly as in the OOP lecture's delegation principle.
- The only shared mutable object is `RoomState`, and it **encapsulates its own lock**. `snapshot()`
  returns a copy so that the controller never iterates over a dictionary that another thread is
  modifying.
- The `%(threadName)s` field of the log format tells you, for every line, which thread produced it:
  invaluable when debugging concurrent code.
- The shutdown is **immediate and clean**: every thread notices the stop event within milliseconds.

**Limits of this design** (addressed in the next lectures):
- One thread per task does not scale to thousands of tasks (here: thousands of devices) → **thread pools**
  ([Lecture 02](02_thread_pools_and_futures.md)) or **asyncio** ([Lecture 03](03_asyncio.md)).
- The consumer *polls* the shared state every 250 ms, even when nothing has changed → passing data
  through a **queue** ([Lecture 04](04_queues.md)) and reacting to **events**
  ([Lecture 05](05_event_driven_architecture.md)).
- How do we **test** multithreaded code (e.g. that the controller really switches the HVAC) without
  relying on `time.sleep()`? → [Lecture 06](06_testing.md).

---

# 10 References

- Python documentation - [`threading` - Thread-based parallelism](https://docs.python.org/3/library/threading.html)
- Python documentation - [Python support for free threading](https://docs.python.org/3/howto/free-threading-python.html)
- [PEP 703 - Making the Global Interpreter Lock Optional in CPython](https://peps.python.org/pep-0703/)
- [PEP 779 - Criteria for supported status for free-threaded Python](https://peps.python.org/pep-0779/)
- Real Python - [What Is the Python Global Interpreter Lock (GIL)?](https://realpython.com/python-gil/)
- Real Python - [An Intro to Threading in Python](https://realpython.com/intro-to-python-threading/)
- Real Python - [Speed Up Your Python Program With Concurrency](https://realpython.com/python-concurrency/)
