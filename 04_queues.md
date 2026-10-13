<!-- omit in toc -->
# The `queue` Module: Thread-Safe Queues

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - The `queue` Module and the Producer-Consumer Pattern    |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **Prerequisites:** [01 - Concurrency Fundamentals & Threading](01_concurrency_and_threading.md)
> (threads, locks, `Condition`). Sections 6.2 and 6.3 also refer to [03 - Asyncio](03_asyncio.md) and to
> `ProcessPoolExecutor` from [02 - Thread Pools & Futures](02_thread_pools_and_futures.md).
> **Runnable code:** [`code/04_queues/`](code/04_queues/). Some examples require **Python 3.13+**.

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 Why a Thread-Safe Queue?](#1-why-a-thread-safe-queue)
  - [1.1 Passing Data Between Threads](#11-passing-data-between-threads)
  - [1.2 The `queue` Module at a Glance](#12-the-queue-module-at-a-glance)
- [2 The Queue Classes](#2-the-queue-classes)
  - [2.1 FIFO, LIFO, Priority and Simple Queues](#21-fifo-lifo-priority-and-simple-queues)
  - [2.2 Robust Priority Messages with Dataclasses](#22-robust-priority-messages-with-dataclasses)
- [3 The Queue API Step by Step](#3-the-queue-api-step-by-step)
  - [3.1 Blocking, Timeouts and Non-Blocking Calls](#31-blocking-timeouts-and-non-blocking-calls)
  - [3.2 Tracking Completion: `task_done()` and `join()`](#32-tracking-completion-task_done-and-join)
- [4 The Producer-Consumer Pattern and Backpressure](#4-the-producer-consumer-pattern-and-backpressure)
- [5 Clean Shutdown of Consumers](#5-clean-shutdown-of-consumers)
  - [5.1 The Sentinel (Poison Pill) Pattern](#51-the-sentinel-poison-pill-pattern)
  - [5.2 `Queue.shutdown()` (Python 3.13+)](#52-queueshutdown-python-313)
- [6 Related Data Structures](#6-related-data-structures)
  - [6.1 `collections.deque`](#61-collectionsdeque)
  - [6.2 `asyncio.Queue`](#62-asyncioqueue)
  - [6.3 `multiprocessing.Queue`](#63-multiprocessingqueue)
  - [6.4 Which Queue Should I Use?](#64-which-queue-should-i-use)
- [7 Let's See How It Works: a Multi-Stage Pipeline](#7-lets-see-how-it-works-a-multi-stage-pipeline)
- [8 References](#8-references)
<!-- /TOC -->

---

# 1 Why a Thread-Safe Queue?

## 1.1 Passing Data Between Threads

In Lecture 01 threads communicated through **shared state**: an object protected by a lock, which
the consumer thread had to **poll** periodically. In
[Section 6.2 of Lecture 01](01_concurrency_and_threading.md#62-condition-waiting-for-a-state-change)
we also built a buffer between a producer thread and a consumer thread with a `list` plus a
`Condition`: it worked, but it needed a lock, a `wait_for()` predicate, `notify()` calls, and care
to avoid holding the lock during the slow processing.

This situation, where some threads **produce** data and others **consume** it, is so common that it has
a name, the **producer-consumer pattern**, and a standard data structure that solves it: a
**thread-safe blocking queue**.

```
  producers                         queue (FIFO)                        consumers
┌───────────┐  put()   ┌───────────────────────────────────┐   get()  ┌────────────┐
│ temp-01   │ ───────► │  r7  r6  r5  r4  r3               │ ───────► │ uploader-0 │
│ hum-01    │ ───────► │  (thread-safe, optionally bounded)│ ───────► │ uploader-1 │
└───────────┘          └───────────────────────────────────┘          └────────────┘
   producers block if the queue is FULL        consumers block if the queue is EMPTY
```

A queue gives you four things at once:

1. **thread-safety**: `put()` and `get()` can be called from any number of threads with no extra locks;
2. **blocking semantics**: a consumer that calls `get()` on an empty queue **sleeps** (without using
   CPU) until an item arrives, so no polling is needed;
3. **decoupling**: producers and consumers do not know about each other, they only know the queue;
4. **backpressure**: a bounded queue (`maxsize`) slows down producers that are faster than consumers.

## 1.2 The `queue` Module at a Glance

The standard library module is called **`queue`** (all lowercase; in Python 2 it was `Queue`). If you
know Java, `queue.Queue` is the conceptual equivalent of a `LinkedBlockingQueue`. The module provides:

- four queue classes: `Queue`, `LifoQueue`, `PriorityQueue`, `SimpleQueue`;
- two exceptions raised by non-blocking operations: `queue.Empty` and `queue.Full`;
- since Python 3.13, the `queue.ShutDown` exception, raised after `Queue.shutdown()`.

---

# 2 The Queue Classes

## 2.1 FIFO, LIFO, Priority and Simple Queues

The four classes differ mainly in the **order in which items are extracted**:

| Class | Extraction order | Typical use (IoT example) |
|---|---|---|
| `queue.Queue(maxsize=0)` | **FIFO**: first in, first out | Jobs for a worker pool, streams of messages (a telemetry stream). The default choice |
| `queue.LifoQueue(maxsize=0)` | **LIFO**: last in, first out (a stack) | "The most recent item matters most", e.g. undo stacks, depth-first work (the latest position of a vehicle) |
| `queue.PriorityQueue(maxsize=0)` | **Smallest item first** (uses `heapq` internally) | Scheduling by urgency or deadline (alarms before commands before telemetry) |
| `queue.SimpleQueue()` | FIFO | Lightweight FIFO: no `maxsize`, no `task_done()`/`join()`; safe to use from signal handlers and `__del__` methods (*reentrant*) |

`maxsize=0` (the default) means **unbounded**.

**IoT example:** the same three temperature readings put into each kind of queue, and a priority queue holding
alarms, commands and telemetry.


`code/04_queues/01_queue_types.py`
```python
import queue

# The same readings are put into every queue, in this order
READINGS = ["temp-01=21.5", "temp-01=21.7", "temp-01=22.0"]


def drain(q: queue.Queue | queue.SimpleQueue) -> list[str]:
    """Take every item out of the queue (non-blocking) and return them in extraction order."""
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            return items


def main() -> None:
    fifo: queue.Queue[str] = queue.Queue()
    lifo: queue.LifoQueue[str] = queue.LifoQueue()
    simple: queue.SimpleQueue[str] = queue.SimpleQueue()
    for q in (fifo, lifo, simple):
        for r in READINGS:
            q.put(r)

    print(f"Queue       (FIFO): {drain(fifo)}")      # oldest first: a telemetry stream
    print(f"LifoQueue   (LIFO): {drain(lifo)}")      # newest first: 'latest value wins'
    print(f"SimpleQueue (FIFO): {drain(simple)}")

    # PriorityQueue: the SMALLEST item comes out first -> (priority, payload) tuples
    prio: queue.PriorityQueue[tuple[int, str]] = queue.PriorityQueue()
    prio.put((5, "telemetry: temp-01=21.5"))
    prio.put((1, "ALARM: smoke detected in room-104"))
    prio.put((3, "command: light-02 OFF"))
    prio.put((1, "ALARM: CO2 above 1500 ppm in room-101"))
    print("PriorityQueue:")
    for item in drain(prio):
        print(f"   {item}")


if __name__ == "__main__":
    main()
```

**Output:**
```
Queue       (FIFO): ['temp-01=21.5', 'temp-01=21.7', 'temp-01=22.0']
LifoQueue   (LIFO): ['temp-01=22.0', 'temp-01=21.7', 'temp-01=21.5']
SimpleQueue (FIFO): ['temp-01=21.5', 'temp-01=21.7', 'temp-01=22.0']
PriorityQueue:
   (1, 'ALARM: CO2 above 1500 ppm in room-101')
   (1, 'ALARM: smoke detected in room-104')
   (3, 'command: light-02 OFF')
   (5, 'telemetry: temp-01=21.5')
```

**Key Points:**
- The priority queue extracts the alarms first, then the command, then the telemetry.
- Look closely at the two alarms with priority `1`: the **smoke** alarm was inserted first, but the
  **CO2** alarm came out first. When priorities are equal, tuples are compared on the **next element**
  (here the string, alphabetically). So a `PriorityQueue` is **not FIFO among equal priorities**,
  and this tie-breaking causes a real bug when the payload is not comparable (next section).

## 2.2 Robust Priority Messages with Dataclasses

If the payload is a `dict` (or any object that does not support `<`), two items with the same
priority make `put()` crash. The robust solution is a **`dataclass(order=True)`** with:

- the priority as the first field;
- a **monotonic sequence number** as the second field: it breaks ties and restores FIFO order among
  equal priorities;
- the payload marked with `field(compare=False)`, so it never takes part in comparisons.

**IoT example:** gateway messages (alarms, actuator commands, telemetry) whose payload is a `dict`.


`code/04_queues/02_priority_messages.py`
```python
import itertools
import queue
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class Priority(IntEnum):
    """Lower value = more urgent (PriorityQueue extracts the smallest item first)."""
    ALARM = 0
    COMMAND = 1
    TELEMETRY = 2


_sequence = itertools.count()          # thread-safe enough for a counter (next() is atomic in CPython)


@dataclass(order=True)
class GatewayMessage:
    priority: Priority
    seq: int = field(default_factory=lambda: next(_sequence))   # FIFO order among equal priorities
    payload: dict[str, Any] = field(default_factory=dict, compare=False)


def main() -> None:
    # 1. The problem: tuples with equal priority compare the payloads
    broken: queue.PriorityQueue[tuple[int, dict[str, Any]]] = queue.PriorityQueue()
    broken.put((2, {"sensor": "temp-01", "value": 21.5}))
    try:
        broken.put((2, {"sensor": "temp-02", "value": 22.1}))
    except TypeError as exc:
        print(f"tuple with dict payload -> TypeError: {exc}")

    # 2. The solution
    q: queue.PriorityQueue[GatewayMessage] = queue.PriorityQueue()
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "temp-01", "value": 21.5}))
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "temp-02", "value": 22.1}))
    q.put(GatewayMessage(Priority.COMMAND, payload={"actuator": "hvac-01", "mode": "COOLING"}))
    q.put(GatewayMessage(Priority.ALARM, payload={"sensor": "smoke-04", "room": "room-104"}))
    q.put(GatewayMessage(Priority.TELEMETRY, payload={"sensor": "hum-01", "value": 48.0}))

    while not q.empty():
        msg = q.get()
        print(f"{msg.priority.name:9} seq={msg.seq} {msg.payload}")


if __name__ == "__main__":
    main()
```

**Output:**
```
tuple with dict payload -> TypeError: '<' not supported between instances of 'dict' and 'dict'
ALARM     seq=3 {'sensor': 'smoke-04', 'room': 'room-104'}
COMMAND   seq=2 {'actuator': 'hvac-01', 'mode': 'COOLING'}
TELEMETRY seq=0 {'sensor': 'temp-01', 'value': 21.5}
TELEMETRY seq=1 {'sensor': 'temp-02', 'value': 22.1}
TELEMETRY seq=4 {'sensor': 'hum-01', 'value': 48.0}
```

**Key Points:**
- The three telemetry messages come out in **insertion order** (`seq` 0, 1, 4), thanks to the
  sequence number.
- An `IntEnum` makes priorities **readable** (`Priority.ALARM` instead of a magic `0`) while keeping
  them comparable as integers.
- `order=True` generates `__lt__`, `__le__`... that compare the fields **in declaration order**,
  skipping those with `compare=False`.

---

# 3 The Queue API Step by Step

## 3.1 Blocking, Timeouts and Non-Blocking Calls

| Method | Behaviour |
|---|---|
| `put(item, block=True, timeout=None)` | Insert an item. If the queue is full, **block** until a slot is free. With `timeout`, raise `queue.Full` after that many seconds |
| `get(block=True, timeout=None)` | Remove and return an item. If the queue is empty, **block** until one arrives. With `timeout`, raise `queue.Empty` after that many seconds |
| `put_nowait(item)` / `get_nowait()` | Same as `block=False`: raise `queue.Full` / `queue.Empty` **immediately** instead of waiting |
| `qsize()`, `empty()`, `full()` | Current state of the queue, **approximate** (see the note below) |

**IoT example:** a bounded buffer (`maxsize=2`) between a sensor and a slow uplink.


`code/04_queues/03_blocking_api.py`
```python
import queue
import threading
import time


def main() -> None:
    buffer: queue.Queue[float] = queue.Queue(maxsize=2)
    start = time.perf_counter()

    def log(msg: str) -> None:
        print(f"[{time.perf_counter() - start:.2f}s] {msg}")

    # --- non-blocking variants raise immediately ---
    try:
        buffer.get_nowait()
    except queue.Empty:
        log("get_nowait() on an empty queue -> queue.Empty")

    buffer.put(21.5)
    buffer.put(21.6)
    try:
        buffer.put_nowait(21.7)
    except queue.Full:
        log(f"put_nowait() on a full queue (size {buffer.qsize()}/{buffer.maxsize}) -> queue.Full")

    # --- timeout: wait at most N seconds, then raise ---
    try:
        buffer.put(21.7, timeout=0.3)
    except queue.Full:
        log("put(timeout=0.3) -> queue.Full after waiting 0.3 s")

    # --- blocking put: waits until a consumer frees a slot ---
    def slow_uplink() -> None:
        time.sleep(0.5)
        log(f"uplink: took {buffer.get()} from the queue")

    threading.Thread(target=slow_uplink).start()
    log("put(21.7) blocks until there is room...")
    buffer.put(21.7)
    log(f"put(21.7) completed, queue size = {buffer.qsize()}")


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.00s] get_nowait() on an empty queue -> queue.Empty
[0.00s] put_nowait() on a full queue (size 2/2) -> queue.Full
[0.30s] put(timeout=0.3) -> queue.Full after waiting 0.3 s
[0.30s] put(21.7) blocks until there is room...
[0.80s] uplink: took 21.5 from the queue
[0.80s] put(21.7) completed, queue size = 2
```

> **Note:** `qsize()`, `empty()` and `full()` are **snapshots**: by the time you act on the answer,
> another thread may have changed the queue. Code like `if not q.empty(): item = q.get()` can still
> block forever if another consumer takes the last item in between. Use `get_nowait()` with
> `except queue.Empty`, or `get(timeout=...)`, instead. Checking `empty()` is fine only when you know
> that no other thread is using the queue (as in the single-threaded demos above).

## 3.2 Tracking Completion: `task_done()` and `join()`

Often the producer needs to know when all the work has been **processed**, not just **taken** from
the queue. `Queue` (but not `SimpleQueue`) keeps a counter of *unfinished tasks*:

- every `put()` increments it;
- every `task_done()` decrements it: consumers call it when they have **finished processing** an item;
- `join()` blocks until the counter goes back to zero.

**IoT example:** three workers push a new configuration to six smart plugs; the main thread waits until all
of them are configured.


`code/04_queues/04_task_done_and_join.py`
```python
import queue
import threading
import time


def config_pusher(jobs: queue.Queue[str]) -> None:
    """Worker: push a new configuration to each device."""
    while True:
        device_id = jobs.get()
        try:
            time.sleep(0.2)                            # simulated configuration upload
            print(f"  [{threading.current_thread().name}] configuration pushed to {device_id}")
        finally:
            jobs.task_done()                           # ALWAYS, even if the processing fails


def main() -> None:
    jobs: queue.Queue[str] = queue.Queue()
    for i in range(3):
        threading.Thread(target=config_pusher, args=(jobs,), name=f"pusher-{i}", daemon=True).start()

    start = time.perf_counter()
    for device in ["plug-01", "plug-02", "plug-03", "plug-04", "plug-05", "plug-06"]:
        jobs.put(device)

    jobs.join()                                        # blocks until 6 task_done() calls
    print(f"all devices configured in {time.perf_counter() - start:.2f}s "
          f"(unfinished tasks: {jobs.unfinished_tasks})")


if __name__ == "__main__":
    main()
```

**Output:**
```
  [pusher-0] configuration pushed to plug-01
  [pusher-2] configuration pushed to plug-02
  [pusher-1] configuration pushed to plug-03
  [pusher-0] configuration pushed to plug-04
  [pusher-2] configuration pushed to plug-05
  [pusher-1] configuration pushed to plug-06
all devices configured in 0.40s (unfinished tasks: 0)
```

**Key Points:**
- Three workers process six jobs in two waves (0.4 s). This is a **hand-made thread pool**: compare
  it with Section 7 of [Lecture 02](02_thread_pools_and_futures.md#7-under-the-hood-a-pool-is-a-queue-plus-workers).
- Put `task_done()` in a **`finally`** block: if the processing raises an exception and
  `task_done()` is skipped, `join()` will wait **forever**.
- The workers here are daemon threads that loop forever, which is acceptable for a demo. The next
  sections show how to stop them properly.

---

# 4 The Producer-Consumer Pattern and Backpressure

When producers are **faster** than consumers, an unbounded queue grows without limit, and sooner or
later the process runs out of memory (very soon on an embedded device with little RAM). A **bounded** queue solves this with
**backpressure**: when it is full, `put()` blocks, so the producers are automatically slowed down to
the consumers' speed.

**IoT example:** two sensors produce 20 readings/s each (40/s in total), while the uploader sends
only ~12 readings/s over a cellular link:

`code/04_queues/05_producer_consumer.py`
```python
import queue
import threading
import time
from dataclasses import dataclass

READINGS_PER_SENSOR = 10


@dataclass
class Reading:
    sensor_id: str
    seq: int
    value: float


def sensor_producer(sensor_id: str, q: queue.Queue[Reading], start: float) -> None:
    for seq in range(READINGS_PER_SENSOR):
        time.sleep(0.05)                                   # 20 Hz sampling
        t_before = time.perf_counter()
        q.put(Reading(sensor_id, seq, 20.0 + seq / 10))    # blocks while the queue is full
        waited = time.perf_counter() - t_before
        if waited > 0.01:
            print(f"[{time.perf_counter() - start:.2f}s] {sensor_id}: blocked {waited * 1000:.0f} ms "
                  f"on put() (queue full)")


def uploader(q: queue.Queue[Reading], total: int, start: float) -> None:
    for _ in range(total):
        q.get()                                            # take the next reading...
        time.sleep(0.08)                                   # ...and send it: slow uplink, ~12 readings/s
        q.task_done()
    print(f"[{time.perf_counter() - start:.2f}s] uploader: {total} readings sent")


def main() -> None:
    q: queue.Queue[Reading] = queue.Queue(maxsize=5)
    start = time.perf_counter()
    total = 2 * READINGS_PER_SENSOR
    threads = [
        threading.Thread(target=sensor_producer, args=("temp-01", q, start)),
        threading.Thread(target=sensor_producer, args=("hum-01", q, start)),
        threading.Thread(target=uploader, args=(q, total, start)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
```

**Output:**
```
[0.29s] temp-01: blocked 40 ms on put() (queue full)
[0.37s] hum-01: blocked 110 ms on put() (queue full)
[0.45s] temp-01: blocked 110 ms on put() (queue full)
[0.53s] hum-01: blocked 110 ms on put() (queue full)
[0.61s] temp-01: blocked 110 ms on put() (queue full)
[0.69s] hum-01: blocked 110 ms on put() (queue full)
[0.77s] temp-01: blocked 110 ms on put() (queue full)
[0.85s] hum-01: blocked 110 ms on put() (queue full)
[0.93s] temp-01: blocked 110 ms on put() (queue full)
[1.01s] hum-01: blocked 110 ms on put() (queue full)
[1.09s] temp-01: blocked 110 ms on put() (queue full)
[1.17s] hum-01: blocked 110 ms on put() (queue full)
[1.65s] uploader: 20 readings sent
```

**Key Points:**
- After ~0.3 s the queue is full and the producers start **blocking** on `put()`: from then on, each
  producer can insert a reading only when the uploader frees a slot. The memory used by the queue
  never exceeds 5 readings.
- **Backpressure is a design decision.** Blocking the producer is right when no data may be lost.
  When fresh data matters more than old data (e.g. telemetry that is re-sampled anyway), other
  strategies are better: `put_nowait()` and **drop** the new item on `queue.Full`, or drop the
  **oldest** one, or aggregate items before queueing them (as in [Section 7](#7-lets-see-how-it-works-a-multi-stage-pipeline)).
- Producers and consumers never reference each other: we could add a third producer (sensor), or a
  second consumer (uploader), without changing any of the existing code.

---

# 5 Clean Shutdown of Consumers

Consumer threads usually loop forever on `get()`. How do we tell them to stop, **after** they have
processed everything that is already in the queue?

## 5.1 The Sentinel (Poison Pill) Pattern

The classic solution works on every Python version: the producer puts a special **sentinel** value
in the queue, **one per consumer**. A consumer that receives it exits its loop. Since the queue is
FIFO, the sentinels arrive **after** all the real items.

**IoT example:** three workers persist sensor readings to a local database and must stop after the last one.


`code/04_queues/06_sentinel_shutdown.py`
```python
import queue
import threading
import time

SENTINEL = object()        # a unique object: it can never be confused with a real reading
NUM_WORKERS = 3


def storage_worker(q: queue.Queue[object]) -> None:
    """Persist readings to the local time-series database until the sentinel arrives."""
    name = threading.current_thread().name
    stored = 0
    while True:
        item = q.get()
        try:
            if item is SENTINEL:                   # identity check: `is`, not `==`
                print(f"  {name}: sentinel received, exiting after {stored} readings")
                return
            time.sleep(0.02)                       # simulated DB write
            stored += 1
        finally:
            q.task_done()


def main() -> None:
    q: queue.Queue[object] = queue.Queue()
    workers = [threading.Thread(target=storage_worker, args=(q,), name=f"db-writer-{i}")
               for i in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    for i in range(30):
        q.put({"sensor": "temp-01", "seq": i})

    for _ in workers:                              # ONE sentinel per worker
        q.put(SENTINEL)
    for w in workers:
        w.join()
    print(f"all workers stopped, items left in the queue: {q.qsize()}")


if __name__ == "__main__":
    main()
```

**Output:**
```
  db-writer-0: sentinel received, exiting after 10 readings
  db-writer-1: sentinel received, exiting after 10 readings
  db-writer-2: sentinel received, exiting after 10 readings
all workers stopped, items left in the queue: 0
```

**Key Points:**
- `object()` creates a value that is **unique** and compared with `is`: it can never collide with a
  real reading (using `None` works too, as long as `None` is never a legitimate item).
- **One sentinel per consumer**: a worker that consumes a sentinel exits and does not take a second
  one, so with 3 workers you need 3 sentinels.
- Limits: the producer must know **how many** consumers there are, and a sentinel in a
  `PriorityQueue` must be given the **lowest** priority to arrive last.

## 5.2 `Queue.shutdown()` (Python 3.13+)

Python 3.13 added a built-in shutdown mechanism to `Queue`, `LifoQueue` and `PriorityQueue` (and to
`asyncio.Queue`):

| Call | Effect on `put()` | Effect on `get()` |
|---|---|---|
| `q.shutdown()` (i.e. `immediate=False`) | Raises `queue.ShutDown` | Keeps returning the remaining items; raises `ShutDown` once the queue is **empty**: a **graceful drain** |
| `q.shutdown(immediate=True)` | Raises `queue.ShutDown` | The queue is emptied **at once**: every blocked and future `get()` raises `ShutDown`: a **fast stop** |

Blocked callers are woken up in both cases, and the producer no longer needs to know how many
consumers there are.

**IoT example:** two uploader threads send batches of readings; the service is asked to stop while batches
are still queued, first gracefully and then immediately.


`code/04_queues/07_queue_shutdown.py`
```python
import queue
import sys
import threading
import time

if sys.version_info < (3, 13):
    sys.exit("This example requires Python 3.13+ (use the sentinel pattern of 06 instead)")


def uploader(q: queue.Queue[int], results: list[int]) -> None:
    while True:
        try:
            batch = q.get()                         # raises queue.ShutDown after shutdown()
        except queue.ShutDown:
            print(f"  {threading.current_thread().name}: queue shut down, exiting")
            return
        time.sleep(0.05)                            # simulated upload
        results.append(batch)
        q.task_done()


def run(immediate: bool) -> None:
    q: queue.Queue[int] = queue.Queue()
    results: list[int] = []
    workers = [threading.Thread(target=uploader, args=(q, results), name=f"uploader-{i}") for i in range(2)]
    for w in workers:
        w.start()
    for batch in range(10):
        q.put(batch)

    time.sleep(0.12)                                # the service is asked to stop...
    q.shutdown(immediate=immediate)
    try:
        q.put(99)
    except queue.ShutDown:
        print("  put() after shutdown -> queue.ShutDown")
    for w in workers:
        w.join()
    print(f"shutdown(immediate={immediate}): {len(results)}/10 batches uploaded\n")


def main() -> None:
    run(immediate=False)
    run(immediate=True)


if __name__ == "__main__":
    main()
```

**Output:**
```
  put() after shutdown -> queue.ShutDown
  uploader-1: queue shut down, exiting
  uploader-0: queue shut down, exiting
shutdown(immediate=False): 10/10 batches uploaded

  put() after shutdown -> queue.ShutDown
  uploader-1: queue shut down, exiting
  uploader-0: queue shut down, exiting
shutdown(immediate=True): 6/10 batches uploaded
```

**Key Points:**
- With the **graceful** shutdown all 10 batches are uploaded: the workers drain the queue and exit
  when it is empty. This is the equivalent of the sentinel pattern, without sentinels.
- With the **immediate** shutdown, the batches still in the queue are **discarded**: only those
  already taken by a worker are completed. Useful on a fatal error or when the process must exit now.
- Choose the pattern by version: `shutdown()` on 3.13+, sentinels for code that must also run on older
  Pythons.

---

# 6 Related Data Structures

## 6.1 `collections.deque`

`collections.deque` is a double-ended queue with fast appends and pops at both ends. Individual
`append()` and `popleft()` calls are atomic in CPython, but a `deque` has **no blocking
semantics** (no waiting `get()`, no `maxsize` blocking, no `task_done()`/`join()`). Use it for queues
**inside a single thread** (it is much faster than `queue.Queue`), or as a bounded "last N items"
buffer: `deque(maxlen=100)` silently discards the oldest element when full.

## 6.2 `asyncio.Queue`

`queue.Queue` blocks the **thread** in `get()`. In asyncio code that would freeze the whole event
loop ([Lecture 03, Section 6](03_asyncio.md#6-blocking-code-the-number-one-pitfall)).
`asyncio.Queue` has the **same API** (`put`, `get`, `task_done`, `join`, `maxsize`, `shutdown` in
3.13+, `QueueEmpty`/`QueueFull`), but `put()` and `get()` are **coroutines** that suspend the *task*:

**IoT example:** two sensor coroutines produce readings and a rule-engine coroutine consumes them.


`code/04_queues/08_asyncio_queue.py`
```python
import asyncio
import time


async def sensor(sensor_id: str, q: asyncio.Queue[tuple[str, float]], samples: int) -> None:
    for i in range(samples):
        await asyncio.sleep(0.05)
        await q.put((sensor_id, 20.0 + i))          # suspends this task while the queue is full


async def rule_engine(q: asyncio.Queue[tuple[str, float]], start: float) -> None:
    while True:
        sensor_id, value = await q.get()            # suspends until an item is available
        if value >= 22.0:
            print(f"[{time.perf_counter() - start:.2f}s] rule: {sensor_id} = {value} -> ALERT")
        q.task_done()


async def main() -> None:
    q: asyncio.Queue[tuple[str, float]] = asyncio.Queue(maxsize=10)
    start = time.perf_counter()

    consumer = asyncio.create_task(rule_engine(q, start))
    async with asyncio.TaskGroup() as producers:
        producers.create_task(sensor("temp-01", q, samples=3))
        producers.create_task(sensor("temp-02", q, samples=4))

    await q.join()                  # every reading has been evaluated by the rule engine
    consumer.cancel()               # the consumer loops forever: cancel it (or use q.shutdown() in 3.13+)
    print(f"[{time.perf_counter() - start:.2f}s] all readings processed")


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
[0.15s] rule: temp-01 = 22.0 -> ALERT
[0.15s] rule: temp-02 = 22.0 -> ALERT
[0.20s] rule: temp-02 = 23.0 -> ALERT
[0.20s] all readings processed
```

The consumer **wakes up only when an item arrives**: this removes the polling loop of the asyncio
gateway in Lecture 03, whose rule engine checked the latest values every 100 ms.

> **Note:** `asyncio.Queue` is **not thread-safe**. To pass data from a normal thread into an event
> loop, use `loop.call_soon_threadsafe(q.put_nowait, item)`, or
> `asyncio.run_coroutine_threadsafe(q.put(item), loop)`.

## 6.3 `multiprocessing.Queue`

`queue.Queue` lives in the memory of **one process**. Processes do not share memory, so to exchange
data between them you need `multiprocessing.Queue`: similar API, but the items are **pickled** and
sent through a pipe.

**IoT example:** a child process computes a CPU-heavy feature (the RMS) of batches of vibration-sensor
samples.


`code/04_queues/09_multiprocessing_queue.py`
```python
import math
import multiprocessing as mp
import os


def feature_extractor(inbox: "mp.Queue[list[float] | None]", outbox: "mp.Queue[tuple[int, float]]") -> None:
    """Runs in a child process: compute the RMS of each batch until the None sentinel arrives."""
    while (batch := inbox.get()) is not None:
        rms = math.sqrt(sum(x * x for x in batch) / len(batch))
        outbox.put((os.getpid(), round(rms, 3)))


def main() -> None:
    inbox: mp.Queue = mp.Queue()
    outbox: mp.Queue = mp.Queue()
    worker = mp.Process(target=feature_extractor, args=(inbox, outbox))
    worker.start()

    for amplitude in (0.5, 1.0, 2.0):             # three batches from a vibration sensor
        inbox.put([amplitude * math.sin(i / 10) for i in range(10_000)])
    inbox.put(None)                               # sentinel

    for _ in range(3):
        pid, rms = outbox.get()
        print(f"main pid {os.getpid()} <- child pid {pid}: RMS = {rms}")
    worker.join()


if __name__ == "__main__":                        # mandatory with multiprocessing
    main()
```

**Output** (PIDs change at every run):
```
main pid 76696 <- child pid 76699: RMS = 0.353
main pid 76696 <- child pid 76699: RMS = 0.707
main pid 76696 <- child pid 76699: RMS = 1.414
```

## 6.4 Which Queue Should I Use?

| Producers and consumers are... | Use |
|---|---|
| threads of the same process | `queue.Queue` (or `LifoQueue` / `PriorityQueue`) |
| coroutines in the same event loop | `asyncio.Queue` |
| different processes on the same machine | `multiprocessing.Queue` |
| in a single thread (just a buffer) | `collections.deque` |
| different machines or services, or data must survive a crash | an external broker (MQTT, Kafka, RabbitMQ, Redis Streams...) |

---

# 7 Let's See How It Works: a Multi-Stage Pipeline

Queues can be **chained** to build pipelines, where each stage runs in its own thread(s) and
communicates with the next one only through a queue. This is a very common architecture for data
processing (ETL jobs, log processing, media pipelines): each stage has one job, stages run
concurrently, and a slow stage is buffered by the queue in front of it.

**IoT example: a telemetry pipeline** that acquires readings, validates and aggregates them, and sends
them to the cloud with alarms first:

```
┌──────────────┐          ┌────────────────────────┐              ┌────────────┐
│ acquisition  │  raw_q   │ processing             │  uplink_q    │ uplink     │
│ temp-01 ───► │ ───────► │ - drop invalid values  │ ───────────► │ ALARMs     │ ──► cloud
│ temp-02 ───► │ (max 20) │ - ALARM on threshold   │ (priority)   │ first      │
└──────────────┘          │ - average every 5      │              └────────────┘
                          └────────────────────────┘
shutdown:  producers end ──► raw_q.shutdown() ──► processing flushes ──► uplink_q.shutdown() ──► uplink drains
```

`code/04_queues/10_telemetry_pipeline.py`
```python
import itertools
import logging
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-11s] %(message)s")
logger = logging.getLogger("pipeline")

WINDOW = 5                                   # readings per aggregate
VALID_RANGE = (-40.0, 125.0)                 # physical range of the temperature sensor
ALARM_THRESHOLD_C = 30.0


@dataclass
class Reading:
    sensor_id: str
    value: float


class Priority(IntEnum):
    ALARM = 0
    AGGREGATE = 1


_seq = itertools.count()


@dataclass(order=True)
class UplinkMessage:
    priority: Priority
    seq: int = field(default_factory=lambda: next(_seq))
    body: str = field(default="", compare=False)


def acquisition(sensor_id: str, samples: list[float], raw_q: queue.Queue[Reading]) -> None:
    """Stage 1: emit the (simulated) samples of one sensor at 50 Hz."""
    for value in samples:
        time.sleep(0.02)
        raw_q.put(Reading(sensor_id, value))
    logger.info("done, %d samples produced", len(samples))


def processing(raw_q: queue.Queue[Reading], uplink_q: queue.PriorityQueue[UplinkMessage]) -> None:
    """Stage 2: validate, raise alarms, aggregate."""
    windows: dict[str, list[float]] = {}
    dropped = 0
    while True:
        try:
            reading = raw_q.get()
        except queue.ShutDown:
            break
        lo, hi = VALID_RANGE
        if not lo <= reading.value <= hi:
            dropped += 1                                          # e.g. a disconnected probe
        elif reading.value > ALARM_THRESHOLD_C:
            uplink_q.put(UplinkMessage(Priority.ALARM, body=f"ALARM {reading.sensor_id}={reading.value}"))
        else:
            window = windows.setdefault(reading.sensor_id, [])
            window.append(reading.value)
            if len(window) == WINDOW:
                avg = sum(window) / WINDOW
                uplink_q.put(UplinkMessage(Priority.AGGREGATE, body=f"avg {reading.sensor_id}={avg:.2f}"))
                window.clear()
        raw_q.task_done()

    # input exhausted: flush partial windows, then propagate the shutdown downstream
    for sensor_id, window in windows.items():
        if window:
            avg = sum(window) / len(window)
            uplink_q.put(UplinkMessage(Priority.AGGREGATE, body=f"avg {sensor_id}={avg:.2f} (partial)"))
    logger.info("input closed, %d invalid readings dropped", dropped)
    uplink_q.shutdown()                                           # graceful: uplink drains the rest


def uplink(uplink_q: queue.PriorityQueue[UplinkMessage], sent: list[str]) -> None:
    """Stage 3: send messages over a slow link, most urgent first."""
    while True:
        try:
            msg = uplink_q.get()
        except queue.ShutDown:
            break
        time.sleep(0.08)                                          # slow cellular link
        sent.append(msg.body)
        logger.info("sent %-9s %s", msg.priority.name, msg.body)
        uplink_q.task_done()
    logger.info("queue closed, %d messages sent", len(sent))


def make_samples(sensor_id: str, n: int) -> list[float]:
    rng = random.Random(sensor_id)
    samples = [round(rng.uniform(21.0, 23.0), 1) for _ in range(n)]
    samples[2] = -127.0                                       # an invalid reading (probe error)...
    samples[11] = 85.0 if sensor_id == "temp-02" else 31.5   # ...and a late spike -> ALARM
    return samples


def main() -> None:
    raw_q: queue.Queue[Reading] = queue.Queue(maxsize=20)
    uplink_q: queue.PriorityQueue[UplinkMessage] = queue.PriorityQueue()
    sent: list[str] = []

    producers = [threading.Thread(target=acquisition, args=(s, make_samples(s, 14), raw_q), name=s)
                 for s in ("temp-01", "temp-02")]
    processor = threading.Thread(target=processing, args=(raw_q, uplink_q), name="processing")
    sender = threading.Thread(target=uplink, args=(uplink_q, sent), name="uplink")

    for t in (*producers, processor, sender):
        t.start()

    for p in producers:                  # 1. wait for the sources to finish...
        p.join()
    raw_q.shutdown()                     # 2. ...then close the first queue: the shutdown cascades
    processor.join()
    sender.join()
    logger.info("pipeline stopped: %d messages delivered", len(sent))


if __name__ == "__main__":
    main()
```

**Output:**
```
  203ms [uplink     ] sent AGGREGATE avg temp-01=21.78
  284ms [uplink     ] sent AGGREGATE avg temp-02=21.84
  284ms [temp-01    ] done, 14 samples produced
  284ms [temp-02    ] done, 14 samples produced
  284ms [processing ] input closed, 2 invalid readings dropped
  364ms [uplink     ] sent ALARM     ALARM temp-01=31.5
  444ms [uplink     ] sent ALARM     ALARM temp-02=85.0
  524ms [uplink     ] sent AGGREGATE avg temp-01=22.14
  605ms [uplink     ] sent AGGREGATE avg temp-02=21.92
  685ms [uplink     ] sent AGGREGATE avg temp-01=22.20 (partial)
  765ms [uplink     ] sent AGGREGATE avg temp-02=22.45 (partial)
  765ms [uplink     ] queue closed, 8 messages sent
  765ms [MainThread ] pipeline stopped: 8 messages delivered
```

**Key Points:**
- **Priority in action:** the second averages of both sensors were queued (sample #10) *before* the
  alarms (sample #11), but the uplink sent the **alarms first** because it is slower than the
  producers and the `PriorityQueue` reorders what is waiting.
- **Data reduction:** 28 raw items became 8 messages (2 invalid readings dropped, 2 alarms, 4 full
  averages, 2 partial averages flushed at shutdown). In IoT this is typical edge-computing work for a
  gateway with a limited uplink.
- **Cascading shutdown:** each stage closes its **output** queue only after it has drained its
  **input** queue and flushed its internal state. No data is lost, no thread is left running, and no
  stage needs to know how many threads the next stage has.
- **Stages are independent:** each one can be tested in isolation by feeding its input queue and
  reading its output queue. This is exactly what we will do in [Lecture 06](06_testing.md).

**Towards event-driven architectures:** in this pipeline every stage knows **which queue** to write
to. The next lecture generalises the idea: components publish **events** on a **bus**, and any number
of handlers can subscribe to the event types they care about.
→ [Lecture 05 - Event-Driven Architectures](05_event_driven_architecture.md).

---

# 8 References

- Python documentation - [`queue` - A synchronized queue class](https://docs.python.org/3/library/queue.html)
- Python documentation - [`asyncio` Queues](https://docs.python.org/3/library/asyncio-queue.html)
- Python documentation - [`multiprocessing` - Exchanging objects between processes](https://docs.python.org/3/library/multiprocessing.html#exchanging-objects-between-processes)
- Python documentation - [`collections.deque`](https://docs.python.org/3/library/collections.html#collections.deque)
- Python documentation - [What's New in Python 3.13](https://docs.python.org/3/whatsnew/3.13.html) (`Queue.shutdown()`)
- Real Python - [Python's Queue: 3 Types and Their Uses](https://realpython.com/python-queue/)
