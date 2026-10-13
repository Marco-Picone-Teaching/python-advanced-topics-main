# Python Advanced Topics: Concurrency, Queues, Event-Driven Architectures & Testing

<!-- omit in toc -->
## Lecture Information

| **Title**           | Python Advanced Topics: Concurrency, Queues, Event-Driven Architectures & Testing|
|---------------------|----------------------------------------------------------------------------------|
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |


---

## 📚 About This Repository

This repository collects the lecture notes for the **Python Advanced Topics** module of the
*Intelligent Internet of Things* course. It follows the
[Python Best Practices & OOP](../python-best-practices-and-oop/) module and assumes its content
(type hints, logging, exceptions, classes, inheritance, abstract classes, the Observer pattern).

The focus is on **software engineering and coding in Python**: the concepts and the techniques are
general and apply to any kind of application. The material is written as long-form Markdown notes that
build every concept **step by step**, with many small examples and a precise explanation of *how things
work*. Only the **examples** are taken from the IoT domain, and they are all set in the same running
scenario: a **Smart Building IoT gateway** that reads sensors (temperature, humidity,
CO2, presence), drives actuators (HVAC, windows, lights, valves) and talks to the cloud. The same
gateway is rebuilt across the lectures: first with threads, then with a thread pool, with asyncio,
with queues, as an event-driven system, and finally covered by automated tests.

Every code snippet is **complete, commented and runnable**, and is available in the
[`code/`](code/) folder. The outputs shown in the notes come from actually running the examples.

## 🗂️ How It Is Organised

The content is split into **six lecture files**, meant to be read in order:

### 1️⃣ [`01_concurrency_and_threading.md`](01_concurrency_and_threading.md): *Concurrency Fundamentals & Threading*

- Why concurrency matters in software: the cost of waiting for I/O
- Processes vs. threads, concurrency vs. parallelism, I/O-bound vs. CPU-bound work
- The Global Interpreter Lock (GIL): what it is and what it means in practice
- The `threading` module: creating, starting, joining threads; daemon threads; `Thread` subclasses
- Race conditions, `Lock`, `RLock`, and deadlocks
- Coordination primitives: `Event`, `Condition`, `Semaphore`, `Timer`, `threading.local`
- Graceful shutdown of worker threads
- Free-threaded Python (PEP 703, Python 3.13/3.14)
- **Worked example** (IoT: a Smart Room gateway): putting it all together in a multithreaded application

### 2️⃣ [`02_thread_pools_and_futures.md`](02_thread_pools_and_futures.md): *Thread Pools & Futures*

- Why one thread per task does not scale, and the thread pool idea
- `ThreadPoolExecutor`, `submit()` and the `Future` object; callbacks and cancellation
- `executor.map()` vs. `as_completed()`, and `wait()` with deadlines
- Exceptions and timeouts in futures
- Executor lifecycle, `shutdown()`, and the default number of workers (Python 3.13 change)
- `ProcessPoolExecutor` for CPU-bound analytics
- Under the hood: a pool is a queue plus workers
- **Worked example** (IoT: polling a whole building): a batch of jobs with a deadline on a bounded pool

### 3️⃣ [`03_asyncio.md`](03_asyncio.md): *Asyncio*

- The event loop and cooperative scheduling
- Coroutines step by step: `async def`, `await`, `asyncio.run()`
- Running coroutines concurrently: `gather()`, tasks, cancellation
- Structured concurrency with `TaskGroup` and `except*` (Python 3.11+)
- Timeouts and drift-free periodic sampling
- The number one pitfall, blocking calls, and how to bridge blocking code (`to_thread`, `run_in_executor`)
- asyncio synchronization primitives
- Choosing between threads, processes and asyncio
- **Worked example** (IoT: a Smart Room gateway): a complete asyncio application

### 4️⃣ [`04_queues.md`](04_queues.md): *The `queue` Module*

- Why a thread-safe blocking queue: the producer-consumer pattern
- `Queue`, `LifoQueue`, `PriorityQueue`, `SimpleQueue`, with robust priority messages
- The API: blocking, timeouts, `*_nowait`, `task_done()` / `join()`
- Backpressure with bounded queues
- Clean shutdown: sentinels and `Queue.shutdown()` (Python 3.13+)
- `collections.deque`, `asyncio.Queue`, `multiprocessing.Queue`: which queue to use
- **Worked example** (IoT: a telemetry pipeline): a multi-stage pipeline with cascading shutdown

### 5️⃣ [`05_event_driven_architecture.md`](05_event_driven_architecture.md): *Event-Driven Architectures*

- From polling to events: producers, consumers, event bus, pub/sub, message brokers
- The limits of the Observer pattern
- Step by step: a synchronous bus, a thread-based bus (`EventBus`, `EventHandler`, `EventBusClient`),
  and an asyncio bus
- Robustness: wildcards, unsubscribe, error isolation, graceful stop, statistics
- Experiment: threads vs. asyncio on the same workload
- Limits of an in-process bus and when to move to an external broker
- **Worked example** (IoT: a Smart Room): an event-driven application (producers → rules → executors → observers)

### 6️⃣ [`06_testing.md`](06_testing.md): *Testing*

- Why test, test levels, the test pyramid, Arrange-Act-Assert
- `unittest`: `TestCase`, assertions, fixtures, `subTest`, test suites and runners
- `pytest`: plain `assert`, parametrization, `approx`, fixtures, `conftest.py`, configuration
- Test doubles: stubs, fakes, mocks; `unittest.mock`, `patch()`, `monkeypatch`; dependency injection
- **Testing concurrent code:** deterministic tests for threads, thread pools, asyncio (`pytest-asyncio`,
  `IsolatedAsyncioTestCase`, `AsyncMock`), queue pipelines and the event bus of the previous lectures
- Coverage and flakiness checks

## ▶️ Running the Examples

Python **3.13+** is required (some examples use `Queue.shutdown()`; examples are tested on 3.14).
The examples of Lectures 01-05 use **only the standard library**:

```bash
cd code/01_threading
python3 03_first_threads.py
```

The test suite of Lecture 06 needs `pytest`, `pytest-asyncio` and `pytest-cov`. With
[`uv`](https://docs.astral.sh/uv/):

```bash
cd code
uv sync          # creates .venv and installs the dev dependencies from pyproject.toml
uv run pytest    # runs the whole test suite
```

See [`code/README.md`](code/README.md) for details and for the setup without `uv`.

## 📁 Repository Layout

```
.
├── 01_concurrency_and_threading.md     # Lecture 01
├── 02_thread_pools_and_futures.md      # Lecture 02
├── 03_asyncio.md                       # Lecture 03
├── 04_queues.md                        # Lecture 04
├── 05_event_driven_architecture.md     # Lecture 05
├── 06_testing.md                       # Lecture 06
├── code/                               # runnable examples, one folder per lecture
│   ├── pyproject.toml                  # Python version, dev dependencies, pytest configuration
│   ├── 01_threading/
│   ├── 02_thread_pools/
│   ├── 03_asyncio/
│   ├── 04_queues/
│   ├── 05_event_driven/
│   └── 06_testing/                     # src/smart_gateway (code under test) + tests/
├── LICENSE                             # Creative Commons Attribution 4.0
└── README.md                           # This file
```

## 📄 License

This work is released under the
[Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/)
license. You are free to share and adapt the material, provided you give
appropriate credit to the author.
