# Python Advanced Topics - Playground

Runnable code playground for the **Python Advanced Topics** lecture: concurrency and threading, thread pools and
futures, asyncio, thread-safe queues, event-driven architectures and testing.

All the examples are set in the same running scenario: a **Smart Building IoT gateway** that reads sensors
(temperature, humidity, CO2, presence), drives actuators (HVAC, windows, lights, valves) and talks to the cloud.
The same gateway is rebuilt step by step: first with threads, then with a thread pool, with asyncio, with
queues, as an event-driven system, and finally covered by automated tests.

## 📖 Lecture

The theory is maintained in the [mp-lectures](https://github.com/Marco-Picone-Teaching/mp-lectures) repository:

- 📝 [Python Advanced Topics: Concurrency, Queues, Event-Driven Architectures & Testing](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/python_advanced_topics.md)

Each folder of this playground matches one section of the lecture, and each file is one **step**. The module
docstring at the top of every file says which section of the notes it belongs to and what to look at while it
runs.

| Lecture Section | Folder |
|---|---|
| [A - Concurrency Fundamentals & Threading](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/concurrency_and_threading.md) | [`01_threading/`](01_threading/) |
| [B - Thread Pools & Futures](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/thread_pools_and_futures.md) | [`02_thread_pools/`](02_thread_pools/) |
| [C - Asyncio](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/asyncio.md) | [`03_asyncio/`](03_asyncio/) |
| [D - The `queue` Module](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/queues.md) | [`04_queues/`](04_queues/) |
| [E - Event-Driven Architectures](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/event_driven_architecture.md) | [`05_event_driven/`](05_event_driven/) |
| [F - Testing](https://github.com/Marco-Picone-Teaching/mp-lectures/blob/main/Lectures/programming-and-software-engineering/python-advanced-topics/testing.md) | [`06_testing/`](06_testing/) |

## ⚙️ Requirements

- **Python 3.13 or newer**. Examples use `Queue.shutdown()` / `asyncio.QueueShutDown` (3.13) and
  `logging`'s `%(taskName)s` (3.12). They have been tested with Python 3.14.
- Sections A-E: **standard library only**, nothing to install.
- Section F: `pytest`, `pytest-asyncio`, `pytest-cov` (declared in [`pyproject.toml`](pyproject.toml)).

```bash
python3 --version
```

## ▶️ Running the Examples (Sections A-E)

Run each script **from its own folder** (the event-driven examples import the local modules
`event_bus.py` and `async_event_bus.py`):

```bash
cd 01_threading
python3 01_sequential_polling.py
python3 03_first_threads.py

cd ../05_event_driven
python3 07_smart_room_automation.py
```

Timings and the order of lines printed by different threads/tasks can vary slightly from run to run
and from machine to machine: this is expected, and it is discussed in the notes.

To try the **free-threaded** interpreter (no GIL) without touching your system Python:

```bash
cd 01_threading
uv run --python 3.14t 15_free_threading_check.py
uv run --python 3.14t 02_io_bound_vs_cpu_bound.py
```

## 🧪 Running the Test Suite (Section F)

The pytest configuration (`testpaths`, `pythonpath`, asyncio mode) lives in `pyproject.toml`, so
`pytest` must be launched from the root folder of the repository.

### With `uv` (recommended)

```bash
uv sync                                              # creates .venv with the dev dependencies
uv run pytest                                        # whole suite
uv run pytest -v 06_testing/tests/test_event_bus.py  # a single file, verbose
uv run pytest --cov=smart_gateway --cov-report=term-missing   # with coverage
uv run python 06_testing/run_suite.py                # hand-built unittest TestSuite
```

### With `venv` + `pip`

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install pytest pytest-asyncio pytest-cov
python -m pytest
```

### Only the Standard Library (`unittest`)

The unittest-style test files do not need pytest:

```bash
PYTHONPATH=06_testing/src python3 -m unittest discover -s 06_testing/tests -p "test_*unittest.py" -v
```

## 📁 Project Structure

```
.
├── pyproject.toml        # Python version, dev dependencies, pytest configuration
├── 01_threading/         01_sequential_polling.py ... 16_threaded_gateway.py
├── 02_thread_pools/      01_thread_per_device_vs_pool.py ... 09_building_poller.py
├── 03_asyncio/           01_first_coroutine.py ... 11_async_gateway.py
├── 04_queues/            01_queue_types.py ... 10_telemetry_pipeline.py
├── 05_event_driven/      event_bus.py, async_event_bus.py (reusable modules)
│                         01_observer_limits.py ... 07_smart_room_automation.py
└── 06_testing/
    ├── run_suite.py
    ├── src/smart_gateway/   the library under test
    └── tests/               conftest.py + test_*.py
```

## 📄 License

This work is released under the
[Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/)
license. You are free to share and adapt the material, provided you give
appropriate credit to the author.
