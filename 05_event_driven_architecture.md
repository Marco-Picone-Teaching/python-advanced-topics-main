<!-- omit in toc -->
# Event-Driven Architectures in Python

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - Event-Driven Architectures                              |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **Prerequisites:** [01 - Threading](01_concurrency_and_threading.md), [03 - Asyncio](03_asyncio.md),
> [04 - The `queue` Module](04_queues.md), and the Observer pattern from the OOP lecture.
> **Runnable code:** [`code/05_event_driven/`](code/05_event_driven/) (Python 3.13+).

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 From Polling to Events](#1-from-polling-to-events)
  - [1.1 Two Ways of Reacting to Change](#11-two-ways-of-reacting-to-change)
  - [1.2 Vocabulary: Events, Producers, Consumers, Bus, Broker](#12-vocabulary-events-producers-consumers-bus-broker)
- [2 Starting Point: the Observer Pattern and Its Limits](#2-starting-point-the-observer-pattern-and-its-limits)
- [3 Step 1: a Synchronous Event Bus](#3-step-1-a-synchronous-event-bus)
- [4 Step 2: an Asynchronous Event Bus with Threads and `queue.Queue`](#4-step-2-an-asynchronous-event-bus-with-threads-and-queuequeue)
  - [4.1 Design: `EventBus`, `EventHandler`, `EventBusClient`](#41-design-eventbus-eventhandler-eventbusclient)
  - [4.2 Let's See How It Works: a Slow Handler No Longer Blocks the Publisher](#42-lets-see-how-it-works-a-slow-handler-no-longer-blocks-the-publisher)
- [5 Making the Bus Robust](#5-making-the-bus-robust)
- [6 Step 3: the Same Bus with `asyncio`](#6-step-3-the-same-bus-with-asyncio)
  - [6.1 An `asyncio.Queue`-Based Event Bus](#61-an-asyncioqueue-based-event-bus)
  - [6.2 Experiment: Threads vs. Asyncio](#62-experiment-threads-vs-asyncio)
- [7 Limits of an In-Process Event Bus](#7-limits-of-an-in-process-event-bus)
- [8 Let's See How It Works: an Event-Driven Application](#8-lets-see-how-it-works-an-event-driven-application)
- [9 References](#9-references)
<!-- /TOC -->

---

# 1 From Polling to Events

## 1.1 Two Ways of Reacting to Change

A very common way for a component to find out about changes is to **ask** repeatedly "*has
anything changed?*". This is **polling**, and several of our earlier examples used it (the controller
of Lecture 01 read the shared state every 250 ms, the rule engine of Lecture 03 every 100 ms).
Polling has two costs:

- **wasted work:** most checks find nothing new;
- **latency:** a change is noticed only at the next check (up to one full period later).

The alternative is to **invert the flow**: the component where the change happens **announces**
it, and whoever is interested **reacts** immediately. The announcement is called an **event**.

| | Polling / request-response | Event-driven |
|---|---|---|
| Who starts the interaction | The consumer ("give me the value") | The producer ("this just happened") |
| Latency | Up to one polling period | As soon as the event is delivered |
| Work when nothing happens | Continuous checks | None |
| Coupling | The consumer must know whom to ask | Producer and consumer only share the event name |

Many kinds of software are naturally event-driven: graphical user interfaces (a click, a key press),
web applications (a new order, a payment received), microservices (a user signed up), and IoT systems
(a motion sensor fires, a threshold is crossed, a device goes offline).

## 1.2 Vocabulary: Events, Producers, Consumers, Bus, Broker

- **Event:** an immutable record of **something that happened** ("order 42 was paid", or in the IoT
  example "temperature in room-101 is now 25.3 °C"). It has a **type** (or *topic*) and a **payload**.
  Events are named in the past tense or as facts, never as orders. An order ("ship order 42", "open the
  window") is a **command**, which in practice is often delivered through the same mechanism.
- **Producer (publisher):** the component that emits events.
- **Consumer (subscriber, handler):** a component that registers interest in some event types and
  is invoked when one occurs.
- **Event bus:** the in-process component that receives events from producers and **dispatches**
  them to the matching consumers.
- **Publish/subscribe (pub/sub):** the interaction style where producers publish to a *topic*
  without knowing the subscribers, and subscribers receive what is published on the topics they chose.
- **Message broker:** an *external* server that plays the role of the bus between different
  processes and machines: Kafka, RabbitMQ, Redis Streams, and, in IoT, **MQTT** (e.g. Mosquitto, EMQX,
  HiveMQ), where devices publish on topics like `building/room-101/temperature` and services
  subscribe with wildcards (`building/+/temperature`, `building/#`).

In this lecture we build an **in-process event bus** step by step. It is a small, self-contained
version of what a broker does, and it makes the underlying ideas concrete.

---

# 2 Starting Point: the Observer Pattern and Its Limits

The Observer pattern of the OOP lecture is the simplest form of event notification: a *subject*
keeps a list of *observers* and calls `update()` on each of them.

**IoT example:** a temperature sensor (the subject) notifies a dashboard and a slow cloud logger (the
observers) of every new reading.


`code/05_event_driven/01_observer_limits.py`
```python
import time
from typing import Protocol


class Observer(Protocol):
    """Anything with an update(sensor_id, value) method can observe a sensor (duck typing)."""

    def update(self, sensor_id: str, value: float) -> None: ...


class TemperatureSensor:
    """Subject: keeps a list of observers and notifies them on every new reading."""

    def __init__(self, sensor_id: str) -> None:
        self.sensor_id = sensor_id
        self._observers: list[Observer] = []

    def add_observer(self, observer: Observer) -> None:
        self._observers.append(observer)

    def publish_reading(self, value: float) -> None:
        for observer in self._observers:          # synchronous: one after the other
            observer.update(self.sensor_id, value)


class Dashboard:
    def update(self, sensor_id: str, value: float) -> None:
        print(f"    [dashboard] {sensor_id} = {value}")


class CloudLogger:
    def update(self, sensor_id: str, value: float) -> None:
        time.sleep(0.3)                           # slow HTTP request to the cloud
        print(f"    [cloud] {sensor_id} = {value} stored")


def main() -> None:
    sensor = TemperatureSensor("temp-01")
    sensor.add_observer(Dashboard())
    sensor.add_observer(CloudLogger())

    start = time.perf_counter()
    for value in (21.0, 21.2, 21.4):              # the sensor wants to sample every 0.1 s...
        t0 = time.perf_counter()
        sensor.publish_reading(value)
        print(f"[{time.perf_counter() - start:.2f}s] publish_reading() blocked the sensor "
              f"for {time.perf_counter() - t0:.2f}s")


if __name__ == "__main__":
    main()
```

**Output:**
```
    [dashboard] temp-01 = 21.0
    [cloud] temp-01 = 21.0 stored
[0.30s] publish_reading() blocked the sensor for 0.30s
    [dashboard] temp-01 = 21.2
    [cloud] temp-01 = 21.2 stored
[0.60s] publish_reading() blocked the sensor for 0.30s
    [dashboard] temp-01 = 21.4
    [cloud] temp-01 = 21.4 stored
[0.90s] publish_reading() blocked the sensor for 0.30s
```

The Observer pattern already gives us decoupling of **behaviour** (the subject does not know what the
observers do), but it has three limits:

1. **Synchronous delivery:** `notify()` runs every observer **in the publisher's thread**. One slow
   observer slows down the publisher. In the example, the cloud logger slows down the sensor: it wanted
   to sample every 0.1 s, it can only do it every 0.3 s.
2. **Fragility:** an observer that raises an exception interrupts the loop, so the remaining observers
   are never notified, and the exception propagates **into the publisher**.
3. **Direct references:** observers must obtain a reference to the **specific subject** to register.
   An observer interested in *all* the subjects of a kind (a dashboard interested in all temperature
   sensors) must know every one of them.

The following steps remove these limits one at a time.

---

# 3 Step 1: a Synchronous Event Bus

The first improvement addresses the third limit: we put a **bus** between publishers and subscribers.
Both sides know only the bus and the **event type** (a string), never each other.

**IoT example:** a dashboard and a CO2 rule subscribe to sensor events published on the bus.


`code/05_event_driven/02_sync_event_bus.py`
```python
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Event:
    """An immutable fact that happened in the system."""
    type: str                                       # e.g. "sensor.temperature"
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


Handler = Callable[[Event], None]


class SyncEventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        # still synchronous: every handler runs in the publisher's thread, now
        for handler in self._handlers.get(event.type, []):
            handler(event)


# --- three independent components, each knows only the bus ---

def dashboard(event: Event) -> None:
    print(f"[dashboard] {event.type}: {event.payload}")


def co2_rule(event: Event) -> None:
    if event.payload["ppm"] > 1000:
        print(f"[co2-rule]  {event.payload['room']}: CO2 too high -> ventilation needed")


def main() -> None:
    bus = SyncEventBus()
    bus.subscribe("sensor.temperature", dashboard)
    bus.subscribe("sensor.co2", dashboard)
    bus.subscribe("sensor.co2", co2_rule)

    bus.publish(Event("sensor.temperature", {"room": "room-101", "celsius": 22.4}))
    bus.publish(Event("sensor.co2", {"room": "room-101", "ppm": 1180}))
    bus.publish(Event("sensor.humidity", {"room": "room-101", "percent": 51}))   # nobody listens


if __name__ == "__main__":
    main()
```

**Output:**
```
[dashboard] sensor.temperature: {'room': 'room-101', 'celsius': 22.4}
[dashboard] sensor.co2: {'room': 'room-101', 'ppm': 1180}
[co2-rule]  room-101: CO2 too high -> ventilation needed
```

**Key Points:**
- The `Event` is a **frozen dataclass**: once published, nobody can modify it. This matters as soon
  as several handlers (possibly in different threads) receive the **same** event object.
- Event types use a **hierarchical dotted naming** (`sensor.temperature`, `actuator.hvac.command`).
  This convention will let us subscribe with wildcards in Step 3.
- Publishing an event that nobody listens to (`sensor.humidity`) is perfectly legal: the producer does
  not know, and does not need to know, who is interested.
- Delivery is **still synchronous**: a slow handler would still block the publisher. Queues solve that.

---

# 4 Step 2: an Asynchronous Event Bus with Threads and `queue.Queue`

## 4.1 Design: `EventBus`, `EventHandler`, `EventBusClient`

To stop handlers from blocking publishers, we combine the bus with the **producer-consumer
pattern** of Lecture 04: `publish()` only **puts the event in a queue** and returns immediately; a pool
of **worker threads** takes events from the queue and runs the handlers.

We also split the responsibilities into **three classes**:

- **`EventBus`**: the *engine*. It owns the queue, the worker threads and the map *event type →
  handlers*. Application components do not use it directly.
- **`EventHandler`**: an **abstract base class** that defines the **contract** of a callback, a single
  `handle(event)` method. Every component that reacts to events implements a subclass.
- **`EventBusClient`**: the **facade** given to application components. It exposes only two
  operations, **`write`** (publish an event) and **`read`** (subscribe a handler to an event type),
  and hides the internals of the bus (queue, threads).

```
                         ┌──────────────────────── EventBus ────────────────────────┐
 component ─► EventBusClient.write() ─► publish() ─► [ queue.Queue ] ─► bus-worker-0 ─┐
                         │                                         └─► bus-worker-1 ─┤─► handler.handle(event)
 component ─► EventBusClient.read()  ─► subscribe() ─► { type -> [EventHandler, ...] } ┘
                         └──────────────────────────────────────────────────────────┘
```

This separation has a precise benefit: components depend only on `EventBusClient` and
`EventHandler`, two small and stable interfaces, and not on the concrete `EventBus`. Reading a
component's code tells you immediately whether it only publishes, only listens, or both, and in tests
the client can be replaced by a mock ([Lecture 06](06_testing.md)).

## 4.2 Let's See How It Works: a Slow Handler No Longer Blocks the Publisher

**IoT example:** the slow-cloud-logger scenario of Section 2, now on the thread-based bus.

`code/05_event_driven/03_threaded_event_bus.py`
```python
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-14s] %(message)s")
logger = logging.getLogger("event-bus")


@dataclass(frozen=True)
class Event:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class EventHandler(ABC):
    """Contract of a callback: every concrete handler implements handle()."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        ...


class EventBus:
    """Engine of the event layer: queue + worker pool + dispatch to the registered handlers."""

    def __init__(self, num_workers: int = 2) -> None:
        self._queue: queue.Queue[Event] = queue.Queue()
        self._handlers: dict[str, list[EventHandler]] = {}
        self._lock = threading.Lock()                 # protects _handlers (subscribe vs dispatch)
        self._workers = [
            threading.Thread(target=self._run_worker, name=f"bus-worker-{i}", daemon=True)
            for i in range(num_workers)
        ]

    def start(self) -> None:
        for w in self._workers:
            w.start()

    def publish(self, event: Event) -> None:
        self._queue.put(event)                        # returns immediately

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def wait_until_idle(self) -> None:
        """Block until every published event has been dispatched (useful for demos and tests)."""
        self._queue.join()

    def _run_worker(self) -> None:
        while True:
            event = self._queue.get()
            with self._lock:
                handlers = list(self._handlers.get(event.type, []))   # copy: dispatch outside the lock
            for handler in handlers:
                try:
                    handler.handle(event)
                except Exception:
                    logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            self._queue.task_done()


class EventBusClient:
    """Interface used by application components: only write (publish) and read (subscribe)."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        self._bus.publish(Event(type=event_type, payload=payload or {}))

    def read(self, event_type: str, handler: EventHandler) -> None:
        self._bus.subscribe(event_type, handler)


# --------------------------- application components ---------------------------

class DashboardHandler(EventHandler):
    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s = %s", event.type, event.payload)


class CloudLoggerHandler(EventHandler):
    def handle(self, event: Event) -> None:
        time.sleep(0.3)                               # slow HTTP call: no longer blocks the sensor!
        logger.info("[cloud] stored %s", event.payload)


def main() -> None:
    bus = EventBus(num_workers=2)
    bus.start()
    client = EventBusClient(bus)

    client.read("sensor.temperature", DashboardHandler())
    client.read("sensor.temperature", CloudLoggerHandler())

    start = time.perf_counter()
    for value in (21.0, 21.2, 21.4):
        t0 = time.perf_counter()
        client.write("sensor.temperature", {"sensor": "temp-01", "celsius": value})
        logger.info("sensor: published %.1f, write() took %.1f ms", value, (time.perf_counter() - t0) * 1000)
        time.sleep(0.1)

    bus.wait_until_idle()
    logger.info("all events dispatched in %.2fs", time.perf_counter() - start)


if __name__ == "__main__":
    main()
```

**Output:**
```
    3ms [MainThread    ] sensor: published 21.0, write() took 0.0 ms
    3ms [bus-worker-0  ] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.0}
  104ms [MainThread    ] sensor: published 21.2, write() took 0.1 ms
  104ms [bus-worker-1  ] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.2}
  204ms [MainThread    ] sensor: published 21.4, write() took 0.1 ms
  304ms [bus-worker-0  ] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.0}
  304ms [bus-worker-0  ] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.4}
  404ms [bus-worker-1  ] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.2}
  604ms [bus-worker-0  ] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.4}
  604ms [MainThread    ] all events dispatched in 0.60s
```

**Key Points:**
- `write()` now takes **~0.1 ms**, and the publisher (the sensor) runs every 100 ms as it wanted: the first
  limit of the Observer pattern is gone.
- An exception in a handler is **caught and logged** by the worker, so the other handlers and the
  publisher are not affected (the second limit is gone too).
- The worker **copies the handler list** under the lock and dispatches **outside** it: a slow handler
  never blocks `subscribe()`, and no handler code runs while a lock is held (remember the deadlock
  advice of Lecture 01).
- Look at the dashboard line for `21.4`: it was published at 204 ms but displayed at **304 ms**. Both
  workers were busy with the slow handler (the cloud logger), so the fast one (the dashboard) had to
  wait. This is
  **head-of-line blocking**: with a shared pool, slow handlers delay fast ones. Remedies: more workers,
  a separate bus (or queue) for slow handlers, or asyncio (Step 3).
- With more than one worker, **events can be handled out of order**: two events published in sequence
  may be dispatched concurrently by two workers. If the order matters (e.g. for the commands of a
  same entity, like a single actuator), use one worker or route all the events of the same entity to
  the same worker.

---

# 5 Making the Bus Robust

The bus of Step 2 is fine for a demo but lacks features a real application needs. The module
`event_bus.py` extends it with:

- **wildcard subscriptions** with shell-style patterns (`sensor.*`, `*.co2`, `*`), via
  `fnmatch.fnmatchcase()`, similar in spirit to MQTT's `+` and `#`;
- **unsubscribe**: `subscribe()` returns a `Subscription` receipt that can be passed to `unsubscribe()`;
- **statistics** on published, dispatched and failed deliveries;
- a **clean lifecycle**: `start()` / `stop()` with a **graceful drain** via `Queue.shutdown()`
  (Lecture 04), non-daemon workers, and context-manager support (`with EventBus() as bus:`);
- a `source` field in each event, so every event says **who** published it.

`code/05_event_driven/event_bus.py`
```python
from __future__ import annotations

import fnmatch
import itertools
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("event-bus")


@dataclass(frozen=True)
class Event:
    """An immutable fact: something that happened, with its data."""
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"
    timestamp: float = field(default_factory=time.time)


class EventHandler(ABC):
    """Contract of a callback: every concrete handler implements handle()."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        ...


@dataclass(frozen=True)
class Subscription:
    """Receipt returned by subscribe(): pass it to unsubscribe()."""
    id: int
    pattern: str
    handler: EventHandler


@dataclass
class BusStats:
    published: int = 0
    dispatched: int = 0          # successful handler invocations
    failed: int = 0              # handler invocations that raised


class EventBus:
    """Thread-based publish/subscribe engine."""

    def __init__(self, num_workers: int = 2, max_queue_size: int = 0) -> None:
        self._queue: queue.Queue[Event] = queue.Queue(maxsize=max_queue_size)
        self._subscriptions: dict[int, Subscription] = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()                     # protects _subscriptions and stats
        self._num_workers = num_workers
        self._workers: list[threading.Thread] = []
        self.stats = BusStats()

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self._workers = [
            threading.Thread(target=self._run_worker, name=f"bus-worker-{i}")
            for i in range(self._num_workers)
        ]
        for w in self._workers:
            w.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop accepting events, dispatch the ones already queued, then stop the workers."""
        self._queue.shutdown()                            # graceful: workers drain the queue first
        for w in self._workers:
            w.join(timeout)

    def __enter__(self) -> EventBus:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # ------------------------------------------------------------------ pub / sub
    def publish(self, event: Event) -> None:
        """Enqueue an event; raises queue.ShutDown if the bus has been stopped."""
        self._queue.put(event)
        with self._lock:
            self.stats.published += 1

    def subscribe(self, pattern: str, handler: EventHandler) -> Subscription:
        with self._lock:
            sub = Subscription(next(self._ids), pattern, handler)
            self._subscriptions[sub.id] = sub
            return sub

    def unsubscribe(self, subscription: Subscription) -> None:
        with self._lock:
            self._subscriptions.pop(subscription.id, None)

    def wait_until_idle(self) -> None:
        """Block until every event published so far has been fully dispatched."""
        self._queue.join()

    # ------------------------------------------------------------------ internals
    def _matching_handlers(self, event_type: str) -> list[EventHandler]:
        with self._lock:                                  # snapshot under the lock...
            subs = list(self._subscriptions.values())
        return [s.handler for s in subs if fnmatch.fnmatchcase(event_type, s.pattern)]

    def _run_worker(self) -> None:
        while True:
            try:
                event = self._queue.get()
            except queue.ShutDown:
                return
            try:
                for handler in self._matching_handlers(event.type):   # ...dispatch outside it
                    try:
                        handler.handle(event)
                        with self._lock:
                            self.stats.dispatched += 1
                    except Exception:
                        with self._lock:
                            self.stats.failed += 1
                        logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            finally:
                self._queue.task_done()


class EventBusClient:
    """Facade for application components: write() to publish, read() to subscribe."""

    def __init__(self, bus: EventBus, source: str) -> None:
        self._bus = bus
        self._source = source                             # who is publishing (for tracing)

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        self._bus.publish(Event(event_type, payload or {}, source=self._source))

    def read(self, pattern: str, handler: EventHandler) -> Subscription:
        return self._bus.subscribe(pattern, handler)

    def stop_reading(self, subscription: Subscription) -> None:
        self._bus.unsubscribe(subscription)
```

A short demo of the new features:

**IoT example:** an audit log subscribed to everything, a sensor dashboard subscribed to `sensor.*`, and a
buggy third-party plugin.


`code/05_event_driven/04_robust_event_bus.py`
```python
import logging

from event_bus import Event, EventBus, EventBusClient, EventHandler

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-12s] %(message)s")
logger = logging.getLogger("demo")


class AuditLog(EventHandler):
    """Subscribed to '*': records every event of the system."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def handle(self, event: Event) -> None:
        self.entries.append(f"{event.source} -> {event.type}")


class SensorDashboard(EventHandler):
    """Subscribed to 'sensor.*': shows every sensor reading."""

    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s %s", event.type, event.payload)


class BuggyPlugin(EventHandler):
    """A third-party plugin with a bug: it crashes on CO2 events."""

    def handle(self, event: Event) -> None:
        if event.type == "sensor.co2":
            raise KeyError("celsius")


def main() -> None:
    audit = AuditLog()
    with EventBus(num_workers=2) as bus:                  # start() ... stop()
        sensors = EventBusClient(bus, source="room-101-gateway")
        dashboard_sub = sensors.read("sensor.*", SensorDashboard())
        sensors.read("*", audit)
        sensors.read("sensor.co2", BuggyPlugin())

        sensors.write("sensor.temperature", {"celsius": 22.1})
        sensors.write("sensor.co2", {"ppm": 850})         # BuggyPlugin fails, the others are fine
        sensors.write("actuator.window.command", {"state": "OPEN"})
        bus.wait_until_idle()

        sensors.stop_reading(dashboard_sub)               # the dashboard is closed
        sensors.write("sensor.temperature", {"celsius": 22.3})   # not shown anymore
    # leaving the with-block: queued events are dispatched, workers stopped

    logger.info("audit log: %s", audit.entries)
    logger.info("stats: %s", bus.stats)


if __name__ == "__main__":
    main()
```

**Output** (traceback shortened):
```
    5ms [bus-worker-0] [dashboard] sensor.temperature {'celsius': 22.1}
    5ms [bus-worker-1] [dashboard] sensor.co2 {'ppm': 850}
    5ms [bus-worker-1] handler BuggyPlugin failed on sensor.co2
Traceback (most recent call last):
  File ".../event_bus.py", line 131, in _run_worker
    handler.handle(event)
  File ".../04_robust_event_bus.py", line 37, in handle
    raise KeyError("celsius")
KeyError: 'celsius'
    6ms [MainThread  ] audit log: ['room-101-gateway -> sensor.temperature', 'room-101-gateway -> actuator.window.command', 'room-101-gateway -> sensor.co2', 'room-101-gateway -> sensor.temperature']
    6ms [MainThread  ] stats: BusStats(published=4, dispatched=6, failed=1)
```

**Key Points:**
- `sensor.*` matched both sensor events, `*` matched **everything**, including the actuator command.
- The buggy plugin failed with a **full traceback in the log** (`logger.exception`), but the dashboard
  and the audit log still received the CO2 event, and the worker kept running.
- After `stop_reading()`, the last temperature event reached the audit log but **not** the dashboard.
- The audit log shows `actuator.window.command` **before** `sensor.co2`, although it was published
  after: with 2 workers the delivery order is not guaranteed (see Step 2).
- The statistics add up: 4 events published; 6 successful deliveries (dashboard ×2, audit ×4) and
  1 failure.

---

# 6 Step 3: the Same Bus with `asyncio`

## 6.1 An `asyncio.Queue`-Based Event Bus

The same architecture can be built with coroutines: handlers become `async def handle()`,
the queue becomes an `asyncio.Queue`, and the workers become **tasks** in a single thread. The
`Event` dataclass is reused unchanged.

`code/05_event_driven/async_event_bus.py`
```python
from __future__ import annotations

import asyncio
import fnmatch
import itertools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from event_bus import BusStats, Event            # the Event dataclass is reused unchanged

logger = logging.getLogger("async-event-bus")


class AsyncEventHandler(ABC):
    """Contract of an asynchronous callback."""

    @abstractmethod
    async def handle(self, event: Event) -> None:
        ...


@dataclass(frozen=True)
class AsyncSubscription:
    id: int
    pattern: str
    handler: AsyncEventHandler


class AsyncEventBus:
    def __init__(self, num_workers: int = 2, max_queue_size: int = 0) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._subscriptions: dict[int, AsyncSubscription] = {}
        self._ids = itertools.count(1)
        self._num_workers = num_workers
        self._workers: list[asyncio.Task[None]] = []
        self.stats = BusStats()
        # No locks: everything runs in the event loop thread, and the registry and the stats
        # are never modified across an await.

    async def __aenter__(self) -> AsyncEventBus:
        self._workers = [asyncio.create_task(self._run_worker(), name=f"bus-worker-{i}")
                         for i in range(self._num_workers)]
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._queue.shutdown()                            # graceful: drain, then workers exit
        await asyncio.gather(*self._workers)

    async def publish(self, event: Event) -> None:
        await self._queue.put(event)                      # suspends only if the queue is bounded and full
        self.stats.published += 1

    def subscribe(self, pattern: str, handler: AsyncEventHandler) -> AsyncSubscription:
        sub = AsyncSubscription(next(self._ids), pattern, handler)
        self._subscriptions[sub.id] = sub
        return sub

    def unsubscribe(self, subscription: AsyncSubscription) -> None:
        self._subscriptions.pop(subscription.id, None)

    async def wait_until_idle(self) -> None:
        await self._queue.join()

    async def _run_worker(self) -> None:
        while True:
            try:
                event = await self._queue.get()
            except asyncio.QueueShutDown:
                return
            try:
                handlers = [s.handler for s in list(self._subscriptions.values())
                            if fnmatch.fnmatchcase(event.type, s.pattern)]
                for handler in handlers:
                    try:
                        await handler.handle(event)
                        self.stats.dispatched += 1
                    except Exception:
                        self.stats.failed += 1
                        logger.exception("handler %s failed on %s", type(handler).__name__, event.type)
            finally:
                self._queue.task_done()
```

**IoT example:** the slow-cloud-logger scenario, now with coroutines:

`code/05_event_driven/05_async_event_bus.py`
```python
import asyncio
import logging
import time

from async_event_bus import AsyncEventBus, AsyncEventHandler
from event_bus import Event

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(taskName)-12s] %(message)s")
logger = logging.getLogger("demo")


class Dashboard(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        logger.info("[dashboard] %s = %s", event.type, event.payload)


class CloudLogger(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        await asyncio.sleep(0.3)                  # async HTTP call: the loop keeps running
        logger.info("[cloud] stored %s", event.payload)


async def main() -> None:
    start = time.perf_counter()
    async with AsyncEventBus(num_workers=4) as bus:
        bus.subscribe("sensor.*", Dashboard())
        bus.subscribe("sensor.temperature", CloudLogger())
        for value in (21.0, 21.2, 21.4):
            await bus.publish(Event("sensor.temperature", {"sensor": "temp-01", "celsius": value}))
            logger.info("sensor: published %.1f", value)
            await asyncio.sleep(0.1)
        await bus.wait_until_idle()
    logger.info("all events dispatched in %.2fs, stats: %s", time.perf_counter() - start, bus.stats)


if __name__ == "__main__":
    asyncio.run(main())
```

**Output:**
```
   16ms [Task-1      ] sensor: published 21.0
   16ms [bus-worker-0] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.0}
  116ms [Task-1      ] sensor: published 21.2
  116ms [bus-worker-1] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.2}
  217ms [Task-1      ] sensor: published 21.4
  217ms [bus-worker-2] [dashboard] sensor.temperature = {'sensor': 'temp-01', 'celsius': 21.4}
  316ms [bus-worker-0] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.0}
  417ms [bus-worker-1] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.2}
  517ms [bus-worker-2] [cloud] stored {'sensor': 'temp-01', 'celsius': 21.4}
  518ms [Task-1      ] all events dispatched in 0.50s, stats: BusStats(published=3, dispatched=6, failed=0)
```

**Key Points:**
- The code is almost identical to the threaded version: `async`/`await` appear where the thread version
  would block, and **no locks are needed**, because the subscription registry and the counters are
  never modified across an `await`.
- 4 worker *tasks* cost almost nothing, so head-of-line blocking is much easier to avoid: here the
  dashboard is never delayed.
- The golden rule of Lecture 03 applies: an `async` handler that calls blocking code freezes **the
  whole bus**. Wrap blocking calls with `asyncio.to_thread()`.

## 6.2 Experiment: Threads vs. Asyncio

The same workload (1,000 events × 4 handlers) on both buses, in three scenarios:

**IoT example:** 1,000 sensor events dispatched to 4 handlers; the I/O-bound handlers simulate a 2 ms
write to a remote database.


`code/05_event_driven/06_benchmark_threads_vs_asyncio.py`
```python
import asyncio
import threading
import time
from collections.abc import Callable

from async_event_bus import AsyncEventBus, AsyncEventHandler
from event_bus import Event, EventBus, EventHandler

N_EVENTS = 1_000
M_HANDLERS = 4
IO_LATENCY_S = 0.002


class ThreadIOHandler(EventHandler):
    def handle(self, event: Event) -> None:
        time.sleep(IO_LATENCY_S)


class ThreadCounterHandler(EventHandler):
    def __init__(self) -> None:
        self.count = 0

    def handle(self, event: Event) -> None:
        self.count += 1               # approximate under threads, fine for a benchmark


class AsyncIOHandler(AsyncEventHandler):
    async def handle(self, event: Event) -> None:
        await asyncio.sleep(IO_LATENCY_S)


class AsyncCounterHandler(AsyncEventHandler):
    def __init__(self) -> None:
        self.count = 0

    async def handle(self, event: Event) -> None:
        self.count += 1


def run_threaded(handler_factory: Callable[[], EventHandler], workers: int) -> float:
    start = time.perf_counter()
    with EventBus(num_workers=workers) as bus:
        for _ in range(M_HANDLERS):
            bus.subscribe("sensor.*", handler_factory())
        for i in range(N_EVENTS):
            bus.publish(Event("sensor.temperature", {"seq": i}))
        bus.wait_until_idle()
    return time.perf_counter() - start


async def run_async(handler_factory: Callable[[], AsyncEventHandler], workers: int) -> float:
    start = time.perf_counter()
    async with AsyncEventBus(num_workers=workers) as bus:
        for _ in range(M_HANDLERS):
            bus.subscribe("sensor.*", handler_factory())
        for i in range(N_EVENTS):
            await bus.publish(Event("sensor.temperature", {"seq": i}))
        await bus.wait_until_idle()
    return time.perf_counter() - start


def main() -> None:
    sequential_io = N_EVENTS * M_HANDLERS * IO_LATENCY_S
    print(f"{N_EVENTS} events x {M_HANDLERS} handlers (sequential I/O time would be {sequential_io:.1f}s)")
    scenarios = [
        ("A) I/O-bound,  16 workers", ThreadIOHandler, AsyncIOHandler, 16),
        ("B) I/O-bound, 256 workers", ThreadIOHandler, AsyncIOHandler, 256),
        ("C) trivial,    16 workers", ThreadCounterHandler, AsyncCounterHandler, 16),
    ]
    for label, thread_handler, async_handler, workers in scenarios:
        t_threads = run_threaded(thread_handler, workers)
        t_async = asyncio.run(run_async(async_handler, workers))
        print(f"{label} -> threads: {t_threads:.2f}s | asyncio: {t_async:.2f}s")
    print(f"threads alive after the benchmark: {threading.active_count()}")


if __name__ == "__main__":
    main()
```

**Output** (numbers vary between machines and runs):
```
1000 events x 4 handlers (sequential I/O time would be 8.0s)
A) I/O-bound,  16 workers -> threads: 0.53s | asyncio: 0.62s
B) I/O-bound, 256 workers -> threads: 0.16s | asyncio: 0.05s
C) trivial,    16 workers -> threads: 0.01s | asyncio: 0.01s
threads alive after the benchmark: 1
```

**How to read the results:**
- **A) same moderate concurrency:** the two buses are **equivalent**; both turn 8 s of sequential I/O
  into ~0.5 s. Asyncio is not magically faster: with the same degree of concurrency, the I/O waits
  dominate.
- **B) high concurrency:** asyncio pulls ahead. 256 tasks cost almost nothing, while 256 threads mean
  256 OS threads to create, schedule and join. This is the scenario of a server with many open
  connections, or of a gateway talking to hundreds of devices.
- **C) trivial handlers:** the dispatch overhead is negligible in both cases for this load.
- The last line confirms the **clean shutdown** of the thread bus: after 3 benchmark runs with up to 256
  workers, only the main thread is alive.

| Criterion | Threads + `queue.Queue` | `asyncio` + `asyncio.Queue` |
|---|---|---|
| Handlers can call blocking libraries | Yes | Only via `to_thread()` |
| Cost of high concurrency | High (one OS thread per worker) | Low (one task per worker) |
| Locks needed for shared state | Yes | Only across `await` |
| Cancellation / timeouts of handlers | Not possible from outside | Built in |
| Readability | Familiar | Very close, but `async` spreads through the code |

---

# 7 Limits of an In-Process Event Bus

Our bus is a great tool **inside one program**, but it has precise limits:

- **The GIL:** thread-based handlers get real concurrency only when they are I/O-bound. CPU-heavy
  handlers should offload work to a `ProcessPoolExecutor` (Lecture 02), and if producers and consumers
  are separate processes, `queue.Queue` must be replaced by `multiprocessing.Queue` (Lecture 04).
- **One process only:** the queue lives in memory. Other processes, containers or machines cannot
  publish or subscribe.
- **No persistence:** if the process crashes or restarts, the events waiting in the queue are **lost**.
  There is no acknowledgement or redelivery.
- **No delivery guarantees across failures:** at-least-once or exactly-once delivery, retention and replay
  require a broker.

When these limits matter, the same concepts (topics, publish, subscribe, handlers) move to an
**external broker**: **Kafka** for high-throughput, persistent event streams, **RabbitMQ** for
flexible routing and work queues, **MQTT** for devices and constrained networks. A common and effective
architecture is a **hybrid**: an in-process bus inside each service, and a broker client that bridges
selected topics to and from the broker (in IoT, an in-process bus inside each gateway bridged to MQTT).

---

# 8 Let's See How It Works: an Event-Driven Application

The final example shows the typical shape of an event-driven application: **producers** that
publish facts, a **rule/decision component** that turns facts into commands, **executors** that carry
out commands and publish their new state, and **observers** (UI, notifications, audit) that only
listen. No component references another one: they share only the bus and the event names.

**IoT example: an event-driven Smart Room**, the scenario of the previous lectures rebuilt as a set of
components that communicate **only through events**:

```
 temp-01 ─┐                                  ┌─ actuator.hvac.command ───► hvac-01  ─┐
 co2-01  ─┼─ sensor.* ──► RuleEngine ────────┼─ actuator.window.command ─► window-01 ├─ actuator.*.state ─► Dashboard
 pir-01  ─┘                                  ├─ actuator.light.command ──► light-01 ─┘
                                             └─ alarm.co2 ───────────────► NotificationService
```

`code/05_event_driven/07_smart_room_automation.py`
```python
import logging
import threading
import time
from typing import Any

from event_bus import Event, EventBus, EventBusClient, EventHandler

logging.basicConfig(level=logging.INFO, format="%(relativeCreated)5.0fms [%(threadName)-12s] %(message)s")
logger = logging.getLogger("smart-room")

ROOM = "room-101"


# ------------------------------------------------------------------ sensors (publishers)
def sensor_publisher(client: EventBusClient, event_type: str, key: str,
                     values: list[Any], period_s: float) -> None:
    """Publish a scripted sequence of readings, one every period_s seconds."""
    for value in values:
        client.write(event_type, {"room": ROOM, key: value})
        time.sleep(period_s)


# ------------------------------------------------------------------ rule engine
class RuleEngine(EventHandler):
    """Turns sensor events into actuator commands and alarms."""

    TEMP_HIGH_C = 25.0
    CO2_HIGH_PPM = 1000
    CO2_ALARM_PPM = 1200

    def __init__(self, client: EventBusClient) -> None:
        self._client = client
        self._lock = threading.Lock()          # handle() runs concurrently in several bus workers
        self._co2_alarm_active = False

    def handle(self, event: Event) -> None:
        match event.type:
            case "sensor.temperature":
                mode = "COOLING" if event.payload["celsius"] > self.TEMP_HIGH_C else "OFF"
                self._client.write("actuator.hvac.command", {"room": ROOM, "mode": mode})
            case "sensor.co2":
                ppm = event.payload["ppm"]
                state = "OPEN" if ppm > self.CO2_HIGH_PPM else "CLOSED"
                self._client.write("actuator.window.command", {"room": ROOM, "state": state})
                with self._lock:                   # check-then-act on shared state: needs the lock
                    raise_alarm = ppm > self.CO2_ALARM_PPM and not self._co2_alarm_active
                    if raise_alarm:
                        self._co2_alarm_active = True
                if raise_alarm:
                    self._client.write("alarm.co2", {"room": ROOM, "ppm": ppm})
            case "sensor.presence":
                state = "ON" if event.payload["present"] else "OFF"
                self._client.write("actuator.light.command", {"room": ROOM, "state": state})


# ------------------------------------------------------------------ actuators (subscribers + publishers)
class Actuator(EventHandler):
    """Executes commands and publishes its new state only when it actually changes."""

    def __init__(self, name: str, state_key: str, initial: str, client: EventBusClient) -> None:
        self.name = name
        self.state_key = state_key
        self.state = initial
        self._client = client
        self._lock = threading.Lock()

    def handle(self, event: Event) -> None:
        requested = event.payload[self.state_key]
        with self._lock:
            if requested == self.state:
                return                                         # idempotent: nothing to do
            time.sleep(0.02)                                   # command sent to the device
            old, self.state = self.state, requested
        self._client.write(f"actuator.{self.name}.state", {"room": ROOM, "from": old, "to": requested})


# ------------------------------------------------------------------ observers
class Dashboard(EventHandler):
    def handle(self, event: Event) -> None:
        logger.info("[dashboard] %-22s %s -> %s", event.type, event.payload["from"], event.payload["to"])


class NotificationService(EventHandler):
    def handle(self, event: Event) -> None:
        logger.warning("[notify] %s in %s: %s  (sending push notification to the facility manager)",
                       event.type, event.payload["room"], event.payload)


def main() -> None:
    with EventBus(num_workers=3) as bus:
        # each component gets its own client, labelled with its name (event.source)
        rules = RuleEngine(EventBusClient(bus, "rule-engine"))
        hvac = Actuator("hvac", "mode", "OFF", EventBusClient(bus, "hvac-01"))
        window = Actuator("window", "state", "CLOSED", EventBusClient(bus, "window-01"))
        light = Actuator("light", "state", "OFF", EventBusClient(bus, "light-01"))

        wiring = EventBusClient(bus, "bootstrap")
        wiring.read("sensor.*", rules)
        wiring.read("actuator.hvac.command", hvac)
        wiring.read("actuator.window.command", window)
        wiring.read("actuator.light.command", light)
        wiring.read("actuator.*.state", Dashboard())
        wiring.read("alarm.*", NotificationService())

        publishers = [
            threading.Thread(target=sensor_publisher, name="temp-01", args=(
                EventBusClient(bus, "temp-01"), "sensor.temperature", "celsius",
                [24.2, 24.8, 25.3, 25.6, 25.1, 24.7], 0.1)),
            threading.Thread(target=sensor_publisher, name="co2-01", args=(
                EventBusClient(bus, "co2-01"), "sensor.co2", "ppm",
                [820, 950, 1080, 1250, 1150, 980], 0.1)),
            threading.Thread(target=sensor_publisher, name="pir-01", args=(
                EventBusClient(bus, "pir-01"), "sensor.presence", "present",
                [True, True, True, False, False, False], 0.1)),
        ]
        for p in publishers:
            p.start()
        for p in publishers:
            p.join()
        bus.wait_until_idle()

    logger.info("final state: hvac=%s window=%s light=%s", hvac.state, window.state, light.state)
    logger.info("bus stats: %s", bus.stats)


if __name__ == "__main__":
    main()
```

**Output:**
```
   25ms [bus-worker-0] [dashboard] actuator.light.state   OFF -> ON
  225ms [bus-worker-1] [dashboard] actuator.hvac.state    OFF -> COOLING
  225ms [bus-worker-0] [dashboard] actuator.window.state  CLOSED -> OPEN
  305ms [bus-worker-0] [notify] alarm.co2 in room-101: {'room': 'room-101', 'ppm': 1250}  (sending push notification to the facility manager)
  326ms [bus-worker-1] [dashboard] actuator.light.state   ON -> OFF
  525ms [bus-worker-2] [dashboard] actuator.hvac.state    COOLING -> OFF
  526ms [bus-worker-1] [dashboard] actuator.window.state  OPEN -> CLOSED
  606ms [MainThread  ] final state: hvac=OFF window=CLOSED light=OFF
  607ms [MainThread  ] bus stats: BusStats(published=43, dispatched=43, failed=0)
```

**Key Points:**
- **No component references another one.** The rule engine does not know which actuators exist, the
  actuators do not know who sends commands, the dashboard does not know which actuators exist
  (`actuator.*.state`). Adding a new consumer, e.g. a component that stores every `actuator.*.state`
  change in a database, means writing one handler and one `read()` call, with **zero changes** to the
  existing code.
- **Chains of events:** a fact (a sensor reading) produces a command event, which produces a state event. Each
  step is small and has a single responsibility.
- **Idempotent command handlers:** the rule engine sends a command for **every** reading (18
  commands), but the actuators publish a state change only when the state actually changes (6 times).
  This keeps the rule engine stateless and simple, and the system tolerant to duplicate commands.
- **Concurrency still matters:** with 3 workers, `RuleEngine.handle()` and `Actuator.handle()` can run
  in parallel, so the *check-then-act* on `_co2_alarm_active` and on `state` is protected by a lock.
  Without it, two CO2 events processed at the same time could raise **two** alarms.
- The statistics add up: 18 sensor events + 18 commands + 6 state changes + 1 alarm = **43** events,
  all dispatched, none failed.

**What's next:** an event-driven system is highly decoupled, and this makes it **easy to test**:
each handler can be tested by calling `handle()` with a hand-made `Event`, and the `EventBusClient` can
be replaced with a mock that records what the component publishes. The bus itself needs tests that
cope with threads and asynchronous delivery. → [Lecture 06 - Testing](06_testing.md).

---

# 9 References

- Python documentation - [`queue`](https://docs.python.org/3/library/queue.html), [`asyncio` Queues](https://docs.python.org/3/library/asyncio-queue.html), [`fnmatch`](https://docs.python.org/3/library/fnmatch.html)
- Python documentation - [`abc` - Abstract Base Classes](https://docs.python.org/3/library/abc.html)
- Martin Fowler - [What do you mean by "Event-Driven"?](https://martinfowler.com/articles/201701-event-driven.html)
- [MQTT Version 5.0 specification (OASIS)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html), topics and wildcards
- Medium - [Mastering Python's Event-Driven Architecture](https://medium.com/pythoneers/mastering-pythons-event-driven-architecture-building-asynchronous-systems-without-going-insane-5768b6a8cf3d)
- OneUptime blog - [How to Build Event-Driven Systems with Python](https://oneuptime.com/blog/post/2026-02-02-python-event-driven-systems/view)
