"""
Lecture 06 - Section 7.2: testing the thread-based event bus (Lecture 05).

Delivery is ASYNCHRONOUS: after publish() the handler has not run yet. Tests must wait for
it explicitly - with a threading.Event set by the handler, or with bus.wait_until_idle() -
never with time.sleep().
"""

import queue
import threading
from unittest.mock import Mock

import pytest

from smart_gateway.event_bus import Event, EventBus, EventBusClient, EventHandler

TIMEOUT_S = 2.0


class CollectingHandler(EventHandler):
    """Test handler: records events and signals when `expected` events have arrived."""

    def __init__(self, expected: int = 1) -> None:
        self.events: list[Event] = []
        self._expected = expected
        self._lock = threading.Lock()
        self.done = threading.Event()

    def handle(self, event: Event) -> None:
        with self._lock:
            self.events.append(event)
            if len(self.events) >= self._expected:
                self.done.set()

    @property
    def types(self) -> list[str]:
        return sorted(e.type for e in self.events)    # sorted: delivery order is not guaranteed


class FailingHandler(EventHandler):
    def handle(self, event: Event) -> None:
        raise RuntimeError("boom")


def test_handler_receives_published_event(event_bus: EventBus) -> None:
    handler = CollectingHandler()
    client = EventBusClient(event_bus, source="test")
    client.read("sensor.temperature", handler)

    client.write("sensor.temperature", {"celsius": 22.5})

    assert handler.done.wait(TIMEOUT_S), "handler was not called"
    event = handler.events[0]
    assert (event.type, event.payload, event.source) == ("sensor.temperature", {"celsius": 22.5}, "test")


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("sensor.*", ["sensor.co2", "sensor.temperature"]),
        ("*.co2", ["alarm.co2", "sensor.co2"]),
        ("*", ["alarm.co2", "sensor.co2", "sensor.temperature"]),
        ("sensor.temperature", ["sensor.temperature"]),
    ],
)
def test_wildcard_subscriptions(event_bus: EventBus, pattern: str, expected: list[str]) -> None:
    handler = CollectingHandler()
    event_bus.subscribe(pattern, handler)
    for event_type in ("sensor.temperature", "sensor.co2", "alarm.co2"):
        event_bus.publish(Event(event_type))
    event_bus.wait_until_idle()                       # every event dispatched: now we can assert
    assert handler.types == expected


def test_failing_handler_does_not_stop_the_others(event_bus: EventBus,
                                                  caplog: pytest.LogCaptureFixture) -> None:
    good = CollectingHandler(expected=2)
    event_bus.subscribe("sensor.*", FailingHandler())
    event_bus.subscribe("sensor.*", good)

    event_bus.publish(Event("sensor.temperature"))
    event_bus.publish(Event("sensor.co2"))

    assert good.done.wait(TIMEOUT_S)
    event_bus.wait_until_idle()
    assert event_bus.stats.failed == 2
    assert "FailingHandler failed" in caplog.text     # the error was logged, not swallowed


def test_unsubscribed_handler_receives_nothing(event_bus: EventBus) -> None:
    handler = CollectingHandler()
    subscription = event_bus.subscribe("sensor.*", handler)
    event_bus.unsubscribe(subscription)
    event_bus.publish(Event("sensor.temperature"))
    event_bus.wait_until_idle()
    assert handler.events == []


def test_stop_dispatches_pending_events_then_rejects_new_ones(no_thread_leaks: None) -> None:
    handler = CollectingHandler(expected=50)
    bus = EventBus(num_workers=2)
    bus.subscribe("*", handler)
    bus.start()
    for i in range(50):
        bus.publish(Event("sensor.temperature", {"seq": i}))
    bus.stop()                                        # graceful: drains the queue first

    assert len(handler.events) == 50
    with pytest.raises(queue.ShutDown):
        bus.publish(Event("sensor.temperature"))


def test_component_can_be_tested_with_a_mocked_client() -> None:
    """No bus, no threads: the component only sees an EventBusClient, so we mock it."""

    class CO2AlarmRule(EventHandler):
        def __init__(self, client: EventBusClient) -> None:
            self._client = client

        def handle(self, event: Event) -> None:
            if event.payload["ppm"] > 1000:
                self._client.write("alarm.co2", {"room": event.payload["room"]})

    client = Mock(spec=EventBusClient)                # has write/read/stop_reading, nothing else
    rule = CO2AlarmRule(client)

    rule.handle(Event("sensor.co2", {"room": "room-101", "ppm": 850}))
    rule.handle(Event("sensor.co2", {"room": "room-101", "ppm": 1250}))

    client.write.assert_called_once_with("alarm.co2", {"room": "room-101"})
