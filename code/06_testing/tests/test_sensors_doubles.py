"""
Lecture 06 - Section 4: test doubles.
  * stub  - returns canned answers              (StubDriver)
  * fake  - a working, simplified implementation (FakeClock in conftest.py)
  * mock  - records calls so the test can verify interactions (unittest.mock.Mock)
  * patch / monkeypatch - temporarily replace an object where it is looked up
"""

import time
from unittest.mock import Mock, call, patch

import pytest

from smart_gateway.alarm_rules import ThresholdRule
from smart_gateway.sensors import Reading, SensorReadError, TemperatureMonitor, TemperatureSensor


# ---------------------------------------------------------------- 1. a hand-written stub
class StubDriver:
    """Always returns the same raw value: no hardware needed."""

    def __init__(self, raw: int) -> None:
        self.raw = raw

    def read_raw(self) -> int:
        return self.raw


def test_read_converts_raw_value_with_stub(fake_clock) -> None:
    sensor = TemperatureSensor("temp-01", StubDriver(raw=931), clock=fake_clock)
    reading = sensor.read()
    assert reading == Reading("temp-01", pytest.approx(25.03, abs=0.01), fake_clock.now)


# ---------------------------------------------------------------- 2. Mock + side_effect
def test_retries_transient_errors_then_succeeds(fake_clock) -> None:
    # side_effect with a list: one item per call; exceptions are raised, values are returned
    driver = Mock()
    driver.read_raw.side_effect = [SensorReadError("CRC error"), SensorReadError("bus busy"), 931]

    reading = TemperatureSensor("temp-01", driver, clock=fake_clock, retries=2).read()

    assert reading.celsius == pytest.approx(25.03, abs=0.01)
    assert driver.read_raw.call_count == 3


def test_gives_up_after_all_retries() -> None:
    driver = Mock()
    driver.read_raw.side_effect = SensorReadError("sensor unplugged")   # raised at EVERY call

    with pytest.raises(SensorReadError, match="failed after 3 attempts") as exc_info:
        TemperatureSensor("temp-01", driver, retries=2).read()

    assert driver.read_raw.call_count == 3
    assert isinstance(exc_info.value.__cause__, SensorReadError)          # original error chained


def test_mock_with_spec_catches_typos() -> None:
    # spec=... restricts the mock to the attributes of the real interface
    driver = Mock(spec=StubDriver)
    with pytest.raises(AttributeError):
        driver.read_raw_value()                                          # typo -> error, not a silent Mock


# ---------------------------------------------------------------- 3. verifying interactions with a mock publisher
@pytest.fixture
def publisher() -> Mock:
    """Stands in for the EventBusClient of Lecture 05: records every write() call."""
    return Mock()


def make_monitor(raw_values: list[int], publisher: Mock) -> TemperatureMonitor:
    driver = Mock()
    driver.read_raw.side_effect = raw_values
    sensor = TemperatureSensor("temp-01", driver, clock=lambda: 0.0)
    return TemperatureMonitor(sensor, ThresholdRule(on_above=26.0, off_below=24.0), publisher)


def test_monitor_publishes_alarm_once_when_threshold_crossed(publisher: Mock) -> None:
    # raw 950 -> 26.56 C (alarm on), 945 -> 25.16 C (still on, hysteresis)
    monitor = make_monitor([950, 945], publisher)
    monitor.check()
    monitor.check()

    publisher.write.assert_called_once_with(
        "alarm.temperature.raised", {"sensor": "temp-01", "celsius": pytest.approx(26.56, abs=0.01)}
    )


def test_monitor_publishes_raised_then_cleared(publisher: Mock) -> None:
    monitor = make_monitor([950, 900], publisher)        # 26.56 C, then 22.53 C
    monitor.check()
    monitor.check()

    assert publisher.write.call_args_list == [
        call("alarm.temperature.raised", {"sensor": "temp-01", "celsius": pytest.approx(26.56, abs=0.01)}),
        call("alarm.temperature.cleared", {"sensor": "temp-01"}),
    ]


def test_monitor_publishes_nothing_in_normal_conditions(publisher: Mock) -> None:
    make_monitor([900], publisher).check()
    publisher.write.assert_not_called()


# ---------------------------------------------------------------- 4. patch and monkeypatch
def test_default_clock_patched_with_unittest_mock() -> None:
    # patch() takes the dotted path of the name AS SEEN BY THE CODE UNDER TEST
    # (sensors.py does `import time` and calls time.time) and restores it on exit
    with patch("smart_gateway.sensors.time.time", return_value=1234.5) as fake_time:
        sensor = TemperatureSensor("temp-01", StubDriver(931))
        assert sensor.read().timestamp == 1234.5
    fake_time.assert_called_once()


def test_default_clock_patched_with_monkeypatch(monkeypatch: pytest.MonkeyPatch) -> None:
    # monkeypatch undoes the change automatically at the end of the test
    monkeypatch.setattr(time, "time", lambda: 42.0)
    sensor = TemperatureSensor("temp-01", StubDriver(931))
    assert sensor.read().timestamp == 42.0
