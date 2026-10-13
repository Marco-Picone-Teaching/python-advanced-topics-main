<!-- omit in toc -->
# Testing in Python: Unit Tests, Test Suites, Mocks and Concurrent Code

<!-- omit in toc -->
## Lecture Information

| **Master's Degree** | Intelligent Internet of Things (D.M.270/04)                                      |
|---------------------|----------------------------------------------------------------------------------|
| **Course**          | Intelligent Internet of Things                                                   |
| **Lecture Title**   | Python Advanced Topics - Testing (`unittest`, `pytest`, mocks, concurrent code)  |
| **Author**          | Prof. Marco Picone (marco.picone@unimore.it)                                     |
| **License**         | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |

> **How this lecture is organised:** Sections 1-4 are **general**: they apply to any Python code.
> Sections 5-7 apply the same tools to the concurrent code of the previous lectures (threads, thread
> pools, asyncio, queues and the event bus).
> **Runnable code:** [`code/06_testing/`](code/06_testing/): a small library under test
> (`src/smart_gateway/`) and its test suite (`tests/`). Python 3.13+.

<!-- omit in toc -->
# Table of Contents

<!-- TOC -->
- [1 Why and What to Test](#1-why-and-what-to-test)
  - [1.1 Why Automated Tests](#11-why-automated-tests)
  - [1.2 Test Levels and the Test Pyramid](#12-test-levels-and-the-test-pyramid)
  - [1.3 Anatomy of a Test: Arrange, Act, Assert](#13-anatomy-of-a-test-arrange-act-assert)
  - [1.4 The Code Under Test](#14-the-code-under-test)
- [2 The Standard Library: `unittest`](#2-the-standard-library-unittest)
  - [2.1 `TestCase` and Assertions](#21-testcase-and-assertions)
  - [2.2 Fixtures: `setUp`, `tearDown` and Friends](#22-fixtures-setup-teardown-and-friends)
  - [2.3 Let's See How It Works: a First `unittest` Test Module](#23-lets-see-how-it-works-a-first-unittest-test-module)
  - [2.4 Test Suites and Runners](#24-test-suites-and-runners)
- [3 `pytest`: the De Facto Standard](#3-pytest-the-de-facto-standard)
  - [3.1 Plain `assert` and Rich Failure Reports](#31-plain-assert-and-rich-failure-reports)
  - [3.2 Parametrization, `approx` and `raises`](#32-parametrization-approx-and-raises)
  - [3.3 Fixtures and Dependency Injection](#33-fixtures-and-dependency-injection)
  - [3.4 Configuration, Markers and Useful Options](#34-configuration-markers-and-useful-options)
  - [3.5 `unittest` vs. `pytest`](#35-unittest-vs-pytest)
- [4 Test Doubles: Isolating the Code Under Test](#4-test-doubles-isolating-the-code-under-test)
  - [4.1 Stubs, Fakes, Mocks and Spies](#41-stubs-fakes-mocks-and-spies)
  - [4.2 Designing for Testability: Dependency Injection](#42-designing-for-testability-dependency-injection)
  - [4.3 `unittest.mock`: `Mock`, `side_effect`, `spec`, Call Assertions](#43-unittestmock-mock-side_effect-spec-call-assertions)
  - [4.4 `patch()` and `monkeypatch`](#44-patch-and-monkeypatch)
- [5 Testing Threaded Code](#5-testing-threaded-code)
  - [5.1 Why Concurrent Tests Are Hard](#51-why-concurrent-tests-are-hard)
  - [5.2 Five Rules for Deterministic Concurrent Tests](#52-five-rules-for-deterministic-concurrent-tests)
  - [5.3 Let's See How It Works: Locks, Background Threads and Thread Pools](#53-lets-see-how-it-works-locks-background-threads-and-thread-pools)
- [6 Testing Asyncio Code](#6-testing-asyncio-code)
  - [6.1 `IsolatedAsyncioTestCase` (Standard Library)](#61-isolatedasynciotestcase-standard-library)
  - [6.2 `pytest-asyncio`, `AsyncMock` and Async Fixtures](#62-pytest-asyncio-asyncmock-and-async-fixtures)
- [7 Testing Queues and the Event Bus](#7-testing-queues-and-the-event-bus)
  - [7.1 A Queue-Based Pipeline Stage](#71-a-queue-based-pipeline-stage)
  - [7.2 The Event Bus](#72-the-event-bus)
- [8 Running the Suite and Measuring Coverage](#8-running-the-suite-and-measuring-coverage)
- [9 Summary: a Testing Checklist](#9-summary-a-testing-checklist)
- [10 References](#10-references)
<!-- /TOC -->

---

# 1 Why and What to Test

## 1.1 Why Automated Tests

In the previous lectures we checked our code by **running it and reading the output**. This does not
scale:

- it must be repeated **by hand** after every change, and nobody does that consistently;
- a human reading logs misses things: a total of 4,998 instead of 5,000, a message sent twice;
- concurrent code may behave correctly **99 times out of 100**, and the one failure is the one that
  matters.

An **automated test** is code that runs other code and **checks** the result, printing nothing when
everything is fine and a precise message when it is not. A collection of tests, the **test suite**, can
be run in a second, on every change, on every machine, and in continuous integration (CI).

Tests also have a less obvious benefit: **code that is easy to test is usually well designed.** To test
a class without its real dependencies (a database, a remote service, a hardware device) you have to
pass them from outside (*dependency injection*),
which is the same decoupling principle that runs through the OOP lecture and the event-driven
architecture of Lecture 05.

## 1.2 Test Levels and the Test Pyramid

| Level | What it checks | Speed | IoT example |
|---|---|---|---|
| **Unit test** | One function or class, **in isolation** (dependencies replaced by doubles) | Milliseconds | `adc_to_celsius()`, a threshold rule, an event handler |
| **Integration test** | Several real components working **together** | Milliseconds to seconds | Sensor + rule + event bus + actuator handler |
| **System / end-to-end test** | The whole deployed system | Seconds to minutes | Real gateway, real broker, (simulated) devices |

```
            ╱╲
           ╱  ╲         few      system / end-to-end   (slow, fragile, realistic)
          ╱────╲
         ╱      ╲       some     integration
        ╱────────╲
       ╱          ╲     many     unit                  (fast, precise, cheap)
      ╱────────────╲
```

The **test pyramid** suggests many fast unit tests, fewer integration tests, and only a handful of slow
end-to-end tests. This lecture focuses on unit and integration tests.

## 1.3 Anatomy of a Test: Arrange, Act, Assert

Almost every test has three parts:

1. **Arrange:** build the objects and the input data;
2. **Act:** call the code under test, exactly once;
3. **Assert:** verify the result (return value, state change, interactions).

```python
def test_threshold_rule_activates_above_threshold():
    rule = ThresholdRule(on_above=26.0, off_below=24.0)   # Arrange
    active = rule.evaluate(26.5)                         # Act
    assert active is True                                # Assert
```

A good test is **fast**, **isolated** (it does not depend on other tests or on their order),
**repeatable** (same result every time, on every machine), **self-checking** (pass/fail, no human
reading output), and **focused** on one behaviour, with a name that says which.

## 1.4 The Code Under Test

The techniques of this lecture are general, but they need some code to test. As in the previous
lectures, the examples come from the IoT scenario.

**IoT example:** all the tests exercise a small library, `smart_gateway`, which collects in testable
form the concepts of the previous lectures:

```
code/
├── pyproject.toml                 # dev dependencies (pytest, pytest-asyncio, pytest-cov) + pytest config
└── 06_testing/
    ├── run_suite.py               # a hand-built unittest TestSuite (Section 2.4)
    ├── src/smart_gateway/
    │   ├── alarm_rules.py         # pure logic: conversions, CO2 classes, threshold with hysteresis
    │   ├── sensors.py             # sensor with injected driver/clock, monitor that publishes events
    │   ├── energy_meter.py        # lock-protected shared state            (Lecture 01)
    │   ├── sampler.py             # periodic thread with stop Event        (Lecture 01)
    │   ├── poller.py              # polling with ThreadPoolExecutor        (Lecture 02)
    │   ├── async_gateway.py       # asyncio reads with timeouts            (Lecture 03)
    │   ├── telemetry.py           # queue-based aggregation stage          (Lecture 04)
    │   └── event_bus.py           # thread-based event bus                 (Lecture 05)
    └── tests/
        ├── conftest.py                     # shared fixtures
        ├── test_alarm_rules_unittest.py    # Section 2
        ├── test_alarm_rules.py             # Section 3
        ├── test_sensors_doubles.py         # Section 4
        ├── test_threads.py                 # Section 5
        ├── test_async_gateway.py           # Section 6
        ├── test_async_unittest.py          # Section 6
        ├── test_queue_stage.py             # Section 7
        └── test_event_bus.py               # Section 7
```

The first module under test contains only **pure logic** (functions whose result depends only on
their arguments), the easiest kind of code to test. In the IoT example these are unit conversions,
an air-quality classification, and a threshold alarm with hysteresis:

`code/06_testing/src/smart_gateway/alarm_rules.py`
```python
from dataclasses import dataclass, field
from enum import Enum


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert a temperature from degrees Celsius to degrees Fahrenheit."""
    return celsius * 9 / 5 + 32


def adc_to_celsius(raw: int, adc_bits: int = 12, v_ref: float = 3.3) -> float:
    """
    Convert the raw ADC value of an analog temperature sensor (TMP36-like: 10 mV/C, 500 mV offset)
    into degrees Celsius. Raises ValueError if the raw value is outside the ADC range.
    """
    max_raw = 2 ** adc_bits - 1
    if not 0 <= raw <= max_raw:
        raise ValueError(f"raw ADC value {raw} outside [0, {max_raw}]")
    voltage = raw / max_raw * v_ref
    return (voltage - 0.5) * 100


class AirQuality(Enum):
    GOOD = "good"
    MODERATE = "moderate"
    POOR = "poor"
    HAZARDOUS = "hazardous"


def classify_co2(ppm: float) -> AirQuality:
    """Classify indoor air quality from the CO2 concentration (ppm)."""
    if ppm < 0:
        raise ValueError(f"CO2 concentration cannot be negative: {ppm}")
    if ppm < 800:
        return AirQuality.GOOD
    if ppm < 1200:
        return AirQuality.MODERATE
    if ppm < 2000:
        return AirQuality.POOR
    return AirQuality.HAZARDOUS


@dataclass
class ThresholdRule:
    """
    A threshold alarm with HYSTERESIS: the alarm turns on above `on_above` and turns off only
    when the value drops below `off_below`. This avoids alarms flapping on/off when the value
    oscillates around a single threshold.
    """
    on_above: float
    off_below: float
    active: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.off_below >= self.on_above:
            raise ValueError("off_below must be lower than on_above")

    def evaluate(self, value: float) -> bool:
        """Update the alarm state with a new value and return True if the alarm is active."""
        if not self.active and value > self.on_above:
            self.active = True
        elif self.active and value < self.off_below:
            self.active = False
        return self.active
```

---

# 2 The Standard Library: `unittest`

## 2.1 `TestCase` and Assertions

`unittest` is part of the standard library (no installation needed) and follows the **xUnit** style
of JUnit and similar frameworks:

- tests are **methods** of a class that inherits from `unittest.TestCase`;
- method names must start with **`test`**;
- checks are done with **assertion methods** of `TestCase`.

| Assertion | Checks |
|---|---|
| `assertEqual(a, b)` / `assertNotEqual` | `a == b` / `a != b` |
| `assertTrue(x)` / `assertFalse(x)` | `bool(x)` is `True` / `False` |
| `assertIs(a, b)` / `assertIsNone(x)` | `a is b` / `x is None` |
| `assertIn(a, b)` | `a in b` |
| `assertAlmostEqual(a, b, places=7)` | `round(a - b, places) == 0`, **for floats** |
| `assertRaises(Exc)` / `assertRaisesRegex(Exc, regex)` | the block raises `Exc` (with a message matching `regex`) |
| `assertDictEqual`, `assertListEqual`, ... | type-specific comparison with a detailed diff |

## 2.2 Fixtures: `setUp`, `tearDown` and Friends

A **fixture** is the context a test needs: objects, data, open resources. `unittest` provides hook
methods:

| Hook | Runs |
|---|---|
| `setUp()` / `tearDown()` | before / after **every** test method |
| `setUpClass()` / `tearDownClass()` (class methods) | once before / after **all** the tests of the class |
| `asyncSetUp()` / `asyncTearDown()` | the async versions, in `IsolatedAsyncioTestCase` (Section 6) |
| `self.addCleanup(fn)` | registers `fn` to be called after the test, even if `setUp` failed half-way |

`setUp()` runs before **each** test, so every test gets **fresh objects**: a test that modifies the rule
cannot affect the next one (**test isolation**).

## 2.3 Let's See How It Works: a First `unittest` Test Module

**IoT example:** unit tests for the functions and the class of `alarm_rules.py`.

`code/06_testing/tests/test_alarm_rules_unittest.py`
```python
import unittest

from smart_gateway.alarm_rules import (
    AirQuality,
    ThresholdRule,
    adc_to_celsius,
    celsius_to_fahrenheit,
    classify_co2,
)


class TestConversions(unittest.TestCase):
    """Unit tests for the pure conversion functions."""

    def test_freezing_point(self) -> None:
        self.assertEqual(celsius_to_fahrenheit(0), 32)

    def test_body_temperature(self) -> None:
        # floats: never compare with assertEqual, use a tolerance
        self.assertAlmostEqual(celsius_to_fahrenheit(37.0), 98.6, places=6)

    def test_adc_mid_scale(self) -> None:
        # 2048/4095 * 3.3 V = 1.650 V -> (1.650 - 0.5) * 100 = 115.0 C (approximately)
        self.assertAlmostEqual(adc_to_celsius(2048), 115.04, places=2)

    def test_adc_out_of_range_raises(self) -> None:
        with self.assertRaises(ValueError):
            adc_to_celsius(5000)

    def test_adc_error_message_mentions_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "5000"):
            adc_to_celsius(5000)


class TestCo2Classification(unittest.TestCase):

    def test_boundaries(self) -> None:
        cases = [(0, AirQuality.GOOD), (799, AirQuality.GOOD), (800, AirQuality.MODERATE),
                 (1199, AirQuality.MODERATE), (1200, AirQuality.POOR), (2000, AirQuality.HAZARDOUS)]
        for ppm, expected in cases:
            with self.subTest(ppm=ppm):          # each case is reported separately if it fails
                self.assertIs(classify_co2(ppm), expected)


class TestThresholdRule(unittest.TestCase):
    """setUp() runs before EVERY test method: each test gets a fresh rule (test isolation)."""

    def setUp(self) -> None:
        self.rule = ThresholdRule(on_above=26.0, off_below=24.0)

    def test_starts_inactive(self) -> None:
        self.assertFalse(self.rule.active)

    def test_activates_above_threshold(self) -> None:
        self.assertTrue(self.rule.evaluate(26.5))

    def test_hysteresis_keeps_alarm_on_between_thresholds(self) -> None:
        self.rule.evaluate(26.5)                 # alarm on
        self.assertTrue(self.rule.evaluate(25.0))  # still on: 25.0 is not below off_below

    def test_deactivates_below_lower_threshold(self) -> None:
        self.rule.evaluate(26.5)
        self.assertFalse(self.rule.evaluate(23.9))

    def test_invalid_configuration(self) -> None:
        with self.assertRaises(ValueError):
            ThresholdRule(on_above=20.0, off_below=22.0)


if __name__ == "__main__":
    unittest.main()
```

Run it from the `code/` folder with **test discovery**, which finds every file matching the pattern,
imports it, and runs every `TestCase`:

```bash
PYTHONPATH=06_testing/src python -m unittest discover -s 06_testing/tests -p "test_*unittest.py" -v
```

**Key Points:**
- **Test boundaries.** For any function with thresholds or ranges the interesting inputs are the
  values **around each threshold** (for `classify_co2`: 799/800, 1199/1200). Most bugs hide at the edges (`<` instead of `<=`).
- **Floats:** `0.1 + 0.2 != 0.3` in binary floating point. Always compare floats with a tolerance
  (`assertAlmostEqual`, or `pytest.approx` in Section 3).
- **`subTest`** turns a loop of checks into separately reported cases: without it, the first failing
  value would stop the loop and hide the others.
- The test names **describe behaviour** (`test_hysteresis_keeps_alarm_on_between_thresholds`): when
  one fails, its name already says what is broken.

## 2.4 Test Suites and Runners

Discovery builds the suite automatically, but you can also compose a **`TestSuite`** by hand, for
example a quick *smoke test* to run before a deployment (on the gateway, in the IoT example), and run it with a
**`TextTestRunner`**:

`code/06_testing/run_suite.py`
```python
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE / "src"))        # make `smart_gateway` importable
sys.path.insert(0, str(HERE / "tests"))      # make the test modules importable

import test_alarm_rules_unittest as rules_tests   # noqa: E402  (import after sys.path setup)
import test_async_unittest as async_tests         # noqa: E402


def smoke_suite() -> unittest.TestSuite:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(rules_tests.TestConversions("test_freezing_point"))       # a single test method
    suite.addTests(loader.loadTestsFromTestCase(rules_tests.TestThresholdRule))  # a whole class
    suite.addTests(loader.loadTestsFromModule(async_tests))                  # a whole module
    return suite


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(smoke_suite())
    sys.exit(0 if result.wasSuccessful() else 1)
```

**Output** (`python 06_testing/run_suite.py`, from `code/`):
```
test_freezing_point (test_alarm_rules_unittest.TestConversions.test_freezing_point) ... ok
test_activates_above_threshold (test_alarm_rules_unittest.TestThresholdRule.test_activates_above_threshold) ... ok
test_deactivates_below_lower_threshold (test_alarm_rules_unittest.TestThresholdRule.test_deactivates_below_lower_threshold) ... ok
test_hysteresis_keeps_alarm_on_between_thresholds (test_alarm_rules_unittest.TestThresholdRule.test_hysteresis_keeps_alarm_on_between_thresholds) ... ok
test_invalid_configuration (test_alarm_rules_unittest.TestThresholdRule.test_invalid_configuration) ... ok
test_starts_inactive (test_alarm_rules_unittest.TestThresholdRule.test_starts_inactive) ... ok
test_read_all (test_async_unittest.TestAsyncGateway.test_read_all) ... ok
test_timeout_returns_none (test_async_unittest.TestAsyncGateway.test_timeout_returns_none) ... ok
test_value_is_returned (test_async_unittest.TestAsyncGateway.test_value_is_returned) ... ok

----------------------------------------------------------------------
Ran 9 tests in 0.016s

OK
```

The exit code (`0` on success, `1` on failure) is what CI systems and deployment scripts look at.

---

# 3 `pytest`: the De Facto Standard

## 3.1 Plain `assert` and Rich Failure Reports

**pytest** is a third-party framework (`pip install pytest`, or `uv add --dev pytest`) that has become
the standard in the Python ecosystem. Its main ideas:

- a test is a **plain function** whose name starts with `test_`, in a file named `test_*.py` or
  `*_test.py`; classes are optional;
- checks use the **plain `assert` statement**. pytest rewrites assertions at import time to show the
  values involved when they fail;
- fixtures, parametrization and a large plugin ecosystem (`pytest-asyncio`, `pytest-cov`,
  `pytest-xdist`, `pytest-timeout`...);
- it also **runs `unittest` tests unchanged**, so you can migrate gradually.

This is what a failing assertion looks like (a deliberately wrong expected value):

```python
def test_conversion_table() -> None:
    table = {c: celsius_to_fahrenheit(c) for c in (0, 10, 20)}
    assert table == {0: 32, 10: 50, 20: 66}
```

```
    def test_conversion_table() -> None:
        table = {c: celsius_to_fahrenheit(c) for c in (0, 10, 20)}
>       assert table == {0: 32, 10: 50, 20: 66}
E       assert {0: 32.0, 10: 50.0, 20: 68.0} == {0: 32, 10: 50, 20: 66}
E
E         Omitting 2 identical items, use -vv to show
E         Differing items:
E         {20: 68.0} != {20: 66}
```

pytest shows **which item** differs, with no `assertDictEqual` needed.

## 3.2 Parametrization, `approx` and `raises`

**IoT example:** the `alarm_rules.py` tests rewritten with pytest.

`code/06_testing/tests/test_alarm_rules.py`
```python
import pytest

from smart_gateway.alarm_rules import (
    AirQuality,
    ThresholdRule,
    adc_to_celsius,
    celsius_to_fahrenheit,
    classify_co2,
)


def test_freezing_point() -> None:
    assert celsius_to_fahrenheit(0) == 32


def test_body_temperature() -> None:
    assert celsius_to_fahrenheit(37.0) == pytest.approx(98.6)


@pytest.mark.parametrize(
    ("ppm", "expected"),
    [
        (0, AirQuality.GOOD),
        (799, AirQuality.GOOD),
        (800, AirQuality.MODERATE),
        (1199, AirQuality.MODERATE),
        (1200, AirQuality.POOR),
        (1999, AirQuality.POOR),
        (2000, AirQuality.HAZARDOUS),
    ],
)
def test_classify_co2_boundaries(ppm: int, expected: AirQuality) -> None:
    assert classify_co2(ppm) is expected


@pytest.mark.parametrize("raw", [-1, 4096, 10_000])
def test_adc_out_of_range(raw: int) -> None:
    with pytest.raises(ValueError, match="outside"):
        adc_to_celsius(raw)


def test_adc_full_scale_on_10_bit_adc() -> None:
    assert adc_to_celsius(1023, adc_bits=10) == pytest.approx(280.0)


@pytest.fixture
def rule() -> ThresholdRule:
    """A fresh rule for every test that asks for it (the pytest equivalent of setUp)."""
    return ThresholdRule(on_above=26.0, off_below=24.0)


@pytest.mark.parametrize(
    ("values", "expected_states"),
    [
        ([25.0, 26.5, 25.0, 23.5], [False, True, True, False]),     # full on/off cycle
        ([26.1, 25.9, 26.1, 25.9], [True, True, True, True]),       # oscillation: no flapping
        ([23.0, 24.5, 25.9], [False, False, False]),                # never above on_above
    ],
    ids=["on-off-cycle", "no-flapping", "never-triggered"],
)
def test_threshold_rule_sequences(rule: ThresholdRule, values: list[float],
                                  expected_states: list[bool]) -> None:
    assert [rule.evaluate(v) for v in values] == expected_states


def test_invalid_rule_configuration() -> None:
    with pytest.raises(ValueError, match="off_below"):
        ThresholdRule(on_above=20.0, off_below=22.0)
```

**Output** (`uv run pytest -v 06_testing/tests/test_alarm_rules.py`, from `code/`):
```
06_testing/tests/test_alarm_rules.py::test_freezing_point PASSED         [  5%]
06_testing/tests/test_alarm_rules.py::test_body_temperature PASSED       [ 11%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[0-AirQuality.GOOD] PASSED [ 17%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[799-AirQuality.GOOD] PASSED [ 23%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[800-AirQuality.MODERATE] PASSED [ 29%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[1199-AirQuality.MODERATE] PASSED [ 35%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[1200-AirQuality.POOR] PASSED [ 41%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[1999-AirQuality.POOR] PASSED [ 47%]
06_testing/tests/test_alarm_rules.py::test_classify_co2_boundaries[2000-AirQuality.HAZARDOUS] PASSED [ 52%]
06_testing/tests/test_alarm_rules.py::test_adc_out_of_range[-1] PASSED   [ 58%]
06_testing/tests/test_alarm_rules.py::test_adc_out_of_range[4096] PASSED [ 64%]
06_testing/tests/test_alarm_rules.py::test_adc_out_of_range[10000] PASSED [ 70%]
06_testing/tests/test_alarm_rules.py::test_adc_full_scale_on_10_bit_adc PASSED [ 76%]
06_testing/tests/test_alarm_rules.py::test_threshold_rule_sequences[on-off-cycle] PASSED [ 82%]
06_testing/tests/test_alarm_rules.py::test_threshold_rule_sequences[no-flapping] PASSED [ 88%]
06_testing/tests/test_alarm_rules.py::test_threshold_rule_sequences[never-triggered] PASSED [ 94%]
06_testing/tests/test_alarm_rules.py::test_invalid_rule_configuration PASSED [100%]

============================== 17 passed in 0.01s ==============================
```

**Key Points:**
- **`@pytest.mark.parametrize`** runs the same test function once per row of data. Each row is a
  **separate test** with its own id in the report (`[1200-AirQuality.POOR]`), and `ids=` gives
  readable names. 7 lines of data replace 7 almost identical functions.
- **`pytest.approx`** compares floats with a sensible default tolerance, also inside lists and dicts.
- **`pytest.raises(Exc, match=regex)`** checks both the exception type and its message; used as
  `with ... as exc_info`, it also lets you inspect the exception object (Section 4).
- The parametrized sequences test the **behaviour over time** of the stateful rule: the "no-flapping"
  case documents *why* hysteresis exists better than any comment could.

## 3.3 Fixtures and Dependency Injection

A pytest **fixture** is a function decorated with `@pytest.fixture` that prepares something for a
test. A test **requests** a fixture simply by having a **parameter with the same name**: pytest calls
the fixture and passes the result (this is dependency injection). The `rule` fixture above is the
simplest example.

With **`yield`**, a fixture can also **clean up**: the code after `yield` runs when the test is
finished, **even if it failed**:

```python
@pytest.fixture
def event_bus(no_thread_leaks: None) -> Iterator[EventBus]:
    bus = EventBus(num_workers=2)
    bus.start()          # setup
    yield bus            # the test runs here, with `bus` as its argument
    bus.stop()           # teardown: always executed
```

Fixtures can **depend on other fixtures** (`event_bus` requests `no_thread_leaks`), have a **scope**
(`function`, the default: recreated for every test; `class`, `module`, `session`: shared), and when
defined in a **`conftest.py`** file they are available to every test in that directory, without imports.
pytest also provides many built-in fixtures: `tmp_path` (a temporary directory), `monkeypatch`,
`caplog` (captured log records), `capsys` (captured output)...

The `conftest.py` of the example suite defines a fake clock, a fixture that detects leaked threads (used
in Section 5), and a started event bus that is always stopped at the end of the test:

`code/06_testing/tests/conftest.py`
```python
import threading
import time
from collections.abc import Iterator

import pytest

from smart_gateway.event_bus import EventBus


class FakeClock:
    """A controllable clock: tests decide what 'now' is, instead of depending on real time."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def no_thread_leaks() -> Iterator[None]:
    """Fail the test if it leaves threads running after it has finished."""
    before = set(threading.enumerate())
    yield                                                    # <- the test runs here
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:                       # give stopping threads a moment
        leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
        if not leaked:
            return
        time.sleep(0.01)
    pytest.fail(f"threads leaked by the test: {[t.name for t in leaked]}")


@pytest.fixture
def event_bus(no_thread_leaks: None) -> Iterator[EventBus]:
    """A started EventBus, always stopped at the end of the test (even if the test fails)."""
    bus = EventBus(num_workers=2)
    bus.start()
    yield bus
    bus.stop()
```

## 3.4 Configuration, Markers and Useful Options

The pytest configuration lives in `pyproject.toml`:

`code/pyproject.toml` (excerpt)
```toml
[dependency-groups]
dev = ["pytest>=8.3", "pytest-asyncio>=0.25", "pytest-cov>=6.0"]

[tool.pytest.ini_options]
testpaths = ["06_testing/tests"]
pythonpath = ["06_testing/src"]   # makes `import smart_gateway` work inside the tests
asyncio_mode = "strict"            # async tests must be marked with @pytest.mark.asyncio
markers = ["slow: tests that take more than a fraction of a second"]
```

| Command | Effect |
|---|---|
| `uv run pytest` (or `python -m pytest`) | Run the whole suite |
| `pytest -v` / `-q` | Verbose / quiet output |
| `pytest tests/test_event_bus.py::test_wildcard_subscriptions` | Run a single test |
| `pytest -k "threshold and not invalid"` | Run the tests whose names match an expression |
| `pytest -m "not slow"` | Skip the tests marked with `@pytest.mark.slow` |
| `pytest -x` / `--lf` | Stop at the first failure / rerun only the last failures |
| `pytest --cov=smart_gateway --cov-report=term-missing` | Coverage report (Section 8) |

## 3.5 `unittest` vs. `pytest`

| | `unittest` | `pytest` |
|---|---|---|
| Installation | Standard library | Third-party package |
| Test structure | Methods of `TestCase` subclasses | Plain functions (classes optional) |
| Assertions | `self.assertEqual(...)` and ~30 other methods | Plain `assert` with introspection |
| Fixtures | `setUp`/`tearDown`, `setUpClass`... | `@pytest.fixture`, injection by name, scopes, `yield` teardown, `conftest.py` |
| Parametrization | `subTest` (inside one test) | `@pytest.mark.parametrize` (separate tests) |
| Async tests | `IsolatedAsyncioTestCase` | `pytest-asyncio` plugin |
| Plugins | Few | Hundreds (coverage, parallel runs, timeouts...) |
| Compatibility | Runs only `unittest` tests | Runs both `pytest` and `unittest` tests |

**Recommendation:** use **pytest** for new projects. Know `unittest`: it is always available, it is
used in many existing code bases, and its `unittest.mock` module (next section) is what everybody uses,
pytest users included.

---

# 4 Test Doubles: Isolating the Code Under Test

## 4.1 Stubs, Fakes, Mocks and Spies

A unit test must not depend on slow, unpredictable or unavailable things: the network, a database,
the current time, a running message bus, or real hardware.
The solution is to replace these dependencies with **test doubles** (like a stunt double in a film):

| Double | What it does | Example in this lecture |
|---|---|---|
| **Stub** | Returns canned answers | `StubDriver` always returns the same raw sensor value |
| **Fake** | A simplified but working implementation | `FakeClock`, whose time the test controls |
| **Mock** | Records how it is called, so the test can **verify interactions** | A `Mock()` publisher: "was the alarm event published exactly once?" |
| **Spy** | Wraps a real object and records calls to it | `Mock(wraps=real_object)` |

## 4.2 Designing for Testability: Dependency Injection

You can only substitute a dependency if the code lets you: the dependency must be **passed in**
(to the constructor or to the function), not created inside.

**IoT example:** compare two versions of a temperature sensor class:

```python
# Hard to test: the driver and the clock are created INSIDE the class
class TemperatureSensor:
    def __init__(self, sensor_id):
        self._driver = I2CDriver(bus=1, address=0x48)   # real hardware, always
    def read(self):
        return Reading(self.sensor_id, adc_to_celsius(self._driver.read_raw()), time.time())

# Easy to test: dependencies are PASSED IN (dependency injection)
class TemperatureSensor:
    def __init__(self, sensor_id, driver, clock=None): ...
```

`code/06_testing/src/smart_gateway/sensors.py`
```python
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from smart_gateway.alarm_rules import ThresholdRule, adc_to_celsius


class SensorReadError(Exception):
    """The driver could not read the sensor (bus error, CRC error, timeout...)."""


class SensorDriver(Protocol):
    """Low-level access to the hardware: returns the raw ADC value."""

    def read_raw(self) -> int: ...


class Publisher(Protocol):
    """Anything that can publish an event, e.g. the EventBusClient of Lecture 05."""

    def write(self, event_type: str, payload: dict[str, Any] | None = None) -> None: ...


@dataclass(frozen=True)
class Reading:
    sensor_id: str
    celsius: float
    timestamp: float


class TemperatureSensor:
    """Reads a temperature through a driver, retrying transient errors."""

    def __init__(self, sensor_id: str, driver: SensorDriver,
                 clock: Callable[[], float] | None = None, retries: int = 2) -> None:
        self.sensor_id = sensor_id
        self._driver = driver
        # NOT `clock=time.time` as default argument: defaults are evaluated once, when the
        # function is defined, so a later patch of time.time would never be seen.
        self._clock = clock if clock is not None else time.time
        self._retries = retries

    def read(self) -> Reading:
        last_error: SensorReadError | None = None
        for _ in range(1 + self._retries):
            try:
                raw = self._driver.read_raw()
            except SensorReadError as exc:
                last_error = exc                     # transient error: try again
                continue
            return Reading(self.sensor_id, round(adc_to_celsius(raw), 2), self._clock())
        raise SensorReadError(f"{self.sensor_id}: failed after {1 + self._retries} attempts") from last_error


class TemperatureMonitor:
    """Reads a sensor, evaluates a threshold rule and publishes alarm events on state changes."""

    def __init__(self, sensor: TemperatureSensor, rule: ThresholdRule, publisher: Publisher) -> None:
        self._sensor = sensor
        self._rule = rule
        self._publisher = publisher

    def check(self) -> Reading:
        reading = self._sensor.read()
        was_active = self._rule.active
        is_active = self._rule.evaluate(reading.celsius)
        if is_active and not was_active:
            self._publisher.write("alarm.temperature.raised",
                                  {"sensor": reading.sensor_id, "celsius": reading.celsius})
        elif was_active and not is_active:
            self._publisher.write("alarm.temperature.cleared", {"sensor": reading.sensor_id})
        return reading
```

The dependencies are described with **`Protocol`s** (structural typing, *duck typing* with type hints):
anything with a `read_raw()` method is a valid driver, anything with a `write()` method is a valid
publisher, the real `EventBusClient` of Lecture 05 included.

## 4.3 `unittest.mock`: `Mock`, `side_effect`, `spec`, Call Assertions

`unittest.mock.Mock` creates objects that accept **any** attribute access and **any** call, and
**record** everything:

| Feature | Usage |
|---|---|
| `m.method.return_value = x` | Every call returns `x` |
| `m.method.side_effect = [a, Exc(), b]` | Successive calls return `a`, raise `Exc`, return `b` |
| `m.method.side_effect = Exc("...")` | Every call raises |
| `m.method.assert_called_once_with(args)` | Verify exactly one call with those arguments |
| `m.method.assert_not_called()` | Verify it was never called |
| `m.method.call_count`, `m.method.call_args_list` | Inspect the recorded calls |
| `Mock(spec=SomeClass)` | Only the attributes of `SomeClass` exist: typos raise `AttributeError` |
| `AsyncMock()` | Like `Mock`, but calling it returns an awaitable (Section 6) |

**IoT example:** tests for `TemperatureSensor` (with a stub driver, a scripted mock driver that fails
and recovers, a fake clock) and for `TemperatureMonitor` (with a mock publisher):

`code/06_testing/tests/test_sensors_doubles.py`
```python
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
```

**Key Points:**
- The **stub** and the **fake clock** make `read()` fully deterministic: no real dependency, no real time.
- `side_effect` with a list **scripts a scenario** (two transient failures, then success) that would
  be almost impossible to reproduce with the real dependency (here, a real sensor).
- `exc_info.value.__cause__` verifies that the original error is **chained** (`raise ... from ...`),
  so the root cause is not lost in the logs.
- The monitor tests verify **interactions** ("an event was published exactly once, with this
  payload"), not return values. This is what mocks are for.
- **Don't over-mock.** Mock the *boundaries* of your system (network, database, time, message bus, hardware), not
  every internal class. A test that mocks everything tests nothing but the mocks.

## 4.4 `patch()` and `monkeypatch`

When a dependency is **not** injected, it can still be replaced temporarily:

- `unittest.mock.patch("package.module.name", ...)`, as a context manager or decorator; the original
  is restored on exit;
- pytest's **`monkeypatch`** fixture (`monkeypatch.setattr(obj, "name", value)`,
  `monkeypatch.setenv(...)`), undone automatically at the end of the test.

```python
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
```

> **Note:** *patch where the name is looked up, not where it is defined.* If a module does
> `from time import time` and calls `time()`, you must patch `that_module.time`, because patching
> `time.time` no longer affects the name it has already imported. A related trap, which appeared while
> writing this lecture's code: a default argument like `clock=time.time` is evaluated **once, when the
> function is defined**, so no later patch can affect it. That is why `TemperatureSensor` uses
> `clock=None` and resolves the default inside `__init__`. In general, **prefer injection over
> patching**: it makes dependencies explicit and tests simpler.

---

# 5 Testing Threaded Code

## 5.1 Why Concurrent Tests Are Hard

Concurrent code adds three new problems to testing:

- **Non-determinism:** the interleaving of threads changes at every run. A race condition may show up
  once in a thousand runs.
- **Asynchronous results:** when a function starts a thread and returns, the result **does not exist
  yet**. When should the test check it?
- **Hanging:** a deadlock or a missing signal makes a test **wait forever** instead of failing.

The most common, and wrong, answer to the second problem is `time.sleep()`:

```python
# ANTI-PATTERN: sleep and hope
def test_sampler_flaky():
    samples = []
    sampler = PeriodicSampler(read=lambda: 21.0, on_sample=samples.append, period_s=0.01)
    sampler.start()
    time.sleep(0.05)              # "surely 3 samples are there by now..."
    sampler.stop()
    assert len(samples) >= 3      # passes on your laptop, fails on a loaded CI machine
```

This test is **slow** (it always waits the full sleep) and **flaky** (on a busy machine 50 ms may not be
enough). A flaky test is worse than no test: people learn to ignore failures.

## 5.2 Five Rules for Deterministic Concurrent Tests

1. **Wait for a signal, not for time.** Have the code under test (or a test double) **signal** progress
   through a `threading.Event`, a `queue.Queue`, or `join()`, and wait on that.
2. **Every wait has a timeout** (`event.wait(2.0)`, `q.get(timeout=2.0)`, `t.join(2.0)`), and the test
   **asserts** on the result. A bug then produces a clear failure after 2 s instead of a frozen test run.
   The timeout is an *upper bound* that is reached only when something is broken; normally the wait
   ends in microseconds.
3. **Provoke races on purpose.** Use a `threading.Barrier` so that all threads start at the same
   instant, and many iterations, to maximise the chance of interleaving.
4. **Control blocking dependencies.** A fake that blocks on an `Event` controlled by the test simulates
   a slow dependency (e.g. a device that never answers) **without** actually waiting.
5. **Check the cleanup.** Every thread started by a test must be stopped by the test. A fixture can
   verify it (`no_thread_leaks` in `conftest.py`).

## 5.3 Let's See How It Works: Locks, Background Threads and Thread Pools

**IoT example:** the code under test comes from Lectures 01 and 02: a lock-protected energy meter, a
periodic sampling thread, and a function that polls many sensors through a thread pool.

`code/06_testing/src/smart_gateway/energy_meter.py`
```python
import threading
import time


class EnergyMeter:
    """Thread-safe accumulator of the energy reported by smart plugs."""

    def __init__(self) -> None:
        self._total_wh = 0
        self._lock = threading.Lock()

    def add(self, energy_wh: int) -> None:
        if energy_wh < 0:
            raise ValueError("energy cannot be negative")
        with self._lock:
            current = self._total_wh
            time.sleep(0)                     # widen the race window: makes a missing lock obvious
            self._total_wh = current + energy_wh

    @property
    def total_wh(self) -> int:
        with self._lock:
            return self._total_wh
```

`code/06_testing/src/smart_gateway/sampler.py`
```python
import threading
from collections.abc import Callable


class PeriodicSampler(threading.Thread):
    """Calls `read()` every `period_s` seconds and passes the value to `on_sample()`."""

    def __init__(self, read: Callable[[], float], on_sample: Callable[[float], None],
                 period_s: float, name: str = "sampler") -> None:
        super().__init__(name=name)
        self._read = read
        self._on_sample = on_sample
        self._period_s = period_s
        self._stop_event = threading.Event()
        self.errors = 0

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._on_sample(self._read())
            except Exception:
                self.errors += 1                 # a failing read must not kill the sampler
            self._stop_event.wait(self._period_s)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        self.join(timeout)
```

`code/06_testing/src/smart_gateway/poller.py`
```python
from collections.abc import Callable, Mapping
from concurrent.futures import Executor, Future, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass, field


@dataclass
class CycleReport:
    readings: dict[str, float] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


def poll_all(readers: Mapping[str, Callable[[], float]], executor: Executor,
             deadline_s: float) -> CycleReport:
    """
    Run every reader concurrently on `executor` and collect the results within `deadline_s`.
    Failed readers are reported as "error: <message>", readers that miss the deadline as "timeout".
    """
    report = CycleReport()
    futures: dict[Future[float], str] = {executor.submit(read): sid for sid, read in readers.items()}
    try:
        for future in as_completed(futures, timeout=deadline_s):
            sensor_id = futures[future]
            try:
                report.readings[sensor_id] = future.result()
            except Exception as exc:
                report.errors[sensor_id] = f"error: {exc}"
    except FuturesTimeoutError:
        for future, sensor_id in futures.items():
            if not future.done():
                future.cancel()
                report.errors[sensor_id] = "timeout"
    return report
```

And the tests:

`code/06_testing/tests/test_threads.py`
```python
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from smart_gateway.energy_meter import EnergyMeter
from smart_gateway.poller import poll_all
from smart_gateway.sampler import PeriodicSampler

TIMEOUT_S = 2.0          # generous upper bound: reached only if something is broken


# ---------------------------------------------------------------- shared state + lock
def test_energy_meter_is_thread_safe(no_thread_leaks: None) -> None:
    meter = EnergyMeter()
    num_threads, reports = 8, 500
    start_line = threading.Barrier(num_threads)      # releases all threads together

    def plug() -> None:
        start_line.wait()                             # maximise contention
        for _ in range(reports):
            meter.add(1)

    threads = [threading.Thread(target=plug) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(TIMEOUT_S)
        assert not t.is_alive(), "worker thread did not finish in time"

    assert meter.total_wh == num_threads * reports


def test_energy_meter_rejects_negative_values() -> None:
    with pytest.raises(ValueError):
        EnergyMeter().add(-5)


# ---------------------------------------------------------------- a background thread
def test_sampler_delivers_samples_and_stops(no_thread_leaks: None) -> None:
    samples: queue.Queue[float] = queue.Queue()
    values = iter([21.0, 21.5, 22.0])
    sampler = PeriodicSampler(read=lambda: next(values), on_sample=samples.put, period_s=0.01)

    sampler.start()
    # wait for exactly 3 samples, each with a timeout: no sleep, no guessing
    received = [samples.get(timeout=TIMEOUT_S) for _ in range(3)]
    sampler.stop()

    assert received == [21.0, 21.5, 22.0]
    assert not sampler.is_alive()


def test_sampler_survives_failing_reads(no_thread_leaks: None) -> None:
    second_success = threading.Event()
    calls = {"n": 0}

    def flaky_read() -> float:
        calls["n"] += 1
        if calls["n"] % 2:                            # odd calls fail
            raise OSError("I2C bus error")
        return 20.0

    def on_sample(value: float) -> None:
        if calls["n"] >= 4:
            second_success.set()

    sampler = PeriodicSampler(read=flaky_read, on_sample=on_sample, period_s=0.001)
    sampler.start()
    assert second_success.wait(TIMEOUT_S), "sampler stopped producing samples"
    sampler.stop()
    assert sampler.errors >= 2


def test_sampler_stops_immediately_even_with_long_period(no_thread_leaks: None) -> None:
    first_sample = threading.Event()
    sampler = PeriodicSampler(read=lambda: 1.0, on_sample=lambda v: first_sample.set(), period_s=60)
    sampler.start()
    assert first_sample.wait(TIMEOUT_S)
    sampler.stop(timeout=1.0)                         # must not wait for the 60 s period
    assert not sampler.is_alive()


# ---------------------------------------------------------------- thread pool + futures
@pytest.fixture
def executor(no_thread_leaks: None):
    with ThreadPoolExecutor(max_workers=4) as pool:
        yield pool


def test_poll_all_collects_values_and_errors(executor: ThreadPoolExecutor) -> None:
    def broken() -> float:
        raise ConnectionError("device offline")

    report = poll_all({"temp-01": lambda: 21.5, "hum-01": lambda: 48.0, "co2-01": broken},
                      executor, deadline_s=TIMEOUT_S)

    assert report.readings == {"temp-01": 21.5, "hum-01": 48.0}
    assert report.errors == {"co2-01": "error: device offline"}


def test_poll_all_reports_timeout_without_real_waiting(executor: ThreadPoolExecutor) -> None:
    release = threading.Event()

    def stuck_sensor() -> float:
        release.wait(TIMEOUT_S)                       # blocks until the test releases it
        return 0.0

    try:
        report = poll_all({"temp-01": lambda: 21.5, "slow-01": stuck_sensor}, executor, deadline_s=0.05)
    finally:
        release.set()                                 # unblock the worker so the pool can shut down

    assert report.readings == {"temp-01": 21.5}
    assert report.errors == {"slow-01": "timeout"}
```

**Key Points:**
- **Does the race test actually catch races?** A test is only useful if it **fails when the code is
  wrong**. Removing the lock from the meter (an `UnsafeEnergyMeter` identical but without `with
  self._lock:`) makes the same test fail on every run:

  ```
  >       assert meter.total_wh == 8 * 500
  E       assert 500 == (8 * 500)
  ```

  Always check that a new test can fail, for example by temporarily breaking the code.
- `samples.get(timeout=...)` is the answer to "*when should I check?*": exactly when the third sample
  **arrives**, not a guessed number of milliseconds later. The test typically ends in about 20 ms.
- `test_sampler_stops_immediately_even_with_long_period` verifies a **non-functional property** (the
  prompt shutdown of Lecture 01): with `time.sleep(60)` instead of `stop_event.wait(60)` in the sampler,
  the thread would still be alive and the test would fail.
- The timeout test uses a **fake that blocks on an `Event`**: it simulates a sensor that never
  answers, yet the test takes only ~50 ms. The `finally: release.set()` makes sure the stuck worker can
  finish, so the pool shuts down and no thread leaks.

---

# 6 Testing Asyncio Code

## 6.1 `IsolatedAsyncioTestCase` (Standard Library)

A coroutine cannot be called like a normal function in a test: it must run inside an event loop.
`unittest.IsolatedAsyncioTestCase` creates a **fresh event loop for each test**, and lets test methods
(and `asyncSetUp`/`asyncTearDown`) be `async def`.

**IoT example:** the code under test comes from Lecture 03: reading sensors with a timeout, reading
many of them concurrently, and sampling one periodically.

`code/06_testing/src/smart_gateway/async_gateway.py`
```python
import asyncio
from collections.abc import Awaitable, Callable, Mapping

AsyncReader = Callable[[], Awaitable[float]]


async def read_with_timeout(read: AsyncReader, timeout_s: float) -> float | None:
    """Return the value read, or None if the sensor does not answer within timeout_s."""
    try:
        async with asyncio.timeout(timeout_s):
            return await read()
    except TimeoutError:
        return None


async def read_all(readers: Mapping[str, AsyncReader], timeout_s: float) -> dict[str, float | None]:
    """Read every sensor concurrently; a slow sensor yields None instead of delaying the others."""
    async with asyncio.TaskGroup() as tg:
        tasks = {sid: tg.create_task(read_with_timeout(read, timeout_s)) for sid, read in readers.items()}
    return {sid: task.result() for sid, task in tasks.items()}


async def sample_forever(read: AsyncReader, publish: Callable[[float], Awaitable[None]],
                         period_s: float) -> None:
    """Read and publish every period_s seconds, until the task is cancelled."""
    while True:
        await publish(await read())
        await asyncio.sleep(period_s)
```

`code/06_testing/tests/test_async_unittest.py`
```python
import asyncio
import unittest
from unittest.mock import AsyncMock

from smart_gateway.async_gateway import read_all, read_with_timeout


class TestAsyncGateway(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        # runs inside the test's event loop, before every test
        self.fast_sensor = AsyncMock(return_value=21.5)

    async def test_value_is_returned(self) -> None:
        self.assertEqual(await read_with_timeout(self.fast_sensor, timeout_s=1.0), 21.5)

    async def test_timeout_returns_none(self) -> None:
        async def hanging() -> float:
            await asyncio.sleep(10)
            return 0.0

        self.assertIsNone(await read_with_timeout(hanging, timeout_s=0.01))

    async def test_read_all(self) -> None:
        result = await read_all({"temp-01": self.fast_sensor}, timeout_s=1.0)
        self.assertDictEqual(result, {"temp-01": 21.5})
        self.fast_sensor.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
```

## 6.2 `pytest-asyncio`, `AsyncMock` and Async Fixtures

With pytest, the **`pytest-asyncio`** plugin runs `async def` tests marked with
**`@pytest.mark.asyncio`** (in `strict` mode, set in `pyproject.toml`, the marker is required;
`auto` mode treats every `async def` test as asyncio). Async fixtures are declared with
**`@pytest_asyncio.fixture`**.

**`AsyncMock`** is the async version of `Mock`: calling it returns a coroutine, so it can be awaited,
and it offers `assert_awaited_once()`, `assert_awaited_with(...)`, `await_count`...

`code/06_testing/tests/test_async_gateway.py`
```python
import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from smart_gateway.async_gateway import read_all, read_with_timeout, sample_forever


async def never_answers() -> float:
    """A sensor that hangs forever (until cancelled)."""
    await asyncio.Event().wait()
    return 0.0


@pytest.mark.asyncio
async def test_read_with_timeout_returns_value() -> None:
    read = AsyncMock(return_value=21.5)
    assert await read_with_timeout(read, timeout_s=1.0) == 21.5
    read.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_with_timeout_returns_none_on_timeout() -> None:
    assert await read_with_timeout(never_answers, timeout_s=0.01) is None


@pytest.mark.asyncio
async def test_timed_out_read_is_really_cancelled() -> None:
    cancelled = asyncio.Event()

    async def slow_sensor() -> float:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()                  # the sensor must be able to clean up
            raise
        return 0.0

    assert await read_with_timeout(slow_sensor, timeout_s=0.01) is None
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_read_all_slow_sensor_does_not_delay_the_others() -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    result = await read_all(
        {"temp-01": AsyncMock(return_value=21.5), "hum-01": AsyncMock(return_value=48.0),
         "co2-01": never_answers},
        timeout_s=0.05,
    )
    assert result == {"temp-01": 21.5, "hum-01": 48.0, "co2-01": None}
    assert loop.time() - start < 0.5          # bounded by the timeout, not by the slow sensor


@pytest.mark.asyncio
async def test_read_all_propagates_unexpected_errors() -> None:
    failing = AsyncMock(side_effect=ConnectionError("broker unreachable"))
    with pytest.raises(ExceptionGroup) as exc_info:           # raised by the TaskGroup
        await read_all({"temp-01": failing, "co2-01": never_answers}, timeout_s=1.0)
    assert exc_info.group_contains(ConnectionError)


@pytest.mark.asyncio
async def test_sample_forever_publishes_until_cancelled() -> None:
    received: asyncio.Queue[float] = asyncio.Queue()
    read = AsyncMock(side_effect=[20.0, 20.5, 21.0, 21.5])

    task = asyncio.create_task(sample_forever(read, publish=received.put, period_s=0))
    samples = [await asyncio.wait_for(received.get(), timeout=1.0) for _ in range(3)]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert samples == [20.0, 20.5, 21.0]


@pytest_asyncio.fixture          # async fixtures need pytest_asyncio.fixture in "strict" mode
async def running_sampler() -> AsyncIterator[list[float]]:
    """An ASYNC fixture: starts a background sampling task and cancels it at teardown."""
    published: list[float] = []

    async def publish(value: float) -> None:
        published.append(value)

    task = asyncio.create_task(sample_forever(AsyncMock(return_value=22.0), publish, period_s=0.001))
    yield published
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_async_fixture(running_sampler: list[float]) -> None:
    async with asyncio.timeout(1.0):
        while len(running_sampler) < 3:
            await asyncio.sleep(0.001)
    assert set(running_sampler) == {22.0}
```

**Key Points:**
- The same rules of Section 5 apply: **wait for signals** (`await received.get()`), and **bound every
  wait** (`asyncio.wait_for(..., timeout=1.0)`, `asyncio.timeout(1.0)`).
- A sensor that "never answers" is simply `await asyncio.Event().wait()`: no real 10-second sleeps
  needed, and the timeout test takes 10 ms.
- `test_timed_out_read_is_really_cancelled` checks something that was **impossible** with threads:
  the timed-out operation is actually cancelled and can clean up.
- `pytest.raises(ExceptionGroup)` plus `exc_info.group_contains(ConnectionError)` (pytest 8+) test the
  structured error reporting of a `TaskGroup` (Lecture 03, Section 4).
- Background tasks started by a test (or by a fixture) must be **cancelled and awaited**, the asyncio
  equivalent of joining threads.

---

# 7 Testing Queues and the Event Bus

## 7.1 A Queue-Based Pipeline Stage

A stage that reads from one queue and writes to another (Lecture 04) is very easy to test in
isolation: the **test plays both neighbours**, feeding the input queue and draining the output queue.

**IoT example:** an aggregation stage that turns sensor readings into window averages.

`code/06_testing/src/smart_gateway/telemetry.py`
```python
import queue


def aggregation_stage(in_q: queue.Queue[float], out_q: queue.Queue[float], window: int) -> None:
    """
    Consume readings from in_q and put one average every `window` readings on out_q.
    When in_q is shut down (Python 3.13+), flush the partial window and shut down out_q.
    """
    if window <= 0:
        raise ValueError("window must be positive")
    buffer: list[float] = []
    while True:
        try:
            value = in_q.get()
        except queue.ShutDown:
            break
        buffer.append(value)
        if len(buffer) == window:
            out_q.put(sum(buffer) / window)
            buffer.clear()
        in_q.task_done()
    if buffer:
        out_q.put(sum(buffer) / len(buffer))
    out_q.shutdown()
```

`code/06_testing/tests/test_queue_stage.py`
```python
import queue
import threading

import pytest

from smart_gateway.telemetry import aggregation_stage

TIMEOUT_S = 2.0


def drain(q: queue.Queue[float]) -> list[float]:
    """Read everything from a queue until it is shut down (each get() with a timeout)."""
    items = []
    while True:
        try:
            items.append(q.get(timeout=TIMEOUT_S))
        except queue.ShutDown:
            return items


def run_stage(values: list[float], window: int) -> list[float]:
    in_q: queue.Queue[float] = queue.Queue()
    out_q: queue.Queue[float] = queue.Queue()
    worker = threading.Thread(target=aggregation_stage, args=(in_q, out_q, window))
    worker.start()
    for v in values:
        in_q.put(v)
    in_q.shutdown()                                   # "no more data": the stage must flush and stop
    result = drain(out_q)
    worker.join(TIMEOUT_S)
    assert not worker.is_alive()
    return result


def test_averages_every_window(no_thread_leaks: None) -> None:
    assert run_stage([20.0, 22.0, 24.0, 26.0], window=2) == [21.0, 25.0]


def test_flushes_partial_window_on_shutdown(no_thread_leaks: None) -> None:
    assert run_stage([20.0, 22.0, 30.0], window=2) == [21.0, 30.0]


def test_no_input_produces_no_output(no_thread_leaks: None) -> None:
    assert run_stage([], window=3) == []


def test_invalid_window_is_rejected() -> None:
    with pytest.raises(ValueError):
        aggregation_stage(queue.Queue(), queue.Queue(), window=0)


def test_join_confirms_all_inputs_processed(no_thread_leaks: None) -> None:
    in_q: queue.Queue[float] = queue.Queue()
    out_q: queue.Queue[float] = queue.Queue()
    worker = threading.Thread(target=aggregation_stage, args=(in_q, out_q, 3))
    worker.start()
    for v in (1.0, 2.0, 3.0):
        in_q.put(v)
    in_q.join()                                       # returns when task_done() was called 3 times
    assert out_q.get(timeout=TIMEOUT_S) == 2.0
    in_q.shutdown()
    worker.join(TIMEOUT_S)
```

**Key Points:**
- The **shutdown protocol** of the stage is part of its contract, and it is tested: the partial window
  is flushed, the output queue is shut down, the thread terminates.
- `drain()` stops when the stage shuts down its output queue: the test needs no knowledge of how many
  items to expect, and `timeout` protects it from a stage that forgets to shut down.
- `in_q.join()` is a precise synchronisation point: "*the stage has processed everything I sent*".

## 7.2 The Event Bus

The event bus of Lecture 05 (copied unchanged into `smart_gateway/event_bus.py`) delivers events
**asynchronously**: when `publish()` returns, the handlers have not run yet. The tests use two
synchronisation tools:

- a **`CollectingHandler`** that records events and sets a `threading.Event` when the expected number
  has arrived;
- **`bus.wait_until_idle()`** (i.e. `queue.join()`), which returns when everything published so far has
  been dispatched. This is ideal for asserting that something did **not** happen.

`code/06_testing/tests/test_event_bus.py`
```python
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
```

**Key Points:**
- **Two levels of testing** for event-driven code:
  - **Components** (handlers, rules) are tested as plain objects: call `handle()` with a hand-made
    `Event`, and replace the `EventBusClient` with a `Mock(spec=EventBusClient)`. No threads, no
    waiting, fully deterministic. This is the payoff of the `EventBusClient` facade of Lecture 05.
  - **The bus itself** is tested as an integration of queue + threads + registry, with explicit
    synchronisation.
- **Do not depend on delivery order** when the code does not guarantee it: `CollectingHandler.types`
  returns a **sorted** list.
- To assert that something did **not** happen ("the unsubscribed handler received nothing"), you
  need a point in time when you know that everything has been processed: `wait_until_idle()`. Without
  it, the assertion could pass just because the worker has not run *yet*.
- The `caplog` built-in fixture verifies that errors are **logged**, not silently swallowed.
- The `event_bus` fixture uses `no_thread_leaks`, so every bus test also checks that `stop()` really
  terminates the worker threads.

---

# 8 Running the Suite and Measuring Coverage

From the `code/` folder:

```bash
uv sync                       # create .venv and install pytest, pytest-asyncio, pytest-cov
uv run pytest                 # run the whole suite
```

(Without `uv`: create and activate a virtual environment, `pip install pytest pytest-asyncio pytest-cov`,
then `python -m pytest`.)

**Output:**
```
.................................................................. [ 97%]
..                                                                       [100%]
68 passed, 6 subtests passed in 0.54s
```

**Coverage** measures which lines of the code under test are executed by the tests:

```bash
uv run pytest --cov=smart_gateway --cov-report=term-missing
```

```
Name                                            Stmts   Miss  Cover   Missing
-----------------------------------------------------------------------------
06_testing/src/smart_gateway/__init__.py            0      0   100%
06_testing/src/smart_gateway/alarm_rules.py        37      1    97%   34
06_testing/src/smart_gateway/async_gateway.py      17      0   100%
06_testing/src/smart_gateway/energy_meter.py       17      0   100%
06_testing/src/smart_gateway/event_bus.py          95      4    96%   81-82, 85, 148
06_testing/src/smart_gateway/poller.py             23      0   100%
06_testing/src/smart_gateway/sampler.py            20      0   100%
06_testing/src/smart_gateway/sensors.py            40      0   100%
06_testing/src/smart_gateway/telemetry.py          18      0   100%
-----------------------------------------------------------------------------
TOTAL                                             267      5    98%
```

The `Missing` column points to untested lines: line 34 of `alarm_rules.py` is the negative-CO2
validation in `classify_co2()`. Writing that test is left as an exercise, together with the missing
lines of the event bus (the context-manager methods and `stop_reading()`).

> **Note:** coverage tells you what is **not** tested; it does not tell you that what *is* executed is
> tested *well*. 100% coverage with no meaningful assertions is worthless, while a test like the race
> test of Section 5 is valuable even though it adds no coverage. Use coverage as a **map of blind
> spots**, not as a target.

**Checking for flakiness.** Concurrent tests must pass **every** time. Before trusting them, run them
many times, e.g. `for i in $(seq 20); do uv run pytest -q; done`, or use a plugin such as
`pytest-repeat` (`pytest --count=50`). The suite of this lecture passes 20 consecutive runs out of 20.

---

# 9 Summary: a Testing Checklist

| Situation | Technique |
|---|---|
| Pure logic (calculations, rules, classification) | Plain unit tests, `parametrize` over boundary values, `approx` for floats |
| Network, database, time, hardware | Inject the dependency; use stubs, fakes (`FakeClock`), `Mock`/`AsyncMock` |
| Verifying that something was sent/published | `Mock` + `assert_called_once_with` / `call_args_list` |
| Dependency not injectable | `patch()` / `monkeypatch`, patching where the name is looked up |
| Shared state and locks | Many threads + `Barrier` + exact final assertion; check the test fails without the lock |
| Background threads | Signal with `Event`/`Queue`, every wait with a timeout, verify stop and no leaked threads |
| Thread pools / futures | Fakes that raise or block on an `Event` to simulate failures and timeouts |
| asyncio code | `pytest-asyncio` or `IsolatedAsyncioTestCase`, `AsyncMock`, `asyncio.timeout`, cancel background tasks |
| Queue pipelines | The test feeds the input queue and drains the output queue; test the shutdown protocol |
| Event-driven components | Call `handle()` directly with a mocked `EventBusClient` |
| The event bus | Collecting handler + `Event`, `wait_until_idle()` before negative assertions, order-independent checks |

---

# 10 References

- Python documentation - [`unittest` - Unit testing framework](https://docs.python.org/3/library/unittest.html)
- Python documentation - [`unittest.mock` - mock object library](https://docs.python.org/3/library/unittest.mock.html)
- Python documentation - [`unittest.mock` - where to patch](https://docs.python.org/3/library/unittest.mock.html#where-to-patch)
- pytest documentation - [docs.pytest.org](https://docs.pytest.org/) (fixtures, parametrize, `monkeypatch`, `caplog`)
- pytest-asyncio documentation - [pytest-asyncio.readthedocs.io](https://pytest-asyncio.readthedocs.io/)
- pytest-cov documentation - [pytest-cov.readthedocs.io](https://pytest-cov.readthedocs.io/)
- Real Python - [Python's unittest: Writing Unit Tests for Your Code](https://realpython.com/ref/stdlib/unittest/)
- pydevtools handbook - [pytest vs unittest: Which Should You Use?](https://pydevtools.com/handbook/explanation/pytest-vs-unittest/)
- Martin Fowler - [Mocks Aren't Stubs](https://martinfowler.com/articles/mocksArentStubs.html)
