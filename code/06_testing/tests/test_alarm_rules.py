"""
Lecture 06 - Section 3: the same kind of tests written with pytest.
Plain functions, plain `assert`, parametrization, pytest.approx, pytest.raises.
"""

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
