"""
Section F.2: the standard library `unittest` framework.

Run only this file with:
    python -m unittest discover -s 06_testing/tests -p "test_alarm_rules_unittest.py" -v
(from the code/ folder, with 06_testing/src on PYTHONPATH), or simply with pytest, which
also runs unittest-style tests.
"""

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
