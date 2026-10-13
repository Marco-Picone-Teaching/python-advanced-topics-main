"""Pure functions and small value objects: the easiest code to test."""

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
