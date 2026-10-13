"""
Lecture 01 - Concurrency Fundamentals & Threading
Section 6.1 - threading.Event: one thread signals, others react.

A CO2 monitor thread sets an `alarm` Event when the concentration crosses a threshold.
A ventilation actuator thread simply waits on that Event (without busy-polling) and
opens the window as soon as it is set.
"""

import threading
import time

CO2_THRESHOLD_PPM = 1000
# Simulated CO2 readings coming from the sensor, one every 0.2 s
CO2_SAMPLES_PPM = [620, 700, 810, 930, 1050, 1120]


def co2_monitor(alarm: threading.Event) -> None:
    """Read the CO2 sensor and raise the alarm when the threshold is exceeded."""
    for ppm in CO2_SAMPLES_PPM:
        time.sleep(0.2)
        print(f"[co2-monitor] CO2 = {ppm} ppm")
        if ppm > CO2_THRESHOLD_PPM:
            print("[co2-monitor] threshold exceeded -> alarm.set()")
            alarm.set()          # wakes up EVERY thread waiting on this event
            return


def window_actuator(alarm: threading.Event) -> None:
    """Block until the alarm is raised, then open the window."""
    print("[window] waiting for the CO2 alarm...")
    alarm.wait()                 # sleeps without consuming CPU until set() is called
    print("[window] alarm received -> opening the window")


def main() -> None:
    alarm = threading.Event()    # internal flag, initially False
    threads = [
        threading.Thread(target=window_actuator, args=(alarm,)),
        threading.Thread(target=co2_monitor, args=(alarm,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"Alarm flag is set: {alarm.is_set()}")


if __name__ == "__main__":
    main()
