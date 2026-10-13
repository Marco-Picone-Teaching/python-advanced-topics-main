"""
Section F.5: testing threaded code deterministically.

Rules applied in this file:
  1. never "sleep and hope": wait for an explicit signal (Event, Queue, join) WITH a timeout;
  2. every wait has a timeout, so a bug makes the test FAIL instead of hanging forever;
  3. make races likely on purpose (Barrier: all threads start at the same instant);
  4. check that no threads are leaked (the no_thread_leaks fixture in conftest.py).
"""

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
