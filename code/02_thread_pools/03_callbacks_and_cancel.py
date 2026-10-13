"""
Lecture 02 - Thread Pools & Futures
Section 2.2 - Reacting to completion with add_done_callback(), and cancelling pending work.

A pool with a single worker receives 4 firmware-update jobs. The first one starts
immediately; the others wait in the pool's internal queue, so they can still be cancelled.
"""

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor


def update_firmware(device_id: str) -> str:
    time.sleep(0.2)                                   # simulated OTA update
    return f"{device_id} updated to v2.1.0"


def on_update_done(device_id: str, future: Future[str]) -> None:
    """Called when the future finishes OR is cancelled."""
    where = threading.current_thread().name
    if future.cancelled():
        print(f"  [callback in {where}] {device_id}: job cancelled")
    else:
        print(f"  [callback in {where}] {future.result()}")


def main() -> None:
    devices = ["cam-01", "cam-02", "cam-03", "cam-04"]
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="ota") as executor:
        futures = {dev: executor.submit(update_firmware, dev) for dev in devices}
        for dev, f in futures.items():
            # add_done_callback() passes only the future: a lambda binds the device id too
            f.add_done_callback(lambda fut, dev=dev: on_update_done(dev, fut))

        # The maintenance window is closing: cancel what has not started yet
        for dev, f in futures.items():
            cancelled = f.cancel()        # a successful cancel runs the callback right now
            print(f"cancel({dev}) -> {cancelled}")


if __name__ == "__main__":
    main()
