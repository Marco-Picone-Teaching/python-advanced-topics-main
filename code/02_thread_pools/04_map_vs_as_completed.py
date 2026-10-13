"""
Lecture 02 - Thread Pools & Futures
Section 3.1 - executor.map() vs as_completed().

We query the firmware version of 4 devices whose response times differ a lot.
  * map()          -> results in SUBMISSION order (a slow device delays all the next ones)
  * as_completed() -> futures in COMPLETION order (handle each answer as soon as it arrives)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# device id -> simulated response time (s)
DEVICES = {"plug-01": 0.8, "plug-02": 0.2, "plug-03": 0.5, "plug-04": 0.1}


def get_firmware_version(device_id: str) -> tuple[str, str]:
    time.sleep(DEVICES[device_id])                 # simulated HTTP request to the device
    return device_id, "v1.4.2"


def main() -> None:
    print("--- executor.map(): submission order ---")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        for device_id, version in executor.map(get_firmware_version, DEVICES):
            print(f"[{time.perf_counter() - start:.2f}s] {device_id}: {version}")

    print("--- as_completed(): completion order ---")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        # keep a future -> device map, as_completed() yields only the futures
        futures = {executor.submit(get_firmware_version, dev): dev for dev in DEVICES}
        for future in as_completed(futures):
            device_id, version = future.result()
            print(f"[{time.perf_counter() - start:.2f}s] {device_id}: {version}")


if __name__ == "__main__":
    main()
