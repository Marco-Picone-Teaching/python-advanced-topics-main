"""
Section A - Concurrency Fundamentals & Threading
Section A.8 - Is this interpreter a free-threaded (no-GIL) build?

Run it with the standard interpreter and with a free-threaded one, e.g.:
    python3.14  15_free_threading_check.py
    python3.14t 15_free_threading_check.py      # the "t" build (PEP 703 / PEP 779)
    uv run --python 3.14t 15_free_threading_check.py
Then re-run 02_io_bound_vs_cpu_bound.py with both and compare the CPU-bound speed-up.
"""

import sys
import sysconfig


def main() -> None:
    # 1 if the interpreter was COMPILED with free-threading support, 0/None otherwise
    free_threaded_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    # Even a free-threaded build can re-enable the GIL at runtime (PYTHON_GIL=1, or an
    # extension module that does not declare itself free-threading safe)
    gil_enabled_now = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True

    print(f"Python version       : {sys.version.split()[0]}")
    print(f"Free-threaded build  : {free_threaded_build}")
    print(f"GIL currently enabled: {gil_enabled_now}")


if __name__ == "__main__":
    main()
