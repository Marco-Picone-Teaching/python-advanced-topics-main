"""
Section F.2.4: building and running a unittest TestSuite by hand.

Usually test discovery does this for you (python -m unittest discover / pytest), but an
explicit suite is useful to run a chosen subset, e.g. a quick "smoke test" on the gateway
before a deployment.

Run from the code/ folder:
    python 06_testing/run_suite.py
"""

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
