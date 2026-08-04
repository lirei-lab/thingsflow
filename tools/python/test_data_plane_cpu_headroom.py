"""The shipped defaults must not throttle the data plane at its own target rate.

Why this exists: the default profile capped every NATS consumer at 750m and NATS
itself at 500m. Under a plain 3,000 msg/s MQTT run (500 devices) the throttling
gate measured the consumers pinned to their ceiling on 95-99.9% of CFS periods
and NATS on 66%, and the run landed 396,291 of 540,000 expected rows. With the
caps raised and nothing else changed, the identical run landed 540,000 of
540,000 with p95 latency falling from 7.84ms to 2.26ms.

The failure mode is what makes it worth a test: nothing errors. The edge keeps
returning success, every message is accepted, and the shortfall shows up only as
consumer lag. An operator reading acknowledgements sees a healthy platform.

This test pins the measured requirement so the caps cannot silently drift back
under it. It deliberately asserts on `limits` only: CFS throttling is a function
of the limit, while scheduling is a function of the request, which is why the
requests stay small here and a default install still fits a modest node.
"""
import pathlib
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[2]
VALUES = ROOT / "k8s/helm/thingsflow/values.yaml"

# Measured on a 16-core node at 3,000 msg/s over MQTT with 500 devices, one
# replica per consumer, via benchmarks/scripts/throttle-gate.py. Value is the
# CPU the container actually drew over the window, in millicores.
MEASURED_DRAW_M = {
    "nats": 1432,
    "latestKv": 1632,
    "greptimedb": 725,
    "alarms": 724,
}


def _cpu_m(value):
    """Parses a Kubernetes CPU quantity into millicores."""
    text = str(value).strip()
    if text.endswith("m"):
        return int(text[:-1])
    return int(float(text) * 1000)


class DataPlaneCPUHeadroomTest(unittest.TestCase):
    def setUp(self):
        self.values = yaml.safe_load(VALUES.read_text())

    def _limit_m(self, component):
        if component == "nats":
            block = self.values["nats"]["resources"]
        else:
            block = self.values["natsDataPlane"][component]["resources"]
        return _cpu_m(block["limits"]["cpu"])

    def test_cpu_limits_clear_the_measured_draw(self):
        """A limit at or below the measured draw is a cap, not a safety margin."""
        for component, drawn in sorted(MEASURED_DRAW_M.items()):
            with self.subTest(component=component):
                limit = self._limit_m(component)
                self.assertGreater(
                    limit, drawn,
                    f"{component}: CPU limit {limit}m does not clear the {drawn}m "
                    f"measured at 3,000 msg/s, so the container throttles at the "
                    f"platform's own target rate. This is the defect that made a "
                    f"3,000 msg/s run land 396,291 of 540,000 rows with zero "
                    f"errors reported anywhere.",
                )

    def test_the_busiest_consumers_keep_real_headroom(self):
        """Clearing the mean draw is not enough — CFS throttles on the bursts.

        Learned by measuring rather than reasoning: latestKv draws 1,632m on
        average, so a 2000m limit looks like comfortable headroom. It is not.
        At 2000m it still throttled on 15.1% of CFS periods, because the mean
        hides demand that exceeds the cap inside a single 100ms period. Each
        floor below is a value observed to produce no throttling at 3,000 msg/s,
        not a multiple of the mean.
        """
        floors = {"latestKv": 3000, "greptimedb": 2000, "alarms": 2000}
        for component, floor in sorted(floors.items()):
            with self.subTest(component=component):
                limit = self._limit_m(component)
                self.assertGreaterEqual(
                    limit, floor,
                    f"{component}: {limit}m is below the {floor}m that was "
                    f"measured to run without throttling at 3,000 msg/s.",
                )

    def test_requests_stay_small_so_a_default_install_still_schedules(self):
        """The fix must not be paid for with schedulability.

        Throttling is caused by the limit, so the limit is what was raised. If
        the requests are raised alongside it, a default install stops fitting on
        a small node and the fix trades one failure for another.
        """
        total = 0
        for component in ("nats", "latestKv", "greptimedb", "alarms"):
            if component == "nats":
                block = self.values["nats"]["resources"]
            else:
                block = self.values["natsDataPlane"][component]["resources"]
            total += _cpu_m(block["requests"]["cpu"])
        self.assertLessEqual(
            total, 500,
            f"the data plane now requests {total}m up front; the point of "
            f"raising limits rather than requests was that a default install "
            f"keeps fitting on a modest node.",
        )


if __name__ == "__main__":
    unittest.main()
