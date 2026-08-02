"""Accounting primitives for loadgen2.

Two design rules drive this module:

  1. Every number reported is *measured*, never inferred. If a value could not
     be measured (a PUBACK that never came, a response that never arrived) it is
     counted in its own bucket rather than folded into a success or dropped.
  2. Latency is kept in a bounded, mergeable histogram so percentiles stay
     honest at 100k msg/s without holding a sample per message in RAM. Buckets
     are HDR-style (128 sub-buckets per power of two => <=0.8% quantisation
     error), and min/max/sum are tracked exactly alongside.
"""

from __future__ import annotations

SUB_BITS = 7
SUB_COUNT = 1 << SUB_BITS  # 128


def bucket_index(v: int) -> int:
    """Map a microsecond value to an HDR-style bucket index."""
    if v < 0:
        v = 0
    if v < SUB_COUNT:
        return v
    m = v.bit_length() - (SUB_BITS + 1)
    return (m + 1) * SUB_COUNT + ((v >> m) - SUB_COUNT)


def bucket_lower(i: int) -> int:
    """Lower bound (microseconds) of a bucket index."""
    if i < SUB_COUNT:
        return i
    m = i // SUB_COUNT - 1
    return ((i % SUB_COUNT) + SUB_COUNT) << m


class Histogram:
    """Bounded, mergeable latency histogram. Values in microseconds."""

    __slots__ = ("buckets", "count", "total", "min_us", "max_us")

    def __init__(self) -> None:
        self.buckets: dict[int, int] = {}
        self.count = 0
        self.total = 0
        self.min_us = -1
        self.max_us = -1

    def record(self, us: float) -> None:
        v = int(us)
        if v < 0:
            v = 0
        b = bucket_index(v)
        self.buckets[b] = self.buckets.get(b, 0) + 1
        self.count += 1
        self.total += v
        if self.min_us < 0 or v < self.min_us:
            self.min_us = v
        if v > self.max_us:
            self.max_us = v

    def to_dict(self) -> dict:
        return {
            "buckets": self.buckets,
            "count": self.count,
            "total_us": self.total,
            "min_us": self.min_us,
            "max_us": self.max_us,
        }

    @staticmethod
    def merge(dicts) -> "Histogram":
        h = Histogram()
        for d in dicts:
            if not d:
                continue
            for k, v in d["buckets"].items():
                k = int(k)
                h.buckets[k] = h.buckets.get(k, 0) + v
            h.count += d["count"]
            h.total += d["total_us"]
            if d["min_us"] >= 0 and (h.min_us < 0 or d["min_us"] < h.min_us):
                h.min_us = d["min_us"]
            if d["max_us"] > h.max_us:
                h.max_us = d["max_us"]
        return h

    def percentile(self, q: float) -> float:
        """Percentile in milliseconds. q in [0,1]."""
        if self.count == 0:
            return -1.0
        target = q * self.count
        seen = 0
        for b in sorted(self.buckets):
            seen += self.buckets[b]
            if seen >= target:
                return bucket_lower(b) / 1000.0
        return self.max_us / 1000.0

    def summary(self) -> dict:
        if self.count == 0:
            return {
                "count": 0,
                "min_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "p999_ms": None,
                "max_ms": None,
                "mean_ms": None,
            }
        return {
            "count": self.count,
            "min_ms": round(self.min_us / 1000.0, 3),
            "p50_ms": round(self.percentile(0.50), 3),
            "p95_ms": round(self.percentile(0.95), 3),
            "p99_ms": round(self.percentile(0.99), 3),
            "p999_ms": round(self.percentile(0.999), 3),
            "max_ms": round(self.max_us / 1000.0, 3),
            "mean_ms": round(self.total / self.count / 1000.0, 3),
        }


class Schedule:
    """Open-loop send schedule.

    The schedule is a pure function of wall time, never of completions. Under a
    linear ramp of `ramp` seconds toward `rate` msg/s:

        N(t) = rate * t^2 / (2*ramp)                 for t <= ramp
        N(t) = rate*ramp/2 + rate*(t - ramp)         for t >  ramp

    `due()` says how many messages *should* have left by now; `sched_time()`
    inverts it so every individual message has a deadline and its lateness can
    be measured. Nothing here consults how many responses came back -- that is
    the whole point.
    """

    __slots__ = ("rate", "ramp", "_knee")

    def __init__(self, rate: float, ramp: float) -> None:
        self.rate = float(rate)
        self.ramp = float(max(0.0, ramp))
        self._knee = self.rate * self.ramp / 2.0

    # sched_time() takes a square root inside the ramp, so _n(sched_time(k)) can
    # land at k - 1e-12 and truncate one message short -- which would show up as
    # a phantom sub-tick of lag at the ramp boundary. Nudge past the rounding.
    _EPS = 1e-9

    def _n(self, elapsed: float) -> int:
        if elapsed <= 0:
            return 0
        if self.ramp > 0 and elapsed < self.ramp:
            return int(self.rate * elapsed * elapsed / (2.0 * self.ramp) + self._EPS)
        return int(self._knee + self.rate * (elapsed - self.ramp) + self._EPS)

    def due(self, elapsed: float) -> int:
        """How many messages are due to have LEFT by `elapsed`.

        Message k's deadline is sched_time(k), with message 0 due at t=0. So the
        count due at t is N(t)+1, not N(t): releasing message k only once N(t)
        reaches k+1 would hold every message back by a full inter-message gap
        (100 ms at 10 msg/s) and then report that self-inflicted delay as lag.
        """
        if elapsed < 0:
            return 0
        return self._n(elapsed) + 1

    def total(self, duration: float) -> int:
        """Messages the schedule asks for across a window of `duration`."""
        return self._n(duration)

    def sched_time(self, k: int) -> float:
        if self.rate <= 0:
            return float("inf")
        if self.ramp > 0 and k < self._knee:
            return (2.0 * k * self.ramp / self.rate) ** 0.5
        return self.ramp + (k - self._knee) / self.rate
