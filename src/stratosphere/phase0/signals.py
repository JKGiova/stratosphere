"""Exact incremental SMA crossing evaluator for the authored conformance fixture."""

from collections import deque
from fractions import Fraction


class SmaCross:
    def __init__(self, fast: int = 3, slow: int = 8):
        if not 0 < fast < slow:
            raise ValueError("Require 0 < fast < slow")
        self.fast = fast
        self.slow = slow
        self.values = deque(maxlen=slow)
        self.previous = None

    def push(self, rate: str) -> str | None:
        self.values.append(Fraction(rate))
        if len(self.values) < self.slow:
            return None
        values = list(self.values)
        difference = sum(values[-self.fast :]) / self.fast - sum(values) / self.slow
        signal = None
        if self.previous is not None:
            if self.previous <= 0 < difference:
                signal = "BUY"
            elif self.previous >= 0 > difference:
                signal = "SELL"
        self.previous = difference
        return signal
