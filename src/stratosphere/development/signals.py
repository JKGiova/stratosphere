"""Causal exact-rational signal runtime for the application-owned strategy language."""

from collections import deque
from fractions import Fraction

from stratosphere.development.contracts import validate_program
from stratosphere.phase0.contracts import StrategySpec


class SignalProgram:
    def __init__(self, specification):
        self.spec = validate_program(StrategySpec.model_validate(specification))
        self.windows = {i.id: deque(maxlen=i.period) for i in self.spec.indicators}
        self.smoothed = {}
        self.last_input = {}
        self.gains = {}
        self.losses = {}
        self.previous = None
        self.count = 0

    def indicators(self, bar):
        result = {}
        for item in self.spec.indicators:
            value = bar[item.input]
            window = self.windows[item.id]
            if item.algorithm in {"sma", "ema"}:
                window.append(value)
                if len(window) < item.period:
                    result[item.id] = None
                    continue
                average = sum(window) / item.period
                if item.algorithm == "ema":
                    old = self.smoothed.get(item.id)
                    average = (
                        average
                        if old is None
                        else old + Fraction(2, item.period + 1) * (value - old)
                    )
                    self.smoothed[item.id] = average
                result[item.id] = average
            else:
                old = self.last_input.get(item.id)
                self.last_input[item.id] = value
                if old is None:
                    result[item.id] = None
                    continue
                delta = value - old
                window.append((max(delta, Fraction(0)), max(-delta, Fraction(0))))
                if len(window) < item.period:
                    result[item.id] = None
                    continue
                gain = self.gains.get(item.id)
                loss = self.losses.get(item.id)
                if gain is None:
                    gain = sum((v[0] for v in window), Fraction(0)) / item.period
                    loss = sum((v[1] for v in window), Fraction(0)) / item.period
                else:
                    gain = (gain * (item.period - 1) + max(delta, Fraction(0))) / item.period
                    loss = (loss * (item.period - 1) + max(-delta, Fraction(0))) / item.period
                self.gains[item.id], self.losses[item.id] = gain, loss
                result[item.id] = (
                    Fraction(50)
                    if gain == loss == 0
                    else Fraction(100)
                    if loss == 0
                    else 100 - 100 / (1 + gain / loss)
                )
        return result

    def evaluate(self, node, state):
        if state is None:
            return None
        bars, indicators, held = state
        op = node.op
        if op == "decimal":
            return Fraction(node.value)
        if op == "boolean":
            return node.value
        if op == "field":
            return bars[node.value]
        if op == "indicator":
            return indicators[node.value]
        if op == "position":
            return {"flat": not held, "long": held, "short": False}[node.value]
        values = [self.evaluate(arg, state) for arg in node.args]
        if any(value is None for value in values):
            return None
        if op == "not":
            return not values[0]
        a, b = values
        if op == "add":
            return a + b
        if op == "sub":
            return a - b
        if op == "mul":
            return a * b
        if op == "div":
            if b == 0:
                raise ValueError("Division by zero in strategy expression")
            return a / b
        if op == "gt":
            return a > b
        if op == "ge":
            return a >= b
        if op == "lt":
            return a < b
        if op == "le":
            return a <= b
        if op == "eq":
            return a == b
        if op == "and":
            return a and b
        if op == "or":
            return a or b
        previous = [self.evaluate(arg, self.previous) for arg in node.args]
        if any(value is None for value in previous):
            return False
        if op == "crosses_above":
            return previous[0] <= previous[1] and a > b
        if op == "crosses_below":
            return previous[0] >= previous[1] and a < b
        raise ValueError("Unsupported signal expression")

    def push(self, bar, held):
        values = {key: Fraction(value) for key, value in bar.items()}
        self.count += 1
        state = (values, self.indicators(values), held)
        signal = None
        if self.count >= self.spec.data_requirements.warmup_bars:
            if held:
                if any(
                    self.evaluate(rule.condition, state) is True for rule in self.spec.exit_rules
                ):
                    signal = "SELL"
            elif all(self.evaluate(c, state) is True for c in self.spec.market_conditions):
                if any(
                    self.evaluate(rule.condition, state) is True for rule in self.spec.entry_rules
                ):
                    signal = "BUY"
        self.previous = state
        return signal
