"""Reviewed authored closed-bar processing in the account journal, without order delivery."""

import hashlib
from collections import deque
from fractions import Fraction
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.paper.ledger import Instance, PaperError
from stratosphere.phase0.contracts import Contract, Digest, StrategySpec, fingerprint
from stratosphere.phase0.signals import SmaCross
from stratosphere.registry.workspace import encoded
from stratosphere.research.contracts import MarketBar, sma_parameters

HOUR_NS = 3600 * 10**9


def implementation_digest():
    root = Path(__file__).parent.parent
    digest = hashlib.sha256()
    for name in (
        "paper/ledger.py",
        "paper/runtime.py",
        "phase0/contracts.py",
        "phase0/signals.py",
        "research/contracts.py",
        "registry/workspace.py",
    ):
        digest.update(name.encode() + b"\0" + (root / name).read_bytes())
    return digest.hexdigest()


class ClosedBarMandate(Contract):
    schema_version: Literal["authored-closed-bar-mandate/1"] = "authored-closed-bar-mandate/1"
    instance: Instance
    specification: StrategySpec
    feed_profile: Literal["authored-btcusdt-hourly/1"] = "authored-btcusdt-hourly/1"
    first_open_ns: int = Field(strict=True, gt=0)
    max_publication_delay_ns: int = Field(strict=True, ge=0, lt=HOUR_NS)
    max_receipt_age_ns: int = Field(strict=True, gt=0, lt=HOUR_NS)
    gap_policy: Literal["halt_without_backfill"] = "halt_without_backfill"
    outputs: Literal["raw_crossings_only_no_orders"] = "raw_crossings_only_no_orders"

    @model_validator(mode="after")
    def supported(self):
        sma_parameters(self.specification)
        if fingerprint(self.specification) != self.instance.strategy_sha256:
            raise ValueError("The runtime must pin the complete registered strategy")
        if self.first_open_ns % HOUR_NS:
            raise ValueError("The first candle must start at a UTC hourly boundary")
        return self

    @property
    def digest(self):
        return fingerprint(self)


class ClosedObservation(Contract):
    observation_id: Digest
    feed_profile: Literal["authored-btcusdt-hourly/1"] = "authored-btcusdt-hourly/1"
    evidence: Literal["authored_fixture_only"] = "authored_fixture_only"
    bar: MarketBar
    available_ns: int = Field(strict=True, gt=0)


def _advance(runtime, observation, received_ns):
    parameters = runtime["parameters"]
    cross = SmaCross(parameters["fast_period"], parameters["slow_period"])
    cross.values = deque((Fraction(v) for v in runtime["window"]), maxlen=cross.slow)
    cross.previous = Fraction(runtime["previous"]) if runtime["previous"] is not None else None
    side = cross.push(observation.bar.close)
    signal = None
    if side:
        signal = {
            "side": side,
            "signal_ns": observation.bar.close_ns,
            "available_ns": observation.available_ns,
            "nominal_next_open_ns": observation.bar.open_ns + HOUR_NS,
            "received_ns": received_ns,
            "observation_id": observation.observation_id,
            "kind": "raw_crossing_not_order_permission",
        }
        signal["signal_sha256"] = hashlib.sha256(
            encoded({"mandate_sha256": runtime["digest"], **signal}).encode()
        ).hexdigest()
    return (
        [str(v) for v in cross.values],
        str(cross.previous) if cross.previous is not None else None,
        signal,
    )


def _observation_reason(runtime, mandate, observation, received_ns):
    reason = None
    old = runtime["observations"].get(observation.observation_id)
    same_open = runtime["opens"].get(str(observation.bar.open_ns))
    if old is not None:
        if old == observation.model_dump():
            return "duplicate"
        reason = "conflicting_observation_id"
    elif same_open is not None:
        prior = runtime["observations"][same_open]
        if {k: v for k, v in prior.items() if k != "observation_id"} == observation.model_dump(
            exclude={"observation_id"}
        ):
            return "duplicate"
        reason = "conflicting_closed_candle"
    elif runtime["faults"]:
        reason = "runtime_halted"
    elif runtime["last_received_ns"] is not None and received_ns < runtime["last_received_ns"]:
        reason = "receipt_clock_regression"
    elif observation.bar.open_ns != mandate["first_open_ns"] + runtime["accepted_count"] * HOUR_NS:
        reason = "gap_or_out_of_order"
    elif (
        observation.available_ns < observation.bar.open_ns + HOUR_NS
        or received_ns < observation.available_ns
    ):
        reason = "unclosed_or_future_observation"
    elif (
        observation.available_ns - (observation.bar.open_ns + HOUR_NS)
        > mandate["max_publication_delay_ns"]
    ):
        reason = "publication_delay_exceeded"
    elif received_ns - observation.available_ns > mandate["max_receipt_age_ns"]:
        reason = "stale_observation"
    return reason


def reduce_event(state, event):
    """Replay retained v1 stream evidence; current code identity is checked before new work."""
    runtimes = state["runtimes"]
    kind = event["kind"]
    identity = event["instance_id"]
    if kind == "runtime_plan":
        mandate = ClosedBarMandate.model_validate(event["mandate"])
        if identity in runtimes or mandate.instance.instance_id != identity:
            raise PaperError("Conflicting runtime mandate")
        instance = state["instances"].get(identity)
        if instance is None or instance["mandate"] != mandate.instance.model_dump():
            raise PaperError("Runtime mandate differs from its account allocation")
        runtimes[identity] = {
            "mandate": mandate.model_dump(mode="json"),
            "digest": mandate.digest,
            "parameters": sma_parameters(mandate.specification).model_dump(),
            "approved": False,
            "window": [],
            "previous": None,
            "accepted_count": 0,
            "last_received_ns": None,
            "observations": {},
            "opens": {},
            "signals": [],
            "faults": [],
        }
    elif kind == "runtime_approval":
        runtime = runtimes[identity]
        if event["digest"] != runtime["digest"] or runtime["approved"]:
            raise PaperError("Invalid retained runtime approval")
        runtime["approved"] = True
    elif kind in {"runtime_bar", "runtime_fault", "runtime_duplicate"}:
        runtime = runtimes[identity]
        observation = ClosedObservation.model_validate(event["observation"])
        if type(event["received_ns"]) is not int or event["received_ns"] <= 0:
            raise PaperError("Invalid retained runtime receipt time")
        mandate = runtime["mandate"]
        reason = _observation_reason(runtime, mandate, observation, event["received_ns"])
        if kind == "runtime_duplicate":
            if reason != "duplicate":
                raise PaperError("Invalid retained duplicate candle")
            runtime["observations"][observation.observation_id] = observation.model_dump()
            return
        if kind == "runtime_fault":
            if not runtime["approved"] or reason is None or reason != event["reason"]:
                raise PaperError("Invalid retained runtime fault")
            runtime["faults"].append(event)
            return
        if reason is not None:
            raise PaperError("Retained runtime candle bypassed stream controls")
        if not runtime["approved"] or runtime["faults"]:
            raise PaperError("Retained runtime candle bypassed approval or a fault")
        if observation.observation_id in runtime["observations"]:
            raise PaperError("A runtime observation was accepted twice")
        window, previous, signal = _advance(runtime, observation, event["received_ns"])
        if signal != event["signal"]:
            raise PaperError("Retained runtime signal differs from its closed observations")
        runtime["window"], runtime["previous"] = window, previous
        runtime["accepted_count"] += 1
        runtime["last_received_ns"] = event["received_ns"]
        runtime["observations"][observation.observation_id] = observation.model_dump()
        runtime["opens"][str(observation.bar.open_ns)] = observation.observation_id
        if signal:
            runtime["signals"].append(signal)
    else:
        raise PaperError("Unknown runtime journal event")


class ClosedBarRuntime:
    """One reviewed stream per allocated instance, serialized by the account transaction.

    Inputs and receipt times are authored caller observations, not authenticated feed data.
    Raw crossings never reserve capital, dispatch an order, or attest executable eligibility.
    """

    def __init__(self, ledger, registry):
        self.ledger, self.registry = ledger, registry

    def _verify(self, mandate):
        record = self.registry.strategy(mandate.instance.strategy_id)
        if record["spec_sha256"] != mandate.instance.strategy_sha256 or record[
            "specification"
        ] != mandate.specification.model_dump(mode="json"):
            raise PaperError("Runtime strategy differs from its registered definition")
        if (
            mandate.instance.implementation_sha256 != implementation_digest()
            or mandate.instance.implementation_sha256 != _IMPORTED_SOURCE_DIGEST
        ):
            raise PaperError("Runtime implementation changed; a new review is required")

    def prepare(self, mandate: ClosedBarMandate):
        mandate = ClosedBarMandate.model_validate(mandate.model_dump())
        self._verify(mandate)
        identity = mandate.instance.instance_id
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            if old := state["runtimes"].get(identity):
                if old["digest"] != mandate.digest:
                    raise PaperError("An existing runtime mandate cannot change")
                return old["digest"]
            event = {
                "kind": "runtime_plan",
                "instance_id": identity,
                "mandate": mandate.model_dump(mode="json"),
            }
            reduce_event(state, event)
            version = db.execute("PRAGMA user_version").fetchone()[0]
            db.execute(f"PRAGMA user_version={max(2, version)}")
            self.ledger._append(db, chain, event)
        return mandate.digest

    @staticmethod
    def _runtime(state, identity):
        if identity not in state["runtimes"]:
            raise PaperError("Prepare a reviewed closed-bar mandate for this instance")
        return state["runtimes"][identity]

    def approve(self, identity, digest):
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            runtime = self._runtime(state, identity)
            self._verify(ClosedBarMandate.model_validate(runtime["mandate"]))
            if digest != runtime["digest"] or runtime["approved"]:
                raise PaperError("Approve exactly the pending runtime mandate")
            self.ledger._append(
                db, chain, {"kind": "runtime_approval", "instance_id": identity, "digest": digest}
            )

    def ingest(self, identity, observation: ClosedObservation, *, received_ns: int):
        observation = ClosedObservation.model_validate(observation.model_dump())
        if type(received_ns) is not int or received_ns <= 0:
            raise PaperError("Use a positive authored observation receipt time")
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            runtime = self._runtime(state, identity)
            mandate = ClosedBarMandate.model_validate(runtime["mandate"])
            self._verify(mandate)
            if not runtime["approved"]:
                raise PaperError("The runtime mandate has not been approved")
            reason = _observation_reason(runtime, runtime["mandate"], observation, received_ns)
            if reason == "duplicate":
                if observation.observation_id not in runtime["observations"]:
                    self.ledger._append(
                        db,
                        chain,
                        {
                            "kind": "runtime_duplicate",
                            "instance_id": identity,
                            "observation": observation.model_dump(),
                            "received_ns": received_ns,
                        },
                    )
                return {"status": "duplicate", "signal": None}
            event = {
                "kind": "runtime_fault" if reason else "runtime_bar",
                "instance_id": identity,
                "observation": observation.model_dump(),
                "received_ns": received_ns,
            }
            if reason:
                event["reason"] = reason
                self.ledger._append(db, chain, event)
                return {"status": "halted", "reason": reason, "signal": None}
            _, _, signal = _advance(runtime, observation, received_ns)
            event["signal"] = signal
            self.ledger._append(db, chain, event)
            return {"status": "accepted", "signal": signal}

    def inspect(self, identity):
        state = self.ledger.snapshot()
        return self._runtime(state, identity)


# Reject on-disk edits after this module was loaded, even if a new caller supplies their hash.
_IMPORTED_SOURCE_DIGEST = implementation_digest()
