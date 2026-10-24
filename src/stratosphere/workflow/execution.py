"""Account-serialized paper runner: closed bars, binary review, risk gates and no POST retries."""

import asyncio
import json
import time
import uuid
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from stratosphere.connectors.alpaca_paper import number
from stratosphere.models.streaming import ModelError
from stratosphere.phase0.signals import SmaCross
from stratosphere.research.store import ResearchError
from stratosphere.workflow.contracts import PositionSizingProposal, RiskSnapshot
from stratosphere.workflow.portfolios import portfolio_id
from stratosphere.workflow.research import payload_digest
from stratosphere.workflow.risk import (
    AdvisoryAgents,
    current_snapshot,
    portfolio_agent_id,
    validate_capital,
)

TERMINAL = {"filled", "canceled", "expired", "rejected"}


def text(value):
    return format(value, "f")


class PaperRunner:
    def __init__(self, portfolios, adapter, provider, *, clock_ns=time.time_ns, on_update=None):
        if (
            adapter.provider != "alpaca"
            or adapter.mode != "paper"
            or not adapter.supports_automatic_orders
        ):
            raise ResearchError(
                "No verified paper-only submission capability exists for this provider"
            )
        self.store, self.adapter = portfolios, adapter
        self.clock_ns = clock_ns
        self.on_update = on_update
        self.advisory = AdvisoryAgents(
            provider, secret_check=portfolios.secret_check, clock_ns=clock_ns
        )

    async def run(self):
        # One account lease across strategies/clients, including inference and submission.
        with self.store.lease(self.adapter.provider):
            while True:
                await self._tick()
                await asyncio.sleep(30)

    async def tick(self):
        """One iteration for deterministic integration tests and explicit diagnostic runs."""
        with self.store.lease(self.adapter.provider):
            return await self._tick()

    def verify_order_row(self, row):
        package = self.store.package(row["package_id"])
        body = json.loads(row["payload"])
        intent = {
            "package_sha256": package.digest,
            "signal_id": row["signal_id"],
            "body": body,
            "reserved": row["reserved"],
            "created_ns": row["created_ns"],
        }
        if (
            payload_digest(intent) != row["intent_sha256"]
            or body["client_order_id"] != row["id"]
            or body["symbol"] != package.symbol
            or row["signal_id"][:32] != row["id"]
        ):
            raise ResearchError("Retained paper order intent changed")
        if row["observation"]:
            if payload_digest(json.loads(row["observation"])) != row["observation_sha256"]:
                raise ResearchError("Retained provider confirmation changed")

    async def reconcile(self):
        account = await self.adapter.account()
        portfolio = next(p for p in self.store.portfolios() if p["provider"] == "alpaca")
        if portfolio["account_id"] != account["account_id"]:
            raise ResearchError("Paper provider account differs from the bound account")
        with self.store.transaction() as db:
            orders = [
                dict(r)
                for r in db.execute(
                    "SELECT o.* FROM paper_orders o JOIN paper_packages p ON "
                    "p.id=o.package_id WHERE p.portfolio_id=?",
                    (portfolio["id"],),
                )
            ]
        for row in orders:
            self.verify_order_row(row)
            if row["state"] == "terminal":
                continue
            observation = await self.adapter.find_order(row["id"])
            if observation is None:
                # Even a 404 after timeout is not proof that the original POST cannot still land.
                self.store.halt(
                    "alpaca", "An order outcome remains unknown; no resubmission is allowed"
                )
                raise ResearchError(
                    "Paper reconciliation requires a provider-confirmed order outcome"
                )
            body = json.loads(row["payload"])
            old = json.loads(row["observation"]) if row["observation"] else None
            mismatch = any(
                observation.get(k) != body[k] for k in body if k not in {"qty", "limit_price"}
            )
            mismatch = mismatch or Decimal(observation["limit_price"]) != Decimal(
                body["limit_price"]
            )
            filled = Decimal(observation["filled_qty"])
            if (
                mismatch
                or Decimal(observation["qty"]) != Decimal(body["qty"])
                or filled > Decimal(body["qty"])
                or (old and filled < Decimal(old["filled_qty"]))
            ):
                # Retain conflicting confirmations before halting; never release the reservation.
                self.store.receipt(row["signal_id"], "conflict_" + str(time.time_ns()), observation)
                self.store.halt("alpaca", "Conflicting paper order confirmation")
                raise ResearchError("Paper order confirmation conflicts with its reserved intent")
            state = "terminal" if observation["status"] in TERMINAL else "acknowledged"
            if observation != old:
                self.store.receipt(row["signal_id"], "provider_" + uuid.uuid7().hex, observation)
            with self.store.transaction() as db:
                db.execute(
                    "UPDATE paper_orders SET state=?,observation=?,observation_sha256=? WHERE id=?",
                    (state, self.store.clean(observation), payload_digest(observation), row["id"]),
                )
        positions = await self.adapter.positions()
        open_orders = await self.adapter.open_orders()
        with self.store.transaction() as db:
            rows = [
                dict(r)
                for r in db.execute(
                    "SELECT o.* FROM paper_orders o JOIN paper_packages p ON "
                    "p.id=o.package_id WHERE p.portfolio_id=? ORDER BY o.created_ns",
                    (portfolio["id"],),
                )
            ]
        owned, quantities, committed = {}, {}, {}
        pending = Decimal(0)
        active = {r["id"] for r in rows if r["state"] != "terminal"}
        for row in rows:
            self.verify_order_row(row)
            body = json.loads(row["payload"])
            obs = json.loads(row["observation"]) if row["observation"] else None
            filled = Decimal(obs["filled_qty"]) if obs else Decimal(0)
            key = row["package_id"]
            quantities[key] = quantities.get(key, Decimal(0)) + (
                filled if body["side"] == "buy" else -filled
            )
            owned[body["symbol"]] = owned.get(body["symbol"], Decimal(0)) + (
                filled if body["side"] == "buy" else -filled
            )
            if body["side"] == "buy":
                if row["state"] != "terminal":
                    pending += Decimal(row["reserved"])
                if filled or row["state"] != "terminal":
                    # No reinforcement; latest entry's full reservation remains claimed until flat.
                    committed[key] = Decimal(row["reserved"])
            if quantities[key] == 0 and row["state"] == "terminal":
                committed[key] = Decimal(0)
        expected = {s: q for s, q in owned.items() if q}
        actual = {s: Decimal(q) for s, q in positions.items()}
        if expected != actual or {o["client_order_id"] for o in open_orders} != active:
            self.store.halt("alpaca", "External, late or unreconciled account exposure")
            raise ResearchError(
                "Account positions/orders do not match the authoritative paper journal"
            )
        if any(q < 0 for q in quantities.values()):
            self.store.halt("alpaca", "Negative owned exposure")
            raise ResearchError("Paper strategy attribution is inconsistent")
        # Re-read after positions/orders; recheck again after inference.
        final_account = await self.adapter.account()
        if any(final_account[k] != account[k] for k in account if k != "equity"):
            raise ResearchError("Account cash/identity changed during reconciliation")
        return final_account, quantities, committed, pending, payload_digest(rows)

    def baseline(self, account, limits):
        day = datetime.fromtimestamp(self.clock_ns() / 1e9, UTC).date().isoformat()
        with self.store.transaction() as db:
            p = db.execute(
                "SELECT * FROM paper_portfolios WHERE id=?", (portfolio_id("alpaca"),)
            ).fetchone()
            baseline = max(Decimal(account["last_equity"]), Decimal(account["equity"]))
            if p["baseline_day"] == day:
                baseline = max(baseline, Decimal(p["baseline_equity"]))
            db.execute(
                "UPDATE paper_portfolios SET baseline_day=?,baseline_equity=? WHERE id=?",
                (day, text(baseline), p["id"]),
            )
            loss = max(Decimal(0), baseline - Decimal(account["equity"]))
            if loss >= baseline * Decimal(limits.max_daily_loss_fraction):
                db.execute(
                    "UPDATE paper_portfolios SET halted=? WHERE id=?",
                    ("Observed daily loss limit reached; entries remain halted", p["id"]),
                )
        return loss

    def snapshot(self, account, packages, committed, pending):
        with self.store.transaction() as db:
            capital = {
                r["id"]: r["capital"]
                for r in db.execute(
                    "SELECT id,capital FROM paper_packages WHERE portfolio_id=? AND "
                    "state='approved'",
                    (portfolio_id("alpaca"),),
                )
            }
        now = self.clock_ns()
        loss = self.baseline(account, packages[0].limits)
        return RiskSnapshot(
            portfolio_id=portfolio_id("alpaca"),
            currency="USD",
            equity=account["equity"],
            free_cash=text(
                max(Decimal(0), min(Decimal(account["cash"]), Decimal(account["equity"])) - pending)
            ),
            daily_loss=text(loss),
            strategy_capital={p.strategy_id: capital[p.package_id] for p in packages},
            committed_capital={
                p.strategy_id: text(committed.get(p.package_id, Decimal(0))) for p in packages
            },
            as_of_ns=now,
            expires_at_ns=now + 60_000_000_000,
            reconciled=True,
        )

    async def _tick(self):
        packages = self.store.packages("alpaca", approved=True)
        if not packages:
            raise ResearchError("No finished strategy has been approved for this paper portfolio")
        for package in packages:
            self.store.verify(package)
        account, quantities, committed, pending, revision = await self.reconcile()
        self.baseline(account, packages[0].limits)
        if self.on_update:
            with self.store.transaction() as db:
                capital = {
                    r["id"]: r["capital"]
                    for r in db.execute("SELECT id,capital FROM paper_packages")
                }
            self.on_update(
                {
                    "account": account,
                    "as_of_ns": self.clock_ns(),
                    "strategies": [
                        {
                            "package_id": p.package_id,
                            "strategy_id": p.strategy_id,
                            "symbol": p.symbol,
                            "capital": capital[p.package_id],
                            "quantity": text(quantities.get(p.package_id, Decimal(0))),
                            "committed": text(committed.get(p.package_id, Decimal(0))),
                        }
                        for p in packages
                    ],
                }
            )
        results = []
        for package in packages:
            asset = await self.adapter.asset(package.symbol)
            bars = await self.adapter.bars(
                package.symbol, package.request.seconds, package.request.slow + 2, self.clock_ns()
            )
            signal = self.consume_bar(package, bars)
            if signal is None:
                continue
            identity, side, facts, available_ns = signal
            try:
                now = self.clock_ns()
                if not available_ns <= now < available_ns + package.limits.max_signal_age_ns:
                    raise ResearchError("Signal is stale; missed triggers are never backfilled")
                own = quantities.get(package.package_id, Decimal(0))
                if (side == "buy" and own) or (side == "sell" and not own):
                    raise ResearchError("Reinforcement or an unowned exit is not permitted")
                with self.store.transaction() as db:
                    if db.execute(
                        "SELECT 1 FROM paper_orders WHERE package_id=? AND state!='terminal'",
                        (package.package_id,),
                    ).fetchone():
                        raise ResearchError("A strategy order remains pending")
                if side == "buy":
                    with self.store.transaction() as db:
                        p = db.execute(
                            "SELECT halted FROM paper_portfolios WHERE id=?",
                            (package.portfolio_id,),
                        ).fetchone()
                    if p["halted"]:
                        raise ResearchError("Paper entries are halted: " + p["halted"])
                    snapshot = self.snapshot(account, packages, committed, pending)
                    definition = package.limits.definition("alpaca")
                    context = payload_digest(facts)
                    verdict = await self.advisory.review_signal(
                        package.strategy_id, identity, context, facts
                    )
                    self.store.receipt(identity, "verdict", verdict.model_dump(mode="json"))
                    if verdict.verdict != "reliable":
                        raise ResearchError("Strategy agent returned not_reliable")
                    proposal = await self.advisory.capital(
                        definition,
                        snapshot,
                        at_ns=self.clock_ns(),
                        strategies={
                            p.strategy_id: {
                                "symbol": p.symbol,
                                "request": p.request.model_dump(mode="json"),
                                "historical_fixed_quantity_metrics": p.research_metrics,
                                "paper_policy": p.policy,
                            }
                            for p in packages
                        },
                    )
                    self.store.receipt(identity, "capital", proposal.model_dump(mode="json"))
                    validate_capital(definition, snapshot, proposal, at_ns=self.clock_ns())
                    with self.store.transaction() as db:
                        for p in packages:
                            db.execute(
                                "UPDATE paper_packages SET capital=? WHERE id=?",
                                (proposal.allocations[p.strategy_id], p.package_id),
                            )
                    snapshot = snapshot.model_copy(
                        update={"strategy_capital": proposal.allocations}
                    )
                    current_snapshot(definition, snapshot, self.clock_ns())
                    sizing = await self.advisory.call(
                        "Propose requested_notional for this exact signal, within the "
                        "supplied limits. "
                        "No stop exists: the entire long cash position counts as risk. "
                        "Never approve "
                        "or send orders. Copy exact portfolio, strategy, snapshot and signal pins.",
                        {
                            "portfolio_id": package.portfolio_id,
                            "strategy_id": package.strategy_id,
                            "snapshot": snapshot.model_dump(mode="json"),
                            "snapshot_sha256": snapshot.digest,
                            "signal_sha256": identity,
                            "limits": package.limits.model_dump(mode="json"),
                            "maximum_quantity": package.request.quantity,
                            "agent_id": portfolio_agent_id(package.portfolio_id),
                            "strategy": package.request.model_dump(mode="json"),
                            "last_closed_price_not_executable_quote": bars[-1]["close"],
                            "verdict": verdict.model_dump(mode="json"),
                        },
                        PositionSizingProposal,
                    )
                    self.store.receipt(identity, "sizing", sizing.model_dump(mode="json"))
                    if (
                        sizing.portfolio_id != package.portfolio_id
                        or sizing.strategy_id != package.strategy_id
                        or sizing.snapshot_sha256 != snapshot.digest
                        or sizing.signal_sha256 != identity
                    ):
                        raise ResearchError("Position sizing pins changed")
                    current_snapshot(definition, snapshot, self.clock_ns())
                    requested = Decimal(sizing.requested_notional)
                    ceiling = min(
                        Decimal(snapshot.free_cash),
                        Decimal(proposal.allocations[package.strategy_id]),
                        Decimal(snapshot.equity) * Decimal(package.limits.max_position_fraction),
                        Decimal(snapshot.equity) * Decimal(package.limits.max_risk_fraction),
                    )
                    if requested > ceiling:
                        raise ResearchError(
                            "Position capital exceeds approved cash, strategy, position "
                            "or full-loss risk limits"
                        )
                else:
                    # Exits are deterministic risk reduction, independent of all model services.
                    requested, ceiling = None, None
                quote = await self.adapter.quote(package.symbol)
                now = self.clock_ns()
                if (
                    not quote["as_of_ns"]
                    <= now
                    < quote["as_of_ns"] + package.limits.max_quote_age_ns
                    or not available_ns <= now < available_ns + package.limits.max_signal_age_ns
                ):
                    raise ResearchError("Paper quote or strategy trigger expired")
                assigned = self.store.packages("alpaca", approved=True)
                if [p.digest for p in assigned] != [p.digest for p in packages]:
                    raise ResearchError("Portfolio assignments changed during review")
                before = await self.reconcile()
                fresh_account, fresh_quantities, fresh_committed, fresh_pending, fresh_revision = (
                    before
                )
                if any(fresh_account[k] != account[k] for k in account if k != "equity") or (
                    fresh_quantities,
                    fresh_committed,
                    fresh_pending,
                    fresh_revision,
                ) != (quantities, committed, pending, revision):
                    raise ResearchError("Account exposure changed during review; no submission")
                if side == "buy":
                    fresh_snapshot = self.snapshot(fresh_account, packages, committed, pending)
                    current_snapshot(definition, fresh_snapshot, self.clock_ns())
                    fresh_equity = Decimal(fresh_snapshot.equity)
                    if any(
                        Decimal(v) > fresh_equity * Decimal(package.limits.max_strategy_fraction)
                        for v in fresh_snapshot.strategy_capital.values()
                    ):
                        raise ResearchError("Capital allocation exceeds fresh marked equity limits")
                    ceiling = min(
                        ceiling,
                        Decimal(fresh_snapshot.free_cash),
                        fresh_equity * Decimal(package.limits.max_position_fraction),
                        fresh_equity * Decimal(package.limits.max_risk_fraction),
                    )
                    if requested > ceiling:
                        raise ResearchError("Proposed capital exceeds fresh marked equity limits")
                    self.store.receipt(
                        identity,
                        "final_gate",
                        {
                            "review_snapshot_sha256": snapshot.digest,
                            "fresh_snapshot_sha256": fresh_snapshot.digest,
                            "fresh_equity": fresh_snapshot.equity,
                            "ceiling": text(ceiling),
                            "checked_ns": self.clock_ns(),
                        },
                    )
                increment = Decimal(asset["increment"])
                price_increment = Decimal(asset["price_increment"])
                slip = Decimal(package.limits.max_slippage_fraction)
                if side == "buy":
                    price = (
                        Decimal(quote["ask"]) * (1 + slip) / price_increment
                    ).to_integral_value(rounding=ROUND_FLOOR) * price_increment
                    if price < Decimal(quote["ask"]):
                        raise ResearchError(
                            "Provider tick cannot fit the approved entry slippage bound"
                        )
                    # Reserve the fee buffer; actual economics remain provider evidence.
                    unit_reserve = price * (1 + Decimal(package.limits.fee_buffer_fraction))
                    quantity = min(Decimal(package.request.quantity), requested / unit_reserve)
                    quantity = (quantity / increment).to_integral_value(
                        rounding=ROUND_FLOOR
                    ) * increment
                    reserved = quantity * unit_reserve
                    if reserved > ceiling:
                        raise ResearchError("Rounded paper reservation exceeds approved limits")
                else:
                    price = (
                        Decimal(quote["bid"]) * (1 - slip) / price_increment
                    ).to_integral_value(rounding=ROUND_CEILING) * price_increment
                    if price > Decimal(quote["bid"]):
                        raise ResearchError(
                            "Provider tick cannot fit the approved exit slippage bound"
                        )
                    quantity = (own / increment).to_integral_value(rounding=ROUND_FLOOR) * increment
                    reserved = Decimal(0)
                if quantity < Decimal(asset["minimum"]) or price <= 0:
                    raise ResearchError(
                        "Paper quantity/price is below the provider increment or minimum"
                    )
                body = {
                    "client_order_id": identity[:32],
                    "symbol": package.symbol,
                    "side": side,
                    "qty": text(quantity),
                    "type": "limit",
                    "time_in_force": "ioc",
                    "limit_price": text(price),
                }
                now = self.clock_ns()
                if (
                    not quote["as_of_ns"]
                    <= now
                    < quote["as_of_ns"] + package.limits.max_quote_age_ns
                    or not available_ns <= now < available_ns + package.limits.max_signal_age_ns
                ):
                    raise ResearchError("Paper quote or signal expired during final reconciliation")
                if side == "buy":
                    current_snapshot(definition, snapshot, now)
                self.store.verify(package)
                self.reserve(package, identity, body, reserved, side)
                try:
                    confirmation = await self.adapter.submit(body)
                    # Do not trust a POST acknowledgment as terminal reconciliation.
                    self.store.receipt(identity, "acknowledgment", confirmation)
                except BaseException:
                    with self.store.transaction() as db:
                        db.execute(
                            "UPDATE paper_orders SET state='ambiguous' WHERE id=?",
                            (body["client_order_id"],),
                        )
                    raise
                with self.store.transaction() as db:
                    db.execute(
                        "UPDATE paper_orders SET state='acknowledged' WHERE id=?",
                        (body["client_order_id"],),
                    )
                results.append(body["client_order_id"])
                # Reconciliation on the next tick precedes any further strategy allocation/order.
                break
            except (ResearchError, ModelError, ValueError, TimeoutError) as error:
                reason = (
                    str(error)
                    if isinstance(error, ResearchError)
                    else "Model or data validation failed"
                )
                if self.store.secret_check(reason.encode()):
                    reason = "Credential-bearing error omitted"
                self.store.receipt(
                    identity,
                    "denied",
                    {"reason": reason},
                )
                with self.store.transaction() as db:
                    has_order = db.execute(
                        "SELECT 1 FROM paper_orders WHERE signal_id=?", (identity,)
                    ).fetchone()
                    db.execute(
                        "UPDATE paper_signals SET state=? WHERE id=?",
                        ("uncertain" if has_order else "denied", identity),
                    )
        return results

    def consume_bar(self, package, bars):
        step = package.request.seconds * 10**9
        if len(bars) != package.request.slow + 2:
            raise ResearchError("Warmup requires the complete strategy window")
        for bar in bars:
            if (
                set(bar) != {"open_ns", "close"}
                or type(bar["open_ns"]) is not int
                or bar["open_ns"] % step
            ):
                raise ResearchError("Paper feed bars are not aligned closed observations")
            number(bar["close"], positive=True)
        if any(b["open_ns"] - a["open_ns"] != step for a, b in zip(bars, bars[1:], strict=False)):
            raise ResearchError("Paper feed has missing or duplicate bars")
        now, latest = self.clock_ns(), bars[-1]["open_ns"] + step
        if latest > now or now - latest >= step:
            raise ResearchError("Paper feed is not current and closed")
        with self.store.transaction() as db:
            row = db.execute(
                "SELECT cursor,state FROM paper_packages WHERE id=?", (package.package_id,)
            ).fetchone()
            if row["state"] != "approved":
                raise ResearchError("Only an approved finished strategy can consume signals")
            old = json.loads(row["cursor"]) if row["cursor"] else None
            if old:
                if old == bars:
                    return None
                if old[-1]["open_ns"] + step != bars[-1]["open_ns"] or old[1:] != bars[:-1]:
                    raise ResearchError(
                        "Paper stream interrupted or revised; no backfill/replay is permitted"
                    )
            db.execute(
                "UPDATE paper_packages SET cursor=? WHERE id=?",
                (self.store.clean(bars), package.package_id),
            )
            if not old:
                return None  # Warmup cannot emit a historical crossing.
            evaluator = SmaCross(package.request.fast, package.request.slow)
            direction = None
            for b in bars:
                direction = evaluator.push(b["close"])
            if not direction:
                return None
            facts = {
                "strategy": package.request.model_dump(mode="json"),
                "closed_bars": bars,
                "trigger": direction,
                "strategy_agent_id": self.store.database.strategy(package.strategy_id)["agent_id"],
                "available_ns": latest,
            }
            identity = payload_digest({"package": package.digest, "available_ns": latest})
            db.execute(
                "INSERT INTO paper_signals VALUES (?,?,?,'consumed',?)",
                (identity, package.package_id, self.store.clean(facts), now),
            )
            return identity, direction.lower(), facts, latest

    def reserve(self, package, identity, body, reserved, side):
        now = self.clock_ns()
        day_start = int(
            datetime.fromtimestamp(now / 1e9, UTC)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
            * 1e9
        )
        with self.store.transaction() as db:
            p = db.execute(
                "SELECT halted FROM paper_portfolios WHERE id=?", (package.portfolio_id,)
            ).fetchone()
            row = db.execute(
                "SELECT state,digest FROM paper_packages WHERE id=?", (package.package_id,)
            ).fetchone()
            if (
                row["state"] != "approved"
                or row["digest"] != package.digest
                or (p["halted"] and side == "buy")
            ):
                raise ResearchError("Paper approval or entry limits changed before reservation")
            count = db.execute(
                "SELECT COUNT(*) FROM paper_orders o JOIN paper_packages p ON "
                "p.id=o.package_id WHERE p.portfolio_id=? AND o.created_ns>=? "
                "AND json_extract(o.payload,'$.side')='buy'",
                (package.portfolio_id, day_start),
            ).fetchone()[0]
            if side == "buy" and count >= package.limits.max_entries_per_day:
                raise ResearchError("Approved daily entry order budget exhausted")
            if db.execute("SELECT 1 FROM paper_orders WHERE state!='terminal'").fetchone():
                raise ResearchError("Reconcile pending account orders before another reservation")
            intent = {
                "package_sha256": package.digest,
                "signal_id": identity,
                "body": body,
                "reserved": text(reserved),
                "created_ns": now,
            }
            db.execute(
                "INSERT INTO paper_orders VALUES (?,?,?,?, 'dispatching',NULL,?,?,?,NULL)",
                (
                    body["client_order_id"],
                    identity,
                    package.package_id,
                    self.store.clean(body),
                    text(reserved),
                    now,
                    payload_digest(intent),
                ),
            )
