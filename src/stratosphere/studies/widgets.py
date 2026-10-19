"""Bounded experimental choices and hypothesis stay visible in inline chat review."""

from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import DescendantFocus
from textual.message import Message
from textual.widgets import Button, Input, Static, TextArea

from stratosphere.research.contracts import ResearchConfig, SmaParameters
from stratosphere.studies.contracts import Candidate, RegistryStudyAnchor


class StudyForm(Vertical):
    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    class Finished(Message):
        def __init__(self, proposal: dict | None, anchor=None):
            super().__init__()
            self.proposal = proposal
            self.anchor = anchor

    def __init__(self, parent_job, plan, suggestion=None, *, strategy_id=None):
        super().__init__(id="study-form")
        self.parent_job, self.plan = parent_job, plan
        self.strategy_id = strategy_id
        self.registered = isinstance(plan, RegistryStudyAnchor)
        self.parameters = (
            plan.parameters
            if self.registered
            else SmaParameters(
                fast_period=plan.draft.fast_period,
                slow_period=plan.draft.slow_period,
                quantity=plan.draft.quantity,
            )
        )
        self.suggestion = suggestion or {}

    def compose(self):
        yield Static("Bounded experiment · review before execution", classes="setup-heading")
        yield Static(
            "List exact fast/slow SMA pairs. Fixed quantity comes from the anchored definition. "
            "Development/validation use the selected month; the next month is the final holdout. "
            "The plan pins checksums, budget, selection and reset rules before approval. "
            "A consumed holdout cannot be reused for optimization in this workspace. "
            "Binance Vision commercial data rights require a separate license; this "
            "research adapter is not a commercial data entitlement.",
            classes="research-help",
            markup=False,
        )
        if self.registered:
            yield Static(
                f"Registered strategy {self.strategy_id} · {self.plan.name} · "
                f"fixed {self.parameters.quantity} BTC. Include its baseline periods. "
                "The following costs are editable simulation assumptions, not account fees.",
                classes="research-help",
                markup=False,
            )
            for label, identity, value in (
                ("Starting cash (USDT)", "balance", self.plan.config.initial_balance),
                ("Fee rate (decimal)", "fee", self.plan.config.fee_rate),
                ("Spread (bps)", "spread", self.plan.config.spread_bps),
                ("Slippage (bps)", "slippage", self.plan.config.slippage_bps),
            ):
                yield Static(label, classes="field-label")
                yield Input(value, id="study-" + identity, compact=True)
        yield Static("Hypothesis", classes="field-label")
        yield TextArea(self.suggestion.get("hypothesis", ""), id="study-hypothesis", soft_wrap=True)
        yield Static("Candidate pairs · comma-separated, e.g. 3/8, 4/10", classes="field-label")
        pairs = self.suggestion.get("candidates")
        yield Input(
            ",".join(f"{p['fast']}/{p['slow']}" for p in pairs)
            if pairs
            else f"{self.parameters.fast_period}/{self.parameters.slow_period}",
            id="study-candidates",
            compact=True,
        )
        yield Static("Development/validation month · YYYY-MM", classes="field-label")
        yield Input(self.suggestion.get("month", "2025-01"), id="study-month", compact=True)
        yield Static(
            "Maximum native starts · four per candidate + two final holdout runs",
            classes="field-label",
        )
        yield Input(str(self.suggestion.get("max_simulations", 6)), id="study-budget", compact=True)
        yield Static("", id="study-error", markup=False)
        with Horizontal(classes="research-actions"):
            yield Button("Review study", id="prepare-study", variant="primary")
            yield Button("Cancel", id="cancel-study")

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()
        if event.button.id == "cancel-study":
            self.action_cancel()
        else:
            self.prepare()

    def on_input_submitted(self, event: Input.Submitted):
        event.stop()
        self.prepare()

    def on_descendant_focus(self, event: DescendantFocus):
        self.call_after_refresh(lambda: event.widget.scroll_visible(animate=False))

    def prepare(self):
        try:
            hypothesis = self.query_one("#study-hypothesis", TextArea).text.strip()
            pairs = self.query_one("#study-candidates", Input).value.split(",")
            candidates = []
            for pair in pairs:
                fast, slow = pair.strip().split("/")
                candidates.append(Candidate(fast=int(fast), slow=int(slow)).model_dump())
            budget = int(self.query_one("#study-budget", Input).value)
            if not hypothesis or budget < 4 * len(candidates) + 2:
                raise ValueError
            proposal = {
                "hypothesis": hypothesis,
                "candidates": candidates,
                "month": self.query_one("#study-month", Input).value.strip(),
                "max_simulations": budget,
            }
            anchor = None
            if self.registered:
                config = ResearchConfig(
                    initial_balance=self.query_one("#study-balance", Input).value,
                    fee_rate=self.query_one("#study-fee", Input).value,
                    spread_bps=self.query_one("#study-spread", Input).value,
                    slippage_bps=self.query_one("#study-slippage", Input).value,
                )
                anchor = RegistryStudyAnchor.model_validate(
                    self.plan.model_dump() | {"config": config.model_dump()}
                )
                if (self.parameters.fast_period, self.parameters.slow_period) not in {
                    (candidate["fast"], candidate["slow"]) for candidate in candidates
                }:
                    raise ValueError("Include the registered baseline")
        except ValueError:
            self.query_one("#study-error", Static).update(
                "Enter valid costs, a hypothesis, integer pairs including the registered baseline, "
                "and a budget: four runs per pair plus two."
            )
            return
        self.post_message(self.Finished(proposal, anchor))

    def action_cancel(self):
        self.post_message(self.Finished(None))
