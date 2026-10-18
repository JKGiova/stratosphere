"""Bounded experimental choices and hypothesis stay visible in inline chat review."""

from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import DescendantFocus
from textual.message import Message
from textual.widgets import Button, Input, Static, TextArea

from stratosphere.studies.contracts import Candidate


class StudyForm(Vertical):
    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    class Finished(Message):
        def __init__(self, proposal: dict | None):
            super().__init__()
            self.proposal = proposal

    def __init__(self, parent_job, plan, suggestion=None):
        super().__init__(id="study-form")
        self.parent_job, self.plan = parent_job, plan
        self.suggestion = suggestion or {}

    def compose(self):
        yield Static("Bounded experiment · review before execution", classes="setup-heading")
        yield Static(
            "List exact fast/slow SMA pairs. Costs and fixed quantity come from the parent plan. "
            "Development/validation use the selected month; the next month is the final holdout. "
            "The plan pins checksums, budget, selection and reset rules before approval. "
            "A consumed holdout cannot be reused for optimization in this workspace. "
            "Binance Vision commercial data rights require a separate license; this "
            "research adapter is not a commercial data entitlement.",
            classes="research-help",
            markup=False,
        )
        yield Static("Hypothesis", classes="field-label")
        yield TextArea(self.suggestion.get("hypothesis", ""), id="study-hypothesis", soft_wrap=True)
        yield Static("Candidate pairs · comma-separated, e.g. 3/8, 4/10", classes="field-label")
        pairs = self.suggestion.get("candidates")
        yield Input(
            ",".join(f"{p['fast']}/{p['slow']}" for p in pairs)
            if pairs
            else f"{self.plan.draft.fast_period}/{self.plan.draft.slow_period}",
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
        except ValueError:
            self.query_one("#study-error", Static).update(
                "Enter a hypothesis, integer SMA pairs and a budget: four runs per pair plus two."
            )
            return
        self.post_message(self.Finished(proposal))

    def action_cancel(self):
        self.post_message(self.Finished(None))
