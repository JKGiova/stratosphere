"""Inline research input and approval controls inside agent chat."""

from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widgets import Button, Input, Static, TextArea

from stratosphere.research.contracts import ResearchConfig


class ResearchForm(Vertical):
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+enter", "prepare", "Review plan", show=False),
    ]

    class Finished(Message):
        def __init__(self, source: str | None, config: ResearchConfig | None = None):
            super().__init__()
            self.source, self.config = source, config

    def __init__(self, source: str = ""):
        super().__init__(id="research-form")
        self.initial_source = source

    def compose(self):
        yield Static("New research · BTC/USDT spot · hourly SMA crossover", classes="setup-heading")
        yield Static(
            "Paste text or code describing all rules. The first implementation supports "
            "long-only fast/slow SMA crosses and fixed BTC quantity. Missing rules prompt "
            "questions; other behavior stays unsupported. "
            "Local drafts are not shared registrations. Source and results are saved "
            "privately on this computer; the model key stays session-only.",
            classes="research-help",
            markup=False,
        )
        yield TextArea(self.initial_source, id="research-source", soft_wrap=True)
        for name, label, value in [
            ("initial_balance", "Starting USDT balance", "10000"),
            ("fee_rate", "Simulated fee rate per fill · 0.001 = 0.1%", "0.001"),
            ("spread_bps", "Simulated full spread · basis points", "2"),
            ("slippage_bps", "Simulated slippage per side · basis points", "1"),
        ]:
            yield Static(label, classes="field-label", markup=False)
            yield Input(value, id=f"research-{name}", compact=True)
        yield Static("", id="research-error", markup=False)
        with Horizontal(classes="research-actions"):
            yield Button("Review plan", id="prepare-research", variant="primary")
            yield Button("Cancel", id="cancel-research-form")
        yield Static("Enter adds a line · Tab moves fields · Esc cancels", classes="setup-keys")

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()
        if event.button.id == "prepare-research":
            self.action_prepare()
        else:
            self.action_cancel()

    def on_input_submitted(self, event: Input.Submitted):
        event.stop()
        self.action_prepare()

    def action_prepare(self):
        source = self.query_one("#research-source", TextArea).text.strip()
        try:
            config = ResearchConfig(
                **{
                    name: self.query_one(f"#research-{name}", Input).value.strip()
                    for name in ResearchConfig.model_fields
                }
            )
            if not source:
                raise ValueError("Empty source")
        except ValueError:
            self.query_one("#research-error", Static).update(
                "Enter source text and valid nonnegative costs; starting balance must be positive."
            )
            return
        self.post_message(self.Finished(source, config))

    def action_cancel(self):
        self.post_message(self.Finished(None))


class ResearchReview(Horizontal):
    def __init__(self, job_id: str, plan_sha256: str):
        super().__init__(id=f"review-{job_id}", classes="research-review")
        self.job_id, self.plan_sha256 = job_id, plan_sha256

    def compose(self):
        yield Button("Approve 2 simulations", id=f"approve-{self.job_id}", variant="primary")
        yield Button("Cancel plan", id=f"reject-{self.job_id}")
