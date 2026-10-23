"""Historical Yahoo setup and exact approval controls."""

from datetime import UTC, datetime

from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widgets import Button, Input, Select, Static

from stratosphere.research.contracts import ResearchConfig
from stratosphere.yahoo.contracts import YahooRequest


class YahooForm(Vertical):
    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self):
        super().__init__(id="yahoo-form")

    class Finished(Message):
        def __init__(self, request=None):
            super().__init__()
            self.request = request

    def action_cancel(self):
        self.post_message(self.Finished())

    def compose(self):
        today = datetime.now(UTC).date().replace(day=1)
        start = today.replace(year=today.year - 1)
        yield Static("Yahoo historical backtest · long-only SMA", classes="setup-heading")
        yield Static(
            "Historical data only. Prices never authorize or route a trade. "
            "End date is exclusive; costs and the exact snapshot need approval."
        )
        yield Select(
            [("Stocks", "equity"), ("Crypto spot", "crypto_spot"), ("Forex", "forex")],
            value="crypto_spot",
            id="yahoo-asset",
        )
        fields = [
            ("symbol", "Yahoo symbol", "BTC-USD"),
            ("start", "Start date", str(start)),
            ("end", "End date (exclusive)", str(today)),
            ("interval", "Interval: 1h or 1d", "1h"),
            ("fast", "Fast SMA", "20"),
            ("slow", "Slow SMA", "50"),
            ("quantity", "Fixed units / shares / base-currency units", "0.001"),
        ]
        fields += [
            (name, name.replace("_", " ").capitalize(), value)
            for name, value in ResearchConfig().model_dump().items()
        ]
        for name, label, value in fields:
            yield Static(label, classes="field-label")
            yield Input(value, id="yahoo-" + name, compact=True)
        yield Static("", id="yahoo-error", markup=False)
        with Horizontal(classes="research-actions"):
            yield Button("Fetch and review", id="yahoo-prepare", variant="primary")
            yield Button("Cancel", id="yahoo-cancel-form")

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()
        if event.button.id == "yahoo-cancel-form":
            self.post_message(self.Finished())
            return
        try:

            def field(name):
                return self.query_one("#yahoo-" + name, Input).value.strip()

            request = YahooRequest(
                symbol=field("symbol"),
                asset_class=self.query_one("#yahoo-asset", Select).value,
                start=field("start"),
                end=field("end"),
                interval=field("interval"),
                fast=int(field("fast")),
                slow=int(field("slow")),
                quantity=field("quantity"),
                config=ResearchConfig(
                    **{name: field(name) for name in ResearchConfig.model_fields}
                ),
            )
            self.post_message(self.Finished(request))
        except ValueError:
            self.query_one("#yahoo-error", Static).update(
                "Check symbol, market, dates, periods, units and costs. "
                "Stock units must be whole shares."
            )


class YahooReview(Horizontal):
    def __init__(self, job, digest):
        super().__init__(classes="research-review")
        self.job, self.digest = job, digest

    def compose(self):
        yield Button("Approve 2 Yahoo simulations", id="yahoo-approve", variant="primary")
        yield Button("Cancel Yahoo plan", id="yahoo-reject")
