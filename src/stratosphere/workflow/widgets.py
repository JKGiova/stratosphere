"""Explicit human approval controls for a complete bounded research campaign."""

from textual.containers import Horizontal
from textual.widgets import Button


class CampaignReview(Horizontal):
    def __init__(self, identity, digest, starts):
        super().__init__(classes="research-review")
        self.identity, self.digest, self.starts = identity, digest, starts

    def compose(self):
        yield Button(
            f"Approve {self.starts} research simulations", id="flow-approve", variant="primary"
        )
        yield Button("Cancel campaign", id="flow-reject")


class FinishedReview(Horizontal):
    def __init__(self, identity, digest):
        super().__init__(classes="research-review")
        self.identity, self.digest = identity, digest

    def compose(self):
        yield Button("Approve paper strategy", id="strategy-approve", variant="primary")
        yield Button("Reject", id="strategy-reject")
