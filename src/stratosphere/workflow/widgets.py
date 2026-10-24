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
