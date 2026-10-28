"""Masked native-storage setup for an isolated Alpaca paper account."""

from pydantic import SecretStr
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static

from stratosphere.legacy.alpaca_paper import PaperKeys
from stratosphere.research.store import ResearchError


class PaperSetup(ModalScreen):
    DEFAULT_CSS = """
    PaperSetup { align: center middle; background: $background 85%; }
    PaperSetup > Vertical { width: 70; height: auto; max-height: 90%;
        border: round $primary; padding: 1 2; }
    PaperSetup Horizontal { height: 3; }
    PaperSetup Button { margin-right: 1; }
    """
    BINDINGS = [("escape", "cancel", "Cancel")]

    def compose(self):
        with Vertical():
            yield Static("ALPACA PAPER · Native secure storage")
            yield Static(
                "Use paper keys only. This connects the dedicated paper endpoint. "
                "Trading starts only for a separately approved finished strategy. "
                "Stopping the runner or quitting keeps positions open and stops monitoring."
            )
            yield Input(password=True, placeholder="Paper API key", id="paper-key")
            yield Input(password=True, placeholder="Paper secret key", id="paper-secret")
            yield Static("", id="paper-setup-error")
            with Horizontal():
                yield Button("Connect paper account", id="paper-connect", variant="primary")
                yield Button("Cancel", id="paper-setup-cancel")

    def action_cancel(self):
        self.query_one("#paper-key", Input).value = ""
        self.query_one("#paper-secret", Input).value = ""
        self.dismiss(None)

    def on_button_pressed(self, event):
        event.stop()
        if event.button.id == "paper-setup-cancel":
            self.action_cancel()
            return
        try:
            keys = PaperKeys(
                key=SecretStr(self.query_one("#paper-key", Input).value.strip()),
                secret=SecretStr(self.query_one("#paper-secret", Input).value.strip()),
            )
            keys.headers()
        except ValueError, ResearchError:
            self.query_one("#paper-setup-error", Static).update(
                "Enter valid paper keys; never paste them in chat."
            )
            return
        self.query_one("#paper-key", Input).value = ""
        self.query_one("#paper-secret", Input).value = ""
        self.dismiss(keys)
