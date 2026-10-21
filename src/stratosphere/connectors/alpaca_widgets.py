"""Masked inline paper credentials, separate from chat and model configuration."""

import os

from pydantic import SecretStr, ValidationError
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.events import DescendantFocus
from textual.message import Message
from textual.widgets import Button, Input, Static

from stratosphere.connectors.alpaca import PaperCredentials


class AlpacaSetup(Vertical):
    BINDINGS = [("escape", "dismiss_setup", "Cancel")]

    class Finished(Message):
        def __init__(self, credentials):
            super().__init__()
            self.credentials = credentials

    def __init__(self):
        super().__init__(id="setup")

    def compose(self) -> ComposeResult:
        yield Static("Connect Alpaca · paper only", classes="setup-heading")
        yield Static(
            "Checks only paper account status and BTC/USD asset metadata at "
            "paper-api.alpaca.markets. No orders or prices are requested. "
            "Keys stay in memory and never enter model prompts or chat history.",
            classes="muted",
            markup=False,
        )
        yield Static("PAPER API KEY ID · masked", classes="field-label")
        yield Input(
            value=os.getenv("APCA_API_KEY_ID", ""), password=True, id="alpaca-key-id", compact=True
        )
        yield Static("PAPER SECRET KEY · masked", classes="field-label")
        yield Input(
            value=os.getenv("APCA_API_SECRET_KEY", ""),
            password=True,
            id="alpaca-secret-key",
            compact=True,
        )
        yield Static("", id="setup-error", markup=False)
        with Horizontal(id="setup-actions"):
            yield Button("Check paper account", id="check-alpaca", variant="primary")
            yield Button("Cancel", id="cancel-alpaca")
        yield Static("Tab next field · Enter check · Esc cancel", classes="setup-keys")

    def on_descendant_focus(self, event: DescendantFocus):
        self.call_after_refresh(lambda: event.widget.scroll_visible(animate=False))

    def on_input_submitted(self, event: Input.Submitted):
        event.stop()
        if event.input.id == "alpaca-key-id":
            self.query_one("#alpaca-secret-key", Input).focus()
        else:
            self.apply_credentials()

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()
        if event.button.id == "cancel-alpaca":
            self.action_dismiss_setup()
        else:
            self.apply_credentials()

    def clear_fields(self):
        for widget in self.query(Input):
            widget.value = ""

    def apply_credentials(self):
        try:
            credentials = PaperCredentials(
                key_id=SecretStr(self.query_one("#alpaca-key-id", Input).value),
                secret_key=SecretStr(self.query_one("#alpaca-secret-key", Input).value),
            )
        except ValidationError:
            self.query_one("#setup-error", Static).update(
                "Enter both paper credentials without whitespace or controls."
            )
            return
        self.clear_fields()
        self.post_message(self.Finished(credentials))

    def action_dismiss_setup(self):
        self.clear_fields()
        self.post_message(self.Finished(None))
