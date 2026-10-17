"""Phase 1 terminal client: model onboarding, chat and explicit capability panels."""

import asyncio
import os

from pydantic import SecretStr, ValidationError
from textual import work
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Header, Input, Select, Static, TabbedContent, TabPane
from textual.worker import Worker

from stratosphere.chat import ChatSession
from stratosphere.commands import PANELS, route_command
from stratosphere.models import create_provider
from stratosphere.models.settings import (
    DEFAULT_MODELS,
    DEFAULT_PROVIDER,
    KEY_ENVIRONMENTS,
    PROVIDER_LABELS,
    ModelSettings,
)
from stratosphere.models.streaming import ModelError, safe_text


class ModelSetup(ModalScreen[ModelSettings | None]):
    BINDINGS = [("escape", "dismiss_setup", "Cancel")]
    CSS = """
    ModelSetup { align: center middle; }
    #setup { width: 64; max-width: 100%; height: auto; max-height: 100%;
             padding: 1 2; border: round $accent; background: $surface; }
    #setup Input { margin-bottom: 1; }
    #setup-error { color: $error; height: auto; }
    """

    def __init__(self, current: ModelSettings | None = None):
        super().__init__()
        self.current = current
        initial = (
            current.provider if current else os.getenv("STRATOSPHERE_PROVIDER", DEFAULT_PROVIDER)
        )
        self.selected_provider = initial if initial in PROVIDER_LABELS else DEFAULT_PROVIDER

    def defaults(self, provider: str) -> tuple[str, str]:
        if self.current and self.current.provider == provider:
            return self.current.model, self.current.api_key.get_secret_value()
        return DEFAULT_MODELS[provider], os.getenv(KEY_ENVIRONMENTS[provider], "")

    def privacy_notice(self) -> str:
        destination = (
            "OpenRouter and its routed model provider"
            if self.selected_provider == "openrouter"
            else "NVIDIA"
        )
        return (
            f"Chat is sent to {destination}. The app keeps your key and chat only for this "
            "session. Login and trading setup are deferred. Future registered strategy "
            "definitions will be shared automatically; chat is not catalog content."
        )

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="setup"):
            yield Static("Model setup", markup=False)
            yield Select(
                [(label, name) for name, label in PROVIDER_LABELS.items()],
                value=self.selected_provider,
                allow_blank=False,
                id="provider",
            )
            yield Static(self.privacy_notice(), id="privacy-notice", markup=False)
            yield Static("Model ID (sent exactly as entered)")
            yield Input(
                value=self.defaults(self.selected_provider)[0]
                if self.current
                else os.getenv("STRATOSPHERE_MODEL", DEFAULT_MODELS[self.selected_provider]),
                id="model-id",
            )
            yield Static("API key · never enter this in chat")
            yield Input(
                value=self.defaults(self.selected_provider)[1],
                password=True,
                id="api-key",
            )
            yield Static("", id="setup-error", markup=False)
            yield Button("Use model", id="use-model", variant="primary")
            yield Button("Cancel", id="cancel-setup")

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.value not in PROVIDER_LABELS or event.value == self.selected_provider:
            return
        self.selected_provider = event.value
        model, key = self.defaults(self.selected_provider)
        self.query_one("#model-id", Input).value = model
        self.query_one("#api-key", Input).value = key
        self.query_one("#privacy-notice", Static).update(self.privacy_notice())
        self.query_one("#setup-error", Static).update("")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-setup":
            self.action_dismiss_setup()
            return
        self.apply_settings()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()
        if event.input.id == "model-id":
            self.query_one("#api-key", Input).focus()
        elif event.input.id == "api-key":
            self.apply_settings()

    def apply_settings(self) -> None:
        model = self.query_one("#model-id", Input).value.strip()
        key = self.query_one("#api-key", Input).value.strip()
        try:
            settings = ModelSettings(
                provider=self.selected_provider,
                model=model,
                api_key=SecretStr(key),
            )
        except ValueError, ValidationError:
            self.query_one("#setup-error", Static).update(
                "Enter a valid model ID and a nonempty API key. "
                "OpenRouter models must end in :free."
            )
            return
        self.query_one("#api-key", Input).value = ""
        self.dismiss(settings)

    def action_dismiss_setup(self) -> None:
        self.query_one("#api-key", Input).value = ""
        self.dismiss(None)


class StratosphereApp(App[None]):
    TITLE = "stratosphere"
    SUB_TITLE = "Phase 1 · development"
    BINDINGS = [("ctrl+q", "quit", "Quit"), ("ctrl+k", "cancel_response", "Cancel response")]
    CSS = """
    #status { height: auto; padding: 0 1; color: $text-muted; }
    #tabs { height: 1fr; }
    #conversation { height: 1fr; }
    .message { height: auto; margin: 0 1 1 1; }
    #prompt { dock: bottom; }
    .capability { padding: 1; }
    """

    def __init__(self, *, provider_factory=create_provider):
        super().__init__()
        self.provider_factory = provider_factory
        self.settings: ModelSettings | None = None
        self.session: ChatSession | None = None
        self.response_worker: Worker | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Model not configured · /model to begin", id="status", markup=False)
        with TabbedContent(id="tabs", initial="chat"):
            with TabPane("Chat", id="chat"):
                with VerticalScroll(id="conversation"):
                    yield Static(
                        "Welcome to stratosphere. Use /help for commands.",
                        classes="message",
                        markup=False,
                    )
            for panel, title in [
                ("strategies", "Strategies"),
                ("jobs", "Tests"),
                ("portfolio", "Positions"),
            ]:
                with TabPane(title, id=panel):
                    yield Static(PANELS[panel], classes="capability", markup=False)
        yield Input(placeholder="Message or /command", id="prompt")
        yield Footer()

    def on_mount(self) -> None:
        self.push_screen(ModelSetup(), self.configure_model)

    def configure_model(self, settings: ModelSettings | None) -> None:
        if settings:
            self.settings = settings
            self.session = ChatSession(self.provider_factory(settings))
            self.query_one("#status", Static).update(
                f"{PROVIDER_LABELS[settings.provider]} · {settings.model} · not yet verified online"
            )
        self.query_one("#prompt", Input).focus()

    async def add_message(self, text: str) -> Static:
        message = Static(safe_text(text), classes="message", markup=False)
        view = self.query_one("#conversation", VerticalScroll)
        await view.mount(message)
        view.scroll_end(animate=False)
        return message

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "prompt":
            return
        text = event.value.strip()
        event.input.value = ""
        if not text:
            return
        if self.settings and self.settings.api_key.get_secret_value() in text:
            await self.add_message("API key detected. Enter credentials only in /model.")
            return
        if text.startswith("/"):
            command = route_command(text)
            if command.action not in PANELS:
                self.query_one("#tabs", TabbedContent).active = "chat"
            if command.action == "message":
                await self.add_message(command.message)
            elif command.action in PANELS:
                self.query_one("#tabs", TabbedContent).active = command.action
            elif command.action == "cancel":
                self.action_cancel_response()
            elif command.action == "quit":
                self.action_quit()
            elif self.response_worker and not self.response_worker.is_finished:
                await self.add_message("Use /cancel before clearing chat or changing models.")
            elif command.action == "model":
                self.push_screen(ModelSetup(self.settings), self.configure_model)
            elif command.action == "clear":
                if self.session:
                    self.session.history.clear()
                await self.query_one("#conversation", VerticalScroll).remove_children()
                await self.add_message("Conversation cleared from this session.")
            return
        if not self.session:
            await self.add_message("Configure your online model with /model first.")
            return
        if self.response_worker and not self.response_worker.is_finished:
            await self.add_message(
                "A response is running. Use /cancel before sending another message."
            )
            return
        self.query_one("#tabs", TabbedContent).active = "chat"
        await self.add_message(f"You: {text}")
        self.response_worker = self.generate_response(text)

    @work(group="chat", exclusive=True)
    async def generate_response(self, text: str) -> None:
        assert self.session is not None
        bubble = await self.add_message("Assistant: Connecting…")
        reply = ""
        try:
            async for delta in self.session.respond(text):
                reply += delta
                bubble.update(f"Assistant: {reply}")
                self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)
            self.query_one("#status", Static).update(
                f"{PROVIDER_LABELS[self.settings.provider]} · "
                f"{self.settings.model} · last response completed"
            )
        except asyncio.CancelledError:
            bubble.update(f"Assistant (canceled; not kept in model history): {reply}")
            raise
        except ModelError as error:
            bubble.update(f"Assistant (incomplete; not kept in model history): {reply}\n{error}")
            self.query_one("#status", Static).update(
                "Model unavailable · /model to check setup or retry"
            )

    def action_cancel_response(self) -> None:
        if self.response_worker and not self.response_worker.is_finished:
            self.response_worker.cancel()


def main() -> None:
    StratosphereApp().run()
