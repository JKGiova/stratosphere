"""Four-pane terminal workspace with online chat and explicit capability states."""

import asyncio
import os

from pydantic import SecretStr, ValidationError
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.events import DescendantFocus, Resize
from textual.screen import ModalScreen
from textual.theme import Theme
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Select,
    Static,
    TabbedContent,
    TabPane,
)
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
            "session. Future registered strategies will be shared automatically; chat is "
            "separate. Login and trading setup come later."
        )

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="setup"):
            yield Static("Connect your model", classes="setup-heading", markup=False)
            yield Static("Choose a provider. Bring your own key.", classes="muted")
            yield Static("PROVIDER", classes="field-label")
            yield Select(
                [(label, name) for name, label in PROVIDER_LABELS.items()],
                value=self.selected_provider,
                allow_blank=False,
                id="provider",
            )
            yield Static(self.privacy_notice(), id="privacy-notice", markup=False)
            yield Static("MODEL ID · sent exactly as entered", classes="field-label")
            yield Input(
                value=self.defaults(self.selected_provider)[0]
                if self.current
                else os.getenv("STRATOSPHERE_MODEL", DEFAULT_MODELS[self.selected_provider]),
                id="model-id",
            )
            yield Static("API KEY · never enter this in chat", classes="field-label")
            yield Input(
                value=self.defaults(self.selected_provider)[1],
                password=True,
                id="api-key",
            )
            yield Static("", id="setup-error", markup=False)
            with Horizontal(id="setup-actions"):
                yield Button("Use model", id="use-model", variant="primary")
                yield Button("Cancel", id="cancel-setup")

    def on_mount(self) -> None:
        self.query_one("#setup").border_title = "MODEL CONNECTION"

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


class WorkspacePane(Vertical):
    """Allow empty panes to receive keyboard focus before integrations exist."""

    can_focus = True


class StratosphereApp(App[None]):
    TITLE = "stratosphere"
    SUB_TITLE = "research workspace"
    CSS_PATH = "tui.tcss"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        ("f1", "focus_pane('library')", "Library"),
        ("f2", "focus_pane('focus')", "Focus"),
        ("f3", "focus_pane('chat')", "Chat"),
        ("f4", "focus_pane('activity')", "Activity"),
        Binding("ctrl+k", "cancel_response", "Cancel", priority=True),
        ("ctrl+q", "quit", "Quit"),
        Binding("slash", "command", "Command", show=False),
    ]

    def __init__(self, *, provider_factory=create_provider):
        super().__init__()
        self.provider_factory = provider_factory
        self.settings: ModelSettings | None = None
        self.session: ChatSession | None = None
        self.response_worker: Worker | None = None
        self.active_pane = "chat"
        self.compact = False
        self.dense = False
        self.request_state = "MODEL NOT SET"
        self.register_theme(
            Theme(
                name="stratosphere",
                primary="#66c6ff",
                secondary="#9fcfff",
                accent="#66c6ff",
                foreground="#d6e4f2",
                background="#0b1220",
                surface="#101d30",
                panel="#16243b",
                success="#66d8a0",
                warning="#e9b45f",
                error="#f28b91",
                variables={"text-muted": "#8da5bf", "footer-key-foreground": "#66c6ff"},
            )
        )
        self.theme = "stratosphere"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, icon="◆")
        yield Static("", id="status", markup=False)
        with Container(id="panes"):
            with WorkspacePane(id="library-pane", classes="pane"):
                yield Static("CATALOG · NOT CONNECTED", classes="eyebrow")
                yield DataTable(id="strategies", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body"):
                    yield Static("Your strategies start here", classes="empty-heading")
                    yield Static(PANELS["strategies"], classes="muted", markup=False)
            with VerticalScroll(id="focus-pane", classes="pane", can_focus=True):
                yield Static("NO STRATEGY SELECTED", classes="eyebrow")
                yield Static("Strategy details", classes="empty-heading")
                yield Static(
                    "Rules, sources and versions will appear here when the strategy "
                    "library is available.",
                    classes="muted",
                    markup=False,
                )
            with WorkspacePane(id="chat-pane", classes="pane active"):
                with VerticalScroll(id="conversation"):
                    yield Static(
                        "stratosphere / research desk\n\n"
                        "Explore a trading idea with your model.\n"
                        "Use /help for commands · F1–F4 to move between panes.",
                        classes="message system-message welcome",
                        markup=False,
                    )
                yield Input(
                    placeholder="Ask your agent or /command", id="prompt", select_on_focus=False
                )
            with WorkspacePane(id="activity-pane", classes="pane"):
                with TabbedContent(id="workspace-tabs", initial="jobs"):
                    with TabPane("Tests", id="jobs"):
                        yield Static("RESEARCH · UNAVAILABLE", classes="eyebrow")
                        yield DataTable(id="tests", zebra_stripes=True, cursor_type="row")
                        with VerticalScroll(classes="pane-body"):
                            yield Static("No research runs loaded", classes="empty-heading")
                            yield Static(PANELS["jobs"], classes="muted", markup=False)
                    with TabPane("Positions", id="portfolio"):
                        yield Static("ACCOUNT · NOT CONNECTED", classes="eyebrow")
                        yield DataTable(id="positions", zebra_stripes=True, cursor_type="row")
                        with VerticalScroll(classes="pane-body"):
                            yield Static("Positions unknown", classes="empty-heading")
                            yield Static(PANELS["portfolio"], classes="muted", markup=False)
                    with TabPane("Session", id="session"):
                        with VerticalScroll(classes="pane-body"):
                            yield Static("MODEL CONNECTION", classes="eyebrow")
                            yield Static("", id="session-details", markup=False)
                            yield Static(
                                "Chat only. Execution tools are not connected.",
                                classes="session-note muted",
                            )
        yield Footer()

    def on_mount(self) -> None:
        self.panes = self.query_one("#panes", Container)
        for pane, title in [
            ("library", "1  STRATEGY LIBRARY"),
            ("focus", "2  STRATEGY FOCUS"),
            ("chat", "3  AGENT CHAT"),
            ("activity", "4  TESTS / POSITIONS"),
        ]:
            self.query_one(f"#{pane}-pane").border_title = title
        self.query_one("#strategies", DataTable).add_columns("Strategy", "Version", "State")
        self.query_one("#tests", DataTable).add_columns("Run", "Strategy", "State")
        self.query_one("#positions", DataTable).add_columns("Symbol", "Quantity", "P/L")
        self.update_status(self.request_state)
        self.update_layout()
        self.push_screen(ModelSetup(), self.configure_model)

    def on_resize(self, event: Resize) -> None:
        self.compact = event.size.width < 80 or event.size.height < 24
        self.dense = event.size.width < 100 or event.size.height < 30
        if hasattr(self, "panes"):
            self.update_layout()

    def update_layout(self) -> None:
        self.panes.set_class(self.compact, "compact")
        self.panes.set_class(self.dense, "dense")
        self.query_one(Footer).compact = self.dense
        for pane in self.panes.query(".pane"):
            pane.set_class(pane.id == f"{self.active_pane}-pane", "active")

    def on_descendant_focus(self, event: DescendantFocus) -> None:
        widget = event.widget
        while widget is not None:
            if widget.has_class("pane"):
                if self.panes.has_class("compact") and not widget.has_class("active"):
                    return
                self.active_pane = widget.id.removesuffix("-pane")
                self.update_layout()
                break
            widget = widget.parent

    def action_focus_pane(self, pane: str) -> None:
        if isinstance(self.screen, ModelSetup):
            return
        self.active_pane = pane
        self.update_layout()
        self.call_after_refresh(self.focus_active_pane)

    def focus_active_pane(self) -> None:
        if isinstance(self.screen, ModelSetup):
            return
        target = "#prompt" if self.active_pane == "chat" else f"#{self.active_pane}-pane"
        self.query_one(target).focus()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "command":
            return not isinstance(self.focused, Input) and not isinstance(self.screen, ModelSetup)
        return True

    def action_command(self) -> None:
        self.action_focus_pane("chat")
        prompt = self.query_one("#prompt", Input)
        if not prompt.value:
            prompt.value = "/"
            prompt.cursor_position = 1

    def update_status(self, state: str) -> None:
        self.request_state = state
        model = (
            f"{PROVIDER_LABELS[self.settings.provider]} / {self.settings.model}"
            if self.settings
            else "/model to connect"
        )
        self.query_one("#status", Static).update(f"{state}  ·  {model}")
        turns = len(self.session.history) // 2 if self.session else 0
        self.query_one("#session-details", Static).update(
            f"{model}\n\nRequest    {state.lower()}\nCompleted turns    {turns}\n\n"
            "Key and conversation stay in this app session. Model requests go to the "
            "selected provider."
        )

    def configure_model(self, settings: ModelSettings | None) -> None:
        if settings:
            self.settings = settings
            self.session = ChatSession(self.provider_factory(settings))
            self.update_status("MODEL SET · NOT YET VERIFIED")
        self.action_focus_pane("chat")

    async def add_message(self, text: str) -> Static:
        role = (
            "user-message"
            if text.startswith("You:")
            else "assistant-message"
            if text.startswith("Assistant:")
            else "system-message"
        )
        message = Static(safe_text(text), classes=f"message {role}", markup=False)
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
                self.action_focus_pane("chat")
            if command.action == "message":
                await self.add_message(command.message)
            elif command.action in PANELS:
                if command.action == "strategies":
                    self.action_focus_pane("library")
                else:
                    self.query_one("#workspace-tabs", TabbedContent).active = command.action
                    self.action_focus_pane("activity")
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
                self.update_status(self.request_state)
            return
        if not self.session:
            await self.add_message("Configure your online model with /model first.")
            return
        if self.response_worker and not self.response_worker.is_finished:
            await self.add_message(
                "A response is running. Use /cancel before sending another message."
            )
            return
        self.action_focus_pane("chat")
        await self.add_message(f"You: {text}")
        self.response_worker = self.generate_response(text)

    @work(group="chat", exclusive=True)
    async def generate_response(self, text: str) -> None:
        assert self.session is not None
        bubble = await self.add_message("Assistant: Connecting…")
        reply = ""
        self.update_status("CONNECTING")
        try:
            async for delta in self.session.respond(text):
                if not reply:
                    self.update_status("RESPONDING")
                reply += delta
                bubble.update(f"Assistant: {reply}")
                self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)
            self.update_status("READY")
        except asyncio.CancelledError:
            bubble.update(f"Assistant (canceled; not kept in model history): {reply}")
            bubble.add_class("interrupted-message")
            self.update_status("CANCELED")
            raise
        except ModelError as error:
            bubble.update(f"Assistant (incomplete; not kept in model history): {reply}\n{error}")
            bubble.add_class("interrupted-message")
            self.update_status("MODEL UNAVAILABLE · /model or retry")

    def action_cancel_response(self) -> None:
        if self.response_worker and not self.response_worker.is_finished:
            self.response_worker.cancel()


def main() -> None:
    StratosphereApp().run()
