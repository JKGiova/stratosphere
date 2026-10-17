"""Four-pane terminal workspace with online chat and explicit capability states."""

import asyncio
import os

from pydantic import SecretStr, ValidationError
from rich.text import Text
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
    Tab,
    TabbedContent,
    TabPane,
    Tabs,
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
from stratosphere.terminal import request_maximized_window

VIEWS = (
    ("library", "Library"),
    ("focus", "Focus"),
    ("chat", "Chat"),
    ("jobs", "Tests"),
    ("portfolio", "Positions"),
    ("session", "Session"),
)


def message_text(text: str) -> Text:
    """Style trusted role labels while keeping all message content literal."""
    content = Text(safe_text(text))
    if text.startswith("You:"):
        content.stylize("bold #ffffff", 0, 4)
    elif text.startswith("Assistant"):
        end = text.find(":")
        if end >= 0:
            content.stylize("bold #ffffff", 0, end + 1)
    return content


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
        yield Static("stratosphere  /  MODEL SETUP", id="setup-brand", markup=False)
        with Container(id="setup-stage"):
            with VerticalScroll(id="setup"):
                yield Static("MODEL CONNECTION", classes="setup-overline", markup=False)
                yield Static(
                    "Update your model" if self.current else "Welcome to stratosphere",
                    classes="setup-heading",
                    markup=False,
                )
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
        yield Static("Tab next field · Enter connect · Esc cancel", id="setup-keys", markup=False)

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
    SUB_TITLE = "research terminal"
    CSS_PATH = "tui.tcss"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        Binding("tab", "cycle_view(1)", "Next", key_display="Tab", priority=True),
        Binding("shift+tab", "cycle_view(-1)", "Back", key_display="Shift+Tab", priority=True),
        Binding("f1", "focus_pane('library')", "Library", show=False),
        Binding("f2", "focus_pane('focus')", "Focus", show=False),
        Binding("f3", "focus_pane('chat')", "Chat", show=False),
        Binding("f4", "focus_pane('activity')", "Activity", show=False),
        Binding("ctrl+k", "cancel_response", "Stop", priority=True),
        ("ctrl+q", "quit", "Quit"),
        Binding("slash", "command", "Commands", key_display="/"),
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
                primary="#ffffff",
                secondary="#aaaaaa",
                accent="#ffffff",
                foreground="#cccccc",
                background="#000000",
                surface="#0a0a0a",
                panel="#1a1a1a",
                success="#00cc66",
                warning="#ddaa00",
                error="#ff6666",
                variables={"text-muted": "#8a8a8a", "footer-key-foreground": "#ffffff"},
            )
        )
        self.theme = "stratosphere"

    def compose(self) -> ComposeResult:
        yield Header(icon=">")
        yield Tabs(
            *(Tab(label.upper(), id=f"nav-{view}") for view, label in VIEWS),
            active="nav-chat",
            id="navigation",
        )
        yield Static("", id="status", markup=False)
        with Container(id="panes"):
            with WorkspacePane(id="library-pane", classes="pane"):
                yield Static("STRATEGY LIBRARY", classes="pane-title", id="library-title")
                yield Static("Catalog / not connected", classes="eyebrow")
                yield DataTable(id="strategies", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body"):
                    yield Static("No strategies loaded", classes="empty-heading")
                    yield Static(PANELS["strategies"], classes="muted", markup=False)
            with WorkspacePane(id="focus-pane", classes="pane"):
                yield Static("STRATEGY FOCUS", classes="pane-title", id="focus-title")
                with VerticalScroll(classes="pane-body"):
                    yield Static("Selection / none", classes="eyebrow")
                    yield Static("Strategy details", classes="empty-heading")
                    yield Static(
                        "Rules, sources and versions will appear here when the strategy "
                        "library is available.",
                        classes="muted",
                        markup=False,
                    )
            with WorkspacePane(id="chat-pane", classes="pane active"):
                yield Static("AGENT CHAT", classes="pane-title", id="chat-title")
                with VerticalScroll(id="conversation"):
                    yield Static(
                        "stratosphere\nYour research desk.",
                        classes="welcome-brand",
                        markup=False,
                    )
                    yield Static(
                        "Start with a trading idea.\n"
                        "Ask a question, outline your rules, or type /help.",
                        classes="message system-message welcome",
                        markup=False,
                    )
                with Horizontal(id="composer"):
                    yield Static("›", id="prompt-marker")
                    yield Input(
                        placeholder="Message your agent or /help",
                        id="prompt",
                        select_on_focus=False,
                        compact=True,
                    )
            with WorkspacePane(id="activity-pane", classes="pane"):
                yield Static("RESEARCH TESTS", classes="pane-title", id="activity-title")
                with TabbedContent(id="workspace-tabs", initial="jobs"):
                    with TabPane("Tests", id="jobs"):
                        yield Static("Research / not available", classes="eyebrow")
                        yield DataTable(id="tests", zebra_stripes=True, cursor_type="row")
                        with VerticalScroll(classes="pane-body"):
                            yield Static("No research runs loaded", classes="empty-heading")
                            yield Static(PANELS["jobs"], classes="muted", markup=False)
                    with TabPane("Positions", id="portfolio"):
                        yield Static("Account / not connected", classes="eyebrow")
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
        self.workspace_screen = self.screen
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
        self.workspace_screen.set_class(self.compact, "compact-workspace")
        self.panes.set_class(self.compact, "compact")
        self.panes.set_class(self.dense, "dense")
        self.query_one(Footer).compact = self.dense
        for pane in self.panes.query(".pane"):
            pane.set_class(pane.id == f"{self.active_pane}-pane", "active")
        self.sync_navigation()

    @property
    def active_view(self) -> str:
        if self.active_pane == "activity":
            return self.query_one("#workspace-tabs", TabbedContent).active or "jobs"
        return self.active_pane

    def sync_navigation(self) -> None:
        self.query_one("#navigation", Tabs).active = f"nav-{self.active_view}"
        activity = self.query_one("#workspace-tabs", TabbedContent).active or "jobs"
        self.query_one("#activity-title", Static).update(
            "RESEARCH TESTS" if activity == "jobs" else dict(VIEWS)[activity].upper()
        )

    def action_cycle_view(self, direction: int) -> None:
        views = [view for view, _ in VIEWS]
        self.action_select_view(views[(views.index(self.active_view) + direction) % len(views)])

    def action_select_view(self, view: str) -> None:
        if isinstance(self.screen, ModalScreen):
            return
        if view in {"jobs", "portfolio", "session"}:
            self.query_one("#workspace-tabs", TabbedContent).active = view
            self.action_focus_pane("activity")
        else:
            self.action_focus_pane(view)

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if event.tabs.id != "navigation" or not hasattr(self, "panes"):
            return
        view = event.tab.id.removeprefix("nav-")
        if view != self.active_view:
            self.action_select_view(view)

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if hasattr(self, "panes"):
            self.sync_navigation()

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
        if action == "cycle_view":
            return not isinstance(self.screen, ModalScreen)
        if action == "cancel_response":
            return True if self.request_state in {"CONNECTING", "RESPONDING"} else None
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
            f"{PROVIDER_LABELS[self.settings.provider]} · {self.settings.model}"
            if self.settings
            else "/model to connect"
        )
        label = {
            "MODEL NOT SET": "○ Set up a model with /model",
            "MODEL SET · NOT YET VERIFIED": "○ Model set · not yet verified",
            "CONNECTING": "◌ Connecting",
            "RESPONDING": "◌ Responding",
            "READY": "● Ready",
            "CANCELED": "○ Canceled",
        }.get(state, "! Model unavailable · /model or retry")
        self.query_one("#status", Static).update(f"{label}    {model}" if self.settings else label)
        self.query_one("#status").set_class(state.startswith("MODEL UNAVAILABLE"), "error-status")
        self.query_one("#status").set_class(state in {"CONNECTING", "RESPONDING"}, "busy-status")
        self.query_one("#composer").border_subtitle = (
            "Ctrl+K stop" if state in {"CONNECTING", "RESPONDING"} else "Enter send · / commands"
        )
        self.refresh_bindings()
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
        message = Static(message_text(text), classes=f"message {role}", markup=False)
        view = self.query_one("#conversation", VerticalScroll)
        await view.mount(message)
        view.scroll_end(animate=False)
        return message

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "prompt":
            return
        text = event.value.strip()
        if not text:
            return
        if self.settings and self.settings.api_key.get_secret_value() in text:
            event.input.value = ""
            await self.add_message("API key detected. Enter credentials only in /model.")
            return
        if text.startswith("/"):
            event.input.value = ""
            command = route_command(text)
            if command.action not in PANELS:
                self.action_focus_pane("chat")
            if command.action == "message":
                await self.add_message(command.message)
            elif command.action in PANELS:
                self.action_select_view(
                    "library" if command.action == "strategies" else command.action
                )
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
        event.input.value = ""
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
                bubble.update(message_text(f"Assistant: {reply}"))
                self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)
            self.update_status("READY")
        except asyncio.CancelledError:
            bubble.update(message_text(f"Assistant (canceled; not kept in model history): {reply}"))
            bubble.add_class("interrupted-message")
            self.update_status("CANCELED")
            raise
        except ModelError as error:
            bubble.update(
                message_text(f"Assistant (incomplete; not kept in model history): {reply}\n{error}")
            )
            bubble.add_class("interrupted-message")
            self.update_status("MODEL UNAVAILABLE · /model or retry")

    def action_cancel_response(self) -> None:
        if self.response_worker and not self.response_worker.is_finished:
            self.response_worker.cancel()


def main() -> None:
    request_maximized_window()
    StratosphereApp().run(inline=False)
