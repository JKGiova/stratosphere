"""Responsive terminal workspace with online chat and inline model setup."""

import asyncio
import json
import os
import re
from pathlib import Path

from pydantic import SecretStr, ValidationError
from rich.console import Group
from rich.markdown import Markdown
from rich.text import Text
from rich.theme import Theme as RichTheme
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.events import DescendantFocus, Key, Resize
from textual.message import Message
from textual.theme import Theme
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Input,
    Select,
    Static,
    Tab,
    Tabs,
    TextArea,
)
from textual.worker import Worker

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.catalog.contracts import AssessmentTarget
from stratosphere.catalog.service import assessment_markdown, recommendations_markdown
from stratosphere.commands import PANELS, route_command
from stratosphere.connectors.liquid import LiquidConnection
from stratosphere.connectors.liquid_account import LiquidAccountReader, account_markdown
from stratosphere.market_data.routing import capabilities_markdown
from stratosphere.models import create_provider
from stratosphere.models.settings import (
    BASE_URLS,
    DEFAULT_MODELS,
    DEFAULT_PROVIDER,
    KEY_ENVIRONMENTS,
    PROVIDER_LABELS,
    ModelSettings,
)
from stratosphere.models.streaming import ModelError, safe_text
from stratosphere.paper.reviews import PaperReviewService
from stratosphere.paper.reviews import review_markdown as paper_review_markdown
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.report_evidence import ReportQuery, evidence_hint
from stratosphere.research.contracts import ResearchConfig
from stratosphere.research.metrics import report_markdown
from stratosphere.research.service import ResearchService, plan_markdown
from stratosphere.research.store import ResearchError, ResearchStore, default_research_root
from stratosphere.research.widgets import ResearchForm, ResearchReview
from stratosphere.sources.captions import CaptionRequest
from stratosphere.sources.conformance import ConformanceService, conformance_markdown
from stratosphere.sources.contracts import SourceRequest, SuppliedRequest
from stratosphere.sources.pine import PineChartContext
from stratosphere.sources.registration import RegistrationRequest, registration_markdown
from stratosphere.sources.review import review_markdown
from stratosphere.sources.service import SourceService, source_markdown
from stratosphere.sources.store import SourceStore
from stratosphere.studies.reporting import protocol_markdown
from stratosphere.studies.service import StudyService
from stratosphere.studies.store import StudyStore
from stratosphere.studies.widgets import StudyForm
from stratosphere.workspace import CHROME_HEIGHT, choose_layout

VIEWS = (
    ("library", "Library"),
    ("focus", "Focus"),
    ("chat", "Chat"),
    ("jobs", "Tests"),
    ("portfolio", "Positions"),
    ("reports", "Reports"),
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


class AssistantMessage(Static):
    """Render streamed Markdown without interpreting Rich markup or opening links."""

    def __init__(self):
        super().__init__(classes="message assistant-message", markup=False)
        self.source = ""
        self.label = "Assistant:"
        self.update_reply("Connecting…")

    def update_reply(self, text: str, *, label: str = "Assistant:") -> None:
        self.source = safe_text(text)
        self.label = label
        self.update(
            Group(Text(label, style="bold #ffffff"), Markdown(self.source, hyperlinks=False))
        )


class ModelSetup(Vertical):
    BINDINGS = [("escape", "dismiss_setup", "Cancel")]

    class Finished(Message):
        def __init__(self, settings: ModelSettings | None):
            super().__init__()
            self.settings = settings

    def __init__(self, current: ModelSettings | None = None):
        super().__init__(id="setup")
        self.current = current
        initial = (
            current.provider if current else os.getenv("STRATOSPHERE_PROVIDER", DEFAULT_PROVIDER)
        )
        self.selected_provider = initial if initial in PROVIDER_LABELS else DEFAULT_PROVIDER

    def defaults(self, provider: str) -> tuple[str, str]:
        if self.current and self.current.provider == provider:
            return self.current.model, self.current.api_key.get_secret_value()
        return DEFAULT_MODELS[provider], os.getenv(KEY_ENVIRONMENTS[provider], "")

    def default_base_url(self) -> str:
        if self.current and self.current.provider == self.selected_provider:
            return self.current.resolved_base_url
        return os.getenv(
            f"{self.selected_provider.upper()}_BASE_URL", BASE_URLS[self.selected_provider]
        )

    def privacy_notice(self) -> str:
        destination = (
            "OpenRouter and its routed model provider"
            if self.selected_provider == "openrouter"
            else PROVIDER_LABELS[self.selected_provider]
        )
        cost = (
            "OpenRouter is restricted to free models. "
            if self.selected_provider == "openrouter"
            else "Your provider's API pricing applies. "
        )
        if self.selected_provider == "ollama":
            cost = "Local Ollama needs a running server and an installed model; no key required. "
        return (
            f"Chat is sent to {destination} at the API address below. "
            + cost
            + "Keys stay in memory. General chat is session-only; strategy-agent conversations "
            "and evidence are retained privately on this computer. "
            "An explicit paper budget/risk review sends the authored account snapshot "
            "to this model provider and retains its advisory report locally. "
            "Research sources/results are saved privately on this computer. "
            "Future registered strategies will be shared automatically; chat is "
            "separate. Login and trading setup come later."
        )

    def compose(self) -> ComposeResult:
        yield Static(
            "Update your model" if self.current else "Connect your model to start chatting",
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
            compact=True,
        )
        yield Static("API BASE URL · official address; local Ollama allowed", classes="field-label")
        yield Input(value=self.default_base_url(), id="base-url", compact=True)
        yield Static(
            "API KEY · masked, session only; optional for local Ollama", classes="field-label"
        )
        yield Input(
            value=self.defaults(self.selected_provider)[1],
            password=True,
            id="api-key",
            compact=True,
        )
        yield Static("", id="setup-error", markup=False)
        with Horizontal(id="setup-actions"):
            yield Button("Use model", id="use-model", variant="primary")
            yield Button("Cancel", id="cancel-setup")
        yield Static("Tab next field · Enter connect · Esc cancel", classes="setup-keys")

    def on_descendant_focus(self, event: DescendantFocus) -> None:
        self.call_after_refresh(lambda: event.widget.scroll_visible(animate=False))

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.value not in PROVIDER_LABELS or event.value == self.selected_provider:
            return
        self.selected_provider = event.value
        model, key = self.defaults(self.selected_provider)
        self.query_one("#model-id", Input).value = model
        self.query_one("#api-key", Input).value = key
        self.query_one("#base-url", Input).value = self.default_base_url()
        self.query_one("#privacy-notice", Static).update(self.privacy_notice())
        self.query_one("#setup-error", Static).update("")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        event.stop()
        if event.button.id == "cancel-setup":
            self.action_dismiss_setup()
            return
        self.apply_settings()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()
        if event.input.id == "model-id":
            self.query_one("#api-key", Input).focus()
        elif event.input.id == "base-url":
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
                base_url=self.query_one("#base-url", Input).value.strip(),
            )
        except ValueError, ValidationError:
            self.query_one("#setup-error", Static).update(
                "Enter a valid model ID, official API base URL and a nonempty API key "
                "(optional for local Ollama). OpenRouter models must end in :free."
            )
            return
        self.query_one("#api-key", Input).value = ""
        self.post_message(self.Finished(settings))

    def action_dismiss_setup(self) -> None:
        self.query_one("#api-key", Input).value = ""
        self.post_message(self.Finished(None))


class Conversation(VerticalScroll):
    """Keep a focused inline setup field visible when the chat viewport changes."""

    def on_resize(self, event: Resize) -> None:
        focused = self.screen.focused
        if focused and any(
            isinstance(parent, (ModelSetup, ResearchForm)) for parent in focused.ancestors
        ):
            self.call_after_refresh(lambda: focused.scroll_visible(animate=False))


class WorkspacePane(Vertical):
    """Allow empty panes to receive keyboard focus before integrations exist."""

    can_focus = True

    def on_key(self, event: Key):
        if self.id == "reports-pane" and self.has_focus and event.key in {"pagedown", "pageup"}:
            body = self.query_one("#report-body", VerticalScroll)
            body.focus()
            if event.key == "pagedown":
                body.action_page_down()
            else:
                body.action_page_up()
            event.stop()
            event.prevent_default()


class StratosphereApp(App[None]):
    TITLE = "stratosphere"
    SUB_TITLE = "research terminal"
    CSS_PATH = "tui.tcss"
    ENABLE_COMMAND_PALETTE = False
    AUTO_FOCUS = "#model-id"
    BINDINGS = [
        Binding("tab", "cycle_view(1)", "Next", key_display="Tab", priority=True),
        Binding("shift+tab", "cycle_view(-1)", "Back", key_display="Shift+Tab", priority=True),
        Binding("f1", "focus_pane('library')", "Library", show=False),
        Binding("f2", "focus_pane('focus')", "Focus", show=False),
        Binding("f3", "focus_pane('chat')", "Chat", show=False),
        Binding("f4", "focus_pane('jobs')", "Tests", show=False),
        Binding("ctrl+k", "cancel_response", "Stop", priority=True),
        ("ctrl+q", "quit", "Quit"),
        Binding("slash", "command", "Commands", key_display="/"),
    ]

    def __init__(
        self,
        *,
        provider_factory=create_provider,
        research_service=None,
        study_service=None,
        source_service=None,
        liquid_connection=None,
        paper_review_service=None,
    ):
        super().__init__()
        self.console.push_theme(
            RichTheme(
                {
                    "markdown.h1": "bold underline #ffffff",
                    "markdown.h2": "bold #ffffff",
                    "markdown.h3": "bold #eeeeee",
                    "markdown.h4": "bold #dddddd",
                    "markdown.h5": "bold #cccccc",
                    "markdown.h6": "bold #cccccc",
                    "markdown.link": "underline #cccccc",
                    "markdown.link_url": "#aaaaaa",
                    "markdown.code": "#dddddd on #161616",
                    "markdown.block_quote": "#aaaaaa",
                }
            )
        )
        self.provider_factory = provider_factory
        self.liquid = liquid_connection or LiquidConnection()
        self.liquid.catalog_secret_check = self.contains_session_credential
        self.liquid_worker: Worker | None = None
        self.settings: ModelSettings | None = None
        self.session: Orchestrator | None = None
        self.response_worker: Worker | None = None
        self.research = research_service or ResearchService(ResearchStore(default_research_root()))
        self.database = WorkspaceDatabase(self.research.store.root / "workspace.sqlite3")
        self.liquid_accounts = LiquidAccountReader(
            self.liquid,
            ResearchStore(self.research.store.root / "liquid-accounts"),
            online_check=self.research.require_online,
        )
        self.liquid_accounts.secret_check = self.contains_session_credential
        self.paper_reviews = paper_review_service or PaperReviewService(
            self.research.store.root / "paper" / "account.sqlite3",
            ResearchStore(self.research.store.root / "paper-reviews"),
            online_check=self.research.require_online,
        )
        previous_secret_check = self.paper_reviews.secret_check
        self.paper_reviews.secret_check = lambda data: (
            previous_secret_check(data) or self.contains_session_credential(data)
        )
        self.studies = study_service or StudyService(
            StudyStore(self.research.store.root / "studies"), self.database
        )
        self.sources = source_service or SourceService(
            SourceStore(self.research.store.root / "sources"),
            online_check=self.research.require_online,
        )
        self.sources.store.session_secret_check = self.contains_session_credential
        self.workspace_bridge = Orchestrator(
            None, self.database, self.research, studies=self.studies, sources=self.sources
        )
        self.assessments = self.workspace_bridge.assessments
        self.source_reviews = self.workspace_bridge.source_reviews
        self.source_registrations = self.workspace_bridge.source_registrations
        self.source_conformance = ConformanceService(
            self.sources, ResearchStore(self.research.store.root / "source-conformance")
        )
        self.research_worker: Worker | None = None
        self.research_job_id: str | None = None
        self.research_form: ResearchForm | StudyForm | None = None
        self.selected_report_key: str | None = None
        self.active_pane = "chat"
        self.viewport = (80, 24)
        self.layout_mode = choose_layout(*self.viewport)
        self.activity_view = "jobs"
        self.companion_view = "library"
        self.setup: ModelSetup | None = None
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
        yield Static("stratosphere  /  research terminal", id="brand", markup=False)
        yield Tabs(
            *(Tab(label.upper(), id=f"nav-{view}") for view, label in VIEWS),
            active="nav-chat",
            id="navigation",
        )
        yield Static("", id="status", markup=False)
        with Container(id="panes"):
            with WorkspacePane(id="library-pane", classes="pane"):
                yield Static("STRATEGY LIBRARY", classes="pane-title", id="library-title")
                yield Static("Catalog / not connected", classes="eyebrow", id="library-state")
                yield DataTable(id="strategies", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body", id="library-empty"):
                    yield Static("No strategies loaded", classes="empty-heading")
                    yield Static(PANELS["strategies"], classes="muted", markup=False)
            with WorkspacePane(id="focus-pane", classes="pane"):
                yield Static("STRATEGY FOCUS", classes="pane-title", id="focus-title")
                with VerticalScroll(classes="pane-body", id="focus-body"):
                    yield Static("Selection / none", classes="eyebrow")
                    yield Static("Strategy details", classes="empty-heading")
                    yield Static(
                        "Select a strategy in Library to inspect its definition, "
                        "agent, jobs and reports.",
                        classes="muted",
                        markup=False,
                    )
            with WorkspacePane(id="chat-pane", classes="pane active"):
                yield Static("AGENT CHAT", classes="pane-title", id="chat-title")
                with Conversation(id="conversation"):
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
            with WorkspacePane(id="jobs-pane", classes="pane"):
                yield Static("RESEARCH TESTS", classes="pane-title")
                yield Static("Local research / approved jobs", classes="eyebrow")
                yield DataTable(id="tests", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body", id="tests-empty"):
                    yield Static("No research runs loaded", classes="empty-heading")
                    yield Static(PANELS["jobs"], classes="muted", markup=False)
            with WorkspacePane(id="portfolio-pane", classes="pane"):
                yield Static("POSITIONS", classes="pane-title")
                yield Static("Account / not connected", classes="eyebrow")
                yield DataTable(id="positions", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body"):
                    yield Static("Positions unknown", classes="empty-heading")
                    yield Static(PANELS["portfolio"], classes="muted", markup=False)
            with WorkspacePane(id="reports-pane", classes="pane"):
                yield Static("REPORTS", classes="pane-title")
                yield Static("Retained evidence · verified again when opened", classes="eyebrow")
                yield DataTable(id="reports", zebra_stripes=True, cursor_type="row")
                with VerticalScroll(classes="pane-body", id="report-body"):
                    yield Static(
                        "No completed reports", id="reports-empty", classes="empty-heading"
                    )
                    yield Static(
                        "Select a report to inspect its measured results and limitations.",
                        id="report-preview",
                        markup=False,
                    )
        yield Footer()

    async def on_mount(self) -> None:
        self.viewport = (self.size.width, self.size.height)
        self.panes = self.query_one("#panes", Container)
        self.workspace_screen = self.screen
        self.query_one("#strategies", DataTable).add_columns("Strategy", "Public ID", "State")
        self.query_one("#tests", DataTable).add_columns("Run", "Strategy", "State")
        self.query_one("#reports", DataTable).add_columns("Strategy", "Report", "Created")
        self.query_one("#positions", DataTable).add_columns("Symbol", "Quantity", "P/L")
        self.research.recover()
        self.studies.recover()
        self.refresh_research_views()
        self.update_status(self.request_state)
        self.update_layout()
        await self.open_model_setup()

    def on_resize(self, event: Resize) -> None:
        self.viewport = (event.size.width, event.size.height)
        if hasattr(self, "panes"):
            self.update_layout()
            self.call_after_refresh(self.reveal_setup_focus)

    def reveal_setup_focus(self) -> None:
        form = self.setup or self.research_form
        if form and form.has_focus_within and self.focused:
            self.focused.scroll_visible(animate=False)

    def update_layout(self) -> None:
        width, height = self.viewport
        self.layout_mode = choose_layout(width, height)
        self.panes.styles.grid_size_columns = self.layout_mode.columns
        self.panes.styles.grid_size_rows = self.layout_mode.rows
        self.panes.styles.grid_columns = "1fr " * self.layout_mode.columns
        self.panes.styles.grid_rows = "1fr " * self.layout_mode.rows
        if self.layout_mode.count == 6:
            visible = {view for view, _ in VIEWS}
        elif self.layout_mode.count == 4:
            visible = {"library", "focus", "chat", self.activity_view}
        elif self.layout_mode.count == 2:
            visible = {"chat", self.companion_view}
        else:
            visible = {self.active_pane}
        self.workspace_screen.set_class(self.layout_mode.count == 1, "compact-workspace")
        self.panes.set_class(self.layout_mode.count == 1, "compact")
        self.panes.set_class(
            width // self.layout_mode.columns < 60
            or (height - CHROME_HEIGHT) // self.layout_mode.rows < 20,
            "dense",
        )
        self.query_one(Footer).compact = self.layout_mode.count < 4
        for pane in self.panes.query(".pane"):
            view = pane.id.removesuffix("-pane")
            pane.display = view in visible
            pane.set_class(view == self.active_pane, "active")
        self.sync_navigation()
        self.call_after_refresh(self.size_report_list)

    @property
    def active_view(self) -> str:
        return self.active_pane

    def sync_navigation(self) -> None:
        navigation = self.query_one("#navigation", Tabs)
        with navigation.prevent(Tabs.TabActivated):
            navigation.active = f"nav-{self.active_view}"

    def action_cycle_view(self, direction: int) -> None:
        views = [view for view, _ in VIEWS]
        self.action_select_view(views[(views.index(self.active_view) + direction) % len(views)])

    def action_select_view(self, view: str) -> None:
        if view not in dict(VIEWS):
            return
        self.active_pane = view
        if view in {"jobs", "portfolio", "reports"}:
            self.activity_view = view
        if view != "chat":
            self.companion_view = view
        self.update_layout()
        self.call_after_refresh(self.focus_active_pane)

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if event.tabs.id != "navigation" or not hasattr(self, "panes"):
            return
        view = event.tab.id.removeprefix("nav-")
        if view != self.active_view:
            self.action_select_view(view)

    def on_descendant_focus(self, event: DescendantFocus) -> None:
        widget = event.widget
        while widget is not None:
            if widget.has_class("pane"):
                if not widget.display:
                    return
                view = widget.id.removesuffix("-pane")
                if view != self.active_pane:
                    self.active_pane = view
                    if view != "chat":
                        self.companion_view = view
                    if view in {"jobs", "portfolio", "reports"}:
                        self.activity_view = view
                    self.update_layout()
                break
            widget = widget.parent

    def action_focus_pane(self, pane: str) -> None:
        self.action_select_view(pane)

    def focus_active_pane(self) -> None:
        if self.active_pane == "chat":
            form = self.setup or self.research_form
            if form:
                if not form.has_focus_within:
                    target = (
                        "#model-id"
                        if self.setup
                        else (
                            "#study-hypothesis"
                            if isinstance(form, StudyForm)
                            else "#research-source"
                        )
                    )
                    form.query_one(target).focus()
            else:
                self.query_one("#prompt", Input).focus()
        elif self.active_pane == "focus":
            self.query_one("#focus-body", VerticalScroll).focus()
        else:
            self.query_one(f"#{self.active_pane}-pane").focus()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "cycle_view":
            return not (
                (self.setup and self.setup.has_focus_within)
                or (self.research_form and self.research_form.has_focus_within)
            )
        if action == "cancel_response":
            return (
                True
                if self.request_state in {"CONNECTING", "RESPONDING"} or self.research_busy
                else None
            )
        if action == "command":
            return (
                not isinstance(self.focused, (Input, TextArea))
                and self.setup is None
                and self.research_form is None
            )
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
        if state.startswith("RESEARCH · "):
            label = "◌ " + state.removeprefix("RESEARCH · ")
        self.query_one("#status", Static).update(f"{label}    {model}" if self.settings else label)
        self.query_one("#status").set_class(state.startswith("MODEL UNAVAILABLE"), "error-status")
        self.query_one("#status").set_class(state in {"CONNECTING", "RESPONDING"}, "busy-status")
        self.query_one("#composer").border_subtitle = (
            "Ctrl+K stop"
            if state in {"CONNECTING", "RESPONDING"} or state.startswith("RESEARCH · ")
            else "Enter send · / commands"
        )
        self.refresh_bindings()
        turns = len(self.session.history) // 2 if self.session else 0
        agent = self.session.active_strategy if self.session else None
        self.query_one("#status", Static).tooltip = (
            f"{model}\nRequest {state.lower()}\nCompleted turns    {turns}\n"
            + (f"Strategy agent {agent}" if agent else "Orchestrator · no strategy selected")
        )

    async def open_model_setup(self) -> None:
        self.action_select_view("chat")
        if self.setup is None:
            self.setup = ModelSetup(self.settings)
            self.query_one("#prompt", Input).disabled = True
            self.query_one("#composer").display = False
            await self.query_one("#conversation", VerticalScroll).mount(self.setup)
        self.focus_active_pane()
        self.call_after_refresh(self.focus_active_pane)

    async def on_model_setup_finished(self, event: ModelSetup.Finished) -> None:
        event.stop()
        setup, self.setup = self.setup, None
        if setup:
            await setup.remove()
        self.query_one("#prompt", Input).disabled = False
        self.query_one("#composer").display = True
        if event.settings:
            self.settings = event.settings
            self.session = Orchestrator(
                self.provider_factory(event.settings),
                self.database,
                self.research,
                f"{event.settings.provider}/{event.settings.model}",
                studies=self.studies,
                assessments=self.assessments,
                sources=self.sources,
            )
            self.session.sync_research()
            self.update_status("MODEL SET · NOT YET VERIFIED")
            await self.add_message("Model connected for this session. Send a message to test it.")
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

    def contains_session_credential(self, content: bytes) -> bool:
        def matches(key):
            return bool(key) and (
                key.encode() in content
                or json.dumps(key, ensure_ascii=False)[1:-1].encode() in content
            )

        if self.liquid.contains_credential(content):
            return True
        if self.settings and matches(self.settings.api_key.get_secret_value()):
            return True
        return any(matches(os.getenv(name)) for name in set(KEY_ENVIRONMENTS.values()))

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "prompt":
            return
        text = event.value.strip()
        if not text:
            return
        if self.contains_session_credential(text.encode()):
            event.input.value = ""
            await self.add_message(
                "Credential detected. Keep credentials out of chat and commands."
            )
            return
        if text.startswith("/"):
            event.input.value = ""
            command = route_command(text)
            if command.action not in PANELS:
                self.action_focus_pane("chat")
            if command.action in {
                "new",
                "study",
                "approve",
                "pause",
                "resume",
                "show",
                "report",
                "report-data",
            }:
                await self.handle_research_command(command.action, command.args)
            elif command.action in {
                "liquid",
                "liquid-connect",
                "liquid-disconnect",
                "liquid-tools",
                "liquid-tool",
                "liquid-catalog-save",
                "liquid-account",
                "liquid-account-reads",
                "liquid-account-show",
            }:
                if command.action == "liquid-disconnect":
                    if self.liquid_worker and not self.liquid_worker.is_finished:
                        self.liquid_worker.cancel()
                    self.liquid.disconnect()
                    await self.add_message(
                        "Liquid credentials forgotten for this process. Revoke provider consent "
                        "separately in Liquid. No order was submitted."
                    )
                elif self.liquid_worker and not self.liquid_worker.is_finished:
                    await self.add_message(
                        "Liquid connection is running. Use /liquid-disconnect to cancel."
                    )
                else:
                    self.liquid_worker = self.inspect_liquid(command.action, command.args)
            elif command.action == "data":
                try:
                    await self.research.require_online()
                    view = self.query_one("#focus-body", VerticalScroll)
                    text = capabilities_markdown(*command.args)
                    await view.remove_children()
                    await view.mount(Static(Markdown(safe_text(text), hyperlinks=False)))
                    self.action_select_view("focus")
                except ResearchError as error:
                    await self.add_message(str(error))
                except ValueError:
                    await self.add_message("Use a listed data profile and purpose; inspect /data.")
            elif command.action in {"paper-review", "paper-reviews", "paper-review-show"}:
                await self.handle_paper_review_command(command.action, command.args)
            elif command.action in {"source-review", "source-reviews", "source-review-show"}:
                await self.handle_source_review_command(command.action, command.args)
            elif command.action in {"assess", "recommend", "assessment"}:
                await self.handle_assessment_command(command.action, command.args)
            elif command.action in {
                "source",
                "sources",
                "source-show",
                "source-text",
                "source-file",
                "source-captions",
                "source-register",
                "source-registrations",
                "source-registration-show",
                "conformance",
                "conformance-show",
            }:
                await self.handle_source_command(command.action, command.args)
            elif command.action == "agent":
                if not self.session:
                    await self.add_message("Configure a model with /model first.")
                elif not command.args:
                    await self.add_message(
                        "Strategy agents: "
                        + ", ".join(s["public_id"] for s in self.database.catalog())
                        + ". Select one with /agent <strategy-id>."
                    )
                else:
                    try:
                        await self.show_strategy_agent(command.args[0])
                    except ResearchError as error:
                        await self.add_message(str(error))
            elif command.action == "message":
                await self.add_message(command.message)
            elif command.action in PANELS:
                self.action_select_view(
                    "library" if command.action == "strategies" else command.action
                )
            elif command.action == "cancel":
                if command.args:
                    await self.cancel_research_job(command.args[0])
                else:
                    self.action_cancel_response()
            elif command.action == "quit":
                await self.action_quit()
            elif (
                self.response_worker and not self.response_worker.is_finished
            ) or self.research_busy:
                await self.add_message("Use /cancel before clearing chat or changing models.")
            elif command.action == "model":
                await self.open_model_setup()
            elif command.action == "clear":
                if self.session:
                    self.session.clear()
                await self.query_one("#conversation", VerticalScroll).remove_children()
                await self.add_message("Conversation cleared from this session.")
                self.update_status(self.request_state)
            return
        if not self.session:
            await self.add_message("Configure your online model with /model first.")
            return
        if self.research_busy:
            await self.add_message(
                "Research is running. Use /pause or /cancel before a model request."
            )
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

    @work(group="liquid", exclusive=True)
    async def inspect_liquid(self, action: str, args=()):
        try:
            if action == "liquid-connect":
                await self.add_message(
                    "Opening Liquid browser authorization for read-only catalog inspection. "
                    "Approve only read access. No tool or order will be called. "
                    "Use /liquid-disconnect to cancel; sign-in expires after four minutes."
                )
                text = await self.liquid.connect()
            elif action == "liquid-account":
                await self.add_message(
                    "Requesting only Liquid get_account with read access. "
                    "The result stays in private diagnostics; it cannot link or activate trading."
                )
                value = await self.liquid_accounts.read()
                text = account_markdown(value) + (
                    "\n\nPrivate observation: `"
                    + str(self.liquid_accounts.store.directory(value.id) / "observation.json")
                    + "`. Use `/liquid-account-show "
                    + value.id
                    + "` to inspect it later."
                )
            elif action == "liquid-account-show":
                text = account_markdown(await self.liquid_accounts.open(args[0]))
            elif action == "liquid-account-reads":
                await self.research.require_online()
                text = "# Liquid account diagnostic attempts\n\n" + (
                    "\n\n".join(
                        f"- `{r['id']}` · {r['state']}" for r in self.liquid_accounts.attempts()
                    )
                    or "No retained attempts."
                )
                text += (
                    "\n\nHistorical diagnostics do not restore a connection or authorize trading."
                )
            elif action in {"liquid-tools", "liquid-tool", "liquid-catalog-save"}:
                await self.research.require_online()
                catalog = self.liquid.catalog_snapshot()
                if catalog.contains_secret(self.contains_session_credential):
                    raise ResearchError("Credential-bearing Liquid catalog was rejected.")
                if action == "liquid-catalog-save":
                    directory = await asyncio.to_thread(
                        catalog.save, self.research.store.root / "liquid-catalogs"
                    )
                    text = (
                        "# Liquid catalog exported\n\n"
                        f"Private definitions: `{directory / 'catalog.json'}`\n\n"
                        "The adjacent manifest verifies the saved digest. "
                        "OAuth responses, headers and callbacks are not part of this export. "
                        "The snapshot grants no permissions and does not verify an account "
                        "or paper mode. No tool was called."
                    )
                else:
                    text = catalog.markdown(args[0] if action == "liquid-tool" else None)
            else:
                await self.liquid.discover()
                text = self.liquid.status_markdown()
            view = self.query_one("#focus-body", VerticalScroll)
            await view.remove_children()
            await view.mount(Static(Markdown(safe_text(text), hyperlinks=False)))
            self.action_select_view("focus")
        except asyncio.CancelledError:
            self.liquid.disconnect()
            raise
        except ResearchError as error:
            await self.add_message(str(error))

        except OSError:
            await self.add_message(
                "The Liquid diagnostic could not be stored or opened. "
                "No account was linked; an in-flight read may have reached the provider."
            )

    @work(group="chat", exclusive=True)
    async def generate_response(self, text: str) -> None:
        assert self.session is not None
        bubble = AssistantMessage()
        await self.query_one("#conversation", VerticalScroll).mount(bubble)
        reply = ""
        agent_label = "Assistant"
        self.update_status("CONNECTING")
        try:
            async for delta in self.session.respond(text):
                if not reply:
                    self.update_status("RESPONDING")
                    if self.session.responding_strategy:
                        agent_label = f"Agent {self.session.responding_strategy}"
                reply += delta
                bubble.update_reply(reply, label=f"{agent_label}:")
                self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)
            self.update_status("READY")
            action = self.session.last_action
            if action and action["action"] == "new":
                self.call_after_refresh(self.handle_research_command, "new", (action["source"],))
            elif action and action["action"] == "review":
                await self.query_one("#conversation", VerticalScroll).mount(
                    ResearchReview(action["job_id"], action["digest"], action.get("simulations", 2))
                )
            elif action and action["action"] == "study":
                self.call_after_refresh(
                    self.handle_research_command,
                    "study",
                    (action.get("job_id") or action["strategy_id"],),
                    action.get("suggestion"),
                )
            elif action and action["action"] == "resume":
                self.call_after_refresh(self.handle_research_command, "resume", (action["job_id"],))
            elif action and action["action"] == "paper_review":
                if self.research_busy:
                    raise ResearchError(
                        "Pause or cancel the current operation before a paper review."
                    )
                self.research_worker = self.run_paper_review()
            elif action and action["action"] == "report":
                await self.open_report_panel(action["job_id"])
            elif action and action["action"] == "report-data":
                self.show_report_evidence(action["page"])
            elif action and action["action"] == "assessment":
                await self.open_report_panel("assessment:" + action["id"])
            elif action and action["action"] == "source_review":
                await self.open_report_panel("source-review:" + action["id"])
            elif action and action["action"] == "source":
                await self.show_source(await self.sources.open(action["id"]), focus=False)
            elif action and action["action"] == "source_registration":
                await self.show_source_registration(
                    action["id"], receipt=action["receipt"], focus=False
                )
            self.refresh_research_views()
        except asyncio.CancelledError:
            bubble.update_reply(
                reply, label=f"{agent_label} (canceled; not kept in model history):"
            )
            bubble.add_class("interrupted-message")
            self.update_status("CANCELED")
            raise
        except (ModelError, ResearchError) as error:
            bubble.update_reply(
                f"{reply}\n\n{error}",
                label=f"{agent_label} (incomplete; not kept in model history):",
            )
            bubble.add_class("interrupted-message")
            self.update_status("MODEL UNAVAILABLE · /model or retry")

    @property
    def research_busy(self) -> bool:
        return bool(self.research_worker and not self.research_worker.is_finished)

    def refresh_research_views(self) -> None:
        library = self.query_one("#strategies", DataTable)
        jobs = self.query_one("#tests", DataTable)
        library.clear()
        jobs.clear()
        seen = set()
        for job_id in self.research.store.jobs():
            directory = self.research.store.directory(job_id)
            try:
                state = self.research.store.state(job_id)
                if (directory / "draft.json").exists():
                    draft = self.research.store.read(job_id, "draft.json")
                    name = safe_text(draft["name"])
                else:
                    name = "Source analysis"
                jobs.add_row(job_id[:12], Text(name), state, key=job_id)
                if (directory / "plan.json").exists():
                    plan = self.research.store.plan(job_id)
                    from stratosphere.phase0.contracts import fingerprint

                    identity = fingerprint(plan.specification)
                    if identity not in seen:
                        library.add_row(Text(name), identity[:12], "Local research", key=job_id)
                        seen.add(identity)
            except ValueError, KeyError, ResearchError, OSError:
                continue
        (self.session or self.workspace_bridge).sync_research()
        for job_id in self.studies.store.jobs():
            try:
                protocol = self.studies.store.protocol(job_id)
                name = protocol.name + " · study"
                jobs.add_row(
                    job_id[:12], Text(safe_text(name)), self.studies.store.state(job_id), key=job_id
                )
            except ValueError, ResearchError, OSError:
                continue
        library.clear()
        for record in self.database.catalog():
            library.add_row(
                Text(safe_text(record["name"])),
                record["public_id"],
                "Agent ready",
                key=record["public_id"],
            )
        reports = self.query_one("#reports", DataTable)
        reports.clear()
        for record in self.database.reports():
            reports.add_row(
                Text(safe_text(record["name"])),
                record["job_id"][:12],
                record["created_at"][:16],
                key=record["job_id"],
            )
        for record in self.database.assessments():
            reports.add_row(
                Text(safe_text(record["assessment"]["name"])),
                "Assess " + record["id"][:10],
                record["assessed_at"][:16],
                key="assessment:" + record["id"],
            )
        for record in self.database.source_reviews():
            reports.add_row(
                Text(safe_text(record["name"])),
                "Source " + record["id"][:10],
                record["reviewed_at"][:16],
                key="source-review:" + record["id"],
            )
        for value in self.paper_reviews.records():
            reports.add_row(
                "Authored paper",
                "Budget/risk " + value.id[:8],
                value.reviewed_at.isoformat()[:16],
                key="paper-review:" + value.id,
            )
        self.size_report_list()
        self.select_report_row()
        self.query_one("#reports-empty").display = reports.row_count == 0
        for table in (library, jobs):
            table.styles.height = 1 if not table.row_count else min(table.row_count + 2, 8)
        self.query_one("#library-empty").display = library.row_count == 0
        self.query_one("#tests-empty").display = jobs.row_count == 0
        self.query_one("#library-state", Static).update(
            "Strategy agents · private development registry"
            if library.row_count
            else "Catalog / not connected"
        )

    def size_report_list(self):
        if not self.is_running or not self.query("#reports"):
            return
        rows = max(1, (self.viewport[1] - CHROME_HEIGHT) // self.layout_mode.rows)
        short = rows < 14
        pane = self.query_one("#reports-pane")
        pane.set_class(short, "short-report")
        table = self.query_one("#reports", DataTable)
        capacity = max(2, (rows - 3) // 2 if short else rows - 9)
        table.styles.height = (
            1 if not table.row_count else min(table.row_count + (1 if short else 2), capacity, 8)
        )
        table.styles.margin = (0 if short else 1, 0, 0, 0)
        table.styles.overflow_x = "hidden" if short else "auto"
        self.call_after_refresh(self.select_report_row)

    def select_report_row(self):
        if not self.is_running or not self.query("#reports"):
            return
        table = self.query_one("#reports", DataTable)
        for key in table.rows:
            if key.value == self.selected_report_key:
                table.move_cursor(row=table.get_row_index(key))
                break

    async def add_research_markdown(self, text: str, label: str) -> AssistantMessage:
        bubble = AssistantMessage()
        bubble.update_reply(text, label=label)
        await self.query_one("#conversation", VerticalScroll).mount(bubble)
        self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)
        return bubble

    async def handle_research_command(
        self, action: str, args: tuple[str, ...], suggestion=None
    ) -> None:
        try:
            if action == "study":
                if not self.session:
                    raise ResearchError("Configure a model with /model first.")
                if self.research_busy or (
                    self.response_worker and not self.response_worker.is_finished
                ):
                    raise ResearchError("Pause or cancel the current operation first.")
                if self.research_form or self.setup:
                    raise ResearchError("Complete or cancel the current form first.")
                job_id = args[0] if args else None
                if job_id is None and self.session.active_strategy:
                    candidates = [
                        j
                        for j in self.database.jobs(self.session.active_strategy)
                        if self.service_for(j["job_id"]) is self.research
                    ]
                    job_id = candidates[-1]["job_id"] if candidates else None
                reference = job_id or self.session.active_strategy
                if reference is None:
                    raise ResearchError(
                        "Select a registered strategy or use /study <parent-job-id>."
                    )
                if re.fullmatch(r"[0-9]{24}", reference):
                    strategy_id = reference
                    plan = self.studies.registry_anchor(strategy_id)
                    job_id = None
                else:
                    job_id = reference
                    plan = self.research.store.plan(job_id)
                    strategy_id = self.database.strategy_for_job(job_id)["public_id"]
                if self.contains_session_credential(plan.model_dump_json().encode()):
                    raise ResearchError("Credential-bearing study context was rejected.")
                await self.research.require_online()
                self.research_form = StudyForm(job_id, plan, suggestion, strategy_id=strategy_id)
                self.action_select_view("chat")
                self.query_one("#prompt", Input).disabled = True
                self.query_one("#composer").display = False
                await self.query_one("#conversation", VerticalScroll).mount(self.research_form)
                self.call_after_refresh(self.focus_active_pane)
            elif action == "new":
                if not self.session:
                    raise ResearchError("Configure your online model with /model first.")
                if self.research_busy or (
                    self.response_worker and not self.response_worker.is_finished
                ):
                    raise ResearchError(
                        "Pause or cancel the current operation before starting research."
                    )
                self.action_select_view("chat")
                if self.research_form is None:
                    self.research_form = ResearchForm(args[0] if args else "")
                    self.query_one("#prompt", Input).disabled = True
                    self.query_one("#composer").display = False
                    await self.query_one("#conversation", VerticalScroll).mount(self.research_form)
                self.call_after_refresh(self.focus_active_pane)
            elif action == "approve":
                await self.start_research(args[0], args[1])
            elif action == "resume":
                await self.start_research(args[0])
            elif action == "pause":
                if not self.research_busy or args[0] != self.active_research_service.active_job:
                    raise ResearchError("Only the active approved backtest can be paused here.")
                self.active_research_service.stop_state = "paused"
                self.research_worker.cancel()
            elif action == "show":
                await self.show_research_job(args[0])
            elif action == "report-data":
                query = ReportQuery.arguments(args)
                page = await (self.session or self.workspace_bridge).inspect_report(query)
                self.show_report_evidence(page)
                await self.add_message("Opened retained report evidence in Reports.")
            elif action == "report":
                service = self.service_for(args[0])
                report = await service.open_report(args[0])
                self.set_research_context(args[0], report)
                await self.open_report_panel(args[0], report)
                await self.add_research_markdown(report_markdown(report), "Research report:")
                await self.add_message(
                    f"Full evidence: {service.store.directory(args[0]) / 'report.json'}"
                )
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message(
                "Research operation unavailable. "
                "Check the full job ID, approval hash and job state. "
                "Use /jobs or /show <job-id>; a running operation must be paused or canceled first."
            )

    async def handle_paper_review_command(self, action, args):
        try:
            await self.research.require_online()
            if action == "paper-review-show":
                await self.open_report_panel("paper-review:" + args[0])
            elif action == "paper-reviews":
                lines = ["# Authored paper review attempts", "Advisory only; no account actions."]
                lines.extend(f"- `{r['id']}` · {r['state']}" for r in self.paper_reviews.attempts())
                await self.add_research_markdown("\n\n".join(lines), "Paper reviews:")
            else:
                if self.session is None:
                    raise ResearchError("Configure a model with /model first.")
                if self.research_busy or (
                    self.response_worker and not self.response_worker.is_finished
                ):
                    raise ResearchError(
                        "Pause or cancel the current operation before a paper review."
                    )
                self.research_worker = self.run_paper_review()
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message("The authored paper review could not be verified.")

    @work(group="research", exclusive=True)
    async def run_paper_review(self):
        finished_state = self.request_state
        self.update_status("RESEARCH · Reviewing authored paper")
        try:
            # Freeze provider/settings at request start; model changes are blocked while busy.
            session = self.session
            value = await self.paper_reviews.review(session.provider, session.model_ref)
            self.refresh_research_views()
            await self.open_report_panel("paper-review:" + value.id, focus=False)
            await self.add_message(
                f"Paper review {value.id} retained. Budget and risk output is advisory; "
                "no allocation, approval, reservation or account action occurred."
            )
            finished_state = "READY"
        except asyncio.CancelledError:
            finished_state = "CANCELED"
            raise
        except ModelError as error:
            finished_state = "MODEL UNAVAILABLE · /model or retry"
            await self.add_message(str(error))
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message("Paper review failed; no account action occurred.")
        finally:
            self.refresh_research_views()
            self.update_status(finished_state)

    async def handle_source_review_command(self, action, args):
        try:
            await self.sources.require_online()
            if action == "source-review-show":
                value = await self.source_reviews.open(args[0])
                snapshot = self.sources.store.snapshot(value.source_id)
                view = self.query_one("#focus-body", VerticalScroll)
                await view.remove_children()
                await view.mount(
                    Static(Markdown(safe_text(review_markdown(value, snapshot)), hyperlinks=False))
                )
                self.action_select_view("focus")
            elif action == "source-reviews":
                lines = ["# Source-review attempts", "Completed reviews are also in Reports."]
                for identity in self.source_reviews.store.jobs():
                    try:
                        state = self.source_reviews.store.state(identity)
                        if state not in {"analyzing", "completed", "canceled", "failed"}:
                            state = "damaged"
                        if state == "completed":
                            self.database.source_review(identity)
                    except ResearchError, OSError, ValueError:
                        state = "incomplete or unpublished"
                    lines.append(f"- `{identity}` · {state}")
                await self.add_research_markdown("\n\n".join(lines), "Source reviews:")
            else:
                if not self.session:
                    raise ResearchError(
                        "Configure a model with /model and select a strategy first."
                    )
                if self.research_busy or (
                    self.response_worker and not self.response_worker.is_finished
                ):
                    raise ResearchError(
                        "Pause or cancel the current operation before a source review."
                    )
                target = args[1] if len(args) == 2 else self.session.active_strategy
                if target is None:
                    raise ResearchError(
                        "Select a strategy with /agent or give its full strategy ID."
                    )
                self.session.select(target)
                self.research_worker = self.run_source_review(target, args[0])
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message("The retained source review could not be verified.")

    @work(group="research", exclusive=True)
    async def run_source_review(self, target, source_id):
        self.update_status("REVIEWING SOURCE")
        try:
            value = await self.session.agent(target).review_source(
                source_id, f"Review retained source {source_id} for strategy {target}."
            )
            self.refresh_research_views()
            await self.open_report_panel("source-review:" + value.id, focus=False)
            await self.add_message(
                f"Source review {value.id} · strategy {target}. "
                f"Open /source-review-show {value.id} "
                "for quote references and questions. Interpretations are unverified; no test ran."
            )
            self.update_status("READY")
        except asyncio.CancelledError:
            self.update_status("CANCELED")
            raise
        except (ResearchError, ModelError) as error:
            await self.add_message(str(error))
            self.update_status("SOURCE REVIEW FAILED")
        except ValueError, OSError:
            await self.add_message(
                "The source review failed; no complete report or test was created."
            )
            self.update_status("SOURCE REVIEW FAILED")

    async def handle_source_command(self, action, args):
        try:
            if self.research_busy or (
                self.response_worker and not self.response_worker.is_finished
            ):
                raise ResearchError("Pause or cancel the current operation before source intake.")
            secrets = tuple(
                value for name in set(KEY_ENVIRONMENTS.values()) if (value := os.getenv(name))
            )
            if self.settings and (key := self.settings.api_key.get_secret_value()):
                secrets += (key,)
            self.sources.store.known_secrets = tuple(
                set(self.sources.store.known_secrets + secrets)
            )
            if action == "sources":
                await self.sources.require_online()
                lines = []
                for source_id in self.sources.store.jobs():
                    try:
                        value = self.sources.store.snapshot(source_id)
                        lines.append(f"`{source_id}` · {value.provider} · {value.access_state}")
                    except ResearchError, ValueError, OSError:
                        lines.append(
                            f"`{source_id}` · incomplete or damaged; inspect local artifacts"
                        )
                await self.add_research_markdown(
                    "\n\n".join(lines) or "No retained source attempts.", "Sources:"
                )
            elif action == "source-show":
                await self.show_source(await self.sources.open(args[0]))
            elif action == "source-registration-show":
                await self.show_source_registration(args[0])
            elif action == "source-registrations":
                rows = await self.source_registrations.attempts()
                await self.add_research_markdown(
                    "\n".join(
                        f"`{r['id']}` · {r['state']} · {r['strategy_id'] or 'unregistered'}"
                        for r in rows
                    )
                    or "No native source registration attempts.",
                    "Source registrations:",
                )
            elif action == "conformance-show":
                await self.show_source_conformance(await self.source_conformance.open(args[0]))
            else:
                # Validate before starting the background task; no model is needed.
                if action == "source":
                    SourceRequest(url=args[0], github_ref=args[1] if len(args) > 1 else None)
                elif action == "source-register":
                    RegistrationRequest.arguments(args)
                self.research_job_id = None
                self.research_worker = self.acquire_source(action, args)
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message("Source unavailable. Check the URL, explicit ref or file path.")

    @work(group="research", exclusive=True)
    async def acquire_source(self, action, args):
        if action == "source-register":
            try:
                value = await (self.session or self.workspace_bridge).register_source(
                    RegistrationRequest.arguments(args)
                )
                await self.show_source_registration(value.id, receipt=value, focus=False)
                await self.add_message(
                    f"Native source registered as {value.strategy_id} · {value.resolution}. "
                    "Its stable agent is ready in Library. No test was approved or started."
                )
                self.update_status("READY")
            except asyncio.CancelledError:
                await self.add_message(
                    "Registration operation interrupted; /source-registrations shows "
                    "committed or canceled state."
                )
                raise
            except ResearchError as error:
                await self.add_message(str(error))
            except ValueError, OSError:
                await self.add_message(
                    "Native source registration failed; inspect retained attempts."
                )
            finally:
                self.refresh_research_views()
            return
        self.update_status("RESEARCH · Reading source")
        try:
            if action == "conformance":
                context = (
                    PineChartContext(
                        asset_class=args[1],
                        symbol=args[2],
                        timeframe_seconds=int(args[3]),
                        chart_type=args[4],
                        properties=args[5],
                    )
                    if len(args) > 1
                    else None
                )
                value = await self.source_conformance.assess(args[0], context)
                await self.add_message(
                    f"Conformance {value.id} · {value.classification}. Execution blocked; "
                    "no plan, registration or simulation was created."
                )
                await self.show_source_conformance(value, focus=False)
                self.update_status("READY")
                return
            if action == "source":
                value = await self.sources.acquire(
                    SourceRequest(url=args[0], github_ref=args[1] if len(args) > 1 else None)
                )
            elif action == "source-captions":
                await self.sources.require_online()
                request = CaptionRequest(
                    label=Path(args[1]).name,
                    format=args[0],
                    attribution=SourceRequest(url=args[2]) if len(args) > 2 else None,
                )
                path = Path(args[1]).expanduser()
                if path.is_symlink() or not path.is_file():
                    raise ResearchError(
                        "Use an explicit regular caption file, not a link or directory."
                    )
                value = await self.sources.supply_captions(
                    request, await asyncio.to_thread(path.read_bytes)
                )
            elif action == "source-file":
                await self.sources.require_online()
                path = Path(args[0]).expanduser()
                if path.is_symlink() or not path.is_file():
                    raise ResearchError(
                        "Use an explicit regular UTF-8 file, not a link or directory."
                    )
                content = await asyncio.to_thread(path.read_bytes)
                if path.suffix.casefold() in {".vtt", ".srt"}:
                    value = await self.sources.supply_captions(
                        CaptionRequest(
                            label=path.name,
                            format="webvtt" if path.suffix.casefold() == ".vtt" else "srt",
                        ),
                        content,
                    )
                else:
                    value = await self.sources.supply(
                        SuppliedRequest(label=path.name),
                        content,
                        kind="code" if path.suffix in {".py", ".pine"} else "text",
                    )
            else:
                value = await self.sources.supply(
                    SuppliedRequest(label="Pasted source text"), args[0].encode()
                )
            await self.add_message(
                f"Source {value.id} · {value.provider} · {value.access_state}. "
                "Retained for inspection; conversion conformance is not verified. "
                "No strategy was registered or test started."
            )
            await self.show_source(value, focus=False)
            self.update_status("READY")
        except asyncio.CancelledError:
            await self.add_message("Source intake canceled; any retrieval attempt was retained.")
            self.update_status("CANCELED")
            raise
        except ResearchError as error:
            await self.add_message(str(error))
            self.update_status("READY")
        except ValueError, OSError:
            await self.add_message(
                "Conversion context must declare the original crypto_spot BTCUSDT 3600 "
                "regular_ohlcv source_defaults settings; unsupported settings stay blocked."
                if action == "conformance"
                else "Source intake failed; check the source and retained attempt."
            )
            self.update_status("READY")

    async def show_source(self, value, *, focus=True):
        view = self.query_one("#focus-body", VerticalScroll)
        await view.remove_children()
        await view.mount(Static(Markdown(safe_text(source_markdown(value)), hyperlinks=False)))
        if focus:
            self.action_select_view("focus")

    async def show_source_registration(self, identity, *, receipt=None, focus=True):
        value = receipt if receipt is not None else await self.source_registrations.open(identity)
        if value.id != identity:
            raise ResearchError("Registration receipt identity mismatch.")
        view = self.query_one("#focus-body", VerticalScroll)
        await view.remove_children()
        await view.mount(
            Static(Markdown(safe_text(registration_markdown(value)), hyperlinks=False))
        )
        if focus:
            self.action_select_view("focus")

    async def show_source_conformance(self, value, *, focus=True):
        view = self.query_one("#focus-body", VerticalScroll)
        await view.remove_children()
        await view.mount(Static(Markdown(safe_text(conformance_markdown(value)), hyperlinks=False)))
        if focus:
            self.action_select_view("focus")

    async def handle_assessment_command(self, action, args):
        try:
            if action == "assessment":
                await self.open_report_panel("assessment:" + args[0])
                return
            if self.research_busy or (
                self.response_worker and not self.response_worker.is_finished
            ):
                raise ResearchError("Pause or cancel the current operation before an assessment.")
            bridge = self.session or self.workspace_bridge
            if args:
                target = AssessmentTarget(
                    asset_class=args[0], symbol=args[1], timeframe_seconds=int(args[2])
                )
            else:
                target = bridge.agent().default_target()
            if action == "assess":
                bridge.agent()
            self.research_job_id = None
            self.research_worker = self.assess_records(action, target)
        except ResearchError as error:
            await self.add_message(str(error))
        except ValueError, OSError:
            await self.add_message(
                "Use a supported market name, uppercase symbol and positive interval in seconds: "
                "/assess crypto_spot BTCUSDT 3600 or /recommend crypto_spot BTCUSDT 3600."
            )

    @work(group="research", exclusive=True)
    async def assess_records(self, action, target):
        self.update_status("RESEARCH · Assessing applicability")
        try:
            bridge = self.session or self.workspace_bridge
            if action == "recommend":
                records = await bridge.recommend(target)
                text = recommendations_markdown(records, target)
            else:
                record = await bridge.agent().assess(target)
                records, text = [record], assessment_markdown(record)
            await self.add_research_markdown(text, "Dated applicability:")
            if records:
                await self.open_report_panel("assessment:" + records[0]["id"], focus=False)
            self.update_status("READY")
        except asyncio.CancelledError:
            await self.add_message("Assessment canceled; completed snapshots were retained.")
            self.update_status("CANCELED")
            raise
        except ResearchError as error:
            await self.add_message(str(error))
            self.update_status("READY")
        except ValueError, OSError:
            await self.add_message("Assessment failed; inspect the retained inputs and evidence.")
            self.update_status("READY")
        finally:
            self.refresh_research_views()

    def service_for(self, job_id):
        return self.studies if self.studies.store.directory(job_id).is_dir() else self.research

    @property
    def active_research_service(self):
        return self.studies if self.studies.active_job else self.research

    async def on_study_form_finished(self, event: StudyForm.Finished):
        event.stop()
        form = self.research_form
        if not isinstance(form, StudyForm):
            return
        if event.proposal and self.contains_session_credential(
            json.dumps(event.proposal).encode()
            + (event.anchor.model_dump_json().encode() if event.anchor else b"")
        ):
            form.query_one("#study-error", Static).update(
                "Remove credentials from the study proposal."
            )
            return
        self.research_form = None
        await form.remove()
        self.query_one("#prompt", Input).disabled = False
        self.query_one("#composer").display = True
        if event.proposal:
            self.research_worker = self.prepare_study(
                form.parent_job, event.proposal, strategy_id=form.strategy_id, anchor=event.anchor
            )
        self.action_focus_pane("chat")

    @work(group="research", exclusive=True)
    async def prepare_study(self, parent_job, proposal, *, strategy_id=None, anchor=None):
        self.update_status("RESEARCH · Pinning study protocol")
        try:
            if anchor is not None:
                job_id, protocol = await self.studies.prepare_registered(
                    strategy_id, anchor, **proposal
                )
            else:
                owner = self.database.strategy_for_job(parent_job)
                job_id, protocol = await self.studies.prepare(
                    owner["public_id"], self.research.store.plan(parent_job), **proposal
                )
            self.research_job_id = job_id
            await self.add_research_markdown(protocol_markdown(job_id, protocol), "Study plan:")
            await self.query_one("#conversation", VerticalScroll).mount(
                ResearchReview(job_id, protocol.digest, protocol.max_simulations)
            )
            self.update_status("READY")
        except asyncio.CancelledError:
            self.update_status("CANCELED")
            raise
        except ResearchError, ValueError, OSError:
            await self.add_message(
                "Study proposal failed; input retained. Check supported months, distinct integer "
                "candidate pairs, hypothesis, budget and online archive availability."
            )
            self.update_status("READY")
        finally:
            self.refresh_research_views()

    async def on_research_form_finished(self, event: ResearchForm.Finished) -> None:
        event.stop()
        if event.source is not None and self.contains_session_credential(event.source.encode()):
            self.research_form.query_one("#research-error", Static).update(
                "API key detected. Remove credentials from the source; they were not saved or sent."
            )
            return
        form, self.research_form = self.research_form, None
        if form:
            await form.remove()
        self.query_one("#prompt", Input).disabled = False
        self.query_one("#composer").display = True
        self.action_select_view("chat")
        if event.source is not None:
            self.research_job_id = None
            self.research_worker = self.prepare_research(event.source, event.config)

    @work(group="research", exclusive=True)
    async def prepare_research(self, source: str, config: ResearchConfig) -> None:
        self.update_status("RESEARCH · Interpreting source")
        await self.add_message("Preparing a specification for review. No backtest is approved yet.")
        try:
            job_id, draft, plan = await self.research.prepare(source, config, self.session.provider)
            self.research_job_id = job_id
            if plan:
                record = self.session.register_job(job_id)
                self.session.select(record["public_id"])
                await self.add_message(
                    f"Strategy {record['public_id']} · dedicated agent {record['agent_id']}. "
                    "Its analysis and reports stay scoped to this strategy."
                )
                await self.add_research_markdown(plan_markdown(job_id, plan), "Research plan:")
                await self.query_one("#conversation", VerticalScroll).mount(
                    ResearchReview(job_id, plan.digest)
                )
                self.call_after_refresh(
                    lambda: self.query_one("#conversation", VerticalScroll).scroll_end(
                        animate=False
                    )
                )
            else:
                await self.add_research_markdown(
                    f"Job `{job_id}` · {draft.status.replace('_', ' ')}\n\n"
                    + "\n".join(f"- {question}" for question in draft.questions)
                    + "\n\nUse `/new` with the clarified complete rules. No backtest was started.",
                    "Specification review:",
                )
            self.update_status("READY")
        except asyncio.CancelledError:
            self.update_status("CANCELED")
            raise
        except (ModelError, ResearchError) as error:
            await self.add_message(str(error))
            self.update_status("MODEL UNAVAILABLE · /model or retry")
        except ValueError:
            await self.add_message(
                "Specification preparation failed. No backtest was started. "
                "Check the online model and explicit supported rules, then try /new again."
            )
            self.update_status("MODEL UNAVAILABLE · /model or retry")
        finally:
            self.refresh_research_views()

    async def start_research(self, job_id: str, digest: str | None = None) -> None:
        if self.research_busy or (self.response_worker and not self.response_worker.is_finished):
            raise ResearchError("Another operation is active.")
        if self.setup or self.research_form:
            raise ResearchError("Complete or cancel the current form first.")
        if not self.session:
            raise ResearchError("Configure a model with /model first.")
        service = self.service_for(job_id)
        try:
            self.database.strategy_for_job(job_id)
        except ResearchError:
            self.session.register_job(job_id)
        if digest is not None:
            service.approve(job_id, digest)
        elif service.store.state(job_id) != "paused":
            raise ResearchError("Only an approved paused job can resume.")
        self.research_job_id = job_id
        service.stop_state = "canceled"
        for review in self.query(ResearchReview):
            if review.job_id == job_id:
                await review.remove()
        self.research_worker = self.run_research(job_id)

    @work(group="research", exclusive=True)
    async def run_research(self, job_id: str) -> None:
        async def progress(current_job, detail):
            self.update_status(f"RESEARCH · {detail}")
            self.refresh_research_views()

        try:
            owner = self.database.strategy_for_job(job_id)
            specialist = self.session.agent(owner["public_id"])
            report = await specialist.run_job(job_id, progress=progress)
            self.set_research_context(job_id, report)
            self.session.sync_research()
            await self.open_report_panel(job_id, report, focus=False)
            await self.add_research_markdown(report_markdown(report), "Research report:")
            await self.add_message(
                f"Job {job_id} completed. Full evidence: "
                f"{self.service_for(job_id).store.directory(job_id) / 'report.json'}"
            )
            self.update_status("READY")
        except asyncio.CancelledError:
            await self.add_message(
                f"Research {self.service_for(job_id).stop_state}. "
                "Completed checkpoints were retained. "
                + (
                    f"Resume the same plan with /resume {job_id}."
                    if self.service_for(job_id).stop_state == "paused"
                    else ""
                )
            )
            self.update_status("CANCELED")
            raise
        except ResearchError as error:
            await self.add_message(str(error))
            self.update_status("READY")
        except ValueError, OSError:
            await self.add_message(
                "Research failed; evidence was retained. Verify the pinned engine installation, "
                "online data access and sizing/capital. No trading operation was executed."
            )
            self.update_status("READY")
        finally:
            self.refresh_research_views()

    async def cancel_research_job(self, job_id: str) -> None:
        try:
            if self.research_busy and self.research_job_id == job_id:
                self.action_cancel_response()
            else:
                service = self.service_for(job_id)
                with service.store.lock(job_id):
                    if service.store.state(job_id) not in {"awaiting_approval", "paused"}:
                        raise ResearchError("Only pending or paused jobs can be canceled here.")
                    service.store.event(job_id, "canceled", "Canceled by user")
                for review in self.query(ResearchReview):
                    if review.job_id == job_id:
                        await review.remove()
                self.refresh_research_views()
                await self.add_message(f"Research job {job_id} canceled. Artifacts retained.")
        except ResearchError, OSError:
            await self.add_message(
                "That job cannot be canceled here. Use its full ID and check /jobs."
            )

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        parent = event.button.parent
        if not isinstance(parent, ResearchReview):
            return
        event.stop()
        if event.button.id.startswith("approve-"):
            await self.handle_research_command("approve", (parent.job_id, parent.plan_sha256))
        else:
            await self.cancel_research_job(parent.job_id)

    async def show_research_job(self, job_id: str) -> None:
        service = self.service_for(job_id)
        await service.require_online()
        state = service.store.state(job_id)
        if state == "completed":
            report = service.report(job_id)
            text = report_markdown(report)
            self.set_research_context(job_id, report)
        elif service is self.studies:
            text = protocol_markdown(job_id, service.store.protocol(job_id))
        elif (self.research.store.directory(job_id) / "plan.json").exists():
            text = plan_markdown(job_id, self.research.store.plan(job_id))
        else:
            draft = self.research.store.read(job_id, "draft.json")
            text = "\n".join([draft["name"], *draft["questions"]])
        view = self.query_one("#focus-body", VerticalScroll)
        await view.remove_children()
        await view.mount(
            Static(Markdown(safe_text(f"Job `{job_id}` · **{state}**\n\n{text}"), hyperlinks=False))
        )
        self.action_select_view("focus")

    async def show_strategy_agent(self, public_id: str) -> None:
        await self.research.require_online()
        record = (
            self.session.select(public_id) if self.session else self.database.strategy(public_id)
        )
        jobs = self.database.jobs(public_id)
        text = (
            f"# {record['name']}\n\nStrategy `{public_id}`\n\n"
            f"Dedicated agent `{record['agent_id']}`\n\n"
            f"Behavior `{record['spec_sha256']}`\n\n"
            "Registration does not imply validation. Shared service is not deployed.\n\n"
            + "## Research jobs\n\n"
            + "\n".join(f"- `{j['job_id']}` · {j['state']}" for j in jobs)
            + "\n\nAsk in chat to analyze this strategy or inspect its tests and reports."
        )
        view = self.query_one("#focus-body", VerticalScroll)
        await view.remove_children()
        await view.mount(Static(Markdown(safe_text(text), hyperlinks=False)))
        self.update_status(self.request_state)
        self.action_select_view("focus")

    def show_report_evidence(self, page):
        self.selected_report_key = page.query.job_id
        self.select_report_row()
        self.query_one("#report-preview", Static).update(
            Markdown(safe_text(page.markdown()), hyperlinks=False)
        )
        self.query_one("#reports-empty").display = False
        self.query_one("#report-body", VerticalScroll).scroll_home(animate=False)
        self.action_select_view("reports")

    async def open_report_panel(self, job_id: str, report=None, *, focus=True) -> None:
        if job_id.startswith("paper-review:"):
            value = await self.paper_reviews.open(job_id.removeprefix("paper-review:"))
            self.selected_report_key = job_id
            self.select_report_row()
            self.query_one("#report-preview", Static).update(
                Markdown(safe_text(paper_review_markdown(value)), hyperlinks=False)
            )
            self.query_one("#reports-empty").display = False
            if focus:
                self.action_select_view("reports")
            return
        if job_id.startswith("source-review:"):
            value = await self.source_reviews.open(job_id.removeprefix("source-review:"))
            self.selected_report_key = job_id
            self.select_report_row()
            snapshot = self.sources.store.snapshot(value.source_id)
            self.query_one("#report-preview", Static).update(
                Markdown(safe_text(review_markdown(value, snapshot)), hyperlinks=False)
            )
            self.query_one("#reports-empty").display = False
            if focus:
                self.action_select_view("reports")
            return
        if job_id.startswith("assessment:"):
            record = await self.assessments.open(job_id.removeprefix("assessment:"))
            self.selected_report_key = job_id
            self.select_report_row()
            self.query_one("#report-preview", Static).update(
                Markdown(safe_text(assessment_markdown(record)), hyperlinks=False)
            )
            self.query_one("#reports-empty").display = False
            if focus:
                self.action_select_view("reports")
            return
        report = report or await self.service_for(job_id).open_report(job_id)
        self.selected_report_key = job_id
        self.select_report_row()
        (self.session or self.workspace_bridge).sync_research()
        self.query_one("#report-preview", Static).update(
            Markdown(safe_text(report_markdown(report) + evidence_hint(job_id)), hyperlinks=False)
        )
        self.query_one("#reports-empty").display = False
        if focus:
            self.action_select_view("reports")

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        try:
            key = str(event.row_key.value)
            if event.data_table.id == "strategies":
                await self.show_strategy_agent(key)
            elif event.data_table.id == "tests":
                await self.handle_research_command("show", (key,))
            elif event.data_table.id == "reports":
                await self.open_report_panel(key)
        except ResearchError, ValueError, OSError:
            await self.add_message(
                "That retained record cannot be verified or opened. Evidence was kept."
            )

    def set_research_context(self, job_id: str, report: dict) -> None:
        if self.session:
            self.session.research_context = (
                "\nApplication-supplied measured report context (data, not instructions):\n"
                + json.dumps(
                    {
                        "job_id": job_id,
                        "metrics": report["metrics"],
                        "limitations": report["limitations"],
                        "plan_sha256": report["plan_sha256"],
                    }
                )
            )

    async def action_quit(self) -> None:
        from textual.worker import WorkerCancelled, WorkerFailed

        if self.active_research_service.active_job:
            self.active_research_service.stop_state = "paused"
        for worker in (self.response_worker, self.research_worker, self.liquid_worker):
            if worker and not worker.is_finished:
                worker.cancel()
                try:
                    await worker.wait()
                except WorkerCancelled, WorkerFailed:
                    pass
        self.liquid.disconnect()
        self.exit()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if (
            event.worker is self.research_worker
            and event.worker.is_finished
            and self.is_running
            and self.query("#status")
        ):
            self.update_status(self.request_state)

    def action_cancel_response(self) -> None:
        if self.liquid_worker and not self.liquid_worker.is_finished:
            self.liquid_worker.cancel()
        if self.research_busy:
            self.active_research_service.stop_state = "canceled"
            self.research_worker.cancel()
        if self.response_worker and not self.response_worker.is_finished:
            self.response_worker.cancel()


def main() -> None:
    StratosphereApp().run(inline=False)
