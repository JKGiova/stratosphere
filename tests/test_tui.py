import asyncio
import io
from pathlib import Path

import pytest
from rich.console import Console
from textual.widgets import DataTable, Input, Static, Tabs

from stratosphere.models.nim import ModelError
from stratosphere.tui import AssistantMessage, ModelSetup, StratosphereApp
from stratosphere.workspace import choose_layout


class FakeProvider:
    def __init__(self, settings):
        self.settings = settings
        self.requests = []
        self.wait = False
        self.release = asyncio.Event()
        self.fail = False
        self.deltas = ["Hello [literal]"]

    async def stream(self, messages):
        self.requests.append(messages)
        for delta in self.deltas:
            yield delta
        if self.wait:
            await self.release.wait()
        if self.fail:
            raise ModelError("Model request timed out. Please retry; no operation was executed.")


async def configure(app, pilot):
    assert isinstance(app.setup, ModelSetup)
    assert app.setup.parent is app.query_one("#conversation")
    await pilot.pause(0.3)  # Let Textual's button activation effect finish after validation.
    app.query_one("#api-key", Input).value = "test-only-key"
    app.query_one("#api-key", Input).focus()
    await pilot.press("enter")
    await pilot.pause()
    assert app.session is not None and app.setup is None


async def send(app, pilot, text):
    field = app.query_one("#prompt", Input)
    field.value = text
    field.focus()
    await pilot.press("enter")
    await pilot.pause()


def visible(app):
    return "\n".join(
        f"{widget.label} {widget.source}"
        if isinstance(widget, AssistantMessage)
        else str(widget.content)
        for widget in app.query(".message")
    )


def shown(app):
    return [pane for pane in app.query(".pane") if pane.display]


@pytest.mark.asyncio
async def test_onboarding_chat_commands_resize_and_quit(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(100, 36)) as pilot:
        app.query_one("#use-model").focus()
        await pilot.pause()
        await pilot.click("#use-model")
        assert "nonempty" in str(app.query_one("#setup-error", Static).content)
        await configure(app, pilot)
        await send(app, pilot, "Hi [text]")
        assert "You: Hi [text]" in visible(app)
        assert "Assistant: Hello [literal]" in visible(app)
        assert len(app.session.history) == 2
        for command, pane in [
            ("/strategies", "library"),
            ("/jobs", "jobs"),
            ("/portfolio", "portfolio"),
        ]:
            await send(app, pilot, command)
            assert app.active_view == pane and app.query_one(f"#{pane}-pane").display
        assert len(app.session.provider.requests) == 1
        await send(app, pilot, "please use test-only-key")
        assert "test-only-key" not in visible(app)
        assert len(app.session.provider.requests) == 1
        await send(app, pilot, "/help")
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        field = app.query_one("#prompt", Input)
        assert field.region.height > 0 and field.region.bottom <= 12
        Path("reports").mkdir(exist_ok=True)
        Path("reports/phase1-tui.svg").write_text(app.export_screenshot())
        await send(app, pilot, "/clear")
        assert app.session.history == []
        assert "Hi [text]" not in visible(app)
        await pilot.press("ctrl+q")
    assert not app.is_running


@pytest.mark.asyncio
async def test_busy_cancellation_and_retry_without_partial_history():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "first")
        await send(app, pilot, "second")
        assert app.query_one("#prompt", Input).value == "second"
        assert len(app.session.provider.requests) == 1
        await send(app, pilot, "/model")
        assert app.setup is None
        await send(app, pilot, "/cancel")
        assert "canceled" in visible(app)
        assert app.session.history == []
        app.session.provider.wait = False
        app.session.provider.fail = True
        await send(app, pilot, "retry")
        assert "timed out" in visible(app)
        assert app.session.history == []
        app.session.provider.fail = False
        await send(app, pilot, "success")
        assert len(app.session.history) == 2
        assert app.session.history[0]["content"] == "success"
        app.session.provider.wait = True
        await send(app, pilot, "cancel from the input")
        assert app.query_one("#prompt", Input).has_focus
        await pilot.press("ctrl+k")
        await pilot.pause()
        assert app.request_state == "CANCELED"
        assert len(app.session.history) == 2


@pytest.mark.asyncio
async def test_cancel_inline_onboarding_blocks_chat_and_can_reopen(monkeypatch):
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        screen = app.screen
        setup = app.setup
        remove = setup.remove

        async def remove_with_pending_focus():
            await remove()
            app.focus_active_pane()

        monkeypatch.setattr(setup, "remove", remove_with_pending_focus)
        await pilot.press("escape")
        await send(app, pilot, "hello")
        assert "Configure your online model" in visible(app)
        assert app.query_one("#prompt", Input).value == "hello"
        await send(app, pilot, "/model")
        assert app.screen is screen and len(app.screen_stack) == 1
        await configure(app, pilot)
        assert "test-only-key" not in app.export_screenshot()


@pytest.mark.asyncio
@pytest.mark.parametrize("onboarding", [False, True])
async def test_quit_cancels_active_response_or_inline_setup(onboarding):
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        if not onboarding:
            await configure(app, pilot)
            app.session.provider.wait = True
            await send(app, pilot, "question")
        await pilot.press("ctrl+q")
    assert not app.is_running
    if not onboarding:
        assert app.session.history == [] and app.session.busy is False


@pytest.mark.asyncio
async def test_small_terminal_inline_onboarding_with_keyboard():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(45, 12)) as pilot:
        field = app.query_one("#model-id", Input)
        field.value = "my-selected-model:free"
        field.focus()
        await pilot.press("enter")
        key_field = app.query_one("#api-key", Input)
        assert key_field.has_focus
        assert key_field.region.y >= app.query_one("#conversation").region.y
        assert key_field.region.bottom <= app.query_one("#conversation").region.bottom
        key_field.value = "test-only-key"
        await pilot.press("enter")
        await pilot.pause()
        assert app.settings.model == "my-selected-model:free"
        assert app.query_one("#prompt", Input).has_focus


@pytest.mark.parametrize(
    "size,name",
    [
        ((144, 32), "six"),
        ((143, 32), "four"),
        ((144, 31), "two-horizontal"),
        ((96, 32), "four"),
        ((95, 32), "two-vertical"),
        ((96, 31), "two-horizontal"),
        ((96, 18), "two-horizontal"),
        ((96, 17), "one"),
        ((48, 32), "two-vertical"),
        ((47, 32), "one"),
        ((48, 31), "one"),
        ((45, 12), "one"),
    ],
)
def test_layout_thresholds(size, name):
    assert choose_layout(*size).name == name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size,count",
    [
        ((180, 48), 6),
        ((144, 32), 6),
        ((120, 36), 4),
        ((96, 32), 4),
        ((120, 24), 2),
        ((96, 18), 2),
        ((60, 36), 2),
        ((48, 32), 2),
        ((45, 12), 1),
    ],
)
async def test_responsive_panels_fit_without_overlap_and_keep_truthful_states(size, count):
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=size) as pilot:
        await configure(app, pilot)
        assert len(list(app.query(".pane"))) == 6
        assert len(shown(app)) == count
        for index, pane in enumerate(shown(app)):
            assert pane.region.width > 0 and pane.region.height > 0
            assert pane.region.right <= size[0] and pane.region.bottom < size[1]
            for other in shown(app)[index + 1 :]:
                assert not pane.region.overlaps(other.region)
        prompt = app.query_one("#prompt", Input)
        assert prompt.region.height == 1 and prompt.region.bottom < size[1]
        for table in app.query(DataTable):
            assert table.row_count == 0
        if size in {(120, 24), (96, 18)}:
            assert shown(app)[0].region.y == shown(app)[1].region.y
        if size in {(60, 36), (48, 32)}:
            assert shown(app)[0].region.x == shown(app)[1].region.x
        for key, pane in [("f1", "library"), ("f2", "focus"), ("f3", "chat"), ("f4", "jobs")]:
            await pilot.press(key)
            await pilot.pause()
            assert app.active_view == pane
            assert app.query_one(f"#{pane}-pane").display
            assert app.focused.id == ("prompt" if pane == "chat" else f"{pane}-pane")
            assert len(shown(app)) == count
        await send(app, pilot, "/portfolio")
        heading = app.query_one("#portfolio-pane .empty-heading", Static)
        assert heading.content == "Positions unknown" and heading.region.height > 0
        assert app.session.provider.requests == []


@pytest.mark.asyncio
async def test_every_layout_transition_preserves_draft_history_and_selected_view():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(180, 48)) as pilot:
        await configure(app, pilot)
        await send(app, pilot, "a completed turn")
        prompt = app.query_one("#prompt", Input)
        prompt.value = "unfinished [draft]"
        prompt.cursor_position = 5
        await pilot.press("tab", "tab")
        assert app.active_view == "portfolio"
        for width, height, count in [
            (120, 36, 4),
            (60, 36, 2),
            (120, 24, 2),
            (45, 12, 1),
            (180, 48, 6),
        ]:
            await pilot.resize_terminal(width, height)
            await pilot.pause()
            assert app.active_view == "portfolio" and app.focused.id == "portfolio-pane"
            assert len(shown(app)) == count and app.query_one("#portfolio-pane").display
            assert prompt.value == "unfinished [draft]" and prompt.cursor_position == 5
            assert len(app.session.history) == 2
        await pilot.press("f3")
        assert prompt.has_focus
        await pilot.press("f1", "slash")
        assert prompt.has_focus and prompt.value == "unfinished [draft]"
        prompt.value = ""
        await pilot.press("f2", "slash", "h")
        assert prompt.value == "/h"


@pytest.mark.asyncio
async def test_response_does_not_steal_focus_and_session_state_updates():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "a running turn")
        await pilot.press("f2")
        assert app.active_view == "focus" and app.request_state == "RESPONDING"
        await pilot.press("ctrl+k")
        await pilot.pause()
        assert app.active_view == "focus" and app.request_state == "CANCELED"
        assert "Completed turns    0" in str(app.query_one("#session-details", Static).content)
        app.session.provider.wait = False
        await send(app, pilot, "a complete turn")
        assert app.request_state == "READY"
        assert "Completed turns    1" in str(app.query_one("#session-details", Static).content)
        await send(app, pilot, "/clear")
        assert "Completed turns    0" in str(app.query_one("#session-details", Static).content)


@pytest.mark.asyncio
async def test_stream_resize_and_completion_preserve_other_panel_focus():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(180, 48)) as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "finish in the background")
        await pilot.press("tab", "tab", "tab")
        assert app.active_view == "session"
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        app.session.provider.release.set()
        await app.response_worker.wait()
        await pilot.pause()
        assert app.request_state == "READY" and len(app.session.history) == 2
        assert app.active_view == "session" and app.focused.id == "session-pane"
        assert app.query_one("#session-pane").display
        assert not app.query_one("#chat-pane").display
        await pilot.resize_terminal(180, 48)
        await pilot.pause()
        assert len(shown(app)) == 6 and app.active_view == "session"


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(180, 48), (120, 36), (120, 24), (60, 36), (45, 12)])
async def test_tab_cycles_all_views_both_directions_without_losing_draft(size):
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=size) as pilot:
        await configure(app, pilot)
        field = app.query_one("#prompt", Input)
        field.value = "draft [keep literally]"
        field.cursor_position = 5
        for key, views in [
            ("tab", ["jobs", "portfolio", "session", "library", "focus", "chat"]),
            ("shift+tab", ["focus", "library", "session", "portfolio", "jobs", "chat"]),
        ]:
            for view in views:
                await pilot.press(key)
                await pilot.pause()
                assert app.active_view == view
                assert app.query_one("#navigation", Tabs).active == f"nav-{view}"
                assert app.query_one(f"#{view}-pane").display
            assert field.has_focus and field.value == "draft [keep literally]"
            assert field.cursor_position == 5
        assert app.session.provider.requests == []


@pytest.mark.asyncio
async def test_setup_tab_navigation_stays_in_inline_form_and_can_be_canceled():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(45, 12)) as pilot:
        screen = app.screen
        model = app.query_one("#model-id", Input)
        key = app.query_one("#api-key", Input)
        model.focus()
        await pilot.press("tab")
        assert app.query_one("#base-url", Input).has_focus and app.screen is screen
        await pilot.press("tab")
        assert key.has_focus
        await pilot.press("shift+tab", "shift+tab")
        assert model.has_focus and app.active_view == "chat"
        assert app.query_one("#prompt", Input).disabled
        await configure(app, pilot)
        await send(app, pilot, "/model")
        app.query_one("#model-id", Input).focus()
        await pilot.press("tab")
        assert app.query_one("#base-url", Input).has_focus
        await pilot.press("escape")
        await pilot.pause()
        assert app.setup is None and app.session is not None
        await pilot.press("tab")
        assert app.active_view == "jobs"


@pytest.mark.asyncio
async def test_navigation_mouse_commands_and_inert_brand():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(180, 48)) as pilot:
        await configure(app, pilot)
        screen = app.screen
        await pilot.click("#brand")
        await pilot.pause()
        assert app.screen is screen and len(app.screen_stack) == 1
        assert not app.query("Header")
        await pilot.click("#nav-portfolio")
        assert app.active_view == "portfolio"
        await pilot.click("#strategies")
        assert app.active_view == "library"
        assert app.query_one("#navigation", Tabs).active == "nav-library"
        assert app.focused.id == "strategies"
        await send(app, pilot, "/jobs")
        assert app.query_one("#navigation", Tabs).active == "nav-jobs"
        await pilot.press("shift+tab")
        assert app.active_view == "chat" and app.query_one("#prompt", Input).has_focus
        assert app.session.provider.requests == []


@pytest.mark.asyncio
async def test_inline_setup_resize_retains_masked_values_and_keyboard_access():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(180, 48)) as pilot:
        screen = app.screen
        setup = app.setup
        model = setup.query_one("#model-id", Input)
        key = setup.query_one("#api-key", Input)
        model.value = "onboarding-resize:free"
        key.value = "onboarding-only-secret"
        key.focus()
        for width, height in [(120, 36), (60, 36), (120, 24), (45, 12), (180, 48)]:
            await pilot.resize_terminal(width, height)
            await pilot.pause()
            assert app.screen is screen and app.setup is setup
            assert setup.parent is app.query_one("#conversation")
            assert key.password and key.has_focus and key.value == "onboarding-only-secret"
            assert model.value == "onboarding-resize:free"
            assert key.value not in app.export_screenshot()
            conversation = app.query_one("#conversation")
            assert key.region.y >= conversation.region.y
            assert key.region.bottom <= conversation.region.bottom
        await pilot.press("enter")
        await pilot.pause()
        assert app.settings.model == "onboarding-resize:free"
        assert app.query_one("#prompt", Input).has_focus
        assert app.session.provider.requests == []


@pytest.mark.asyncio
async def test_inline_provider_change_does_not_reuse_another_providers_key(monkeypatch):
    from textual.widgets import Select

    monkeypatch.setenv("NVIDIA_API_KEY", "nvidia-test-key")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        app.query_one("#api-key", Input).value = "openrouter-typed-key"
        app.query_one("#provider", Select).value = "nvidia"
        await pilot.pause()
        assert app.query_one("#api-key", Input).value == "nvidia-test-key"
        assert "NVIDIA" in str(app.query_one("#privacy-notice", Static).content)
        app.query_one("#provider", Select).value = "openrouter"
        await pilot.pause()
        assert app.query_one("#api-key", Input).value == ""
        await pilot.press("escape")
        assert app.setup is None
        assert "nvidia-test-key" not in app.export_screenshot()


@pytest.mark.asyncio
async def test_streamed_markdown_formats_headings_lists_tables_and_code_without_rich_injection():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(180, 48)) as pilot:
        await configure(app, pilot)
        app.session.provider.deltas = [
            "# Research plan\n\n**Bold** and `inline` [literal]\n\n- First\n- Second\n\n",
            "| Instrument | State |\n| --- | --- |\n| EUR/USD | Planned |\n\n```python\n",
            'print("example")\n```\n\n[bold red]literal Rich tags[/bold red]',
        ]
        app.session.provider.wait = True
        await send(app, pilot, "Show Markdown")
        bubble = app.query_one(AssistantMessage)
        console = Console(file=io.StringIO(), width=70, record=True, force_terminal=False)
        console.print(bubble.content)
        text = console.export_text()
        assert "Research plan" in text and "# Research plan" not in text
        assert "**Bold**" not in text and "Bold" in text
        assert "`inline`" not in text and "inline" in text
        assert "```" not in text and 'print("example")' in text
        assert "Instrument" in text and "EUR/USD" in text and "| --- |" not in text
        assert "[bold red]literal Rich tags[/bold red]" in text
        assert "[literal]" in text
        assert any(
            segment.style and segment.style.bold for segment in console.render(bubble.content)
        )
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        app.session.provider.release.set()
        await app.response_worker.wait()
        assert app.request_state == "READY"
        assert app.session.history[-1]["content"] == "".join(app.session.provider.deltas)
        assert app.query_one("#prompt", Input).region.bottom < 12
