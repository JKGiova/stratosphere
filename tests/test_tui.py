import asyncio
from pathlib import Path

import pytest
from textual.widgets import DataTable, Input, Static, TabbedContent, Tabs

from stratosphere.models.nim import ModelError
from stratosphere.tui import ModelSetup, StratosphereApp


class FakeProvider:
    def __init__(self, settings):
        self.settings = settings
        self.requests = []
        self.wait = False
        self.release = asyncio.Event()
        self.fail = False

    async def stream(self, messages):
        self.requests.append(messages)
        yield "Hello [literal]"
        if self.wait:
            await self.release.wait()
        if self.fail:
            raise ModelError("Model request timed out. Please retry; no operation was executed.")


async def configure(app, pilot):
    assert isinstance(app.screen, ModelSetup)
    await pilot.pause(0.3)  # Let Textual's button activation effect finish after validation.
    app.screen.query_one("#api-key", Input).value = "test-only-key"
    app.screen.query_one("#use-model").focus()
    await pilot.press("enter")
    await pilot.pause()
    assert app.session is not None


async def send(app, pilot, text):
    field = app.query_one("#prompt", Input)
    field.value = text
    field.focus()
    await pilot.press("enter")
    await pilot.pause()


def visible(app):
    return "\n".join(str(widget.content) for widget in app.query(".message"))


@pytest.mark.asyncio
async def test_onboarding_chat_commands_resize_and_quit(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.click("#use-model")
        assert "nonempty" in str(app.screen.query_one("#setup-error", Static).content)
        await configure(app, pilot)
        await send(app, pilot, "Hi [text]")
        assert "You: Hi [text]" in visible(app)
        assert "Assistant: Hello [literal]" in visible(app)
        assert len(app.session.history) == 2
        for command, pane, tab in [
            ("/strategies", "library", "jobs"),
            ("/jobs", "activity", "jobs"),
            ("/portfolio", "activity", "portfolio"),
        ]:
            await send(app, pilot, command)
            assert app.active_pane == pane
            assert app.query_one("#workspace-tabs", TabbedContent).active == tab
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
        assert not isinstance(app.screen, ModelSetup)
        await send(app, pilot, "/cancel")
        await pilot.pause()
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
async def test_cancel_onboarding_blocks_chat_and_can_reopen():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        await pilot.press("escape")
        await send(app, pilot, "hello")
        assert "Configure your online model" in visible(app)
        assert app.query_one("#prompt", Input).value == "hello"
        await send(app, pilot, "/model")
        await configure(app, pilot)
        assert "test-only-key" not in app.export_screenshot()


@pytest.mark.asyncio
async def test_quit_cancels_active_response():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "question")
        await pilot.press("ctrl+q")
    assert app.session.history == []
    assert app.session.busy is False


@pytest.mark.asyncio
async def test_small_terminal_onboarding_with_keyboard():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(45, 12)) as pilot:
        field = app.screen.query_one("#model-id", Input)
        field.value = "my-selected-model:free"
        field.focus()
        await pilot.press("enter")
        key_field = app.screen.query_one("#api-key", Input)
        assert key_field.has_focus
        key_field.value = "test-only-key"
        await pilot.press("enter")
        await pilot.pause()
        assert app.settings.model == "my-selected-model:free"
        assert app.query_one("#prompt", Input).has_focus


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (120, 36), (160, 48)])
async def test_four_panes_and_truthful_empty_states(size):
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=size) as pilot:
        await configure(app, pilot)
        panes = list(app.query(".pane"))
        assert len(panes) == 4
        for index, pane in enumerate(panes):
            assert pane.display and pane.region.width > 0 and pane.region.height > 0
            assert pane.region.right <= size[0] and pane.region.bottom < size[1]
            for other in panes[index + 1 :]:
                assert not pane.region.overlaps(other.region)
        for table in app.query(DataTable):
            assert table.row_count == 0
        for key, pane in [("f1", "library"), ("f2", "focus"), ("f3", "chat"), ("f4", "activity")]:
            await pilot.press(key)
            await pilot.pause()
            assert app.active_pane == pane
            focused = app.focused
            assert focused.id == ("prompt" if pane == "chat" else f"{pane}-pane")
        await send(app, pilot, "/portfolio")
        heading = app.query_one("#portfolio .empty-heading", Static)
        assert heading.content == "Positions unknown" and heading.region.height > 0
        assert len(app.session.provider.requests) == 0


@pytest.mark.asyncio
async def test_compact_navigation_preserves_draft_and_restores_grid():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(100, 30)) as pilot:
        await configure(app, pilot)
        await send(app, pilot, "a completed turn")
        prompt = app.query_one("#prompt", Input)
        prompt.value = "unfinished [draft]"
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        for key, pane in [("f1", "library"), ("f2", "focus"), ("f4", "activity"), ("f3", "chat")]:
            await pilot.press(key)
            await pilot.pause()
            shown = [widget.id for widget in app.query(".pane") if widget.display]
            assert shown == [f"{pane}-pane"]
        assert prompt.value == "unfinished [draft]"
        assert prompt.has_focus and prompt.region.bottom <= 11
        assert len(app.session.history) == 2
        await pilot.press("f2")
        await pilot.resize_terminal(120, 36)
        await pilot.pause()
        assert all(pane.display for pane in app.query(".pane"))
        assert app.active_pane == "focus"
        await pilot.press("slash")
        await pilot.pause()
        assert prompt.has_focus and prompt.value == "unfinished [draft]"
        prompt.value = ""
        await pilot.press("f1", "slash")
        await pilot.pause()
        assert prompt.has_focus and prompt.value == "/"
        await pilot.press("h")
        assert prompt.value == "/h"


@pytest.mark.asyncio
async def test_response_does_not_steal_focus_and_session_state_updates():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test() as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "a running turn")
        await pilot.press("f2")
        await pilot.pause()
        assert app.active_pane == "focus"
        assert app.request_state == "RESPONDING"
        await pilot.press("ctrl+k")
        await pilot.pause()
        assert app.active_pane == "focus"
        assert app.request_state == "CANCELED"
        assert "Completed turns    0" in str(app.query_one("#session-details", Static).content)
        app.session.provider.wait = False
        await send(app, pilot, "a complete turn")
        assert app.request_state == "READY"
        assert "Completed turns    1" in str(app.query_one("#session-details", Static).content)
        await send(app, pilot, "/clear")
        assert "Completed turns    0" in str(app.query_one("#session-details", Static).content)


@pytest.mark.asyncio
async def test_completed_stream_preserves_other_pane_focus():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(45, 12)) as pilot:
        await configure(app, pilot)
        app.session.provider.wait = True
        await send(app, pilot, "finish in the background")
        await pilot.press("tab", "tab", "tab")
        await pilot.pause()
        assert app.active_view == "session"
        app.session.provider.release.set()
        await app.response_worker.wait()
        await pilot.pause()
        assert app.request_state == "READY" and len(app.session.history) == 2
        assert app.active_pane == "activity" and app.focused.id == "activity-pane"
        assert app.query_one("#activity-pane").display
        assert not app.query_one("#chat-pane").display


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(120, 36), (45, 12)])
async def test_tab_cycles_all_views_in_both_directions_without_losing_draft(size):
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
                pane = "activity" if view in {"jobs", "portfolio", "session"} else view
                assert app.query_one(f"#{pane}-pane").display
            assert field.has_focus and field.value == "draft [keep literally]"
            assert field.cursor_position == 5
        assert app.session.provider.requests == []
        await pilot.resize_terminal(80, 24)
        await pilot.pause()
        await pilot.press("tab")
        assert app.active_view == "jobs"
        assert field.value == "draft [keep literally]"


@pytest.mark.asyncio
async def test_setup_tab_navigation_stays_in_modal():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(45, 12)) as pilot:
        setup = app.screen
        model = setup.query_one("#model-id", Input)
        key = setup.query_one("#api-key", Input)
        model.focus()
        await pilot.press("tab")
        assert key.has_focus and app.screen is setup
        await pilot.press("shift+tab")
        assert model.has_focus and app.screen is setup
        assert app.active_view == "chat"
        await configure(app, pilot)
        await send(app, pilot, "/model")
        app.screen.query_one("#model-id", Input).focus()
        await pilot.press("tab")
        assert app.screen.query_one("#api-key", Input).has_focus
        await pilot.press("escape")
        await pilot.pause()
        await pilot.press("tab")
        assert app.active_view == "jobs"


@pytest.mark.asyncio
async def test_navigation_strip_mouse_and_commands_stay_synchronized():
    app = StratosphereApp(provider_factory=FakeProvider)
    async with app.run_test(size=(120, 36)) as pilot:
        await configure(app, pilot)
        await pilot.click("#nav-portfolio")
        await pilot.pause()
        assert app.active_view == "portfolio"
        assert app.query_one("#workspace-tabs", TabbedContent).active == "portfolio"
        await pilot.click("#strategies")
        await pilot.pause()
        assert app.active_view == "library"
        assert app.query_one("#navigation", Tabs).active == "nav-library"
        assert app.focused.id == "strategies"
        await send(app, pilot, "/jobs")
        assert app.query_one("#navigation", Tabs).active == "nav-jobs"
        await pilot.press("shift+tab")
        assert app.active_view == "chat" and app.query_one("#prompt", Input).has_focus
        assert app.session.provider.requests == []
