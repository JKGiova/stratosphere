import asyncio
from pathlib import Path

import pytest
from textual.widgets import DataTable, Input, Static, TabbedContent

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
        await pilot.press("f4")
        app.query_one("#workspace-tabs", TabbedContent).active = "session"
        await pilot.pause()
        app.session.provider.release.set()
        await app.response_worker.wait()
        await pilot.pause()
        assert app.request_state == "READY" and len(app.session.history) == 2
        assert app.active_pane == "activity" and app.focused.id == "activity-pane"
        assert app.query_one("#activity-pane").display
        assert not app.query_one("#chat-pane").display
