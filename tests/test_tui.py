import asyncio
from pathlib import Path

import pytest
from textual.widgets import Input, Static, TabbedContent

from stratosphere.models.nim import ModelError
from stratosphere.tui import ModelSetup, StratosphereApp


class FakeProvider:
    def __init__(self, settings):
        self.settings = settings
        self.requests = []
        self.wait = False
        self.fail = False

    async def stream(self, messages):
        self.requests.append(messages)
        yield "Hello [literal]"
        if self.wait:
            await asyncio.Event().wait()
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
        for command, tab in [
            ("/strategies", "strategies"),
            ("/jobs", "jobs"),
            ("/portfolio", "portfolio"),
        ]:
            await send(app, pilot, command)
            assert app.query_one("#tabs", TabbedContent).active == tab
        assert len(app.session.provider.requests) == 1
        await send(app, pilot, "please use test-only-key")
        assert "test-only-key" not in visible(app)
        assert len(app.session.provider.requests) == 1
        await send(app, pilot, "/help")
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        field = app.query_one("#prompt", Input)
        assert field.region.height > 0 and field.region.bottom <= 12
        app.query_one("#tabs", TabbedContent).active = "chat"
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
