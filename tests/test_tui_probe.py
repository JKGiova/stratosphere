from pathlib import Path

import pytest
from textual.widgets import Input, Static

from stratosphere.phase0.tui_probe import FrameworkProbe


@pytest.mark.asyncio
async def test_keyboard_render_resize_and_quit():
    app = FrameworkProbe()
    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.press("/", "p", "r", "o", "b", "e", "space", "[", "x", "]", "enter")
        await pilot.pause()
        assert str(app.query_one("#result", Static).content) == "/probe [x]"
        assert app.query_one(Input).value == ""
        svg = app.export_screenshot()
        assert "probe" in svg and "[x]" in svg
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        assert (app.size.width, app.size.height) == (45, 12)
        field = app.query_one(Input)
        assert field.region.width <= 45
        assert field.region.bottom <= 12
        assert field.region.height > 0
        assert field.has_focus
        await pilot.press("o", "k", "enter")
        await pilot.pause()
        assert str(app.query_one("#result", Static).content) == "ok"
        Path("reports").mkdir(exist_ok=True)
        Path("reports/tui-probe.svg").write_text(app.export_screenshot(), encoding="utf-8")
        await pilot.press("ctrl+q")
    assert not app.is_running
