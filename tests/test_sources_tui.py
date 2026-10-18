"""Source commands acquire/inspect only; they cannot register or execute strategies."""

import httpx
import pytest
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from test_sources import github_handler, online
from textual.widgets import DataTable, Input, Static

from stratosphere.commands import route_command
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore
from stratosphere.tui import AssistantMessage, StratosphereApp


@pytest.mark.parametrize(
    "text,action,args",
    [
        (
            "/source https://github.com/owner/repo/blob/main/strategy.py",
            "source",
            ("https://github.com/owner/repo/blob/main/strategy.py",),
        ),
        (
            "/source https://github.com/owner/repo/blob/feature/test/strategy.py feature/test",
            "source",
            ("https://github.com/owner/repo/blob/feature/test/strategy.py", "feature/test"),
        ),
        (
            '/source-text text\nincluding "quotes" and /approve',
            "source-text",
            ('text\nincluding "quotes" and /approve',),
        ),
        (
            '/source-file "/explicit/path with space.py"',
            "source-file",
            ("/explicit/path with space.py",),
        ),
        ("/source-show " + "a" * 32, "source-show", ("a" * 32,)),
        ("/sources", "sources", ()),
        ('/source-file "unterminated', "message", ()),
        ("/source", "message", ()),
        ("/source-text", "message", ()),
        ("/sources unexpected", "message", ()),
    ],
)
def test_source_commands_preserve_payloads_and_validate_arity(text, action, args):
    command = route_command(text)
    assert command.action == action and command.args == args


async def test_tui_acquisition_inspection_resize_and_supplied_file_without_models(
    ui_service,
    research_provider,
    tmp_path,
):
    sources = SourceService(
        SourceStore(tmp_path / "sources"),
        transport=httpx.MockTransport(github_handler),
        online_check=online,
    )
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        source_service=sources,
    )
    file = tmp_path / "authorized rules.py"
    file.write_text('raise RuntimeError("Never execute acquired code")\n')
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/source https://github.com/owner/repo/blob/main/strategy.py")
        await app.research_worker.wait()
        await pilot.pause()
        (source_id,) = sources.store.jobs()
        assert sources.store.snapshot(source_id).access_state == "acquired"
        assert app.active_view == "chat"
        assert app.database.catalog() == [] and app.research.store.jobs() == []
        assert app.query_one("#tests", DataTable).row_count == 0
        await send(app, pilot, "/source-show " + source_id)
        assert app.active_view == "focus"
        for size in ((45, 12), (60, 36), (180, 48)):
            await pilot.resize_terminal(*size)
            await pilot.pause()
            assert app.query_one("#focus-body").size.height >= 2
            assert app.query_one("#focus-body").children
        await send(app, pilot, '/source-file "' + str(file) + '"')
        await app.research_worker.wait()
        await pilot.pause()
        records = [sources.store.snapshot(i) for i in sources.store.jobs()]
        (supplied,) = [v for v in records if v.provider == "supplied"]
        assert supplied.analysis_text == file.read_text() and supplied.title == file.name
        await send(app, pilot, "/sources")
        rendered = "\n".join(
            str(w.content)
            for w in app.query(".message")
            if isinstance(w, Static) and not isinstance(w, AssistantMessage)
        )
        assert source_id in rendered and supplied.id in rendered
        assert research_provider.requests == [] and ui_service.calls == []


async def test_tui_file_rejects_configured_key_before_source_storage(
    ui_service,
    research_provider,
    tmp_path,
):
    sources = SourceService(
        SourceStore(tmp_path / "sources"),
        transport=httpx.MockTransport(github_handler),
        online_check=online,
    )
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        source_service=sources,
    )
    file = tmp_path / "secret.txt"
    file.write_text("tui-private-test-key")
    async with app.run_test() as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/source-file " + str(file))
        await app.research_worker.wait()
        await pilot.pause()
        (source_id,) = sources.store.jobs()
        value = sources.store.snapshot(source_id)
        assert value.access_state == "sensitive_material_rejected"
        assert not list(sources.store.directory(source_id).glob("blob_*"))
        assert research_provider.requests == []
        assert all(
            "tui-private-test-key" not in path.read_text()
            for path in sources.store.directory(source_id).iterdir()
        )
        assert app.query_one("#prompt", Input).value == ""


async def test_tui_conformance_gaps_are_inspectable_and_cannot_create_executable_plan(
    ui_service,
    research_provider,
    tmp_path,
):
    from test_source_conversion import PINE

    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        source_service=sources,
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/source-text " + PINE)
        await app.research_worker.wait()
        await pilot.pause()
        (source_id,) = sources.store.jobs()
        await send(app, pilot, "/conformance " + source_id)
        await app.research_worker.wait()
        await pilot.pause()
        (first,) = app.source_conformance.store.jobs()
        assert (await app.source_conformance.open(first)).classification == "missing_context"
        await send(
            app,
            pilot,
            "/conformance " + source_id + " crypto_spot BTCUSDT 3600 regular_ohlcv source_defaults",
        )
        await app.research_worker.wait()
        await pilot.pause()
        latest = app.source_conformance.store.jobs()[-1]
        value = await app.source_conformance.open(latest)
        assert (
            value.classification == "rules_mapped_conformance_blocked"
            and value.execution == "blocked"
        )
        assert not app.research.store.jobs() and not app.database.catalog()
        assert not research_provider.requests and not ui_service.calls
        await send(app, pilot, "/conformance-show " + latest)
        assert app.active_view == "focus"
        assert "Execution blocked" in app.query_one("#focus-body").children[0].content.markup
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        assert app.query_one("#focus-body").size.height >= 2
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#focus-body").scroll_y > 0
        await send(
            app,
            pilot,
            "/conformance " + source_id + " equity AAPL 3600 regular_ohlcv source_defaults",
        )
        await app.research_worker.wait()
        await pilot.pause()
        assert len(app.source_conformance.store.jobs()) == 2
        assert not app.research.store.jobs() and not app.database.catalog()


async def test_tui_data_capability_inspection_is_online_read_only_and_model_free(
    ui_service,
    research_provider,
):
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider, research_service=ui_service
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/data")
        assert not research_provider.requests and not ui_service.calls
        assert not app.research.store.jobs() and not app.database.catalog()
        await send(app, pilot, "/data authored-fixture paper")
        text = app.query_one("#focus-body").children[0].content.markup
        assert "Eligible for this bounded purpose: **no**" in text
        assert app.active_view == "focus"
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#focus-body").scroll_y > 0

        async def offline():
            raise ValueError("Offline")

        ui_service.online_check = offline
        await send(app, pilot, "/data")
        text = "\n".join(
            str(w.content) for w in app.query(Static) if not isinstance(w, AssistantMessage)
        )
        assert "online access" in text
        assert not research_provider.requests and not ui_service.calls
