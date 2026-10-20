"""Timed file commands retain exact sources before any explicit specialist review."""

import pytest
from test_captions import SRT, VTT, online
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from test_source_reviews import ReviewProvider
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

from stratosphere.commands import route_command
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "text,action,args",
    [
        (
            '/source-captions webvtt "/explicit/space file.vtt"',
            "source-captions",
            ("webvtt", "/explicit/space file.vtt"),
        ),
        (
            '/source-captions srt "/captions.srt" https://youtu.be/dQw4w9WgXcQ',
            "source-captions",
            ("srt", "/captions.srt", "https://youtu.be/dQw4w9WgXcQ"),
        ),
        ("/source-captions", "message", ()),
        ("/source-captions webvtt", "message", ()),
        ('/source-captions srt "unclosed', "message", ()),
    ],
)
def test_explicit_caption_command_arity_and_quoted_paths(text, action, args):
    value = route_command(text)
    assert value.action == action and value.args == args


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_forced_caption_intake_inspection_and_explicit_review(
    ui_service, research_plan, tmp_path, size
):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    provider = ReviewProvider(
        response={
            "findings": [
                {
                    "category": "rule_candidate",
                    "interpretation": "Tentative source entry; conversion is unverified.",
                    "evidence_index": 1,
                }
            ],
            "questions": [],
        }
    )
    app = StratosphereApp(
        research_service=ui_service, source_service=sources, provider_factory=lambda _: provider
    )
    identity = app.database.register(
        research_plan.specification, "Caption owner", {"kind": "authored"}
    )
    path = tmp_path / "explicit captions.vtt"
    path.write_bytes(VTT.encode())
    async with app.run_test(size=size) as pilot:
        await connect(app, pilot)
        await send(
            app, pilot, f'/source-captions webvtt "{path}" https://youtu.be/dQw4w9WgXcQ?t=20'
        )
        await app.research_worker.wait()
        (source_id,) = sources.store.jobs()
        value = sources.store.snapshot(source_id)
        assert value.access_state == "acquired" and not provider.requests
        assert app.active_view == "chat" and not app.database.source_reviews()
        assert not app.research.store.jobs() and app.query_one("#reports", DataTable).row_count == 0
        await send(app, pilot, "/source-show " + source_id)
        body = app.query_one("#focus-body", VerticalScroll)
        assert "File-local cue bounds: 0–2000 ms" in body.query_one(Static).content.markup
        body.focus()
        await pilot.press("end")
        await pilot.pause()
        assert body.scroll_y > 0
        app.session.select(identity["public_id"])
        await send(app, pilot, "/source-review " + source_id)
        await app.research_worker.wait()
        await send(app, pilot, "/reports")
        assert len(provider.requests) == 1 and app.query_one("#reports", DataTable).row_count == 1
        (record,) = app.database.source_reviews(identity["public_id"])
        await app.open_report_panel("source-review:" + record["id"])
        assert (
            "caption-piece bounds 0–2000 ms"
            in app.query_one("#report-preview", Static).content.markup
        )
        assert not app.research.store.jobs() and ui_service.calls == []


@pytest.mark.parametrize(
    "extension,content", [(".VTT", VTT), (".srt", SRT), (".vtt", "broken caption file")]
)
async def test_source_file_uses_explicit_caption_extensions_without_fallback(
    ui_service, research_provider, tmp_path, extension, content
):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    app = StratosphereApp(
        research_service=ui_service,
        source_service=sources,
        provider_factory=lambda _: research_provider,
    )
    path = tmp_path / ("caption" + extension)
    path.write_bytes(content.encode())
    async with app.run_test() as pilot:
        await connect(app, pilot)
        await send(app, pilot, f'/source-file "{path}"')
        await app.research_worker.wait()
        (source_id,) = sources.store.jobs()
        value = sources.store.snapshot(source_id)
        if content == "broken caption file":
            assert value.access_state == "unsupported_format" and not value.pieces
        else:
            assert any(p.kind == "caption" for p in value.pieces)
        assert not research_provider.requests and not app.research.store.jobs()


async def test_caption_links_and_directories_are_not_read(ui_service, research_provider, tmp_path):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    app = StratosphereApp(
        research_service=ui_service,
        source_service=sources,
        provider_factory=lambda _: research_provider,
    )
    path = tmp_path / "actual.vtt"
    path.write_text(VTT)
    link = tmp_path / "linked.vtt"
    link.symlink_to(path)
    async with app.run_test() as pilot:
        await connect(app, pilot)
        for selected in (link, tmp_path):
            await send(app, pilot, f'/source-captions webvtt "{selected}"')
            await app.research_worker.wait()
        assert not sources.store.jobs() and not research_provider.requests
