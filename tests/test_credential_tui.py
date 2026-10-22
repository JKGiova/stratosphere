"""Remembered credentials restore settings, never conversations or trading authority."""

from conftest import MemoryCredentialBackend
from test_liquid_login import RefreshProvider
from test_research_tui import send
from test_research_tui import ui_service as ui_service
from test_tui import FakeProvider, configure

from stratosphere.credentials import CredentialStore
from stratosphere.tui import StratosphereApp


async def test_model_and_liquid_survive_quit_then_explicit_forget_clears_both(ui_service, tmp_path):
    vault = CredentialStore(backend=MemoryCredentialBackend(), lock_root=tmp_path / "locks")
    remote = RefreshProvider()
    remote.connection.credential_store = vault
    app = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        liquid_connection=remote.connection,
        provider_factory=FakeProvider,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await app.liquid_worker.wait()
        await configure(app, pilot)
        await send(app, pilot, "/new")
        notice = str(app.research_form.query_one(".research-help").content)
        assert "OS credential store" in notice and "key stays session-only" not in notice
        await pilot.press("escape")
        await pilot.pause()
        await send(app, pilot, "/liquid-connect")
        await app.liquid_worker.wait()
        assert vault.load_model() is not None and vault.read("liquid") is not None
        await app.action_quit()
    assert vault.load_model() is not None and vault.read("liquid") is not None
    before = len(remote.browser_urls)
    restarted = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        liquid_connection=remote.restarted(vault),
        provider_factory=FakeProvider,
    )
    async with restarted.run_test(size=(45, 12)) as pilot:
        await restarted.liquid_worker.wait()
        assert restarted.setup is None and restarted.session is not None
        assert restarted.settings.api_key.get_secret_value() == "test-only-key"
        assert not restarted.session.provider.requests and not restarted.session.history
        assert len(remote.browser_urls) == before and restarted.liquid.session is not None
        await send(restarted, pilot, "/model-forget")
        assert restarted.session is None and vault.load_model() is None
        await send(restarted, pilot, "/liquid-disconnect")
        assert restarted.liquid.session is None and vault.read("liquid") is None
    empty = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        liquid_connection=remote.restarted(vault),
        provider_factory=FakeProvider,
    )
    async with empty.run_test(size=(100, 30)):
        await empty.liquid_worker.wait()
        assert empty.setup is not None and empty.session is None and empty.liquid.session is None


async def test_unavailable_secure_storage_allows_session_only_model_without_silent_save(
    ui_service, tmp_path, monkeypatch
):
    from textual.widgets import Static

    from stratosphere.credentials import CredentialError

    vault = CredentialStore(backend=MemoryCredentialBackend(), lock_root=tmp_path / "locks")

    def fail(*args):
        raise CredentialError("Secure storage unavailable.")

    monkeypatch.setattr(vault, "save_model", fail)
    app = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        provider_factory=FakeProvider,
        liquid_connection=RefreshProvider().connection,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await configure(app, pilot)
        assert app.session is not None and vault.load_model() is None
        content = " ".join(str(value.content) for value in app.query(Static))
        assert "session only" in content and "test-only-key" not in content
