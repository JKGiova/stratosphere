"""Remembered credentials restore settings, never conversations or trading authority."""

from conftest import MemoryCredentialBackend
from test_liquid_login import RefreshProvider
from test_research_tui import send
from test_research_tui import ui_service as ui_service
from test_tui import FakeProvider, configure

from stratosphere.credentials import CredentialStore
from stratosphere.tui import StratosphereApp


async def test_only_model_restores_and_retired_provider_does_not_connect(ui_service, tmp_path):
    vault = CredentialStore(backend=MemoryCredentialBackend(), lock_root=tmp_path / "locks")
    remote = RefreshProvider()
    app = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        liquid_connection=remote.connection,
        provider_factory=FakeProvider,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await configure(app, pilot)
        assert app.liquid_worker is None
        await app.action_quit()
    restarted = StratosphereApp(
        research_service=ui_service,
        credential_store=vault,
        liquid_connection=remote.restarted(vault),
        provider_factory=FakeProvider,
    )
    async with restarted.run_test(size=(100, 30)) as pilot:
        assert restarted.session is not None and restarted.liquid_worker is None
        assert not remote.browser_urls
        await send(restarted, pilot, "/model-forget")
        assert vault.load_model() is None


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
