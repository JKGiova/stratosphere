"""Native vault boundaries, exact settings and cancellation/locking behavior."""

import asyncio
import threading
from types import SimpleNamespace

import pytest
from conftest import MemoryCredentialBackend
from pydantic import SecretStr

from stratosphere.credentials import CredentialError, CredentialStore, credential_io
from stratosphere.models.settings import ModelSettings


@pytest.fixture
def vault(tmp_path):
    return CredentialStore(backend=MemoryCredentialBackend(), lock_root=tmp_path / "locks")


def test_model_roundtrip_keeps_exact_provider_destination_and_secret(vault, tmp_path):
    settings = ModelSettings(
        provider="openrouter", model="fixture/model:free", api_key=SecretStr("fixture-key")
    )
    vault.save_model(settings)
    restored = vault.load_model()
    assert restored.provider == settings.provider and restored.model == settings.model
    assert restored.endpoint == settings.endpoint and restored.api_key == settings.api_key
    assert "fixture-key" not in repr(restored)
    assert not list(tmp_path.iterdir())
    vault.delete("model")
    assert vault.load_model() is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 2),
        ("version", True),
        ("base_url", "https://untrusted.example/v1"),
        ("api_key", "unsafe\nkey"),
        ("unexpected", "field"),
    ],
)
def test_invalid_saved_model_never_becomes_a_provider_request(vault, field, value):
    vault.save_model(ModelSettings(model="fixture", api_key=SecretStr("private-key")))
    saved = vault.read("model")
    saved[field] = value
    vault.put("model", saved)
    with pytest.raises(CredentialError) as error:
        vault.load_model()
    assert "private-key" not in str(error.value)


@pytest.mark.parametrize("operation", ["get_password", "set_password", "delete_password"])
def test_backend_failures_are_sanitized_without_plaintext_fallback(
    vault, operation, monkeypatch, tmp_path
):
    vault.put("model", {"private": "secret-value"})

    def fail(*args):
        raise RuntimeError("secret-value")

    monkeypatch.setattr(vault.backend, operation, fail)
    with pytest.raises(CredentialError) as error:
        {
            "get_password": lambda: vault.read("model"),
            "set_password": lambda: vault.put("model", {}),
            "delete_password": lambda: vault.delete("model"),
        }[operation]()
    assert "secret-value" not in str(error.value) and not list(tmp_path.iterdir())


def test_json_duplicate_keys_and_nonfinite_values_are_rejected(vault):
    for payload in ('{"version":1,"version":2}', '{"expiry":NaN}', "[]"):
        vault.backend.set_password(vault.service, "liquid", payload)
        with pytest.raises(CredentialError):
            vault.read("liquid")


def test_native_selector_ignores_configured_plaintext_backend(monkeypatch):
    from stratosphere import credentials

    backend = MemoryCredentialBackend()
    backend.priority = 1
    monkeypatch.setattr(credentials.sys, "platform", "darwin")
    monkeypatch.setenv("PYTHON_KEYRING_BACKEND", "keyrings.alt.file.PlaintextKeyring")
    monkeypatch.setitem(
        credentials.sys.modules, "keyring.backends.macOS", SimpleNamespace(Keyring=lambda: backend)
    )
    assert credentials.native_backend() is backend


def test_refresh_lease_excludes_another_store_and_rejects_symlinks(vault):
    other = CredentialStore(backend=vault.backend, lock_root=vault.lock_root)
    with vault.lease("liquid"):
        with pytest.raises(CredentialError):
            with other.lease("liquid"):
                pytest.fail("second refresh owner")
    path = vault.lock_root / "liquid"
    path.unlink()
    path.symlink_to(vault.lock_root / "missing")
    with pytest.raises(CredentialError):
        with vault.lease("liquid"):
            pytest.fail("symlink lock")


async def test_cancellation_finishes_a_pending_write_before_forget(vault):
    entered, release = threading.Event(), threading.Event()

    def delayed():
        entered.set()
        release.wait(5)
        vault.put("liquid", {"value": "fixture"})

    task = asyncio.create_task(credential_io(delayed))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await credential_io(vault.delete, "liquid")
    assert vault.read("liquid") is None


def test_refresh_lock_is_shared_despite_different_research_directories(monkeypatch):
    monkeypatch.setenv("STRATOSPHERE_RESEARCH_DIR", "/tmp/authored-first-workspace")
    first = CredentialStore(backend=MemoryCredentialBackend())
    monkeypatch.setenv("STRATOSPHERE_RESEARCH_DIR", "/tmp/authored-second-workspace")
    second = CredentialStore(backend=MemoryCredentialBackend())
    assert first.lock_root == second.lock_root
