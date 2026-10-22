"""Native secure credential storage, without file or third-party backend fallback."""

import asyncio
import hashlib
import json
import os
import stat
import sys
from contextlib import contextmanager
from pathlib import Path

from stratosphere.connectors.catalog import strict_json
from stratosphere.models.settings import ModelSettings
from stratosphere.research.store import ResearchError

SERVICE = "tradingsphere.stratosphere"


class CredentialError(ResearchError):
    """A sanitized error that never includes backend messages or stored payloads."""


def native_backend():
    # Instantiate the built-in OS implementation directly. User-configured/chained
    # backends (including plaintext keyrings) must not receive these credentials.
    try:
        if sys.platform == "darwin":
            from keyring.backends.macOS import Keyring
        elif sys.platform.startswith("linux"):
            from keyring.backends.SecretService import Keyring
        else:
            raise RuntimeError
        backend = Keyring()
        if backend.priority <= 0:
            raise RuntimeError
        return backend
    except Exception:
        raise CredentialError(
            "Secure storage unavailable. Unlock macOS Keychain or Linux Secret Service; "
            "credentials were not written to a file."
        ) from None


async def credential_io(function, *args):
    """Finish an OS write before cancellation can release its lock or forget it."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    canceled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            canceled = True
            if task.done():
                # Retrieve any failure without exposing it after cancellation.
                if not task.cancelled():
                    task.exception()
                raise
        except Exception:
            if canceled:
                raise asyncio.CancelledError from None
            raise
    if canceled:
        raise asyncio.CancelledError
    return result


class CredentialStore:
    def __init__(self, *, backend=None, service=SERVICE, lock_root=None):
        self._backend = backend
        self.service = service
        if lock_root is None:
            import pwd

            # Research directories may differ between TUIs sharing one OS vault.
            # Resolve the account home independently of per-project/environment state.
            home = Path(pwd.getpwuid(os.getuid()).pw_dir)
            root = home / (
                "Library/Application Support/stratosphere/credential-locks"
                if sys.platform == "darwin"
                else ".local/state/stratosphere/credential-locks"
            )
            lock_root = root / hashlib.sha256(service.encode()).hexdigest()
        self.lock_root = lock_root

    @property
    def backend(self):
        if self._backend is None:
            self._backend = native_backend()
        return self._backend

    def read(self, name):
        try:
            value = self.backend.get_password(self.service, name)
            if value is None:
                return None
            if not isinstance(value, str) or len(value) > 65536:
                raise ValueError
            result = strict_json(value.encode())
            if not isinstance(result, dict):
                raise ValueError
            return result
        except Exception:
            raise CredentialError(
                "Saved credentials could not be read from secure storage."
            ) from None

    def put(self, name, value):
        try:
            payload = json.dumps(value, allow_nan=False, separators=(",", ":"))
            if len(payload) > 65536:
                raise ValueError
            self.backend.set_password(self.service, name, payload)
        except Exception:
            raise CredentialError("Credentials could not be saved in secure storage.") from None

    def delete(self, name):
        try:
            if self.backend.get_password(self.service, name) is not None:
                self.backend.delete_password(self.service, name)
        except Exception:
            raise CredentialError(
                "Saved credentials could not be removed from secure storage."
            ) from None

    def load_model(self):
        value = self.read("model")
        if value is None:
            return None
        try:
            if set(value) != {"version", "provider", "model", "api_key", "base_url"}:
                raise ValueError
            version = value.pop("version")
            if type(version) is not int or version != 1:
                raise ValueError
            return ModelSettings.model_validate(value)
        except Exception:
            raise CredentialError(
                "Saved model settings are invalid; use /model to replace them."
            ) from None

    def save_model(self, settings):
        self.put(
            "model",
            {
                "version": 1,
                "provider": settings.provider,
                "model": settings.model,
                "api_key": settings.api_key.get_secret_value(),
                "base_url": settings.resolved_base_url,
            },
        )

    @contextmanager
    def lease(self, name):
        """Serialize refresh-token consumption across local TUI processes."""
        fd = None
        try:
            import fcntl

            if name not in {"model", "liquid"}:
                raise ValueError
            self.lock_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if self.lock_root.is_symlink():
                raise ValueError
            fd = os.open(self.lock_root / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            if fd is not None:
                os.close(fd)
            raise CredentialError(
                "Saved login is busy or its private lock is unavailable; retry when it is free."
            ) from None
        try:
            yield
        finally:
            os.close(fd)
