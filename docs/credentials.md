# Credentials

The selected model/provider/official endpoint/key uses macOS Keychain or Linux Secret Service
through `keyring`, without plaintext or chained fallback. `/model` saves the selected configuration;
startup restores it without inference. `/model-forget` removes it when no operation is running.
Unavailable secure storage is reported; explicit session-only setup remains possible. Native-store
access is personal OS storage, not a separate multi-user boundary.

MT5 owns the broker login in its existing terminal. The [local connector](mt5-bridge.md) reads no
passwords, copies no account credential files and needs no broker API key, DLL or WebRequest scope.
Its account/session identifiers, policy files and private journal are diagnostics/authority records,
not credentials to forward to models. Keep all account diagnostics outside strategy-model histories.

Alpaca/Liquid connection commands and automatic credential restoration are retired. Existing native
entries were retained, not read, migrated or converted into MT5 authority. Revoking a grant at the
original provider is separate from retiring application routing. Retained historical readers and
secure-store contracts remain compatible; they are not an active connection path.

Credentials stay outside Git, model contexts, SQLite evidence and worker environments. The EA build
helper passes a small explicit environment to MetaEditor/Wine without inherited model/provider keys.
Synthetic macOS native-store checks and authored fixtures are distinct from Linux native-backend
verification or actual broker login persistence.
