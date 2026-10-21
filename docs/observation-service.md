# Authored observation worker

Status: **implemented local process ownership, durable delivery and monitoring for reviewed authored streams**. This is a standalone development service, not an Alpaca feed, paper order adapter or completed production runtime. The complete [Phase 3–5 acceptance ledger](phase3-5.md) remains in progress. The [actual Alpaca account check](alpaca-paper-testing.md#user-performed-paper-check--21-october-2026) does not authorize this worker to acquire prices or trade.

## Reviewed boundary

`paper/service.py` wraps the existing [closed-bar processor](phase5-closed-bars.md). A `ServicePlan` pins the complete authored account, canonical absolute account/registry paths, ordered distinct instance IDs and exact approved stream-mandate digests. It also pins the service/stream source digest, polling interval, online timeout, heartbeat interval and maximum silence. The only supported input remains `authored-btcusdt-hourly/1`; its observations and receipt times are authored declarations. BTC/USD provider data cannot be relabeled as those inputs.

Preparation creates one private service journal at `<account-journal>.service/service.sqlite3`. The account and registry must already exist; no account creation, migration, funding, strategy registration, allocation or stream approval occurs. Both databases are opened with modes that cannot create missing files, and registry access is read-only. Starting requires exact service-plan approval in addition to the existing stream approval. Approval rechecks the current implementation and all pinned streams. The plan cannot be replaced by restarting or pointing the same account at another service directory. Adding streams or recovering a halted service requires a future explicitly reviewed migration; there is no reset/clear operation.

The service source guard binds current files and their import-time snapshot, including the existing stream implementation contract. It is a local consistency guard, not loaded executable, provider or administrator attestation. No model, acquired source, saved provider diagnostic or database record can select an endpoint, supply code or grant approval. The worker has no provider or order transport.

## Ownership and lifecycle

Each cooperating worker holds an exclusive nonblocking OS `flock` on `<account-journal>.runtime.lock` throughout its owner session. A second process is rejected. The persistent lock file is never removed to indicate shutdown; the OS releases the descriptor on normal exit or process death. Every processing tick checks that its held descriptor still refers to the lock file. Linux/macOS are the supported implementation targets; the local process checks were run on the development Mac, not every target OS.

Every session retains a random run identity. A new session records an earlier unclosed run as interrupted, rather than treating its retained ID as live. `probe_owner` probes the actual lock at the time of the call. A held lock shows ownership only: it does not prove a responsive loop, provider connection, fresh market data or execution eligibility. A stored PID, start event or heartbeat cannot prove liveness by itself. The lock serializes these workers; it does not turn the older authored library APIs or a local administrator into an authenticated production writer boundary.

`launch(journal)` explicitly starts a detached, isolated interpreter with closed extra descriptors, no stdin and a private log. Its environment contains only PATH, LANG and PYTHONUNBUFFERED, excluding model, broker and database credentials. It returns the actual `Popen` handle so callers can inspect process completion. No automatic restart is installed. Direct development invocation is also available after review; `run` rejects inherited key/token/secret/password environment names before starting, so use a clean environment:

```sh
env -i PATH=/usr/bin:/bin LANG=C.UTF-8 .venv/bin/python -I -m stratosphere.paper.service run /absolute/account.sqlite3.service
.venv/bin/python -I -m stratosphere.paper.service status /absolute/account.sqlite3.service
```

These are development controls over an existing authored service, not TUI activation commands. TUI launch/stop, production account selection and the confirmed keep-positions/request-all-closures exit workflow remain unimplemented. Stopping this worker performs no account, cancel or position action. SIGTERM/SIGINT request a clean loop stop; stop/cancellation during an online wait prevents another ingestion afterward. A forced death can leave an interrupted session, but does not lose committed account observations.

## Durable queue and recovery

`QueuedObservation` fixes a caller-owned delivery ID, instance, complete supported observation and authored receipt time. Enqueue is explicit and requires service approval. Only pinned instances are accepted. Exact same-ID replay changes no state; changed content under the same ID is retained as a conflict and durably halts the service. Distinct delivery IDs remain ordered attempts, including aliases/revisions that the stream processor must inspect. No sorting, deduplication of acquired market rows, inferred publication time or synthetic price is introduced.

Plan, approval, queued deliveries, outcomes, conflicts, owner sessions, heartbeats and halts are append-only, hash-linked SQLite events. Replay verifies their sequence, shapes, typed contracts and transitions. Unknown versions, damaged chains, symbolic links, non-private files and hardlinked state fail closed. State files use 0600 and the service directory uses 0700. Reading the service snapshot uses a read-only transaction; it cannot approve, acknowledge, launch or clear anything.

Delivery holds the service transaction while the existing account transaction commits its candle/crossing, then appends the service acknowledgement. A crash between those commits leaves the delivery pending. Restart replays it through the account's exact duplicate gate, acknowledges `duplicate` and emits no second crossing. The original crossing remains in the account journal even when the original service acknowledgement was lost. This is replay recovery across two journals, not an atomic distributed commit or an order exactly-once claim. Downstream signal consumption/delivery remains the separate [reviewed decision gateway](phase5-signal-decisions.md).

## Silence, heartbeat and failures

The first start gives every pinned stream an explicit initial silence deadline. Only newly accepted observations advance their own stream's progress time; duplicate deliveries, aliases, heartbeats and restarts cannot refresh it. Missing input for any pinned stream halts the whole account service. Downtime counts against the same retained deadline. No automatic catch-up, gap backfill, resumption or fresh grace period follows reconnect/restart.

Each tick requires the application's existing connectivity check within the approved timeout before processing. This check is not provider authentication or a market feed. A swallowed timeout/cancellation, offline result, elapsed silence, regressing service wall clock, changed implementation/registry/mandate or stream rejection prevents further processing. Clock regression records its observed time separately from the journal's nondecreasing ordering timestamp. Halts and rejected observations remain inspectable, and cannot release pending capital or invent a fill/reconciliation result.

Heartbeat writes follow the reviewed heartbeat interval, rather than appending once per idle poll. Account acknowledgements also update the retained heartbeat. The service currently replays its event history for integrity checks; long-running performance, compaction/checkpoint contracts and a production-duration soak remain unverified. The short actual worker-loop test verifies timed heartbeat/silence behavior, not sustained production operation.

## Inspection

`status` requires the application's online check before reading and reports the service plan/event digests, actual current lock probe, approval/halt state, queue/acknowledgement counts and last heartbeat. `/paper health` additionally includes these observations for the configured account, per-stream progress, interrupted runs and every service fault through the existing paginated view. A damaged or mismatched service appears unavailable; it cannot appear healthy or hide the independently readable account positions. Account and service captures are separate transactions, not a simultaneous portfolio snapshot. Both digests and the account capture time remain visible.

Neither reading nor Tab navigation starts or stops a process. A captured held lock and recent heartbeat remain dated observations. The feed summary continues to state that no provider feed is connected. All service data stays outside model prompts and strategy histories.

## Verification

The verification record is maintained in [Phases 3–5](phase3-5.md). Coverage includes actual competing processes, SIGKILL lock release, interrupted-owner retention, a forced exit immediately after account commit, fresh-process duplicate recovery, independent stream deadlines, downtime/clock stops, withheld approval, changed code/registry, online failure/swallowed timeout/cancellation, removal without account recreation, queue conflicts, journal damage/private paths, retained reservations and read-only health.

The regression passed 1,289 tests; the one sandbox-blocked OAuth listener test subsequently passed with localhost permission. After the final guard changes, 133 affected checks passed. These are overlapping/scoped runs, not a single green full run of the current 1,295-test collection. Lint and formatting passed. An initial targeted status test had a missing test-only JSON import; it was corrected before the final clean run.

An independently installed wheel processed 32 authored candles and retained six unique crossings. It crashed after candle 12 committed with only 11 acknowledgements; a fresh interpreter recovered one duplicate and accepted the remaining 20. A separate live worker continued independently while the TUI inspected health at 180×48 and 45×12, then stopped cleanly on SIGTERM. The installed-package check used fixture connectivity and made zero actual provider, model, native-start or order calls. Source/archive/credential exclusion was checked separately from runtime behavior. Provider feed provenance, Alpaca precision/base-fee accounting, paper transport, broker reconciliation, actual account control, authenticated shared services and the full production lifecycle remain pending.
