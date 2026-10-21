# Authored account positions, orders and health

Status: **read-only local journal inspection implemented**. These views expose retained authored account observations. They do not connect a broker, create an account, start a feed or paper service, or grant execution permission. The [Phase 3–5 acceptance ledger](phase3-5.md) remains in progress.

## Open a view

```text
/paper
/paper positions 1
/paper orders 1
/paper fills 2
/paper health 1
/portfolio
```

`/paper` defaults to positions. `/portfolio` refreshes that same view. The exact chat request `Inspect the authored paper account health`, optionally with `page 2`, uses the same operation; positions, orders and fills are also accepted. The bridge matches these explicit requests before model routing. No account evidence, private conversation or Liquid catalog enters a model request or strategy history. The operation works without a configured model after dismissing inline model setup. Ordinary unrelated chat retains its existing behavior.

The reader uses the existing `<research-state>/paper/account.sqlite3`; there is no arbitrary path argument or account-creation fallback. A missing journal returns an actionable unavailable state without creating a directory or making a model call. An existing journal requires the online gate before opening its read-only SQLite transaction. Unsupported versions, symbolic links, damaged event chains and known configured credentials fail before display. Database versions 1–3 are read without migration, funding, event insertion or approval. Local hashes establish journal consistency, not provider ownership or executable attestation.

## Exact observations and display

Each capture binds its authored account origin, complete event-chain digest and UTC capture time. Replay and reservation projections use exact decimal arithmetic. Positions retain every instance's complete allocation pins, attributed BTC, settled USDT, pending-order count and reserved cash. Flat instances remain visible; a missing account is never shown as a verified zero balance. The table has no invented P/L column or current market valuation.

Orders retain full intent references, side, quantity, price bound, status, actual filled quantity, reported filled notional/fees, cancellation request and remaining reservations. Fills retain their native/authored reported quote notionals and fees rather than replacing them with price multiplication. Partial, late and breaching fills remain visible. A cancellation request or ambiguous dispatch does not imply a terminal cancellation or free pending capital. Terminal observations retain their exact ledger status; reading never settles or reconciles an order.

Health includes account entry blocks, all retained reconciliation blocks, complete stream/decision mandate pins and approval flags, accepted observation counts, last receipt times, stream faults, the latest authored quote with event/receipt ages at capture, quote attempts/conflicts, observed risk stops/findings, consumed signal decisions and legacy denied decisions. Rows preserve journal-derived insertion order. Quote freshness is explicitly authored and dated; a recent declared quote still does not verify a feed, executable liquidity or an active service. The reader does not recalculate historical marks, clear latches or use a current source-file check as permission to act.

Every section exposes all rows in pages of 20, with total row/page counts. No retained evidence is truncated. Each command captures a new transaction: when the journal changes between pages, the displayed digest and capture time change too. This is an on-demand view, not an automatic feed or sustained runtime health monitor. Tab navigation retains the last captured view and its timestamp; use an explicit command to refresh it. A failed refresh clears the Positions table and its preview rather than presenting unavailable balances as current.

When the account has a reviewed [authored observation service](observation-service.md), health also captures its separate event digest, approval/halts, actual current OS-lock probe, queue/outcome counts, heartbeat, per-stream progress, interrupted sessions and all retained faults. The service read is a separate read-only transaction. Damaged/mismatched service state appears unavailable while valid account evidence remains readable. A held lock does not prove a responsive loop, provider feed or paper execution. Inspection cannot launch/stop the worker, change its queue or turn a stopped owner into a live one.

Positions opens in its existing responsive panel, with balances first in the compact table and a keyboard-scrollable detailed preview. Loaded panels shorter than 15 rows hide the table and extra status line so the full evidence remains readable; its capture time is still in the preview. Orders, fills and health open in Focus. These controls preserve the six/four/two/one layouts, Tab navigation, selected strategy, private chat histories and Reports. They add no entry-block, cancellation, closure or exit-choice controls.

## Verification

The 31 new reader/command/TUI checks cover exact partial/late fill economics, cancellation ambiguity, terminal reservation release, complete multi-page fills and denied decisions, read-only bytes, supported/unsupported versions, restart, corruption, symlinks, encoded credential rejection, missing/offline accounts and responsive keyboard inspection. The full regression run passed **1,218 tests with zero skips** and 40 upstream Pandas warnings, including existing native accounting and the actual OAuth callback listener. The final responsive correction separately passed **71 reader/TUI checks**; these overlapping results do not represent two independent full-suite runs. Lint, formatting and changed-document local-link checks passed.

An independently installed wheel read an existing authored two-instance journal with one pending reservation and preserved its exact file bytes across two fresh interpreters. Complete positions/orders/fills/health matched across restart. Keyboard checks and rendered previews passed at 180×48 and 45×12, including a visible scrollable body in the short Positions panel. Wheel/source equality and archive checks excluded private state and configured credentials. The package acceptance constructed no model provider and made zero model, provider, native-start or trading calls. The initial package harness was corrected to dismiss model setup and to check the legacy journal's actual account-health rows rather than inventing stream records. Fixture observations remain distinct from provider or paper-runtime verification.
