# MT5 read-only diagnostic

The FTMO bridge currently has a manually launched diagnostic script, not a trading adapter.
It does not approve a strategy, bind an execution account or submit orders. Alpaca paper remains
the only implemented final-approval/order route.

## Run against the existing terminal

Compile [StratosphereReadOnlyProbe.mq5](../tools/mt5/StratosphereReadOnlyProbe.mq5) in the installed
MetaEditor and put its compiled `.ex5` in the active terminal's `MQL5/Scripts/stratosphere` folder.
Use MT5 **File → Open Data Folder** to identify that directory; do not assume that the roaming
directory is active. The inspected macOS Wine installation uses the installation directory.
Keep compiled binaries and local compiler logs outside Git.

In Navigator, refresh Scripts and run **stratosphere → StratosphereReadOnlyProbe** on a chart.
It requires neither automated trading nor DLL imports. It uses the already signed-in terminal:
do not export passwords, copy account credential files or pass credentials to the script.
The script temporarily selects the gold symbol in Market Watch if necessary and attempts to
restore its previous selection after reading. Historical reads can download missing bars into
the terminal's normal cache; account and trading settings are not changed.

The script records terminal connection/build, reported company/server/demo status, currency and
leverage, gold symbol candidates, current contract/volume/tick/swap specifications and a quote.
It prefers exact `XAUUSD`, otherwise a single matching symbol; ambiguous matches remain unresolved.
It requests at most 512 recent bars for each of M15, H1, H4, D1 and W1 and retains CopyRates errors,
local bar counts, reported server first dates and synchronization status. No credentials, account
login/name, balances, positions or orders are exported. Each run creates a separate
`MQL5/Files/stratosphere-probe-*` directory; completion is marked only after file writes finish.
The Experts tab prints the path.

Validate and preserve the diagnostic locally:

```sh
.venv/bin/python -m stratosphere.connectors.mt5_probe \
  '/absolute/path/to/MQL5/Files/stratosphere-probe-...' \
  'reports/mt5/unique-run-name'
```

The destination must be new. The validator retains exact source bytes and hashes, rejects malformed,
duplicate or unordered bars rather than repairing them, and reports missing intervals, disconnected
state, non-demo status and stale quotes as findings. Diagnostics stay outside strategy/model tools.
Treat local exports as untrusted observations, not independent broker/account attestation.

## Maximum-window gold history

[StratosphereGoldHistory.mq5](../tools/mt5/StratosphereGoldHistory.mq5) is a separate read-only
script using `stratosphere.mt5.history.v2`; the original diagnostic and its v1 reader remain
compatible. Compile/install it beside the diagnostic, refresh Navigator and launch
**stratosphere → StratosphereGoldHistory** once. Leave the terminal running until Experts prints
`maximum-window history export complete`. Do not start another copy on the same chart while it runs.

It requests all 21 standard intervals: M1/M2/M3/M4/M5/M6/M10/M12/M15/M20/M30,
H1/H2/H3/H4/H6/H8/H12, D1/W1/MN1. Coarse intervals run first. It tries the full date range up
to a fixed server-time endpoint, then a bounded latest-window request if needed. Each interval
has at most three acquisition attempts, with every returned count/error retained; these are history
downloads, not research/native-backtest retries. Short successful ranges can be retried because
MT5 can continue downloading history after a timed-out call. The final export retains the largest
returned candidate without sorting, deduplicating, stitching or filling prices.

Acquisition is limited by the existing terminal max-bars setting and a 100,000-bar application cap
per interval. No terminal settings are changed. At the inspected setting of 100,000 bars, this
requests the terminal's permitted window, not necessarily all broker archives. There is a 20-minute
deadline checked between native calls; an in-flight MT5 request can overrun it. Skipped intervals
remain explicit findings. All data are private local files; budget for up to a few hundred MB
across all intervals. An interrupted run has no completion marker and must not be accepted.

Exports appear under `MQL5/Files/stratosphere-history-*`. Validate with the same CLI:

```sh
.venv/bin/python -m stratosphere.connectors.mt5_probe \
  '/absolute/path/to/MQL5/Files/stratosphere-history-...' \
  'reports/mt5/unique-history-run'
```

The v2 validator supports the larger bounded files and produces `summary.md` with all 21 intervals,
actual retrieved counts, first/last raw server date labels, span and whether the bar budget or
retained terminal-series limit was reached. A frozen endpoint can exclude a newer bar even when
the retained series is full. Every interval's maximum-history status remains unverified: a short return, synchronized
series or early server date does not establish complete archives or absence of gaps. The fixed
endpoint is an acquisition cutoff, not a closed-bar guarantee. No backtest/order runs automatically.

## Interpretation and remaining gates

An actual local FTMO-Demo export from terminal build 6275 passed structural validation on
October 28, 2026: connected demo status, a zero-second-old gold quote at capture and 512 bars each
for M15/H1/H4/D1. W1 returned `-1` with error `4401`; its reported 498 local bars did not produce
an accepted sample. The cause remains unverified. Account leverage was 1:100, which does not
establish the intended Swing profile. The complete export, hashes and findings are retained privately
under `reports/mt5/trial-1793210934`; an earlier incomplete export is retained separately.
These are local provider observations, distinct from the nine passing authored validator tests.

The subsequent maximum-window export returned structurally valid candles for all 21 intervals.
H2/H3/H4/H6/H8/H12/D1 reach June 2004; H1 reaches February 2010 at the terminal bar limit.
Weekly returned 1,155 bars from September 2004; monthly returned 188 from March 2011.
The shorter intervals reached the terminal's 100,000-bar retained-series limit (some exports have
99,999 rows because the acquisition endpoint was frozen). Shorter coarse-interval results remain
unverified as complete archives. The snapshot quote was 306 seconds old, so this does not establish
a fresh real-time feed. Private raw evidence and reviewed coverage are under
`reports/mt5/history-1793211240/coverage-review.md`; acquisition failures are retained in metadata.
The exporter compiled with zero errors/warnings; 17 validator tests and five existing portfolio
setup tests passed. No native backtest or order was run.

Demo status and an FTMO server do not prove Free Trial, two-step Swing profile, evaluation phase,
account ownership or execution authority. Confirm the actual profile and dated FTMO rules separately.
A successful sample read does not establish maximum retrievable history; server first dates and
terminal bar counts are metadata, not complete downloaded datasets. The newest bar can still be
forming. Raw MQL5 server/chart timestamps are preserved: historical UTC conversion and market
sessions remain unverified. Quote age is measured against server time at capture, not at report read.
Current swap values do not reconstruct historical swap costs, and commissions are not verified.

Next implementation needs actual maximum-window acquisition/coverage verification, verified timestamp and
contract/cost semantics, an independently identified account/profile, FTMO loss/reset controls and
a separately approved execution bridge. This diagnostic does not run the gold SMA backtest.

Primary references: [CopyRates](https://www.mql5.com/en/docs/series/copyrates),
[series properties](https://www.mql5.com/en/docs/constants/tradingconstants/enum_series_info_integer),
[account properties](https://www.mql5.com/en/docs/constants/environment_state/accountinformation).
