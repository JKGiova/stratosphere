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

## Interpretation and remaining gates

Demo status and an FTMO server do not prove Free Trial, two-step Swing profile, evaluation phase,
account ownership or execution authority. Confirm the actual profile and dated FTMO rules separately.
A successful sample read does not establish maximum retrievable history; server first dates and
terminal bar counts are metadata, not complete downloaded datasets. The newest bar can still be
forming. Raw MQL5 server/chart timestamps are preserved: historical UTC conversion and market
sessions remain unverified. Quote age is measured against server time at capture, not at report read.
Current swap values do not reconstruct historical swap costs, and commissions are not verified.

Next implementation needs bounded full-history acquisition/quality checks, verified timestamp and
contract/cost semantics, an independently identified account/profile, FTMO loss/reset controls and
a separately approved execution bridge. This diagnostic does not run the gold SMA backtest.

Primary references: [CopyRates](https://www.mql5.com/en/docs/series/copyrates),
[series properties](https://www.mql5.com/en/docs/constants/tradingconstants/enum_series_info_integer),
[account properties](https://www.mql5.com/en/docs/constants/environment_state/accountinformation).
