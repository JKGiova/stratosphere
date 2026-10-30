# Strategy research tools and public-reader fallbacks

The orchestrator discovers sources and clarifies a strategy. Its strategy specialist develops a
validated signal program through the persistent v2 loop. These tools expose application operations,
not a general terminal. They cannot inspect accounts, retrieve credentials, execute acquired Python,
change frozen research scopes, approve a strategy or place MT5 orders.

## Specialist tools

| Tool | Behavior |
| --- | --- |
| `inspect_brief` | Read this campaign's sealed intake and citations |
| `research_capabilities` | Report installed public readers and their fallback order |
| `search_sources` | Search public strategy terms under the frozen search allowance |
| `read_source` | Acquire a retained search-result index; return bounded source passages, including PDF page text |
| `inspect_program` | Read the current validated definition and application-generated Python |
| `inspect_data` | Inspect frozen target coverage, gaps, currency and snapshot identities |
| `propose_program` | Propose a complete signal revision while preserving nonsignal behavior |
| `tune_indicator` | Change one existing indicator's period; application increases required warmup |
| `test_program` | Compile and run structural/synthetic signal checks |
| `backtest` | Reserve and run native base/stress experiments on the frozen datasets |
| `compare_results` | Compare this campaign's measured summaries |
| `analyze_results` | Inspect aggregate return, drawdown, costs, turnover, exposure and trade distribution |
| `analyze_conditions` | Inspect same-sample year and prior-direction attribution |
| `build_report` | Preview the retained best case and evidence; grants no approval |
| `finish` | Ask the application to seal the final report |

The available-tool schema changes with application state. A structural test must precede a native
backtest; economic revision or completion requires evaluation. Technical repair remains separately
bounded. Indicator edits pass the same validation, duplicate, exact variant-owner and progress gates
as full proposals. All tool turns consume the existing model/time ceilings. Analysis exposes the
latest eight result rows with an explicit omitted count; full attempts remain in the sealed report.
Raw fills/equity curves are excluded from these analysis observations. Search response evidence stays
in receipts, while model observations omit raw search payloads.

Market/asset suitability is measured only across the preparation's four frozen target scopes.
These tools do not expand that universe mid-loop or establish a global optimum. See
[strategy development](strategy-development.md) for supported language, budgets, stopping and
checkpoint rules.

## Public source routing

For general public HTTPS pages, the reader tries this finite sequence:

1. Direct application HTTP/text or local PDF extraction.
2. Agent Reach's `web read`, using its public Jina/Exa readers.
3. OpenCLI's public Wikipedia adapter, when the URL is an English Wikipedia article.
4. agent-browser in a fresh anonymous, temporary browser session.

Search first uses the existing Wikipedia/Crossref indexes. If they fail technically or produce no
hits, it tries Agent Reach's web search and then OpenCLI's public arXiv API adapter. Browser search
and arbitrary OpenCLI commands are not exposed. Search results remain subject to canonical HTTPS
validation and acquisition checks. Provider-specific source readers keep their own identity,
commit/blob pinning and format restrictions; this fallback does not reinterpret a blocked GitHub,
TradingView or authenticated platform source as executable source code.

Each backend is attempted at most once by the application per operation, with finite time/byte
limits. Missing tools, unsupported adapters, empty text, timeouts and technical failures can move
to the next reader. HTTP 401/403/429, publisher 404, recognized login/paywall/CAPTCHA text, sensitive
output and public-address failures stop acquisition. Structured CLI authentication/rate errors also
stop escalation. Index access denials stop search fallback. No tool installs itself, logs in,
reuses browser cookies or purchases capacity during a research turn. Upstream services have their
own availability, parsing and quota limits; fallback is an additional route, not guaranteed access.

Acquisition retains safe raw responses, per-backend attempt states, content digests and a versioned
`research-fallback/1` audit. Failed/cancelled attempts remain readable without publishing late text.
Hosted-reader text is derived material tied to the requested URL; it is not proof of publication
identity, faithful conversion or execution authority. PDF extraction retains its `pdf-pages/1`
reader and physical-page citations. No OCR or verified equation interpretation is added.

## Optional local installation

These readers are optional dependencies for a **source checkout**, separate from the application
Python environment. The checkout works without them and reports unavailable capabilities explicitly.
Install with Node >=20.18.1, npm and uv available:

```sh
./tools/research/setup.sh
```

The script uses npm's committed lockfile without lifecycle scripts and a separate
`.tools/agent-reach` environment with pinned Python requirements. It does not run Agent Reach setup,
import browser logins, install extensions or modify application credentials. Current pins:

- [Agent Reach](https://github.com/panniantong/agent-reach), commit
  `14c213405669414492bda0f8d52cc6531d062c45` (not the unrelated similarly named PyPI package).
- [OpenCLI](https://github.com/jackwener/opencli), `@jackwener/opencli` 2.0.2.
- [agent-browser](https://agent-browser.dev/security), 0.41.0.

Browser reading additionally needs an installed Chrome/Chromium executable. macOS uses the installed
Google Chrome app with a new temporary profile, never its personal profile. Linux resolves an installed `google-chrome`, `google-chrome-stable`, `chromium` or
`chromium-browser` from PATH. Missing executables make the browser capability unavailable.
This installation does not download a browser. Linux and each operating-system binary still need
separate runtime verification; fixture coverage alone does not establish compatibility. Optional runtimes
are not bundled in the application wheel. Dependency manifests join the development runtime digest;
changing them blocks continuation of an old active campaign rather than resetting its budget.
Local locks and digests are not executable attestation of installed third-party binaries.

## Process and network boundaries

Only fixed argument arrays are launched, without a shell. Child environments omit operator API keys,
provider settings, browser profiles and application configuration. A short, fresh physical temporary
HOME avoids macOS symlink/socket-path problems. Standard TLS validation uses the application's CA
bundle; certificate verification is never disabled.

An application-owned CONNECT proxy permits HTTPS to the exact backend hosts, validates every
resolved peer as public and connects to that checked IP. Browser navigation is restricted to the
requested public host, with implicit loopback proxy bypass removed. Its deny-default action policy
allows only launch, navigation and text extraction; no model-selected clicks, evaluation, uploads,
logins, plugins or WebMCP. Explicit close, process-group cleanup, proxy shutdown and a short idle
limit bound owned sessions. Cross-domain redirects/CDNs may fail under these restrictions.

This is process/environment isolation and constrained cooperative network routing, **not an OS
filesystem/network sandbox** for third-party executables or hostile browser content. Hosted readers
perform their own remote fetching; local peer pinning verifies connections to those services, not
their internal retrieval chain. Never send private URLs, files, identifiers or credentials to them.

## Verification

Authored tests exercise tool schemas/state, deterministic tuning, exact variant lineage, native
reservations, source fallback order, missing readers, policy stops, secret rejection, retained
failures, PDF handling, proxy peer checks and process cancellation/timeout cleanup. These tests need
no installed optional reader or external network.

Actual public checks on October 30, 2026 exercised OpenCLI arXiv search, Agent Reach/Jina reading and
anonymous Chrome text extraction through the proxy. They verify those installed public paths on
this Mac, not authenticated sites, all platforms or autonomous research quality. Local operational
logs remain under `reports/research-tools-20261030`; failed startup checks are retained alongside
successful results. An application-level check also acquired and reopened the public security guide
through the browser fallback, with earlier-reader outages explicitly injected as fixtures.
Strategy performance, model reliability and MT5 deployment remain separate gates.
