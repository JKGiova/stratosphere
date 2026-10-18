"""Inspect input as received: no sorting, deduplication, price filling or clock invention."""

from stratosphere.market_data.contracts import DataBatch, DataQuality, QualityFinding, QualityPolicy


def inspect_batch(batch: DataBatch, policy: QualityPolicy, *, now_ns: int) -> DataQuality:
    findings = []

    def add(code, detail, indices=()):
        findings.append(QualityFinding(code=code, detail=detail, indices=tuple(indices)))

    rows = batch.rows
    if not rows:
        add("empty", "No observations acquired.")
    if len(rows) < policy.warmup_bars:
        add("insufficient_warmup", "Retained observations do not satisfy the declared warmup.")
    interval = batch.identity.interval_seconds * 10**9
    opens = {}
    for index, row in enumerate(rows):
        if row.open_ns in opens:
            add(
                "duplicate",
                "Repeated opening timestamp; neither observation was removed.",
                (opens[row.open_ns], index),
            )
        else:
            opens[row.open_ns] = index
        if row.end_ns - row.open_ns != interval:
            add(
                "interval_mismatch",
                "Observation width differs from the requested interval.",
                (index,),
            )
        if (row.open_ns - policy.alignment_origin_ns) % interval:
            add("unaligned_interval", "Bar is not aligned to the declared UTC interval.", (index,))
        if not row.complete:
            add(
                "incomplete",
                "Running/incomplete observation cannot produce a closed-bar signal.",
                (index,),
            )
        if row.volume is None and batch.identity.price_kind != "reference_rate":
            add("missing_volume", "Required OHLCV volume is absent.", (index,))
        if batch.availability == "observed_receipt" and row.available_ns != row.received_ns:
            add(
                "receipt_provenance_mismatch",
                "Receipt-only evidence cannot assert an earlier publication time.",
                (index,),
            )
        if batch.availability == "assumed_close" and row.available_ns != row.end_ns:
            add(
                "close_assumption_mismatch",
                "The declared close-time assumption differs from the retained availability.",
                (index,),
            )
        if row.end_ns > now_ns or row.available_ns > now_ns or row.received_ns > now_ns:
            add(
                "future_observation",
                "Event, availability or receipt is later than the check clock.",
                (index,),
            )
        if row.available_ns < row.end_ns:
            add(
                "availability_before_close",
                "A closed observation cannot be available before its interval ends.",
                (index,),
            )
        if row.received_ns < row.available_ns:
            add(
                "receipt_before_availability",
                "Provider availability is later than recorded receipt.",
                (index,),
            )
        if row.received_ns - row.end_ns > policy.max_receipt_lag_ns:
            add(
                "receipt_lag",
                "Recorded receipt exceeds the explicitly configured latency bound.",
                (index,),
            )
        if index:
            previous = rows[index - 1]
            if row.open_ns < previous.open_ns:
                add(
                    "unordered",
                    "Input order is not chronological; no sorting was performed.",
                    (index - 1, index),
                )
            elif row.open_ns > previous.end_ns:
                if (
                    batch.session_calendar != "verified_exchange_calendar"
                    and batch.session_calendar != "reference_publication_calendar"
                ):
                    add(
                        "gap",
                        "Missing interval in a continuous or unverified calendar.",
                        (index - 1, index),
                    )
                else:
                    # A label alone cannot establish that this particular closure is expected.
                    add(
                        "calendar_gap_requires_schedule",
                        "Calendar-aware gaps require the actual pinned schedule; none was "
                        "supplied.",
                        (index - 1, index),
                    )
            elif previous.open_ns < row.open_ns < previous.end_ns:
                add("overlap", "Observations overlap.", (index - 1, index))
    if rows and now_ns - max(row.end_ns for row in rows) > policy.max_age_ns:
        add("stale", "Newest observation is older than the configured freshness bound.")
    if (
        rows
        and policy.expected_start_ns is not None
        and (
            rows[0].open_ns != policy.expected_start_ns or rows[-1].end_ns != policy.expected_end_ns
        )
    ):
        add("incomplete_requested_window", "Retained rows do not cover the exact requested bounds.")
    if batch.availability == "assumed_close" and not policy.allow_assumed_availability:
        add(
            "unverified_availability",
            "Candle close is an assumption, not provider publication evidence.",
        )
    if batch.identity.asset_class in {"equity", "etf"}:
        add(
            "calendar_adapter_not_verified",
            "The v1 checker has no pinned exchange-session schedule/action adapter; "
            "metadata labels alone do not verify stock/ETF bars.",
        )
        if batch.session_calendar != "verified_exchange_calendar":
            add(
                "unverified_sessions",
                "Equity/ETF sessions, timezone and closures are not verified.",
            )
        if batch.corporate_actions not in {
            "unadjusted_with_verified_actions",
            "adjusted_with_verified_actions",
        }:
            add(
                "unverified_corporate_actions",
                "Split/dividend treatment and adjustment semantics are unverified.",
            )
    if batch.identity.asset_class == "crypto_perpetual" and batch.funding != "retained":
        add("missing_funding", "Perpetual funding evidence was not retained.")
    if batch.identity.asset_class == "crypto_perpetual":
        add(
            "perpetual_adapter_not_verified",
            "Contract multipliers, settlement and funding records have no verified adapter in v1.",
        )
    if batch.identity.asset_class == "forex" and batch.identity.price_kind != "reference_rate":
        add(
            "tradable_forex_adapter_not_verified",
            "Venue quotes, sessions and execution units have no verified tradable forex adapter.",
        )
    if batch.identity.price_kind == "reference_rate":
        add("reference_only", "Reference rates are not executable or tradable-market OHLCV.")
    return DataQuality(
        batch_sha256=batch.digest,
        checked_at_ns=now_ns,
        findings=tuple(findings),
        accepted_rows=len(rows) if not findings else 0,
    )
