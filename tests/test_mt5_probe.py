from pathlib import Path

import pytest

from stratosphere.connectors.mt5_probe import (
    HEADER,
    HISTORY_FRAMES,
    HISTORY_SCHEMA,
    SCHEMA,
    inspect_probe,
    main,
)


@pytest.fixture
def export(tmp_path: Path) -> Path:
    (tmp_path / "complete.txt").write_text(SCHEMA + "\n")
    fields = {
        "schema": SCHEMA,
        "captured_utc_epoch": "1000",
        "finished_utc_epoch": "1001",
        "connected": "1",
        "account_mode": "ACCOUNT_TRADE_MODE_DEMO",
        "company": "FTMO",
        "server": "FTMO-Demo",
        "symbol": "XAUUSD",
        "time_semantics": "raw server time",
        "profile_verification": "not verified",
        "quote_server_age_seconds": "1",
        "PERIOD_D1.copy_count": "2",
        "PERIOD_D1.copy_error": "0",
        "PERIOD_D1.synchronized": "1",
    }
    (tmp_path / "metadata.tsv").write_text(
        "".join(f"{key}\t{value}\n" for key, value in fields.items())
    )
    (tmp_path / "PERIOD_D1.tsv").write_text(
        "\t".join(HEADER) + "\n100\t10\t12\t9\t11\t5\t2\t0\n200\t11\t12\t10\t11\t5\t2\t0\n"
    )
    return tmp_path


def test_preserves_bytes_and_separates_sample_from_unverified_profile(export):
    report, raw = inspect_probe(export)
    assert raw["metadata.tsv"] == (export / "metadata.tsv").read_bytes()
    assert report["coverage"]["PERIOD_D1"]["retrieved_bars"] == 2
    assert len(report["sha256"]["metadata.tsv"]) == 64
    assert "not verified" in report["limits"][1]
    assert len(report["findings"]) == 4  # Missing intervals remain visible.


@pytest.mark.parametrize(
    "replacement",
    [
        "100\t11\t12\t10\t11\t5\t2\t0",  # Duplicate timestamp.
        "200\t11\t10\t9\t11\t5\t2\t0",  # Impossible high.
        "200\tNaN\t12\t10\t11\t5\t2\t0",
        "200\t11\t12\t10\t11\t-1\t2\t0",
    ],
)
def test_rejects_corrupt_bars_without_repair(export, replacement):
    path = export / "PERIOD_D1.tsv"
    path.write_text(path.read_text().replace("200\t11\t12\t10\t11\t5\t2\t0", replacement))
    with pytest.raises(ValueError):
        inspect_probe(export)


def test_rejects_partial_export(export):
    (export / "complete.txt").unlink()
    with pytest.raises(ValueError):
        inspect_probe(export)


def test_rejects_duplicate_metadata(export):
    path = export / "metadata.tsv"
    path.write_text(path.read_text() + "connected\t0\n")
    with pytest.raises(ValueError, match="duplicate"):
        inspect_probe(export)


def test_disconnected_real_account_and_stale_quote_remain_failures(export):
    path = export / "metadata.tsv"
    path.write_text(
        path.read_text()
        .replace("connected\t1", "connected\t0")
        .replace("ACCOUNT_TRADE_MODE_DEMO", "ACCOUNT_TRADE_MODE_REAL")
        .replace("quote_server_age_seconds\t1", "quote_server_age_seconds\t120")
    )
    report, _ = inspect_probe(export)
    assert any("disconnected" in finding for finding in report["findings"])
    assert any("not reported as demo" in finding for finding in report["findings"])
    assert any("60 seconds" in finding for finding in report["findings"])


def test_rejects_count_mismatch_and_linked_files(export):
    path = export / "metadata.tsv"
    original = path.read_text()
    path.write_text(original.replace("PERIOD_D1.copy_count\t2", "PERIOD_D1.copy_count\t3"))
    with pytest.raises(ValueError, match="count"):
        inspect_probe(export)
    path.write_text(original)
    bars = export / "PERIOD_D1.tsv"
    bars.rename(export / "elsewhere.tsv")
    bars.symlink_to(export / "elsewhere.tsv")
    with pytest.raises(ValueError, match="linked"):
        inspect_probe(export)


@pytest.fixture
def history_export(export):
    path = export / "metadata.tsv"
    path.write_text(
        path.read_text().replace(SCHEMA, HISTORY_SCHEMA)
        + "bar_budget_per_interval\t100000\nterminal_max_bars\t100000\n"
        + "attempt_budget_per_interval\t3\n"
        + "PERIOD_D1.attempt_1.error\t4401\n"
    )
    (export / "complete.txt").write_text(HISTORY_SCHEMA + "\n")
    return export


def test_history_keeps_all_intervals_and_failed_acquisition_attempts(history_export):
    report, _ = inspect_probe(history_export)
    assert len(HISTORY_FRAMES) == 21
    assert len(report["findings"]) == 20
    assert report["metadata"]["PERIOD_D1.attempt_1.error"] == "4401"
    assert report["coverage"]["PERIOD_D1"]["maximum_history_verified"] is False
    assert report["coverage"]["PERIOD_D1"]["first_server_label"] == "1970-01-01 00:01"


def test_history_accepts_more_than_old_sample_limit(history_export):
    path = history_export / "metadata.tsv"
    path.write_text(
        path.read_text().replace("PERIOD_D1.copy_count\t2", "PERIOD_D1.copy_count\t600")
    )
    (history_export / "PERIOD_D1.tsv").write_text(
        "\t".join(HEADER) + "\n" + "".join(f"{i + 1}\t10\t12\t9\t11\t5\t2\t0\n" for i in range(600))
    )
    report, _ = inspect_probe(history_export)
    assert report["coverage"]["PERIOD_D1"]["retrieved_bars"] == 600


@pytest.mark.parametrize(
    "budget,terminal", [("100001", "200000"), ("0", "100000"), ("100000", "99999")]
)
def test_history_rejects_invalid_or_broadened_budgets(history_export, budget, terminal):
    path = history_export / "metadata.tsv"
    path.write_text(
        path.read_text()
        .replace("bar_budget_per_interval\t100000", f"bar_budget_per_interval\t{budget}")
        .replace("terminal_max_bars\t100000", f"terminal_max_bars\t{terminal}")
    )
    with pytest.raises(ValueError, match="budget"):
        inspect_probe(history_export)


def test_history_marks_cap_and_does_not_claim_complete_history(history_export):
    path = history_export / "metadata.tsv"
    path.write_text(
        path.read_text().replace("bar_budget_per_interval\t100000", "bar_budget_per_interval\t2")
    )
    report, _ = inspect_probe(history_export)
    assert report["coverage"]["PERIOD_D1"]["bar_budget_reached"] is True
    assert any("older history may exist" in finding for finding in report["findings"])


def test_history_recognizes_terminal_cap_even_when_frozen_end_excludes_new_bar(history_export):
    path = history_export / "metadata.tsv"
    path.write_text(path.read_text() + "PERIOD_D1.local_bars\t100000\n")
    report, _ = inspect_probe(history_export)
    item = report["coverage"]["PERIOD_D1"]
    assert item["bar_budget_reached"] is False
    assert item["terminal_series_at_budget"] is True
    assert any("terminal series reached" in finding for finding in report["findings"])


def test_history_cli_retains_raw_bytes_and_writes_21_interval_table(history_export, monkeypatch):
    output = history_export / "retained"
    monkeypatch.setattr("sys.argv", ["mt5_probe", str(history_export), str(output)])
    main()
    assert (output / "PERIOD_D1.tsv").read_bytes() == (
        history_export / "PERIOD_D1.tsv"
    ).read_bytes()
    text = (output / "summary.md").read_text()
    assert "| D1 | 2 |" in text
    assert "| MN1 | 0 | unavailable" in text
    with pytest.raises(FileExistsError):
        main()
