"""Tests for the notify-metrics golden-master harness (`skannonser/verify/metrics.py`)
and the `skannonser verify metrics` CLI command.

No network, no subprocess: `verify_metrics` drives the REAL legacy
`main.notify.listing_metrics.compute_daily_metrics` (pure set arithmetic,
zero I/O) against `skannonser.notifications.compute_daily_metrics`. A zero
`api_usage` row count is asserted, matching the pattern in
`test_verify_enrich.py`/`test_verify_sheets.py`.
"""
from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.verify.metrics import verify_metrics


@pytest.fixture()
def db_path(tmp_path) -> Path:
    path = tmp_path / "verify_metrics.db"
    conn = connection.connect(path)
    migrations.migrate(conn)
    conn.close()
    return path


def _api_usage_count(db_path: Path) -> int:
    conn = connection.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) AS c FROM api_usage").fetchone()["c"]
    finally:
        conn.close()


def test_verify_metrics_zero_diffs_on_empty_db(db_path):
    result = verify_metrics(db_path)
    assert result.diffs == []
    assert _api_usage_count(db_path) == 0


def test_verify_metrics_zero_diffs_with_live_db_data(db_path):
    """A populated live_db scenario (previous snapshot + active listings +
    a sold removal) still agrees with legacy -- the golden master proper."""
    conn = connection.connect(db_path)
    repo = ListingsRepo(conn)
    repo.replace_active_snapshot({"1", "2"})
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
        "VALUES ('2', 1, 3000000, 80, NULL, 'https://finn.no/2')"
    )
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
        "VALUES ('3', 1, 3000000, 80, NULL, 'https://finn.no/3')"
    )
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
        "VALUES ('1', 0, 3000000, 80, 'Solgt', 'https://finn.no/1')"
    )
    conn.commit()
    conn.close()

    result = verify_metrics(db_path)
    assert result.diffs == []


def test_verify_metrics_detects_a_real_disagreement(db_path, monkeypatch):
    """Negative control: prove the harness actually detects a diff, by
    monkeypatching the NEW side to return a wrong value for one scenario."""
    import skannonser.verify.metrics as metrics_mod

    real_compute = metrics_mod.compute_daily_metrics

    def broken_compute(previous, current, sold_finnkodes):
        result = real_compute(previous, current, sold_finnkodes)
        result["added"] += 1  # deliberately wrong
        return result

    monkeypatch.setattr(metrics_mod, "compute_daily_metrics", broken_compute)
    result = verify_metrics(db_path)
    assert len(result.diffs) >= 1
    assert any(d.field == "added" for d in result.diffs)


def test_cli_verify_metrics_zero_diffs_exits_zero(db_path):
    result = CliRunner().invoke(app, ["verify", "metrics", "--db", str(db_path)])
    assert result.exit_code == 0
    assert "metrics diffs: 0" in result.output


def test_cli_verify_metrics_missing_db_exits_nonzero(tmp_path):
    missing = tmp_path / "nope.db"
    result = CliRunner().invoke(app, ["verify", "metrics", "--db", str(missing)])
    assert result.exit_code != 0


def test_cli_verify_metrics_exits_nonzero_on_diff(db_path, monkeypatch):
    import skannonser.commands.verify_cmd as verify_cmd_mod
    from skannonser.verify.metrics import MetricsDiff, VerifyMetricsResult

    def fake_verify_metrics(path):
        return VerifyMetricsResult(diffs=[MetricsDiff("fake", "added", 1, 2)])

    monkeypatch.setattr(verify_cmd_mod, "verify_metrics", fake_verify_metrics)
    result = CliRunner().invoke(app, ["verify", "metrics", "--db", str(db_path)])
    assert result.exit_code == 1
    assert "metrics diffs: 1" in result.output
