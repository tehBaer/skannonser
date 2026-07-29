from typer.testing import CliRunner

from skannonser.cli import app


def test_cli_help_exits_zero():
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "skannonser" in result.output


def test_drift_ok_true_when_no_findings():
    from skannonser.commands.run_cmd import _drift_ok

    sent = []
    assert _drift_ok("FINN", {"drift": []}, send=lambda *a, **k: sent.append(a)) is True
    assert sent == []


def test_drift_ok_false_and_notifies_on_findings(capsys):
    from skannonser.commands.run_cmd import _drift_ok
    from skannonser.ingest.drift import DriftFinding

    sent = []
    ok = _drift_ok(
        "FINN",
        {"drift": [DriftFinding("info_primary_area", 0.19, 0.0, 1000)]},
        send=lambda title, message, priority=0: sent.append((title, message, priority)),
    )
    assert ok is False
    assert sent[0][0] == "Parser drift"
    assert "info_primary_area" in sent[0][1]
    assert sent[0][2] == 1
    assert "info_primary_area" in capsys.readouterr().err


def test_drift_ok_tolerates_missing_key():
    """Older stats dicts, and run_dnb_ingest, carry no `drift` key."""
    from skannonser.commands.run_cmd import _drift_ok

    assert _drift_ok("DNB", {}, send=lambda *a, **k: None) is True


def test_cli_nightly_exits_nonzero_on_drift(tmp_path, monkeypatch):
    """Fix 1: the production cron runs `run nightly`, not `run ingest` --
    drift found in the FINN ingest step must surface here too, exactly like
    `run ingest` already does via `_drift_ok`. Follows the fake-`run_nightly`
    CLI-integration pattern in `tests/rebuild/test_nightly.py`."""
    from skannonser.commands import run_cmd
    from skannonser.ingest.drift import DriftFinding
    from skannonser.store import connection, migrations

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))

    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()

    def fake_run_nightly(conn, domain, gateway, api_key, client, fetch=None, post=None, sheets_writer=None):
        return {
            "steps": {
                "ingest_finn": {
                    "ok": True,
                    "stats": {
                        "drift": [DriftFinding("info_primary_area", 0.19, 0.0, 1000)],
                    },
                }
            },
            "failed": [],
            "budget_exhausted": [],
        }

    monkeypatch.setattr(run_cmd, "run_nightly", fake_run_nightly)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1, result.output
    assert "info_primary_area" in result.output


def test_cli_nightly_no_drift_key_when_finn_step_hard_failed(tmp_path, monkeypatch):
    """A hard `ingest_finn` failure carries no `stats` key at all (see
    `nightly.py`'s `_record_failure`) -- the drift extraction must tolerate
    that instead of raising, and `result["failed"]` alone still exits 1."""
    from skannonser.commands import run_cmd
    from skannonser.store import connection, migrations

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))

    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()

    def fake_run_nightly(conn, domain, gateway, api_key, client, fetch=None, post=None, sheets_writer=None):
        return {
            "steps": {"ingest_finn": {"ok": False, "error": "boom"}},
            "failed": ["ingest_finn"],
            "budget_exhausted": [],
        }

    monkeypatch.setattr(run_cmd, "run_nightly", fake_run_nightly)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1, result.output
