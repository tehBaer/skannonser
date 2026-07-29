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
