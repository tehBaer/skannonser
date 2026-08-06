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


def _stub_drift_ok_sender(monkeypatch, run_cmd, fake_send) -> None:
    """Redirect `_drift_ok`'s notification send to `fake_send` for the
    duration of a test.

    `_drift_ok(source, stats, send=default_send)` binds `send`'s default at
    *function-definition* time -- `_drift_ok.__defaults__` already holds a
    direct reference to the real `skannonser.notifications.default_send`
    function object. The nightly command calls `_drift_ok("finn",
    finn_stats)` with no explicit `send=`, so a plain
    `monkeypatch.setattr(run_cmd, "default_send", fake_send)` (the usual
    pattern -- see `test_notifications.py`'s `test_cli_notify_daily_routes_
    to_daily_summary`, which patches `notify_cmd.default_send`) does NOT
    intercept it here: that only rebinds the module attribute name, and
    `_drift_ok`'s already-created default tuple never looks the name up
    again. Without patching `__defaults__` directly, a test that drives this
    path end-to-end shells out to the real `notify` binary via
    `subprocess.run` -- a real priority-1 push notification on a machine
    that has `notify` installed, or up to a 15s hang on one that doesn't.
    Both the attribute and `__defaults__` are patched here so the stub holds
    regardless of which one a caller ends up depending on.
    """
    monkeypatch.setattr(run_cmd, "default_send", fake_send)
    monkeypatch.setattr(run_cmd._drift_ok, "__defaults__", (fake_send,))


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

    calls = []

    def fake_send(title, message, priority=0):
        calls.append((title, message, priority))
        return True

    _stub_drift_ok_sender(monkeypatch, run_cmd, fake_send)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1, result.output
    assert len(calls) == 1
    assert calls[0][0] == "Parser drift"
    assert "info_primary_area" in calls[0][1]
    assert calls[0][2] == 1


def test_cli_nightly_no_drift_key_when_finn_step_hard_failed(tmp_path, monkeypatch):
    """A hard `ingest_finn` failure carries no `stats` key at all (see
    `nightly.py`'s `_record_failure`) -- the drift extraction must tolerate
    that instead of raising, and `result["failed"]` alone still exits 1.

    `_drift_ok` is never reached with findings on this path (`finn_stats`
    resolves to `{}`, so it returns True with no send), but the sender is
    stubbed anyway as cheap insurance against a subprocess call."""
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

    calls = []

    def fake_send(title, message, priority=0):
        calls.append((title, message, priority))
        return True

    _stub_drift_ok_sender(monkeypatch, run_cmd, fake_send)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1, result.output
    assert calls == []


# --- tools classify-tilstand -----------------------------------------------


def _migrated_db(tmp_path):
    from skannonser.store import connection, migrations

    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "tilstand-cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_classify_tilstand_status_prints_coverage(tmp_path):
    db = _migrated_db(tmp_path)
    result = CliRunner().invoke(
        app, ["tools", "classify-tilstand", "--db", str(db), "--status"]
    )
    assert result.exit_code == 0, result.output
    assert "tilstand_rows" in result.output


def test_classify_tilstand_refuses_pending_migrations(tmp_path):
    from skannonser.store import connection

    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(
        app, ["tools", "classify-tilstand", "--db", str(db), "--status"]
    )
    assert result.exit_code == 1, result.output
    assert "pending migrations" in result.output


def test_classify_tilstand_refuses_unbounded_run(tmp_path):
    """The spend guard: a bare invocation (no --limit, no --all) must exit 1
    before any API-touching code runs. No seam is injected here -- an
    attempted classification would crash on the missing anthropic import --
    so exit 1 also proves the guard fired first."""
    db = _migrated_db(tmp_path)
    result = CliRunner().invoke(
        app, ["tools", "classify-tilstand", "--db", str(db)]
    )
    assert result.exit_code == 1, result.output
    assert "--limit" in result.output


def test_classify_tilstand_help_text_renders_readable(tmp_path):
    """Guards against Rich-markup mangling: `[llm]` in the docstring gets
    stripped as a style tag, and `%%` in a help string renders literally
    since Typer/Click does not %-format help text."""
    result = CliRunner().invoke(app, ["tools", "classify-tilstand", "--help"])
    assert result.exit_code == 0, result.output
    assert "llm" in result.output
    assert "[llm]" not in result.output
    assert "50%" in result.output
    assert "%%" not in result.output


def test_classify_tilstand_wipe_clears_tables_without_api_call(tmp_path):
    """--wipe must clear listing_tilstand even in the (default, non-batch)
    path with --limit 0 -- no seam injected, so a real API call would crash
    on the missing anthropic import; exit 0 here proves none was attempted."""
    from skannonser.store import connection

    db = _migrated_db(tmp_path)
    conn = connection.connect(db)
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES (?, ?)", ("12345678", "u"))
    conn.execute(
        "INSERT INTO listing_tilstand (finnkode, tg2_count, tg3_count, classified_at) "
        "VALUES (?, 0, 0, ?)",
        ("12345678", "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    result = CliRunner().invoke(
        app,
        [
            "tools", "classify-tilstand", "--db", str(db),
            "--wipe", "--limit", "0", "--project-dir", str(tmp_path / "eiendom"),
        ],
    )
    assert result.exit_code == 0, result.output

    conn = connection.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 0


def test_export_then_import_tilstand_cache_roundtrips(tmp_path):
    """The local-classify -> server-publish path, end to end. Only the cache
    moves; the receiving side derives findings/rollups from it."""
    import json

    from skannonser.enrich.tilstand import cache_put, export_cache

    src = _migrated_db(tmp_path / "a")
    from skannonser.store import connection

    conn = connection.connect(src)
    cache_put(conn, "a" * 64, '{"findings": []}', model="claude-opus-5")
    conn.close()

    out = tmp_path / "cache.json"
    result = CliRunner().invoke(
        app, ["tools", "export-tilstand-cache", "--db", str(src), "--out", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert "1" in result.output
    assert json.loads(out.read_text())[0]["content_sha256"] == "a" * 64

    dst = _migrated_db(tmp_path / "b")
    result = CliRunner().invoke(
        app,
        ["tools", "import-tilstand-cache", "--db", str(dst), "--in", str(out), "--no-derive"],
    )
    assert result.exit_code == 0, result.output
    assert export_cache(connection.connect(dst))[0]["response_json"] == '{"findings": []}'


def test_import_tilstand_cache_errors_on_missing_file(tmp_path):
    db = _migrated_db(tmp_path)
    result = CliRunner().invoke(
        app,
        ["tools", "import-tilstand-cache", "--db", str(db),
         "--in", str(tmp_path / "nope.json"), "--no-derive"],
    )
    assert result.exit_code == 1, result.output
    assert "not found" in result.output


def test_export_tilstand_cache_refuses_pending_migrations(tmp_path):
    from skannonser.store import connection

    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(
        app, ["tools", "export-tilstand-cache", "--db", str(db), "--out", str(tmp_path / "o.json")]
    )
    assert result.exit_code == 1, result.output
    assert "pending migrations" in result.output
