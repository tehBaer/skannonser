from skannonser.store import connection, migrations

EXPECTED_TABLES = {
    "eiendom", "eiendom_processed", "dnbeiendom", "manual_overrides",
    "listing_comments", "stations", "station_lines", "station_travel",
}


def _tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {r["name"] for r in rows}


def test_migrate_fresh_db_creates_full_schema(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert ran == ["001_adopt_live_schema"]
    assert EXPECTED_TABLES <= _tables(conn)
    assert "schema_migrations" in _tables(conn)


def test_migrate_is_idempotent(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    assert migrations.migrate(conn) == []
    assert migrations.pending(conn) == []


def test_migrate_adopts_preexisting_schema(tmp_path):
    """Simulates the live DB: schema already exists, migration must no-op cleanly."""
    conn = connection.connect(tmp_path / "live.db")
    sql = (migrations.MIGRATIONS_DIR / "001_adopt_live_schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)  # pre-existing schema, no migration bookkeeping
    ran = migrations.migrate(conn)
    assert ran == ["001_adopt_live_schema"]
    assert EXPECTED_TABLES <= _tables(conn)


def test_connection_settings(tmp_path):
    conn = connection.connect(tmp_path / "x.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_migrate_cli_fails_loud_when_db_missing(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app

    missing = tmp_path / "does-not-exist.db"
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(missing))
    result = CliRunner().invoke(app, ["db", "migrate"])
    assert result.exit_code == 1
    assert not missing.exists()
