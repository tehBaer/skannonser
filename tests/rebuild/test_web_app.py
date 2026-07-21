import sqlite3

from fastapi.testclient import TestClient

from skannonser.store import connection, migrations
from skannonser.web.app import create_app


def _migrated_db(tmp_path):
    db_path = tmp_path / "migrated.db"
    conn = connection.connect(db_path)
    migrations.migrate(conn)
    conn.close()
    return db_path


def test_healthz_ok_on_migrated_db(tmp_path):
    db_path = _migrated_db(tmp_path)
    client = TestClient(create_app(db_path))

    resp = client.get("/healthz")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "db": True}


def test_healthz_degraded_on_unmigrated_db(tmp_path):
    """A fresh DB has no schema_migrations table yet. healthz must report
    503 WITHOUT writing anything -- migrations.pending() would otherwise
    try to CREATE TABLE IF NOT EXISTS schema_migrations, which fails on a
    read-only connection when the table doesn't already exist."""
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE placeholder (x INTEGER)")
    conn.commit()
    conn.close()

    before_stat = db_path.stat()

    client = TestClient(create_app(db_path))
    resp = client.get("/healthz")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert "reason" in body

    after_stat = db_path.stat()
    assert before_stat.st_size == after_stat.st_size
    assert before_stat.st_mtime == after_stat.st_mtime


def test_healthz_degraded_on_pending_migrations(tmp_path, monkeypatch):
    """schema_migrations exists but not every migration has been applied
    yet -- also degraded, and still no write attempted."""
    mig_dir = tmp_path / "migs"
    mig_dir.mkdir()
    (mig_dir / "001_first.sql").write_text("CREATE TABLE a (x INTEGER);")
    (mig_dir / "002_second.sql").write_text("CREATE TABLE b (x INTEGER);")
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", mig_dir)

    db_path = tmp_path / "partial.db"
    conn = connection.connect(db_path)
    conn.execute(
        "CREATE TABLE schema_migrations ("
        " id TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
    )
    conn.execute("INSERT INTO schema_migrations (id) VALUES ('001_first')")
    conn.commit()
    conn.close()

    client = TestClient(create_app(db_path))
    resp = client.get("/healthz")

    assert resp.status_code == 503
    assert resp.json()["status"] == "degraded"


def test_healthz_degraded_on_missing_db_file(tmp_path):
    db_path = tmp_path / "does-not-exist.db"
    client = TestClient(create_app(db_path))

    resp = client.get("/healthz")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["db"] is False
    assert not db_path.exists()


def test_root_serves_placeholder_index(tmp_path):
    db_path = _migrated_db(tmp_path)
    client = TestClient(create_app(db_path))

    resp = client.get("/")

    assert resp.status_code == 200
    assert "skannonser" in resp.text


def test_cli_web_command_fails_loud_on_pending_migrations(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app
    from skannonser.commands import web_cmd

    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE placeholder (x INTEGER)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(db_path))

    called = {"uvicorn_run": False}

    def _fake_run(*args, **kwargs):
        called["uvicorn_run"] = True

    monkeypatch.setattr("uvicorn.run", _fake_run)

    result = CliRunner().invoke(app, ["web"])

    assert result.exit_code == 1
    assert not called["uvicorn_run"]
    assert web_cmd.app is not None


def test_cli_web_command_registered():
    from typer.testing import CliRunner

    from skannonser.cli import app

    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "web" in result.output
