import sqlite3
import warnings

import pytest
from starlette.exceptions import StarletteDeprecationWarning

# starlette's TestClient nudges toward the httpx2 fork; the contract pins
# plain httpx (>=0.27) for the dev extra, and it works fine here. The warning
# fires at IMPORT time (fastapi/testclient.py) during collection, so it must
# be suppressed at the import site -- pytestmark/filterwarnings config cannot
# intercept collection-time warnings.
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="Using `httpx` with `starlette.testclient` is deprecated",
        category=StarletteDeprecationWarning,
    )
    from fastapi.testclient import TestClient

from skannonser.store import connection, migrations
from skannonser.web.app import create_app, ro_conn


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


# ---------------------------------------------------------------------------
# GET /thumbs/{identifier}.jpg (Phase 5 Task 5)
# ---------------------------------------------------------------------------


def test_thumb_serves_cached_file(tmp_path):
    db_path = _migrated_db(tmp_path)
    thumbs_dir = tmp_path / "thumbs"
    thumbs_dir.mkdir()
    (thumbs_dir / "12345.jpg").write_bytes(b"jpeg-bytes")
    client = TestClient(create_app(db_path, thumbs_dir=thumbs_dir))

    resp = client.get("/thumbs/12345.jpg")

    assert resp.status_code == 200
    assert resp.content == b"jpeg-bytes"
    assert resp.headers["content-type"] == "image/jpeg"


def test_thumb_serves_dnb_synthetic_identifier(tmp_path):
    db_path = _migrated_db(tmp_path)
    thumbs_dir = tmp_path / "thumbs"
    thumbs_dir.mkdir()
    (thumbs_dir / "dnb:abcdef0123456789.jpg").write_bytes(b"jpeg-bytes")
    client = TestClient(create_app(db_path, thumbs_dir=thumbs_dir))

    resp = client.get("/thumbs/dnb:abcdef0123456789.jpg")

    assert resp.status_code == 200
    assert resp.content == b"jpeg-bytes"


def test_thumb_404_when_file_missing(tmp_path):
    db_path = _migrated_db(tmp_path)
    thumbs_dir = tmp_path / "thumbs"
    thumbs_dir.mkdir()
    client = TestClient(create_app(db_path, thumbs_dir=thumbs_dir))

    resp = client.get("/thumbs/nope.jpg")

    assert resp.status_code == 404
    assert resp.json() == {"detail": "not found"}


def test_thumb_404_when_no_thumbs_dir_configured(tmp_path):
    db_path = _migrated_db(tmp_path)
    client = TestClient(create_app(db_path, thumbs_dir=None))

    resp = client.get("/thumbs/12345.jpg")

    assert resp.status_code == 404


@pytest.mark.parametrize(
    "identifier",
    [
        "..%2F..%2Fetc%2Fpasswd",  # encoded traversal, single segment
        "..",
        "a.b",  # literal dot not in the allowed charset
        "a/b",  # would only reach the handler via an already-decoded slash
    ],
)
def test_thumb_traversal_attempt_400_without_touching_fs_outside_dest(tmp_path, identifier):
    """Every value that could plausibly reach the handler as `identifier`
    either fails the shared IDENTIFIER_RE (400, checked before any
    filesystem access) or fails routing entirely (multi-segment paths never
    reach the handler) -- neither ever stats/opens anything outside
    `thumbs_dir`."""
    db_path = _migrated_db(tmp_path)
    thumbs_dir = tmp_path / "thumbs"
    thumbs_dir.mkdir()
    # A real file OUTSIDE thumbs_dir, at the path a naive ".." join could
    # reach -- must never be served.
    secret = tmp_path / "secret.jpg"
    secret.write_bytes(b"do-not-serve-me")

    client = TestClient(create_app(db_path, thumbs_dir=thumbs_dir))
    resp = client.get(f"/thumbs/{identifier}.jpg")

    assert resp.status_code in (400, 404)
    assert resp.content != b"do-not-serve-me"


def test_thumb_valid_identifier_charset_accepted(tmp_path):
    """Sanity check that the traversal-blocking regex isn't so strict it
    rejects legitimate identifiers (plain finnkode digits, and the
    dnb:<hex> synthetic id)."""
    db_path = _migrated_db(tmp_path)
    thumbs_dir = tmp_path / "thumbs"
    thumbs_dir.mkdir()
    (thumbs_dir / "9001.jpg").write_bytes(b"x")
    client = TestClient(create_app(db_path, thumbs_dir=thumbs_dir))

    resp = client.get("/thumbs/../thumbs/9001.jpg")
    # Browsers/clients normalize ".." before sending; httpx's TestClient
    # does the same -- this just confirms the legitimate path still works
    # after any client-side normalization.
    assert resp.status_code == 200


def _run_in_other_thread(fn):
    import threading

    box = {}

    def target():
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 - capture for the assertion
            box["error"] = exc

    t = threading.Thread(target=target)
    t.start()
    t.join()
    if "error" in box:
        raise box["error"]
    return box["result"]


def test_ro_conn_usable_across_threadpool_threads(tmp_path):
    """Regression: FastAPI resolves a sync generator dependency and its sync
    endpoint on potentially different anyio threadpool threads, so a
    per-request connection opened in one and used in the other must NOT trip
    sqlite3's same-thread guard (`check_same_thread=False`). Without the fix,
    executing on the connection from another thread raises
    sqlite3.ProgrammingError; with it, the query runs."""
    db_path = _migrated_db(tmp_path)

    class MockRequest:
        class MockApp:
            state = type("State", (), {"db_path": db_path})()

        app = MockApp()

    gen = ro_conn(MockRequest())
    conn = next(gen)
    try:
        rows = _run_in_other_thread(
            lambda: conn.execute("SELECT 1").fetchone()
        )
        assert tuple(rows) == (1,)
    finally:
        try:
            next(gen)
        except StopIteration:
            pass


def test_rw_conn_usable_across_threadpool_threads(tmp_path):
    """Same cross-thread guarantee for the writable annotations connection
    (the popup's PUT save path runs the endpoint off-thread too)."""
    from skannonser.web.app import rw_conn

    db_path = _migrated_db(tmp_path)

    class MockRequest:
        class MockApp:
            state = type("State", (), {"db_path": db_path})()

        app = MockApp()

    gen = rw_conn(MockRequest())
    conn = next(gen)
    try:
        rows = _run_in_other_thread(
            lambda: conn.execute("SELECT 1").fetchone()
        )
        assert tuple(rows) == (1,)
    finally:
        try:
            next(gen)
        except StopIteration:
            pass


def test_ro_conn_rejects_writes(tmp_path):
    """Verify that ro_conn is genuinely read-only by attempting an INSERT
    on the generator-yielded connection."""
    db_path = _migrated_db(tmp_path)

    class MockRequest:
        class MockApp:
            state = type("State", (), {"db_path": db_path})()

        app = MockApp()

    # Obtain connection from the ro_conn dependency generator
    gen = ro_conn(MockRequest())
    conn = next(gen)
    try:
        # Attempt to write to the read-only connection
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO schema_migrations (id) VALUES ('test')")
    finally:
        # Properly close the generator
        try:
            next(gen)
        except StopIteration:
            pass
