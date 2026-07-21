"""FastAPI skeleton for the skannonser web UI/API.

Per-request sqlite connections: GET endpoints open a read-only connection
(`file:...?mode=ro` URI) via `ro_conn` and close it after the request; a
writable variant (`rw_conn`) is provided for later tasks (e.g. the
annotations PUT endpoint) but unused here.

`/healthz` deliberately avoids `migrations.pending()`'s implicit
`CREATE TABLE IF NOT EXISTS schema_migrations` on a connection that might be
read-only: that DDL is a no-op (and thus safe) once the table already
exists, but raises `sqlite3.OperationalError` on a fresh, unmigrated DB
where it doesn't. So we check `sqlite_master` for the table ourselves first
-- absence means "unmigrated", reported as degraded without ever attempting
a write.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from skannonser.config.domain import DomainConfig
from skannonser.store import connection as connection_module
from skannonser.store import migrations

STATIC_DIR = Path(__file__).parent / "static"


def _ro_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def ro_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """Per-request read-only connection dependency. Closed after the
    request completes; never writes."""
    conn = _ro_connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


def rw_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """Per-request writable connection dependency (unused by any route yet
    -- reserved for the annotations PUT endpoint in a later task)."""
    conn = connection_module.connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


def _degraded(reason: str, *, db_reachable: bool) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "db": db_reachable, "reason": reason},
    )


def _healthz(db_path: Path) -> JSONResponse | dict:
    if not db_path.exists():
        return _degraded(f"database not found at {db_path}", db_reachable=False)

    try:
        conn = _ro_connect(db_path)
    except sqlite3.Error as exc:
        return _degraded(str(exc), db_reachable=False)

    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ).fetchone()
        if row is None:
            return _degraded("unmigrated (no schema_migrations table)", db_reachable=True)

        pending = migrations.pending(conn)
        if pending:
            names = [p.stem for p in pending]
            return _degraded(f"pending migrations: {names}", db_reachable=True)

        return {"status": "ok", "db": True}
    except sqlite3.Error as exc:
        return _degraded(str(exc), db_reachable=True)
    finally:
        conn.close()


def create_app(
    db_path: Path,
    domain: DomainConfig | None = None,
    thumbs_dir: Path | None = None,
) -> FastAPI:
    app = FastAPI(title="skannonser")
    app.state.db_path = db_path
    app.state.domain = domain
    app.state.thumbs_dir = thumbs_dir

    @app.get("/healthz", response_model=None)
    def healthz() -> JSONResponse | dict:
        return _healthz(app.state.db_path)

    # Registered before the static mount so it always takes precedence
    # (StaticFiles(html=True) would otherwise happily 404/serve for
    # anything not matched by an earlier route).
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    return app


__all__ = ["create_app", "ro_conn", "rw_conn"]
