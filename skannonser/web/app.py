"""FastAPI skeleton for the skannonser web UI/API.

Per-request sqlite connections: GET endpoints open a read-only connection
(`file:...?mode=ro` URI) via `ro_conn` and close it after the request; a
writable variant (`rw_conn`) is used by the annotations PUT endpoint (see
`skannonser/web/api.py`) to create/update/tombstone a single `annotations`
row.

`/healthz` deliberately avoids `migrations.pending()`'s implicit
`CREATE TABLE IF NOT EXISTS schema_migrations` on a connection that might be
read-only: that DDL is a no-op (and thus safe) once the table already
exists, but raises `sqlite3.OperationalError` on a fresh, unmigrated DB
where it doesn't. So we check `sqlite_master` for the table ourselves first
-- absence means "unmigrated", reported as degraded without ever attempting
a write.

`GET /thumbs/{identifier}.jpg` (Phase 5 Task 5) serves a cached listing
thumbnail straight off disk under `app.state.thumbs_dir` (default:
`data/thumbs`, the same directory the nightly `thumbs` step
(`skannonser.nightly.run_nightly`/`skannonser.enrich.thumbs.cache_thumbnails`)
downloads into). `identifier` is validated against `skannonser.ids`'s shared
`IDENTIFIER_RE` (the SAME charset `skannonser.web.api`'s annotations routes
validate a finnkode against) BEFORE it ever touches the filesystem -- that
charset excludes `.`/`/`, so no value that passes it can encode a
`..`/path-traversal segment or escape the single `{identifier}.jpg` path
segment. An invalid identifier is a 400; a valid one with no cached file is
a 404 -- neither ever stats/opens anything outside `thumbs_dir`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from skannonser.config.domain import DomainConfig
from skannonser.ids import IDENTIFIER_RE
from skannonser.store import connection as connection_module
from skannonser.store import migrations

STATIC_DIR = Path(__file__).parent / "static"


def _ro_connect(db_path: Path) -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may resolve the `ro_conn` dependency
    # and run the sync endpoint on different anyio threadpool threads, so a
    # per-request connection created in one and used in the other would trip
    # sqlite3's same-thread guard. The connection is single-request-scoped
    # (opened + closed within the request, never shared), so relaxing the
    # guard is safe. (mode=ro still forbids writes -- see test_ro_conn_rejects_writes.)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
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
    """Per-request writable connection dependency -- wired to
    ``PUT /api/annotations/{finnkode}`` (see ``skannonser/web/api.py``'s
    "Annotations CRUD" section)."""
    conn = connection_module.connect(
        request.app.state.db_path, check_same_thread=False
    )
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


def _thumb_response(thumbs_dir: Path | None, identifier: str) -> FileResponse | JSONResponse:
    if not IDENTIFIER_RE.match(identifier or ""):
        return JSONResponse(
            status_code=400, content={"detail": f"invalid identifier: {identifier!r}"}
        )
    if thumbs_dir is None:
        return JSONResponse(status_code=404, content={"detail": "not found"})
    path = Path(thumbs_dir) / f"{identifier}.jpg"
    if not path.is_file():
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return FileResponse(path, media_type="image/jpeg")


def create_app(
    db_path: Path,
    domain: DomainConfig | None = None,
    thumbs_dir: Path | None = Path("data/thumbs"),
) -> FastAPI:
    app = FastAPI(title="skannonser")
    app.state.db_path = db_path
    app.state.domain = domain
    app.state.thumbs_dir = thumbs_dir

    @app.get("/healthz", response_model=None)
    def healthz() -> JSONResponse | dict:
        return _healthz(app.state.db_path)

    @app.get("/thumbs/{identifier}.jpg", response_model=None)
    def get_thumb(identifier: str) -> FileResponse | JSONResponse:
        return _thumb_response(app.state.thumbs_dir, identifier)

    # Deferred import: skannonser.web.api imports `ro_conn` back out of this
    # module. By the time create_app() actually RUNS, this module has already
    # finished executing (ro_conn is defined above), so the import resolves
    # cleanly -- a top-of-file import would instead race a half-initialized
    # module during the very first import of either file.
    from skannonser.web.api import router as api_router

    # Registered before the static mount so it always takes precedence
    # (StaticFiles(html=True) would otherwise happily 404/serve for
    # anything not matched by an earlier route).
    app.include_router(api_router)
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    return app


__all__ = ["create_app", "ro_conn", "rw_conn"]
