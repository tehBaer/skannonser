import sqlite3
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _applied(conn: sqlite3.Connection) -> set[str]:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " id TEXT PRIMARY KEY,"
        " applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
    )
    return {row["id"] for row in conn.execute("SELECT id FROM schema_migrations")}


def pending(conn: sqlite3.Connection) -> list[Path]:
    if not MIGRATIONS_DIR.is_dir():
        raise FileNotFoundError(
            f"migrations directory missing: {MIGRATIONS_DIR} (broken install?)"
        )
    applied = _applied(conn)
    return [p for p in sorted(MIGRATIONS_DIR.glob("*.sql")) if p.stem not in applied]


def _statements(sql: str) -> list[str]:
    """Split a script into complete statements using sqlite3.complete_statement."""
    statements, buf = [], ""
    for line in sql.splitlines(keepends=True):
        stripped = line.strip()
        if not buf and (not stripped or stripped.startswith("--")):
            continue
        buf += line
        if sqlite3.complete_statement(buf):
            statements.append(buf.strip())
            buf = ""
    if buf.strip():
        statements.append(buf.strip())
    return statements


def migrate(conn: sqlite3.Connection) -> list[str]:
    ran: list[str] = []
    for path in pending(conn):
        stmts = _statements(path.read_text(encoding="utf-8"))
        try:
            conn.execute("BEGIN IMMEDIATE")
            for stmt in stmts:
                conn.execute(stmt)
            conn.execute("INSERT INTO schema_migrations (id) VALUES (?)", (path.stem,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        ran.append(path.stem)
    return ran
