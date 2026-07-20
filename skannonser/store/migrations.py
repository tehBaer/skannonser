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
    applied = _applied(conn)
    return [p for p in sorted(MIGRATIONS_DIR.glob("*.sql")) if p.stem not in applied]


def migrate(conn: sqlite3.Connection) -> list[str]:
    ran: list[str] = []
    for path in pending(conn):
        conn.executescript(path.read_text(encoding="utf-8"))
        conn.execute("INSERT INTO schema_migrations (id) VALUES (?)", (path.stem,))
        conn.commit()
        ran.append(path.stem)
    return ran
