import sqlite3
from pathlib import Path


def connect(db_path: Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    # `check_same_thread=False` is opt-in for the web layer only: FastAPI
    # resolves a sync generator dependency and its sync endpoint on
    # potentially different anyio threadpool threads, so a per-request
    # connection opened in one and used in the other trips sqlite3's
    # same-thread guard. The connection is still only ever used by one
    # request (opened + closed within it, never shared/concurrent), so
    # relaxing the guard is safe. Default stays True for every other caller.
    conn = sqlite3.connect(db_path, check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
