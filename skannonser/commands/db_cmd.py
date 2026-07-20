import sqlite3
from datetime import datetime
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=True, help="Database maintenance")


@app.command()
def backup(
    dest_dir: Path = typer.Option(Path("backups"), help="Backup directory"),
    keep: int = typer.Option(30, help="How many newest backups to keep (0 = keep all)"),
) -> None:
    """Copy the live DB via SQLite's online backup API (safe under WAL)."""
    src = get_secrets().db_path
    if not src.exists():
        typer.echo(f"Error: database not found at {src}", err=True)
        raise typer.Exit(code=1)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"properties-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    src_conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    dest_conn = sqlite3.connect(dest)
    dest_closed = False
    try:
        with dest_conn:
            src_conn.backup(dest_conn)
    except Exception:
        dest_conn.close()
        dest_closed = True
        dest.unlink(missing_ok=True)
        raise
    finally:
        src_conn.close()
        if not dest_closed:
            dest_conn.close()
    typer.echo(f"Backed up {src} -> {dest}")
    if keep > 0:
        backups = sorted(dest_dir.glob("properties-*.db"))
        for old in backups[:-keep]:
            old.unlink()
            typer.echo(f"Pruned {old.name}")


@app.command()
def migrate() -> None:
    """Apply pending schema migrations (versioned, explicit — never on connect)."""
    db_path = get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    ran = migrations.migrate(conn)
    typer.echo(f"Applied: {', '.join(ran) if ran else 'nothing (up to date)'}")


@app.command()
def stats() -> None:
    """Row counts per table — the quick health/acceptance check."""
    db_path = get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    tables = [
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for t in tables:
        n = conn.execute(f'SELECT COUNT(*) AS n FROM "{t}"').fetchone()["n"]
        typer.echo(f"{t}: {n}")
