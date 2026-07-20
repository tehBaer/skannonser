import sqlite3
from datetime import datetime
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=True, help="Database maintenance")


@app.command()
def backup(dest_dir: Path = typer.Option(Path("backups"), help="Backup directory")) -> None:
    """Copy the live DB via SQLite's online backup API (safe under WAL)."""
    src = get_secrets().db_path
    if not src.exists():
        typer.echo(f"Error: database not found at {src}", err=True)
        raise typer.Exit(code=1)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"properties-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    src_conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    dest_conn = sqlite3.connect(dest)
    try:
        with dest_conn:
            src_conn.backup(dest_conn)
    finally:
        src_conn.close()
        dest_conn.close()
    typer.echo(f"Backed up {src} -> {dest}")


@app.command()
def migrate() -> None:
    """Apply pending schema migrations (versioned, explicit — never on connect)."""
    conn = connection.connect(get_secrets().db_path)
    ran = migrations.migrate(conn)
    typer.echo(f"Applied: {', '.join(ran) if ran else 'nothing (up to date)'}")
