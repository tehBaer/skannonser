import sqlite3
from datetime import datetime
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets

app = typer.Typer(no_args_is_help=True, help="Database maintenance")


@app.command()
def backup(dest_dir: Path = typer.Option(Path("backups"), help="Backup directory")) -> None:
    """Copy the live DB via SQLite's online backup API (safe under WAL)."""
    src = get_secrets().db_path
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"properties-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    src_conn = sqlite3.connect(src)
    dest_conn = sqlite3.connect(dest)
    with dest_conn:
        src_conn.backup(dest_conn)
    src_conn.close()
    dest_conn.close()
    typer.echo(f"Backed up {src} -> {dest}")
