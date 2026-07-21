from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.notifications import daily_summary, default_send, weekly_summary
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=True, help="Send daily/weekly listing summaries")


def _require_no_pending_migrations(conn) -> bool:
    """Mirrors `run_cmd._require_no_pending_migrations`: notify must never
    auto-apply migrations -- only `skannonser db migrate` does. Fails loud
    instead."""
    pending = migrations.pending(conn)
    if pending:
        typer.echo(
            "Error: pending migrations - run 'skannonser db migrate' first",
            err=True,
        )
        return False
    return True


@app.command()
def daily(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
) -> None:
    """Send today's added/removed listing summary via the notify CLI (or
    set the baseline snapshot on first run)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    stats = daily_summary(conn, default_send)
    typer.echo(f"daily: {stats}")
    if not stats.get("sent"):
        raise typer.Exit(code=1)


@app.command()
def weekly(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
) -> None:
    """Send the trailing-7-day added/sold summary via the notify CLI."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    stats = weekly_summary(conn, default_send)
    typer.echo(f"weekly: {stats}")
    if not stats.get("sent"):
        raise typer.Exit(code=1)
