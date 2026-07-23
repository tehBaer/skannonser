"""`skannonser tools` -- one-off / rescue utilities that sit outside the
regular ingest/enrich/publish pipeline.

The `import_sheet_annotations` logic itself lives in
`skannonser.publish.annotations` (alongside `SheetsClient`, since it only
ever talks to Sheets + the `annotations` table -- it has nothing to do with
Typer or CLI wiring). It's re-exported here so
`from skannonser.commands.tools_cmd import import_sheet_annotations` also
resolves, matching the task brief's stated interface path; the command below
is a thin wrapper.
"""
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.publish.annotations import import_sheet_annotations
from skannonser.publish.sheets_client import SheetsClient
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=True, help="One-off rescue / migration utilities")

__all__ = ["app", "import_sheet_annotations"]


@app.command(name="import-sheet-annotations")
def import_sheet_annotations_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    tab: str = typer.Option("Eie", "--tab", help="Sheet tab to read Finnkode/Kommentar/Tag from"),
) -> None:
    """One-time (idempotent) rescue: pull the sheet's manually-typed
    Kommentar/Tag columns into the `annotations` table, keyed by Finnkode.
    Read-only on the sheet -- this never writes it back."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    pending = migrations.pending(conn)
    if pending:
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    spreadsheet_id = get_secrets().spreadsheet_id
    if not spreadsheet_id:
        typer.echo("Error: SPREADSHEET_ID not set", err=True)
        raise typer.Exit(code=1)

    client = SheetsClient(spreadsheet_id)
    result = import_sheet_annotations(conn, client, tab=tab)
    typer.echo(f"import-sheet-annotations ({tab}): {result}")


@app.command(name="backfill-details")
def backfill_details_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir", help="FINN cache root (html_extracted/ lives here)"
    ),
    wipe: bool = typer.Option(False, "--wipe", help="Clear both details tables first, then rebuild"),
    status: bool = typer.Option(False, "--status", help="Print coverage only; parse nothing"),
) -> None:
    """(Re)build the listing_details/listing_facilities derived cache from
    already-downloaded ad HTML. Purely local -- zero FINN traffic. Safe to
    re-run any time; use --wipe after a parser change."""
    from skannonser.ingest.finn.backfill import backfill_details
    from skannonser.store.repositories.details import DetailsRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    if status:
        typer.echo(f"backfill-details coverage: {DetailsRepo(conn).coverage()}")
        return

    result = backfill_details(conn, project_dir, wipe=wipe)
    typer.echo(f"backfill-details: {result}")
