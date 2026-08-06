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


@app.command(name="backfill-salgsoppgave")
def backfill_salgsoppgave_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir", help="FINN cache root (html_extracted/ lives here)"
    ),
    wipe: bool = typer.Option(False, "--wipe", help="Clear the salgsoppgave tables first, then rebuild"),
    status: bool = typer.Option(False, "--status", help="Print coverage only; parse nothing"),
) -> None:
    """(Re)build the listing_salgsoppgave derived cache from already-downloaded
    ad HTML. Purely local -- zero FINN traffic, zero API calls. Safe to re-run
    any time; use --wipe after a parser change."""
    from skannonser.ingest.finn.backfill_salgsoppgave import backfill_salgsoppgave
    from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    repo = SalgsoppgaveRepo(conn)
    if status:
        typer.echo(f"backfill-salgsoppgave status: {repo.coverage()}")
        return

    result = backfill_salgsoppgave(conn, project_dir, wipe=wipe)
    typer.echo(f"backfill-salgsoppgave: {result}")
    typer.echo(f"coverage: {repo.coverage()}")


@app.command(name="classify-tilstand")
def classify_tilstand_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir",
        help="FINN cache root (html_extracted/ lives here)"),
    limit: int | None = typer.Option(
        None, "--limit",
        help="Max NEW API classifications this run -- the spend control. "
             "Cache replays are unlimited and free."),
    all_: bool = typer.Option(
        False, "--all",
        help="Explicitly allow an unbounded run over every uncached ad "
             "(full-corpus cost). Without this, --limit is required."),
    wipe: bool = typer.Option(
        False, "--wipe",
        help="Clear the tilstand tables first. The LLM cache survives, so the "
             "rebuild replays paid responses for free."),
    batch: bool = typer.Option(
        False, "--batch",
        help="Submit uncached ads via the Batch API (50% cheaper), poll until "
             "done, then derive rows from the cache."),
    validate: bool = typer.Option(
        False, "--validate",
        help="Stage-1 harness: blind-estimate ads that carry surveyor-stated "
             "costs and score against them. Calls the API; respects --limit."),
    status: bool = typer.Option(False, "--status", help="Print coverage only"),
) -> None:
    """Classify TG2/TG3 condition findings from cached salgsoppgave text
    (Claude Opus 5). COSTS MONEY on uncached ads -- run staged: --limit 200,
    check, --limit 1000, check, then the rest with --batch. Requires the
    anthropic package (install the llm extra from pyproject.toml's
    optional-dependencies) and ANTHROPIC_API_KEY locally; the server never
    needs either."""
    from skannonser.enrich.tilstand_backfill import (
        classify_tilstand, classify_tilstand_batch,
    )
    from skannonser.enrich.tilstand_validate import validate_estimates
    from skannonser.store.repositories.tilstand import TilstandRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    repo = TilstandRepo(conn)
    if status:
        typer.echo(f"classify-tilstand status: {repo.coverage()}")
        return
    if validate:
        report = validate_estimates(conn, project_dir, limit=50 if limit is None else limit)
        typer.echo(f"validate: {report}")
        return
    if limit is None and not all_:
        typer.echo(
            "Error: classification costs money per uncached ad. Pass --limit N "
            "to bound this run, or --all to explicitly process everything.",
            err=True,
        )
        raise typer.Exit(code=1)
    if wipe:
        repo.wipe()
    if batch:
        result = classify_tilstand_batch(conn, project_dir, limit=limit)
    else:
        result = classify_tilstand(conn, project_dir, limit=limit)
    typer.echo(f"classify-tilstand: {result}")
    typer.echo(f"coverage: {repo.coverage()}")


@app.command(name="export-tilstand-cache")
def export_tilstand_cache_cmd(
    out: Path = typer.Option(..., "--out", help="File to write the cache JSON to"),
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
) -> None:
    """Dump the classifier response cache to a JSON file, for moving results
    to another machine (typically local -> server).

    Only the cache travels: findings and rollups are derived from it, so the
    receiving side rebuilds them for free with import-tilstand-cache. Nothing
    here calls the API or costs anything."""
    import json

    from skannonser.enrich.tilstand import export_cache

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    rows = export_cache(conn)
    out.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    typer.echo(f"export-tilstand-cache: {len(rows)} rows -> {out}")


@app.command(name="import-tilstand-cache")
def import_tilstand_cache_cmd(
    in_: Path = typer.Option(..., "--in", help="Cache JSON produced by export-tilstand-cache"),
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir",
        help="FINN cache root (html_extracted/ lives here) -- needed by --derive"
    ),
    derive: bool = typer.Option(
        True, "--derive/--no-derive",
        help="After importing, rebuild findings and rollups from the cache. "
             "Replays cached responses only -- never calls the API."),
) -> None:
    """Load a cache file produced by export-tilstand-cache, then (by default)
    derive findings and rollups from it.

    The derive pass reads the local html_extracted cache to match each ad to a
    cached response by content hash, so ads sharing identical condition text
    are all filled from one response. No API key needed on this side."""
    import json

    from skannonser.enrich.tilstand import import_cache
    from skannonser.enrich.tilstand_backfill import classify_tilstand
    from skannonser.store.repositories.tilstand import TilstandRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    if not in_.is_file():
        typer.echo(f"Error: cache file not found at {in_}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    try:
        rows = json.loads(in_.read_text(encoding="utf-8"))
        result = import_cache(conn, rows)
    except (ValueError, json.JSONDecodeError) as exc:
        typer.echo(f"Error: malformed cache file: {exc}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"import-tilstand-cache: {result}")

    if derive:
        derived = classify_tilstand(conn, project_dir, cache_only=True)
        typer.echo(f"derive: {derived}")
        typer.echo(f"coverage: {TilstandRepo(conn).coverage()}")
