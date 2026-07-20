from pathlib import Path

import typer

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets
from skannonser.ingest.finn.refresh import MODES as REFRESH_MODES
from skannonser.ingest.finn.refresh import refresh_listings
from skannonser.pipeline import FAILURE_RATE_THRESHOLD, run_dnb_ingest, run_finn_ingest
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=True, help="Run ingest pipelines")


def _failure_rate_ok(source: str, stats: dict) -> bool:
    """Report (not enforce) the failure-rate breach for operator/cron
    visibility. The actual protection -- skipping mark_inactive /
    deactivate_missing so a bad run can't wipe the active set -- already
    happened inside run_finn_ingest/run_dnb_ingest before this ever runs
    (see skannonser/pipeline.py's module docstring, guard 2)."""
    crawled = stats["crawled"]
    failed = stats["failed"]
    if crawled == 0:
        return True
    rate = failed / crawled
    if rate > FAILURE_RATE_THRESHOLD:
        typer.echo(
            f"ERROR: {source} parse-failure rate {failed}/{crawled} "
            f"({rate:.0%}) exceeds {FAILURE_RATE_THRESHOLD:.0%} threshold",
            err=True,
        )
        return False
    return True


@app.command()
def ingest(
    source: str = typer.Option("all", "--source", help="finn|dnb|all"),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), help="FINN ad HTML cache root"
    ),
) -> None:
    """Run the FINN and/or DNB ingest pipeline against the configured (or
    overridden) database. Non-interactive; exits non-zero if any run
    source's parse-failure rate exceeds 20%."""
    if source not in ("finn", "dnb", "all"):
        typer.echo(f"Error: --source must be finn, dnb, or all (got {source!r})", err=True)
        raise typer.Exit(code=2)

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    migrations.migrate(conn)
    domain = load_domain()

    ok = True

    if source in ("finn", "all"):
        # Archive raw crawl result pages next to the ad cache, mirroring
        # legacy's data/eiendom/html_crawled/ (debuggability + parallel-run
        # diff classification).
        stats = run_finn_ingest(
            domain, conn, project_dir, archive_dir=project_dir / "html_crawled"
        )
        typer.echo(f"finn: {stats}")
        if not _failure_rate_ok("finn", stats):
            ok = False

    if source in ("dnb", "all"):
        stats = run_dnb_ingest(domain, conn)
        typer.echo(f"dnb: {stats}")
        if not _failure_rate_ok("dnb", stats):
            ok = False

    if not ok:
        raise typer.Exit(code=1)


@app.command()
def refresh(
    mode: str = typer.Option(
        "all", "--mode", help=f"Row-selection scope: {'|'.join(REFRESH_MODES)}"
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), help="FINN ad HTML cache root"
    ),
) -> None:
    """Re-download listings from FINN.no and record status changes to
    `eiendom_status_history`. Never touches `active` -- that lifecycle is
    exclusively `run ingest`'s job (see skannonser/pipeline.py's module
    docstring)."""
    if mode not in REFRESH_MODES:
        typer.echo(
            f"Error: --mode must be one of {REFRESH_MODES} (got {mode!r})", err=True
        )
        raise typer.Exit(code=2)

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    migrations.migrate(conn)
    domain = load_domain()

    stats = refresh_listings(conn, domain, project_dir, mode)
    typer.echo(f"refresh ({mode}): {stats}")
