"""`skannonser estimate` -- predict Routes API attempts for an enrich run
without calling any API. Ports the two previews in
``main/post_process.py:637-777`` (max = fixed seed-donor reuse, simulated =
optimistic in-run donor growth)."""

from pathlib import Path

import typer

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets
from skannonser.enrich.travel import VALID_TARGETS, estimate
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=False, help="Estimate enrich API attempts (no API calls)")


@app.callback(invoke_without_command=True)
def estimate_cmd(
    targets: str = typer.Option(
        "all", "--targets", help="Destination group: all|brj|mvv|mvv_uni"
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run"
    ),
) -> None:
    """Print per-destination max/simulated API attempts and totals. Never
    calls any paid API."""
    if str(targets or "").strip().lower() not in VALID_TARGETS:
        typer.echo(
            f"Error: --targets must be one of {sorted(VALID_TARGETS)} (got {targets!r})",
            err=True,
        )
        raise typer.Exit(code=2)

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    pending = migrations.pending(conn)
    if pending:
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    domain = load_domain()
    result = estimate(conn, domain, targets=targets)

    for key, counts in result["per_destination"].items():
        typer.echo(
            f"{key}: max {counts['max_attempts']} / simulated {counts['simulated_attempts']}"
        )
    totals = result["totals"]
    typer.echo(
        f"total: max {totals['max_attempts']} / simulated {totals['simulated_attempts']}"
    )
