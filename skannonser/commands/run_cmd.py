import json
from pathlib import Path

import typer

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets
from skannonser.enrich.dnb_travel import run_dnb_travel
from skannonser.enrich.geocode import run_geocode
from skannonser.enrich.travel import VALID_TARGETS, run_enrich
from skannonser.enrich.validate import validate_travel
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.ingest.finn.refresh import MODES as REFRESH_MODES
from skannonser.ingest.finn.refresh import refresh_listings
from skannonser.nightly import run_nightly, run_sheets
from skannonser.pipeline import FAILURE_RATE_THRESHOLD, run_dnb_ingest, run_finn_ingest
from skannonser.publish.sheets_client import SheetsClient
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


def _crawled_ok(source: str, stats: dict) -> bool:
    """Fail loud when a crawl finds zero URLs (mirrors legacy DNB's
    RuntimeError on an empty crawl). The pipeline itself already skips
    mark_inactive/deactivate_missing in this case (guard 1) -- this is the
    operational alert layered on top, matching Fix 4's ask that `run
    ingest` exit non-zero rather than silently succeed with nothing done."""
    if stats["crawled"] == 0:
        typer.echo(f"ERROR: {source} crawl returned zero URLs", err=True)
        return False
    return True


def _require_no_pending_migrations(conn) -> bool:
    """`run ingest`/`run refresh` must never auto-apply migrations (Fix 5)
    -- only `skannonser db migrate` does, explicitly. Fails loud instead."""
    pending = migrations.pending(conn)
    if pending:
        typer.echo(
            "Error: pending migrations - run 'skannonser db migrate' first",
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
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)
    domain = load_domain()

    ok = True

    if source in ("finn", "all"):
        # Archive raw crawl result pages next to the ad cache. Deliberately
        # NOT legacy's data/eiendom/html_crawled/ -- that directory is
        # legacy's own, and writing into it here risks a clobber during the
        # parallel-run era. Separate from legacy's archive dir until
        # phase-4 cutover.
        stats = run_finn_ingest(
            domain, conn, project_dir, archive_dir=project_dir / "html_crawled_rebuild"
        )
        typer.echo(f"finn: {stats}")
        if not _crawled_ok("finn", stats):
            ok = False
        if not _failure_rate_ok("finn", stats):
            ok = False

    if source in ("dnb", "all"):
        stats = run_dnb_ingest(domain, conn)
        typer.echo(f"dnb: {stats}")
        if not _crawled_ok("dnb", stats):
            ok = False
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
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)
    domain = load_domain()

    stats = refresh_listings(conn, domain, project_dir, mode)
    typer.echo(f"refresh ({mode}): {stats}")


@app.command()
def geocode(
    limit: int = typer.Option(
        0, "--limit", help="Max candidates to attempt this run (0 = no limit)"
    ),
    include_inactive: bool = typer.Option(
        False, "--include-inactive", help="Also geocode inactive/solgt/inaktiv listings"
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
) -> None:
    """Fill missing lat/lng via the Google Geocoding API's three-pass Norway
    strategy, through the shared Gateway (monthly budget + rate limiting)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    api_key = get_secrets().google_maps_api_key
    if not api_key:
        typer.echo("Error: GOOGLE_MAPS_API_KEY not set", err=True)
        raise typer.Exit(code=1)

    domain = load_domain()
    gateway = Gateway(conn, domain.budget)

    try:
        stats = run_geocode(
            conn, domain, gateway, api_key, limit=limit, include_inactive=include_inactive
        )
    except BudgetExceeded:
        typer.echo("geocode budget exhausted - resumes next window", err=True)
        raise typer.Exit(code=3)

    typer.echo(f"geocode: {stats}")


@app.command()
def enrich(
    targets: str = typer.Option(
        "all", "--targets", help="Destination group: all|brj|mvv|mvv_uni"
    ),
    force_api: bool = typer.Option(
        False, "--force-api", help="Ignore donor reuse and call the API for every missing row"
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
) -> None:
    """Fill missing public-transit commute times (and post-process
    derivations) for the selected destination(s), through the shared Gateway.
    Exits 3 when the Routes monthly budget is exhausted (remaining rows resume
    next window)."""
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
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    api_key = get_secrets().google_maps_api_key
    if not api_key:
        typer.echo("Error: GOOGLE_MAPS_API_KEY not set", err=True)
        raise typer.Exit(code=1)

    domain = load_domain()
    gateway = Gateway(conn, domain.budget)

    try:
        stats = run_enrich(
            conn, domain, gateway, api_key, targets=targets, force_api=force_api
        )
    except BudgetExceeded:
        typer.echo("enrich budget exhausted - resumes next window", err=True)
        raise typer.Exit(code=3)

    if stats.get("budget_exhausted"):
        typer.echo("enrich budget exhausted - resumes next window", err=True)
        typer.echo(f"enrich: {stats}")
        raise typer.Exit(code=3)

    typer.echo(f"enrich: {stats}")


@app.command(name="enrich-dnb")
def enrich_dnb(
    limit: int = typer.Option(
        0, "--limit", help="Max Routes API calls to make this run (0 = no limit)"
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
) -> None:
    """Fill missing public-transit commute times (BRJ + MVV only) on active,
    FINN-unmatched ``dnbeiendom`` rows, through the shared Gateway. Exits 3
    when the Routes monthly budget is exhausted (remaining rows resume next
    window)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    api_key = get_secrets().google_maps_api_key
    if not api_key:
        typer.echo("Error: GOOGLE_MAPS_API_KEY not set", err=True)
        raise typer.Exit(code=1)

    domain = load_domain()
    gateway = Gateway(conn, domain.budget)

    try:
        stats = run_dnb_travel(conn, domain, gateway, api_key, limit=limit)
    except BudgetExceeded:
        typer.echo("dnb travel budget exhausted - resumes next window", err=True)
        raise typer.Exit(code=3)

    typer.echo(f"enrich-dnb: {stats}")


@app.command(name="validate-travel")
def validate_travel_cmd(
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
) -> None:
    """Read-only heuristic scan for suspicious stored travel values (neighbor-
    radius, postcode-group, and donor-distance checks with MAD-based outlier
    scoring). Port of `main/tools/validate_travel_values.py`'s scoring core --
    see `skannonser.enrich.validate` for the full heuristic writeup.
    Read-only; never calls any API. Exits 1 only when the DB is missing;
    findings are informational (exit 0)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    domain = load_domain()

    findings = validate_travel(
        conn,
        domain,
        radius_m=domain.travel.reuse_within_meters,
        max_travel_minutes=domain.travel.max_travel_minutes,
    )

    typer.echo("=" * 72)
    typer.echo("Travel Value Validation Report")
    typer.echo("=" * 72)
    typer.echo(f"Flagged findings: {len(findings)}")

    if not findings:
        typer.echo("No suspicious travel values found with current thresholds.")
        return

    typer.echo()
    typer.echo(f"{'score':>5}  {'column':<26}  {'finnkode':<12}  {'value':>6}  reason")
    for f in findings:
        reason = " | ".join(f["reasons"])
        typer.echo(
            f"{f['score']:>5}  {f['column']:<26}  {f['finnkode']:<12}  {f['value']:>6}  {reason}"
        )


def _require_sheets_configured() -> bool:
    """`run sheets` / `run nightly` (without --dry-run-sheets) both need a
    real Sheets destination -- fail loud instead of letting SheetsClient's
    lazy `_build_service` raise deep inside the sheets step (for `nightly`,
    that means only AFTER the whole -- potentially hours-long, budget-
    consuming -- pipeline has already run). Checks both that the values are
    present AND that the configured service-account path actually exists on
    disk, so a stale/typo'd path is caught up front too."""
    secrets = get_secrets()
    if not secrets.spreadsheet_id or secrets.google_service_account_file is None:
        typer.echo(
            "Error: spreadsheet_id / google_service_account_file not configured", err=True
        )
        return False
    if not secrets.google_service_account_file.exists():
        typer.echo(
            "Error: google_service_account_file not found at "
            f"{secrets.google_service_account_file}",
            err=True,
        )
        return False
    return True


@app.command()
def sheets(
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
) -> None:
    """Rewrite the Eie, Sold, DNB, and Stations Google Sheets tabs from the
    current DB state (full clear-and-rewrite, Task 3's export builders)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    if not _require_sheets_configured():
        raise typer.Exit(code=1)

    client = SheetsClient(get_secrets().spreadsheet_id)
    stats = run_sheets(conn, client)
    typer.echo(f"sheets: {stats}")


@app.command()
def nightly(
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run (supervised parallel runs)"
    ),
    dry_run_sheets: Path | None = typer.Option(
        None,
        "--dry-run-sheets",
        help=(
            "Write sheet payloads as {eie,sold,dnb,stations}.json to DIR "
            "instead of touching Sheets"
        ),
    ),
) -> None:
    """The legacy `make full` cron replacement: ingest finn -> ingest dnb ->
    geocode -> enrich(all) -> enrich(mvv_uni) -> enrich-dnb ->
    refresh(stale-open) -> sheets, run sequentially with per-step failure
    isolation (see `skannonser/nightly.py`'s module docstring -- no step's
    failure skips a later one; a Routes/Geocode budget stop is recorded, not
    treated as a failure). Exits non-zero only if a real step failed; a
    budget-exhausted-only run still exits 0."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if not _require_no_pending_migrations(conn):
        raise typer.Exit(code=1)

    api_key = get_secrets().google_maps_api_key
    if not api_key:
        typer.echo("Error: GOOGLE_MAPS_API_KEY not set", err=True)
        raise typer.Exit(code=1)

    client = None
    sheets_writer = None
    if dry_run_sheets is not None:
        dry_run_sheets.mkdir(parents=True, exist_ok=True)

        def _write_payload(tab: str, header: list, rows: list) -> None:
            path = dry_run_sheets / f"{tab.lower()}.json"
            path.write_text(json.dumps({"header": header, "rows": rows}, default=str))

        sheets_writer = _write_payload
    else:
        if not _require_sheets_configured():
            raise typer.Exit(code=1)
        client = SheetsClient(get_secrets().spreadsheet_id)

    domain = load_domain()
    gateway = Gateway(conn, domain.budget)

    result = run_nightly(conn, domain, gateway, api_key, client, sheets_writer=sheets_writer)

    typer.echo(f"nightly: {result}")
    if result["budget_exhausted"]:
        typer.echo(
            "nightly: budget exhausted on: " + ", ".join(result["budget_exhausted"]), err=True
        )
    if result["failed"]:
        raise typer.Exit(code=1)
