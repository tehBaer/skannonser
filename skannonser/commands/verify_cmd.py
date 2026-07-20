import shutil
import tempfile
import tomllib
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.verify.enrich import verify_enrich
from skannonser.verify.parse import verify_parse

app = typer.Typer(no_args_is_help=True, help="Golden-master verification against legacy")

DEFAULT_ALLOWLIST = Path("config/verify-allowlist.toml")


def _load_allowlist(path: Path) -> dict:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


@app.command()
def parse(
    limit: int | None = typer.Option(None, help="Only compare the first N cached ads"),
    cache_dir: Path = typer.Option(Path("data/eiendom"), help="Ad HTML cache root"),
    allowlist_path: Path = typer.Option(
        DEFAULT_ALLOWLIST, "--allowlist", help="Sanctioned-diffs TOML file"
    ),
) -> None:
    """Compare the rebuilt FINN parser against the legacy extractors over
    the cached ad corpus. Prints a summary and the first 20 unexplained
    diffs; exits 1 if any unexplained diff remains."""
    allowlist = _load_allowlist(allowlist_path)
    result = verify_parse(cache_dir, limit, allowlist)

    typer.echo(
        f"total: {result.total}  identical: {result.identical}  "
        f"allowlisted: {result.allowlisted}  unexplained diffs: {len(result.diffs)}"
    )

    if result.diffs:
        typer.echo("")
        typer.echo("First 20 unexplained diffs:")
        for d in result.diffs[:20]:
            typer.echo(f"  {d.finnkode}  {d.field}: legacy={d.legacy_value!r} new={d.new_value!r}")
        raise typer.Exit(code=1)


@app.command()
def enrich(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
) -> None:
    """Compare the rebuilt enrich pipeline (estimate / donor pre-pass /
    sheet-value resolution) against legacy `main/post_process.py`.

    Runs entirely against a disposable COPY of the DB (made here via
    `shutil.copy` to a tempdir) -- the source DB is never opened for
    writing. Makes zero API calls. Prints per-comparison diff counts and the
    first 20 diffs from each; exits 1 if any diff remains.
    """
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_copy = Path(tmp_dir) / db_path.name
        shutil.copy(db_path, db_copy)
        result = verify_enrich(db_copy)

    typer.echo(
        f"estimate diffs: {len(result.estimate_diffs)}  "
        f"donor diffs: {len(result.donor_diffs)}  "
        f"sheet-value diffs: {len(result.sheet_value_diffs)}"
    )

    if result.estimate_diffs:
        typer.echo("")
        typer.echo("First 20 estimate diffs:")
        for d in result.estimate_diffs[:20]:
            typer.echo(
                f"  [{d.domain_target}] {d.destination}.{d.field}: "
                f"legacy={d.legacy_value!r} new={d.new_value!r}"
            )

    if result.donor_diffs:
        typer.echo("")
        typer.echo("First 20 donor diffs:")
        for d in result.donor_diffs[:20]:
            typer.echo(f"  {d.finnkode}: legacy={d.legacy_donor!r} new={d.new_donor!r}")

    if result.sheet_value_diffs:
        typer.echo("")
        typer.echo("First 20 sheet-value diffs:")
        for d in result.sheet_value_diffs[:20]:
            typer.echo(f"  {d.finnkode}  {d.field}: legacy={d.legacy_value!r} new={d.new_value!r}")

    if result.estimate_diffs or result.donor_diffs or result.sheet_value_diffs:
        raise typer.Exit(code=1)
