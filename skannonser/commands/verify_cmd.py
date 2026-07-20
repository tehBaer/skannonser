import tomllib
from pathlib import Path

import typer

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
