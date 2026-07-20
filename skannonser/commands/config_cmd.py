import typer

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets

app = typer.Typer(no_args_is_help=True, help="Configuration inspection")


@app.command()
def show() -> None:
    """Print effective configuration (secrets masked)."""
    secrets = get_secrets()
    typer.echo(f"db_path: {secrets.db_path}")
    typer.echo(f"google_maps_api_key: {'set' if secrets.google_maps_api_key else 'MISSING'}")
    typer.echo(f"spreadsheet_id: {'set' if secrets.spreadsheet_id else 'MISSING'}")
    typer.echo(f"google_service_account_file: {secrets.google_service_account_file}")
    typer.echo(f"notify_bin: {secrets.notify_bin}")
    typer.echo(load_domain().model_dump_json(indent=2))
