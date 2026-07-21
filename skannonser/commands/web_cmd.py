"""`skannonser web` -- serve the FastAPI read view over the live DB.

`uvicorn`/`fastapi` (and the app module itself) are imported lazily inside
the command body so `skannonser --help` and every other non-web CLI path
stay cheap and don't need the web stack importable."""

from pathlib import Path

import typer

from skannonser.config.settings import get_secrets
from skannonser.store import connection, migrations

app = typer.Typer(no_args_is_help=False, help="Serve the skannonser web UI/API")


@app.callback(invoke_without_command=True)
def web_cmd(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host"),
    port: int = typer.Option(8377, "--port", help="Bind port"),
    db: Path | None = typer.Option(
        None, "--db", help="Override the DB path for this run"
    ),
) -> None:
    """Run the FastAPI web app under uvicorn. Fails loud on pending
    migrations before binding -- never auto-migrates, same rule as
    `run ingest`/`run refresh` (see run_cmd._require_no_pending_migrations)."""
    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    try:
        pending = migrations.pending(conn)
    finally:
        conn.close()
    if pending:
        typer.echo(
            "Error: pending migrations - run 'skannonser db migrate' first",
            err=True,
        )
        raise typer.Exit(code=1)

    import uvicorn

    from skannonser.config.domain import load_domain
    from skannonser.web.app import create_app

    fastapi_app = create_app(db_path, domain=load_domain())
    uvicorn.run(fastapi_app, host=host, port=port)
