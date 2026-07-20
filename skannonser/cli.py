import typer

from skannonser.commands import config_cmd, db_cmd, verify_cmd

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")
app.add_typer(config_cmd.app, name="config")
app.add_typer(db_cmd.app, name="db")
app.add_typer(verify_cmd.app, name="verify")


@app.callback(invoke_without_command=True)
def default_callback() -> None:
    """skannonser — rebuilt pipeline CLI"""
    pass


def main() -> None:
    app()
