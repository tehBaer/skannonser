import typer

from skannonser.commands import config_cmd, db_cmd, estimate_cmd, run_cmd, tools_cmd, verify_cmd

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")
app.add_typer(config_cmd.app, name="config")
app.add_typer(db_cmd.app, name="db")
app.add_typer(verify_cmd.app, name="verify")
app.add_typer(run_cmd.app, name="run")
app.add_typer(estimate_cmd.app, name="estimate")
app.add_typer(tools_cmd.app, name="tools")


@app.callback(invoke_without_command=True)
def default_callback() -> None:
    """skannonser — rebuilt pipeline CLI"""
    pass


def main() -> None:
    app()
