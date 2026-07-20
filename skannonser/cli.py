import typer

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")


@app.callback(invoke_without_command=True)
def default_callback() -> None:
    """skannonser — rebuilt pipeline CLI"""
    pass


def main() -> None:
    app()
