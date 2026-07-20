from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.store import connection, migrations


def test_stats_lists_tables_with_counts(tmp_path, monkeypatch):
    db = tmp_path / "s.db"
    conn = connection.connect(db)
    migrations.migrate(conn)
    conn.execute("INSERT INTO stations (name, lat, lng) VALUES ('Test st', 59.9, 10.7)")
    conn.commit()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(db))

    result = CliRunner().invoke(app, ["db", "stats"])

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert "eiendom: 0" in lines
    assert "stations: 1" in lines
