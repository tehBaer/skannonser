import sqlite3

from typer.testing import CliRunner

from skannonser.cli import app


def test_backup_copies_database(tmp_path, monkeypatch):
    src = tmp_path / "live.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.execute("INSERT INTO t VALUES (42)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))

    dest_dir = tmp_path / "backups"
    result = CliRunner().invoke(app, ["db", "backup", "--dest-dir", str(dest_dir)])

    assert result.exit_code == 0, result.output
    copies = list(dest_dir.glob("properties-*.db"))
    assert len(copies) == 1
    check = sqlite3.connect(copies[0])
    assert check.execute("SELECT x FROM t").fetchone()[0] == 42


def test_backup_fails_loud_when_source_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "does-not-exist.db"))
    dest_dir = tmp_path / "backups"
    result = CliRunner().invoke(app, ["db", "backup", "--dest-dir", str(dest_dir)])
    assert result.exit_code == 1
    assert not (tmp_path / "does-not-exist.db").exists()
    assert not dest_dir.exists() or not list(dest_dir.glob("properties-*.db"))
