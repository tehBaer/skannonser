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


def test_backup_prunes_old_backups_beyond_keep(tmp_path, monkeypatch):
    src = tmp_path / "live.db"
    sqlite3.connect(src).execute("CREATE TABLE t (x INTEGER)").connection.commit()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))
    dest_dir = tmp_path / "backups"
    dest_dir.mkdir()
    for stamp in ("20260101-000000", "20260102-000000", "20260103-000000"):
        (dest_dir / f"properties-{stamp}.db").write_bytes(b"old")

    result = CliRunner().invoke(
        app, ["db", "backup", "--dest-dir", str(dest_dir), "--keep", "2"])

    assert result.exit_code == 0, result.output
    remaining = sorted(p.name for p in dest_dir.glob("properties-*.db"))
    assert len(remaining) == 2                       # newest 2 kept (incl. the one just made)
    assert "properties-20260101-000000.db" not in remaining
    assert "properties-20260102-000000.db" not in remaining


def test_backup_removes_partial_file_on_failure(tmp_path, monkeypatch):
    # sqlite3.Connection is an immutable C type on this Python (3.12+): neither
    # class- nor instance-level monkeypatching of `.backup` is possible
    # (TypeError / "attribute is read-only"). Induce a real backup failure
    # instead: a src file that exists (passes the exists() check) but is not
    # a valid sqlite database, so src_conn.backup(dest_conn) genuinely raises.
    src = tmp_path / "live.db"
    src.write_bytes(b"not a valid sqlite database, just garbage bytes")
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))
    dest_dir = tmp_path / "backups"

    result = CliRunner().invoke(app, ["db", "backup", "--dest-dir", str(dest_dir)])

    assert result.exit_code != 0
    assert not list(dest_dir.glob("properties-*.db"))
