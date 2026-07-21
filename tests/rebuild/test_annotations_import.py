import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import tools_cmd
from skannonser.publish.annotations import import_sheet_annotations
from skannonser.store import connection, migrations


class FakeClient:
    """Stands in for SheetsClient: `read_tab` returns canned rows; any write
    call is a test failure -- this tool must be read-only on the sheet."""

    def __init__(self, rows):
        self._rows = rows
        self.read_calls = []

    def read_tab(self, tab):
        self.read_calls.append(tab)
        return self._rows

    def rewrite_tab(self, *a, **kw):
        raise AssertionError("import_sheet_annotations must never write the sheet")


@pytest.fixture
def conn(tmp_path):
    c = connection.connect(tmp_path / "x.db")
    migrations.migrate(c)
    return c


def _annotations(conn):
    return {
        r["finnkode"]: dict(r)
        for r in conn.execute("SELECT * FROM annotations")
    }


HEADER = [" finnkode ", "KOMMENTAR", "tag"]  # case/whitespace-aliased header

ROWS = [
    HEADER,
    ["12345678", "nice view", "A"],
    ['=HYPERLINK("http://finn.no/xyz","23456789")', "", "B"],  # tag-only, hyperlink finnkode
    ["34567890", "", ""],  # both empty -> skipped
    ["", "orphan comment", "C"],  # no finnkode -> skipped
]


def test_reads_tab_exactly_once_and_never_writes(conn):
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")
    assert client.read_calls == ["Eie"]


def test_aliased_header_located_and_rows_upserted(conn):
    client = FakeClient(ROWS)
    result = import_sheet_annotations(conn, client, tab="Eie")

    rows = _annotations(conn)
    assert set(rows) == {"12345678", "23456789"}
    assert rows["12345678"]["kommentar"] == "nice view"
    assert rows["12345678"]["tag"] == "A"
    assert rows["23456789"]["kommentar"] == ""
    assert rows["23456789"]["tag"] == "B"
    assert rows["12345678"]["imported_at"] == rows["12345678"]["updated_at"]

    assert result["rows_read"] == 4
    assert result["candidates"] == 2
    assert result["inserted"] == 2
    assert result["updated"] == 0
    assert result["skipped"] == 0


def test_empty_kommentar_and_tag_row_skipped(conn):
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")
    assert "34567890" not in _annotations(conn)


def test_missing_finnkode_row_skipped(conn):
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")
    rows = _annotations(conn)
    assert not any(r["kommentar"] == "orphan comment" for r in rows.values())


def test_idempotent_rerun_does_not_bump_updated_at(conn):
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")
    before = _annotations(conn)

    result = import_sheet_annotations(conn, client, tab="Eie")

    after = _annotations(conn)
    assert after == before
    assert result["inserted"] == 0
    assert result["updated"] == 0
    assert result["skipped"] == 2


def test_legit_update_bumps_both_timestamps(conn):
    """Positive case for the idempotency SQL's LEGIT-UPDATE branch: a row
    that's untouched since import (updated_at == imported_at) whose sheet
    value actually changes on re-import must update, bumping BOTH
    timestamps to the new run's ts. (The protection branch -- a web-UI-edited
    row with updated_at != imported_at -- is covered by
    test_web_ui_edited_row_is_not_overwritten below.)"""
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")

    # Pin the stored row to an old-but-still-"untouched" timestamp: the SQL
    # only cares that updated_at == imported_at, not any particular value,
    # so this makes the bump assertion deterministic regardless of the
    # real clock's resolution.
    conn.execute(
        "UPDATE annotations SET imported_at = ?, updated_at = ? WHERE finnkode = ?",
        ("2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00", "12345678"),
    )
    conn.commit()

    changed_rows = [
        HEADER,
        ["12345678", "new view from balcony", "A"],
        ['=HYPERLINK("http://finn.no/xyz","23456789")', "", "B"],  # unchanged
    ]
    client2 = FakeClient(changed_rows)
    result = import_sheet_annotations(conn, client2, tab="Eie")

    row = _annotations(conn)["12345678"]
    assert row["kommentar"] == "new view from balcony"
    assert row["tag"] == "A"
    assert row["imported_at"] != "2020-01-01T00:00:00+00:00"
    assert row["updated_at"] != "2020-01-01T00:00:00+00:00"
    assert row["imported_at"] == row["updated_at"]
    assert result["inserted"] == 0
    assert result["updated"] == 1
    assert result["skipped"] == 1  # 23456789: sheet value unchanged -> no-op


def test_web_ui_edited_row_is_not_overwritten(conn):
    client = FakeClient(ROWS)
    import_sheet_annotations(conn, client, tab="Eie")

    # Simulate Phase 5's web UI: it bumps updated_at without touching
    # imported_at, per the documented contract.
    conn.execute(
        "UPDATE annotations SET kommentar = ?, updated_at = ? WHERE finnkode = ?",
        ("edited via web UI", "2099-01-01T00:00:00+00:00", "12345678"),
    )
    conn.commit()

    changed_rows = [
        HEADER,
        ["12345678", "sheet says something else now", "A"],
        ['=HYPERLINK("http://finn.no/xyz","23456789")', "", "B"],
    ]
    client2 = FakeClient(changed_rows)
    result = import_sheet_annotations(conn, client2, tab="Eie")

    rows = _annotations(conn)
    assert rows["12345678"]["kommentar"] == "edited via web UI"
    assert rows["12345678"]["updated_at"] == "2099-01-01T00:00:00+00:00"
    # Both rows are "skipped": 12345678 because it's protected by the web-UI
    # edit, 23456789 because its sheet value is unchanged (idempotent no-op).
    assert result["skipped"] == 2
    assert result["inserted"] == 0
    assert result["updated"] == 0


def test_no_finnkode_column_is_a_noop(conn):
    client = FakeClient([["Adresse", "Kommentar", "Tag"], ["Foo", "bar", "baz"]])
    result = import_sheet_annotations(conn, client, tab="Eie")
    assert result == {"rows_read": 0, "candidates": 0, "inserted": 0, "updated": 0, "skipped": 0}
    assert _annotations(conn) == {}


def test_no_kommentar_or_tag_column_is_a_noop(conn):
    client = FakeClient([["Finnkode", "Adresse"], ["12345678", "Foo"]])
    result = import_sheet_annotations(conn, client, tab="Eie")
    assert result["rows_read"] == 0
    assert _annotations(conn) == {}


def test_empty_sheet_is_a_noop(conn):
    client = FakeClient([])
    result = import_sheet_annotations(conn, client, tab="Eie")
    assert result == {"rows_read": 0, "candidates": 0, "inserted": 0, "updated": 0, "skipped": 0}


# --- CLI ------------------------------------------------------------------


def _seeded_db(tmp_path):
    db = tmp_path / "seeded.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    result = CliRunner().invoke(app, ["tools", "import-sheet-annotations"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_exits_nonzero_when_migrations_pending(tmp_path, monkeypatch):
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(
        app, ["tools", "import-sheet-annotations", "--db", str(db)]
    )
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_missing_spreadsheet_id_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("SPREADSHEET_ID", "")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(
        app, ["tools", "import-sheet-annotations", "--db", str(db)]
    )
    assert result.exit_code == 1
    assert "SPREADSHEET_ID not set" in result.output


def test_cli_routes_to_import_sheet_annotations_and_never_writes_sheet(tmp_path, monkeypatch):
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET123")
    db = _seeded_db(tmp_path)

    built = []

    class FakeClientForCli(FakeClient):
        def __init__(self, spreadsheet_id):
            super().__init__(ROWS)
            built.append(spreadsheet_id)

    monkeypatch.setattr(tools_cmd, "SheetsClient", FakeClientForCli)

    result = CliRunner().invoke(
        app, ["tools", "import-sheet-annotations", "--db", str(db), "--tab", "Eie"]
    )
    assert result.exit_code == 0, result.output
    assert built == ["SHEET123"]

    c = connection.connect(db)
    assert set(_annotations(c)) == {"12345678", "23456789"}
