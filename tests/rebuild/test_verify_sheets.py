"""Tests for the sheets golden-master harness (`skannonser/verify/sheets.py`).

No network, no Sheets service, no API key anywhere: every `_legacy_*`
builder in the module under test only ever computes a DataFrame or a list
of rows -- `get_sheets_service`/`SheetsClient` are never constructed on
either side. A zero `api_usage` row count after every call is asserted
directly, matching the pattern in `test_verify_enrich.py`.
"""
from pathlib import Path

import pytest

from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo
from skannonser.ingest.base import NormalizedListing
from skannonser.verify import sheets as sheets_mod
from skannonser.verify.sheets import verify_sheets


@pytest.fixture
def db_path(tmp_path) -> Path:
    path = tmp_path / "verify_sheets.db"
    conn = connection.connect(path)
    migrations.migrate(conn)
    conn.close()
    return path


def _seed_listing(
    conn,
    finnkode,
    *,
    adresse="Gata 1",
    postnummer="0575",
    pris=5_000_000,
    active=True,
):
    """Seed an eiendom row via the real repo (two upserts -> active=1),
    legacy-style, matching test_verify_enrich.py's `_seed_listing`."""
    repo = ListingsRepo(conn)
    listing = NormalizedListing(
        **{
            "Finnkode": finnkode,
            "URL": f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
            "Adresse": adresse,
            "Postnummer": postnummer,
            "Pris": pris,
            "Internt bruksareal (BRA-i)": 80,
        }
    )
    repo.upsert([listing])
    if active:
        repo.upsert([listing])


def _seed_processed(conn, finnkode, *, lat=None, lng=None, travel=None, link=None, adresse="Gata 1"):
    ProcessedRepo(conn).upsert(
        finnkode,
        adresse,
        "0575",
        lat=lat,
        lng=lng,
        travel=travel or {},
        travel_copy_from_finnkode=link,
    )


def _mark_sold(conn, finnkode):
    conn.execute(
        "UPDATE eiendom SET tilgjengelighet = 'Solgt', active = 0 WHERE finnkode = ?",
        (finnkode,),
    )
    conn.commit()


def _seed_station(conn, name, lat, lng, lines_travel):
    cur = conn.execute("INSERT INTO stations (name, lat, lng) VALUES (?, ?, ?)", (name, lat, lng))
    station_id = cur.lastrowid
    for line, travels in lines_travel.items():
        cur = conn.execute(
            "INSERT INTO station_lines (station_id, line) VALUES (?, ?)", (station_id, line)
        )
        sl_id = cur.lastrowid
        for dest, minutes in travels.items():
            conn.execute(
                "INSERT INTO station_travel (station_line_id, destination, minutes) "
                "VALUES (?, ?, ?)",
                (sl_id, dest, minutes),
            )
    conn.commit()


def _api_usage_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM api_usage").fetchone()["c"]


def _seed_scenario(db_path: Path) -> None:
    """A visible Eie row, a donor-resolved travel value, a sold row, and a
    station with lines + travel (incl. the Sandvika Transfer column)."""
    conn = connection.connect(db_path)
    try:
        # Visible row.
        _seed_listing(conn, "1001", adresse="Storgata 1")
        _seed_processed(conn, "1001", lat=59.91, lng=10.75, travel={
            "pendl_rush_brj": 20,
            "pendl_rush_mvv": 25,
            "pendl_rush_mvv_uni_rush": 30,
        })

        # Donor pair: 1003 (invisible, Solgt) supplies travel to 1002 (visible).
        _seed_listing(conn, "1003", adresse="Donorveien 3")
        _mark_sold(conn, "1003")
        _seed_processed(conn, "1003", travel={
            "pendl_rush_brj": 44,
            "pendl_rush_mvv": 55,
            "pendl_rush_mvv_uni_rush": 66,
        })
        _seed_listing(conn, "1002", adresse="Nabogata 2")
        _seed_processed(conn, "1002", link="1003")

        # A dedicated sold row (visible on the Sold tab, not on Eie).
        _seed_listing(conn, "2001", adresse="Solgtgata 1")
        _mark_sold(conn, "2001")
        _seed_processed(conn, "2001")

        # A station with two lines, one of which has a Sandvika Transfer value.
        _seed_station(
            conn,
            "Sandvika",
            59.89,
            10.52,
            {
                "L1": {"Sandvika": 0, "Sandvika Transfer": 5},
                "L2": {"Sandvika": 2},
            },
        )
    finally:
        conn.close()


def test_verify_sheets_matches_on_constructed_scenario(db_path):
    _seed_scenario(db_path)

    result = verify_sheets(db_path)

    assert result.eie_diffs == []
    assert result.sold_diffs == []
    assert result.stations_diffs == []


def test_verify_sheets_makes_zero_api_calls(db_path):
    _seed_scenario(db_path)

    verify_sheets(db_path)

    conn = connection.connect(db_path)
    try:
        assert _api_usage_count(conn) == 0
    finally:
        conn.close()


def test_verify_sheets_detects_desync_eie(db_path, monkeypatch):
    _seed_scenario(db_path)

    real_eie_rows = sheets_mod.eie_rows

    def _wrong_eie_rows(conn):
        header, rows = real_eie_rows(conn)
        adresse_idx = header.index("ADRESSE")
        wrong_rows = []
        for row in rows:
            row = list(row)
            row[adresse_idx] = "WRONG ADDRESS"
            wrong_rows.append(row)
        return header, wrong_rows

    monkeypatch.setattr(sheets_mod, "eie_rows", _wrong_eie_rows)

    result = verify_sheets(db_path)

    assert result.eie_diffs != []
    assert all(d.new_value == "WRONG ADDRESS" for d in result.eie_diffs if d.field == "ADRESSE")


def test_verify_sheets_detects_desync_sold(db_path, monkeypatch):
    _seed_scenario(db_path)

    real_sold_rows = sheets_mod.sold_rows

    def _wrong_sold_rows(conn):
        header, rows = real_sold_rows(conn)
        pris_idx = header.index("Pris")
        wrong_rows = []
        for row in rows:
            row = list(row)
            row[pris_idx] = 999
            wrong_rows.append(row)
        return header, wrong_rows

    monkeypatch.setattr(sheets_mod, "sold_rows", _wrong_sold_rows)

    result = verify_sheets(db_path)

    assert result.sold_diffs != []
    assert all(d.new_value == 999 for d in result.sold_diffs if d.field == "Pris")


def test_verify_sheets_tolerates_unpadded_legacy_postnummer(db_path):
    """Regression test for the checkpoint finding: real DB rows sometimes
    store `postnummer` WITHOUT a leading zero (e.g. "581" not "0581") --
    legacy's `sanitize_for_sheets` never touches Postnummer, so it emits
    that raw string verbatim, while `norm_postnummer` always zero-pads.
    Both parse to the identical number under Sheets' `USER_ENTERED`, so
    `verify_sheets` must NOT flag this as a diff (see `_NUMERIC_STRING_FIELDS`
    in `skannonser/verify/sheets.py`)."""
    _seed_listing(conn := connection.connect(db_path), "3001", postnummer="581")
    _seed_processed(conn, "3001")
    conn.close()

    result = verify_sheets(db_path)

    assert result.eie_diffs == []


def test_verify_sheets_still_catches_a_genuinely_different_postnummer(db_path, monkeypatch):
    """The numeric-tolerant Postnummer comparison must not mask an actual
    wrong-postal-code regression -- only the leading-zero formatting is
    moot, not the value itself."""
    _seed_listing(conn := connection.connect(db_path), "3002", postnummer="581")
    _seed_processed(conn, "3002")
    conn.close()

    real_eie_rows = sheets_mod.eie_rows

    def _wrong_postnummer(conn):
        header, rows = real_eie_rows(conn)
        idx = header.index("Postnummer")
        wrong_rows = []
        for row in rows:
            row = list(row)
            row[idx] = "0582"
            wrong_rows.append(row)
        return header, wrong_rows

    monkeypatch.setattr(sheets_mod, "eie_rows", _wrong_postnummer)

    result = verify_sheets(db_path)

    assert result.eie_diffs != []
    assert all(d.new_value == "0582" for d in result.eie_diffs if d.field == "Postnummer")


def test_verify_sheets_detects_desync_stations(db_path, monkeypatch):
    _seed_scenario(db_path)

    real_stations_rows = sheets_mod.stations_rows

    def _wrong_stations_rows(conn):
        header, rows = real_stations_rows(conn)
        travel_idx = header.index("TO_SANDVIKA")
        wrong_rows = []
        for row in rows:
            row = list(row)
            row[travel_idx] = "999"
            wrong_rows.append(row)
        return header, wrong_rows

    monkeypatch.setattr(sheets_mod, "stations_rows", _wrong_stations_rows)

    result = verify_sheets(db_path)

    assert result.stations_diffs != []
    assert all(
        d.new_value == "999" for d in result.stations_diffs if d.field == "TO_SANDVIKA"
    )
