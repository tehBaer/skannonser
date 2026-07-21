"""Tests for skannonser.publish.export payload builders.

Seeds tmp DBs via raw SQL (full control over every column) and asserts the
legacy-faithful header/format contract. No google imports, no pandas.

Header expectations are transcribed from the legacy reading and pinned as
literal lists (with source line refs) so any drift is caught:
  * Eie/Sold: get_eiendom_for_sheets db.py:806-853 MINUS the four *_CNTR
    columns dropped by filter_hidden_sheet_columns (helper:108-113).
  * DNB: sync_dnbeiendom_sheet.py FULL_COL_ORDER (25-28).
  * Stations: sync_stations_to_sheet.py:71 for destination="Sandvika".
"""

import pytest

from skannonser.store import connection, migrations
from skannonser.publish import export
from skannonser.publish.export import (
    DNB_HEADER,
    EIE_HEADER,
    SOLD_HEADER,
    STATIONS_HEADER,
    dnb_rows,
    eie_rows,
    norm_cell,
    norm_postnummer,
    sold_rows,
    stations_rows,
)


# ---------------------------------------------------------------------------
# Fixtures + seeding helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def _ins_eiendom(
    conn,
    finnkode,
    *,
    tilgjengelighet="Til salgs",
    active=1,
    pris=5_000_000,
    bra_i=80,
    adresse="Gata 1",
    postnummer="0581",
    url=None,
    image_url="img",
    image_hosted_url="imghost",
    pris_kvm=50_000,
    byggear=1990,
    boligtype="Leilighet",
    scraped_at="2026-01-01T00:00:00",
):
    url = url if url is not None else f"https://www.finn.no/{finnkode}"
    conn.execute(
        """
        INSERT INTO eiendom (
            finnkode, tilgjengelighet, active, adresse, postnummer, pris, url,
            image_url, image_hosted_url, info_usable_i_area, info_construction_year,
            info_property_type, pris_kvm, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            finnkode, tilgjengelighet, active, adresse, postnummer, pris, url,
            image_url, image_hosted_url, bra_i, byggear, boligtype, pris_kvm, scraped_at,
        ),
    )
    conn.commit()


def _ins_processed(
    conn,
    finnkode,
    *,
    lat=59.9,
    lng=10.7,
    brj=None,
    mvv=None,
    mvv_uni=None,
    travel_copy_from_finnkode=None,
    google_maps_url="https://maps/x",
):
    conn.execute(
        """
        INSERT INTO eiendom_processed (
            finnkode, lat, lng, pendl_rush_brj, pendl_rush_mvv,
            pendl_rush_mvv_uni_rush, travel_copy_from_finnkode, google_maps_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (finnkode, lat, lng, brj, mvv, mvv_uni, travel_copy_from_finnkode, google_maps_url),
    )
    conn.commit()


def _ins_annotation(conn, finnkode, kommentar, tag):
    conn.execute(
        "INSERT INTO annotations (finnkode, kommentar, tag, imported_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (finnkode, kommentar, tag, "2026-01-01", "2026-01-01"),
    )
    conn.commit()


def _ins_dnb(
    conn,
    url,
    *,
    adresse="DNB Gata 2",
    postnummer="0582",
    pris=4_000_000,
    property_type="Enebolig",
    lat=59.8,
    lng=10.6,
    duplicate_of_finnkode=None,
    active=1,
    brj=None,
    mvv=None,
    scraped_at="2026-01-02T00:00:00",
):
    conn.execute(
        """
        INSERT INTO dnbeiendom (
            url, adresse, postnummer, pris, property_type, lat, lng,
            duplicate_of_finnkode, active, pendl_rush_brj, pendl_rush_mvv, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (url, adresse, postnummer, pris, property_type, lat, lng,
         duplicate_of_finnkode, active, brj, mvv, scraped_at),
    )
    conn.commit()


def _ins_station(conn, name, lat, lng, lines_travel):
    """lines_travel: {line: {destination: minutes}}."""
    cur = conn.execute(
        "INSERT INTO stations (name, lat, lng) VALUES (?, ?, ?)", (name, lat, lng)
    )
    station_id = cur.lastrowid
    for line, travels in lines_travel.items():
        cur = conn.execute(
            "INSERT INTO station_lines (station_id, line) VALUES (?, ?)",
            (station_id, line),
        )
        sl_id = cur.lastrowid
        for dest, minutes in travels.items():
            conn.execute(
                "INSERT INTO station_travel (station_line_id, destination, minutes) "
                "VALUES (?, ?, ?)",
                (sl_id, dest, minutes),
            )
    conn.commit()


def _cell(header, rows, finnkode_or_pred, column):
    """Return the `column` cell of the row whose Finnkode matches."""
    fk_idx = header.index("Finnkode")
    col_idx = header.index(column)
    for row in rows:
        if row[fk_idx] == finnkode_or_pred:
            return row[col_idx]
    raise AssertionError(f"row {finnkode_or_pred!r} not found")


# ---------------------------------------------------------------------------
# norm_cell / norm_postnummer matrix
# ---------------------------------------------------------------------------

def test_norm_cell_none_and_nan_to_empty():
    assert norm_cell(None) == ""
    assert norm_cell(float("nan")) == ""


def test_norm_cell_numbers_pass_through():
    assert norm_cell(5) == 5
    assert norm_cell(5) is not True  # not coerced to bool
    assert norm_cell(3.14) == 3.14
    assert norm_cell(0) == 0


def test_norm_cell_strings_cleaned():
    assert norm_cell("  Storgata 5  ") == "Storgata 5"
    assert norm_cell("line1\nline2\rline3") == "line1 line2 line3"
    assert norm_cell("") == ""


def test_norm_postnummer_matrix():
    assert norm_postnummer("581") == "0581"
    assert norm_postnummer(581) == "0581"
    assert norm_postnummer("0581") == "0581"
    assert norm_postnummer(1234) == "1234"
    assert norm_postnummer("") == ""
    assert norm_postnummer(None) == ""
    assert norm_postnummer(float("nan")) == ""
    # Non-numeric returned stripped, not zero-padded.
    assert norm_postnummer(" abc ") == "abc"


def test_norm_postnummer_payload_has_no_apostrophe():
    # Controller bug-compat ruling (commit d3eda31): payload is the bare string,
    # NOT apostrophe-escaped -- USER_ENTERED coerces it sheet-side.
    assert not norm_postnummer("0581").startswith("'")


# ---------------------------------------------------------------------------
# Eie header pin
# ---------------------------------------------------------------------------

def test_eie_header_pinned():
    assert EIE_HEADER == [
        "Finnkode",
        "Tilgjengelighet",
        "active",
        "ADRESSE",
        "Postnummer",
        "Pris",
        "URL",
        "IMAGE_URL",
        "IMAGE_HOSTED_URL",
        "Bruksareal",
        "Internt bruksareal (BRA-i)",
        "Primærrom",
        "Bruttoareal",
        "Eksternt bruksareal (BRA-e)",
        "Innglasset balkong (BRA-b)",
        "Balkong/Terrasse (TBA)",
        "Tomteareal",
        "Eierskap, tomt",
        "Boligtype",
        "Byggeår",
        "LAT",
        "LNG",
        "PRIS KVM",
        "PENDL RUSH BRJ",
        "PENDL RUSH MVV",
        "MVV UNI RUSH",
        "TRAVEL_COPY_FROM_FINNKODE",
        "GOOGLE_MAPS_URL",
        "SCRAPED_AT",
        "Kommentar",
        "Tag",
    ]


def test_eie_header_returned_matches_constant(conn):
    header, _ = eie_rows(conn)
    assert header == EIE_HEADER


# ---------------------------------------------------------------------------
# Eie visibility filter matrix
# ---------------------------------------------------------------------------

def test_eie_visibility_matrix(conn):
    # Included: active, for-sale, price under cap, bra over floor.
    _ins_eiendom(conn, "100", tilgjengelighet="Til salgs", active=1, pris=5_000_000, bra_i=80)
    _ins_processed(conn, "100")
    # Excluded dimensions:
    _ins_eiendom(conn, "200", active=0)                               # inactive
    _ins_eiendom(conn, "300", tilgjengelighet="Solgt")               # sold
    _ins_eiendom(conn, "400", tilgjengelighet="Inaktiv")             # inaktiv
    _ins_eiendom(conn, "500", pris=9_000_000)                        # over price cap
    _ins_eiendom(conn, "600", bra_i=50)                              # under bra floor
    _ins_eiendom(conn, "700", bra_i=None)                           # NULL bra -> excluded
    _ins_eiendom(conn, "800", pris=None)                            # NULL price -> excluded

    header, rows = eie_rows(conn)
    fk_idx = header.index("Finnkode")
    finnkoder = {row[fk_idx] for row in rows}
    assert finnkoder == {"100"}


def test_eie_case_insensitive_status_excluded(conn):
    _ins_eiendom(conn, "101", tilgjengelighet="solgt")
    _ins_eiendom(conn, "102", tilgjengelighet="  INAKTIV  ")
    _, rows = eie_rows(conn)
    assert rows == []


# ---------------------------------------------------------------------------
# Finnkode is a RAW string, NOT a HYPERLINK formula (pin)
# ---------------------------------------------------------------------------

def test_eie_finnkode_is_raw_not_hyperlink(conn):
    _ins_eiendom(conn, "12345678", url="https://www.finn.no/realestate/12345678")
    _ins_processed(conn, "12345678")
    header, rows = eie_rows(conn)
    finn_cell = _cell(header, rows, "12345678", "Finnkode")
    url_cell = _cell(header, rows, "12345678", "URL")
    # RAW finnkode string -- the only legacy HYPERLINK writer (googleUtils.py:117,
    # the dead read_csv path) is never on the DB->Sheets sync path.
    assert finn_cell == "12345678"
    assert not str(finn_cell).upper().startswith("=HYPERLINK")
    # URL lives in its own column.
    assert url_cell == "https://www.finn.no/realestate/12345678"


# ---------------------------------------------------------------------------
# Donor-resolved travel appears in payload
# ---------------------------------------------------------------------------

def test_eie_donor_resolved_travel(conn):
    # Donor B carries the travel values.
    _ins_eiendom(conn, "B", tilgjengelighet="Solgt", active=0)  # donor need not be visible
    _ins_processed(conn, "B", brj=11, mvv=22, mvv_uni=33)
    # A points at B and has NULL own travel -> inherits B's.
    _ins_eiendom(conn, "A")
    _ins_processed(conn, "A", brj=None, mvv=None, mvv_uni=None, travel_copy_from_finnkode="B")

    header, rows = eie_rows(conn)
    assert _cell(header, rows, "A", "PENDL RUSH BRJ") == 11
    assert _cell(header, rows, "A", "PENDL RUSH MVV") == 22
    assert _cell(header, rows, "A", "MVV UNI RUSH") == 33


def test_eie_own_travel_wins_without_donor(conn):
    _ins_eiendom(conn, "C")
    _ins_processed(conn, "C", brj=7, mvv=8, mvv_uni=9)
    header, rows = eie_rows(conn)
    assert _cell(header, rows, "C", "PENDL RUSH BRJ") == 7
    assert _cell(header, rows, "C", "MVV UNI RUSH") == 9


def test_eie_own_value_kept_when_donor_value_null(conn):
    # Link set, but donor has NULL for a column -> listing's own value survives.
    _ins_eiendom(conn, "D2", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "D2", brj=None, mvv=None, mvv_uni=None)
    _ins_eiendom(conn, "D1")
    _ins_processed(conn, "D1", brj=5, mvv=None, mvv_uni=None, travel_copy_from_finnkode="D2")
    header, rows = eie_rows(conn)
    assert _cell(header, rows, "D1", "PENDL RUSH BRJ") == 5


# ---------------------------------------------------------------------------
# Normalization inside the payload (NULL text -> "", numbers)
# ---------------------------------------------------------------------------

def test_eie_null_text_renders_empty_and_price_zero(conn):
    _ins_eiendom(conn, "900", pris=5_000_000, bra_i=80, boligtype=None, pris_kvm=None)
    _ins_processed(conn, "900", google_maps_url=None)
    header, rows = eie_rows(conn)
    assert _cell(header, rows, "900", "Boligtype") == ""
    assert _cell(header, rows, "900", "GOOGLE_MAPS_URL") == ""
    # PRIS KVM is fillna(0).astype(int) -> NULL renders 0 (not "").
    assert _cell(header, rows, "900", "PRIS KVM") == 0
    # Postnummer 0581 preserved as 4-digit string.
    assert _cell(header, rows, "900", "Postnummer") == "0581"


# ---------------------------------------------------------------------------
# Annotations re-exported
# ---------------------------------------------------------------------------

def test_eie_annotations_appear(conn):
    _ins_eiendom(conn, "1000")
    _ins_processed(conn, "1000")
    _ins_annotation(conn, "1000", "Fin utsikt", "A")
    # A visible listing without an annotation -> blank Kommentar/Tag.
    _ins_eiendom(conn, "1001")
    _ins_processed(conn, "1001")

    header, rows = eie_rows(conn)
    assert _cell(header, rows, "1000", "Kommentar") == "Fin utsikt"
    assert _cell(header, rows, "1000", "Tag") == "A"
    assert _cell(header, rows, "1001", "Kommentar") == ""
    assert _cell(header, rows, "1001", "Tag") == ""


# ---------------------------------------------------------------------------
# Sold tab
# ---------------------------------------------------------------------------

def test_sold_header_pinned():
    assert SOLD_HEADER == [
        "Finnkode", "Tilgjengelighet", "active", "ADRESSE", "Postnummer", "Pris",
        "URL", "IMAGE_URL", "IMAGE_HOSTED_URL", "Bruksareal",
        "Internt bruksareal (BRA-i)", "Primærrom", "Bruttoareal",
        "Eksternt bruksareal (BRA-e)", "Innglasset balkong (BRA-b)",
        "Balkong/Terrasse (TBA)", "Tomteareal", "Eierskap, tomt", "Boligtype",
        "Byggeår", "LAT", "LNG", "PRIS KVM", "PENDL RUSH BRJ", "PENDL RUSH MVV",
        "MVV UNI RUSH", "TRAVEL_COPY_FROM_FINNKODE", "GOOGLE_MAPS_URL", "SCRAPED_AT",
    ]
    assert "Kommentar" not in SOLD_HEADER


def test_sold_visibility_matrix(conn):
    # Included: inactive + Solgt, within filters.
    _ins_eiendom(conn, "10", tilgjengelighet="Solgt", active=0, pris=5_000_000, bra_i=80)
    _ins_processed(conn, "10")
    # Excluded: active listing (even if Solgt-labelled) -> not on Sold.
    _ins_eiendom(conn, "11", tilgjengelighet="Solgt", active=1)
    # Excluded: inactive but for-sale label (not solgt/inaktiv).
    _ins_eiendom(conn, "12", tilgjengelighet="Til salgs", active=0)
    # Excluded: sold but over price cap.
    _ins_eiendom(conn, "13", tilgjengelighet="Inaktiv", active=0, pris=9_000_000)
    # Excluded: sold but under bra floor.
    _ins_eiendom(conn, "14", tilgjengelighet="Solgt", active=0, bra_i=50)
    # Included: sold with NULL price -> COALESCE(pris,0)=0 <= cap (legacy fillna(0)).
    _ins_eiendom(conn, "15", tilgjengelighet="Solgt", active=0, pris=None, bra_i=80)
    _ins_processed(conn, "15")

    header, rows = sold_rows(conn)
    fk_idx = header.index("Finnkode")
    assert {row[fk_idx] for row in rows} == {"10", "15"}
    # NULL price rendered 0.
    assert _cell(header, rows, "15", "Pris") == 0


def test_sold_donor_resolved_travel(conn):
    _ins_eiendom(conn, "S2", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "S2", brj=44)
    _ins_eiendom(conn, "S1", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "S1", brj=None, travel_copy_from_finnkode="S2")
    header, rows = sold_rows(conn)
    assert _cell(header, rows, "S1", "PENDL RUSH BRJ") == 44


# ---------------------------------------------------------------------------
# DNB tab
# ---------------------------------------------------------------------------

def test_dnb_header_pinned():
    assert DNB_HEADER == [
        "Adresse", "Postnummer", "Pris", "Boligtype", "URL", "LAT", "LNG",
        "PENDL RUSH BRJ", "PENDL RUSH MVV", "MVV UNI RUSH",
    ]


def test_dnb_only_row_uses_own_travel(conn):
    _ins_dnb(conn, "https://dnb.no/x", brj=15, mvv=25, duplicate_of_finnkode=None)
    header, rows = dnb_rows(conn)
    assert len(rows) == 1
    row = dict(zip(header, rows[0]))
    assert row["Adresse"] == "DNB Gata 2"
    assert row["Postnummer"] == "0582"
    assert row["Boligtype"] == "Enebolig"
    assert row["PENDL RUSH BRJ"] == 15
    assert row["PENDL RUSH MVV"] == 25
    # No dnbeiendom mvv_uni column -> blank for DNB-only.
    assert row["MVV UNI RUSH"] == ""


def test_dnb_matched_row_inherits_finn_donor_values(conn):
    # FINN listing F with a donor G supplying donor-resolved travel.
    _ins_eiendom(conn, "G", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "G", brj=111, mvv=222, mvv_uni=333)
    _ins_eiendom(conn, "F")
    _ins_processed(conn, "F", brj=None, mvv=None, mvv_uni=None, travel_copy_from_finnkode="G")
    # Matched DNB row inherits F's donor-resolved travel (incl. MVV UNI RUSH),
    # ignoring its own dnbeiendom travel columns.
    _ins_dnb(conn, "https://dnb.no/matched", duplicate_of_finnkode="F", brj=1, mvv=2)

    header, rows = dnb_rows(conn)
    row = dict(zip(header, rows[0]))
    assert row["PENDL RUSH BRJ"] == 111
    assert row["PENDL RUSH MVV"] == 222
    assert row["MVV UNI RUSH"] == 333


def test_dnb_price_cap_filter(conn):
    _ins_dnb(conn, "https://dnb.no/cheap", pris=4_000_000)
    _ins_dnb(conn, "https://dnb.no/pricey", pris=9_000_000)
    _ins_dnb(conn, "https://dnb.no/inactive", pris=1_000_000, active=0)
    header, rows = dnb_rows(conn)
    urls = {dict(zip(header, r))["URL"] for r in rows}
    assert urls == {"https://dnb.no/cheap"}


# ---------------------------------------------------------------------------
# Stations tab
# ---------------------------------------------------------------------------

def test_stations_header_pinned():
    assert STATIONS_HEADER == ["Name", "LAT", "LNG", "Line", "TO_SANDVIKA", "TO_SANDVIKA_TRANSFER"]


def test_stations_rows_strified_and_ordered(conn):
    _ins_station(
        conn, "Sandvika", 59.89, 10.52,
        {
            "L1": {"Sandvika": 0, "Sandvika Transfer": 5},
            "L2": {"Sandvika": 2},
        },
    )
    _ins_station(
        conn, "Asker", 59.83, 10.43,
        {"L1": {"Sandvika": 12, "Sandvika Transfer": 17}},
    )

    header, rows = stations_rows(conn)
    assert header == STATIONS_HEADER
    # Ordered by name then line: Asker/L1, Sandvika/L1, Sandvika/L2.
    assert rows[0] == ["Asker", "59.83", "10.43", "L1", "12", "17"]
    assert rows[1] == ["Sandvika", "59.89", "10.52", "L1", "0", "5"]
    # L2 has no transfer minute -> blank string; every cell is a str.
    assert rows[2] == ["Sandvika", "59.89", "10.52", "L2", "2", ""]
    for row in rows:
        assert all(isinstance(cell, str) for cell in row)


def test_stations_missing_coords_render_blank(conn):
    _ins_station(conn, "NoCoord", None, None, {"L1": {"Sandvika": 3}})
    header, rows = stations_rows(conn)
    row = dict(zip(header, rows[0]))
    assert row["LAT"] == ""
    assert row["LNG"] == ""
    assert row["TO_SANDVIKA"] == "3"
