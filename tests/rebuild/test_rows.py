"""Tests for skannonser.publish.rows.listing_rows -- the shared Eie
listing-row query extracted out of export.eie_rows.

Golden-master intent: listing_rows(conn) (the default path) must produce
exactly the row *content* export.eie_rows already builds and returns
(cross-checked here on a seeded DB), and include_hidden_fields=True must add
strictly additive underscore-prefixed keys that never leak into the sheet
header set and never appear on the default path.
"""

import pytest

from skannonser.store import connection, migrations
from skannonser.publish import export
from skannonser.publish.export import EIE_HEADER, eie_rows
from skannonser.publish.rows import listing_rows

_HIDDEN_KEYS = {"_finnkode", "_active", "_lat", "_lng", "_boligtype_raw", "_image_url"}


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


def _seed_mixed(conn):
    """A handful of rows exercising visibility filter, donor-resolved
    travel, annotations, and NULL lat/lng -- everything eie_rows' output
    could vary on."""
    _ins_eiendom(conn, "100", tilgjengelighet="Til salgs", active=1, pris=5_000_000, bra_i=80)
    _ins_processed(conn, "100", lat=59.91, lng=10.71)
    _ins_annotation(conn, "100", "nice one", "hot")

    _ins_eiendom(conn, "200", tilgjengelighet="Til salgs", active=1, pris=3_000_000, bra_i=90,
                 boligtype="Enebolig", image_url="img200")
    _ins_processed(conn, "200", brj=None, travel_copy_from_finnkode="100")

    # No eiendom_processed row at all -> NULL lat/lng/boligtype-donor etc.
    _ins_eiendom(conn, "300", tilgjengelighet="Til salgs", active=1, pris=4_000_000, bra_i=75)

    # Excluded by visibility (sold) -- must not appear in either output.
    _ins_eiendom(conn, "400", tilgjengelighet="Solgt", active=0)


# ---------------------------------------------------------------------------
# Default path == eie_rows content
# ---------------------------------------------------------------------------

def test_listing_rows_default_matches_eie_rows_content(conn):
    _seed_mixed(conn)

    header, sheet_rows = eie_rows(conn)
    records = listing_rows(conn)

    assert header == EIE_HEADER
    assert len(records) == len(sheet_rows)
    for rec, row in zip(records, sheet_rows):
        expected = [export._norm_base_cell(h, rec.get(h)) for h in EIE_HEADER]
        assert row == expected


def test_listing_rows_default_row_count_and_finnkoder_match_visibility(conn):
    _seed_mixed(conn)
    header, sheet_rows = eie_rows(conn)
    fk_idx = header.index("Finnkode")
    eie_finnkoder = {row[fk_idx] for row in sheet_rows}

    records = listing_rows(conn)
    listing_finnkoder = {rec["Finnkode"] for rec in records}

    assert eie_finnkoder == listing_finnkoder == {"100", "200", "300"}


# ---------------------------------------------------------------------------
# Hidden fields
# ---------------------------------------------------------------------------

def test_hidden_fields_absent_by_default(conn):
    _seed_mixed(conn)
    records = listing_rows(conn)
    assert records  # sanity: seeded rows survived the filter
    for rec in records:
        assert not (_HIDDEN_KEYS & rec.keys())


def test_hidden_fields_present_and_correct_when_requested(conn):
    _seed_mixed(conn)
    records = listing_rows(conn, include_hidden_fields=True)
    by_fk = {rec["_finnkode"]: rec for rec in records}

    r100 = by_fk["100"]
    assert _HIDDEN_KEYS <= r100.keys()
    assert r100["_finnkode"] == "100"
    assert r100["_active"] == 1
    assert r100["_lat"] == pytest.approx(59.91)
    assert r100["_lng"] == pytest.approx(10.71)
    assert isinstance(r100["_lat"], float)
    assert isinstance(r100["_lng"], float)
    assert r100["_boligtype_raw"] == "Leilighet"
    assert r100["_image_url"] == "img"

    r200 = by_fk["200"]
    assert r200["_boligtype_raw"] == "Enebolig"
    assert r200["_image_url"] == "img200"

    # No eiendom_processed row for 300 -> lat/lng missing -> None, not 0/"".
    r300 = by_fk["300"]
    assert r300["_lat"] is None
    assert r300["_lng"] is None
    assert r300["_active"] == 1
    assert r300["_finnkode"] == "300"


def test_hidden_keys_never_collide_with_header_names(conn):
    assert not (_HIDDEN_KEYS & set(EIE_HEADER))


def test_hidden_fields_do_not_alter_default_path_output(conn):
    """Requesting hidden fields must be purely additive -- the header-keyed
    values (and therefore eie_rows' downstream output) are unaffected."""
    _seed_mixed(conn)
    plain = listing_rows(conn)
    hidden = listing_rows(conn, include_hidden_fields=True)
    assert len(plain) == len(hidden)
    for p, h in zip(plain, hidden):
        for key in EIE_HEADER:
            assert p.get(key) == h.get(key)


def test_underscore_keys_never_leak_into_eie_rows_output(conn):
    _seed_mixed(conn)
    header, rows = eie_rows(conn)
    # eie_rows rows are plain lists positioned by EIE_HEADER -- there is no
    # dict to leak underscore keys into, but pin the header itself as the
    # authoritative "no hidden keys" contract, and pin row width to match.
    assert not any(h.startswith("_") for h in header)
    for row in rows:
        assert len(row) == len(header)
