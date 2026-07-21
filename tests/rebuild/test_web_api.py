"""Tests for skannonser.web.api (Phase 5 Task 3: listings/meta/missing-coords).

Seeds tmp DBs via raw SQL (same helpers/conventions as test_export.py -- full
control over every column, no reliance on ListingsRepo's activation timing).
Note (per docs/rebuild/STATUS.md): first-appearance activation is now LIVE
(a single upsert is `active=1`), but since these tests write `eiendom` rows
directly with an explicit `active=` column, that pipeline-level change has no
bearing here -- every row's visibility is exactly what its `active`/
`tilgjengelighet` args say.
"""

import warnings

import pytest
from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="Using `httpx` with `starlette.testclient` is deprecated",
        category=StarletteDeprecationWarning,
    )
    from fastapi.testclient import TestClient

from skannonser.config.domain import load_domain
from skannonser.store import connection, migrations
from skannonser.web.app import create_app


# ---------------------------------------------------------------------------
# Fixtures + seeding helpers (mirrors tests/rebuild/test_export.py)
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "t.db"
    c = connection.connect(path)
    migrations.migrate(c)
    c.close()
    return path


@pytest.fixture()
def client(db_path):
    return TestClient(create_app(db_path))


def _conn(db_path):
    import sqlite3

    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
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
    dnb_id=None,
    scraped_at="2026-01-02T00:00:00",
):
    conn.execute(
        """
        INSERT INTO dnbeiendom (
            dnb_id, url, adresse, postnummer, pris, property_type, lat, lng,
            duplicate_of_finnkode, active, pendl_rush_brj, pendl_rush_mvv, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (dnb_id, url, adresse, postnummer, pris, property_type, lat, lng,
         duplicate_of_finnkode, active, brj, mvv, scraped_at),
    )
    conn.commit()


def _ins_station(conn, name, lat, lng, lines_travel, *, radius_m=None):
    cur = conn.execute(
        "INSERT INTO stations (name, lat, lng, radius_m) VALUES (?, ?, ?, ?)",
        (name, lat, lng, radius_m),
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


def _by_finnkode(listings, finnkode):
    for item in listings:
        if item["finnkode"] == finnkode:
            return item
    raise AssertionError(f"listing {finnkode!r} not found in {listings!r}")


# ---------------------------------------------------------------------------
# /api/listings -- Eie shape + donor travel + annotations
# ---------------------------------------------------------------------------

def test_listing_shape_and_donor_resolved_travel(db_path, client):
    conn = _conn(db_path)
    # Donor carries the travel values; A points at it with NULL own travel.
    _ins_eiendom(conn, "B", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "B", brj=11, mvv=22, mvv_uni=33)
    _ins_eiendom(conn, "A", pris=5_000_000, bra_i=80)
    _ins_processed(conn, "A", brj=None, mvv=None, mvv_uni=None, travel_copy_from_finnkode="B")
    conn.close()

    resp = client.get("/api/listings")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {"listings"}
    item = _by_finnkode(body["listings"], "A")

    assert set(item.keys()) == {
        "finnkode", "adresse", "postnummer", "pris", "pris_kvm", "boligtype",
        "tilgjengelighet", "lat", "lng", "travel", "bra_i", "byggeaar", "url",
        "image", "kommentar", "tag", "source", "sold",
    }
    assert item["finnkode"] == "A"
    assert item["adresse"] == "Gata 1"
    assert item["postnummer"] == "0581"
    assert item["pris"] == 5_000_000
    assert item["lat"] == 59.9
    assert item["lng"] == 10.7
    assert item["source"] == "eie"
    assert item["sold"] is False
    assert item["image"] is True
    # Donor-resolved: A inherits B's travel values.
    assert item["travel"] == {"brj": 11, "mvv": 22, "mvv_uni": 33}


def test_listing_annotations_joined(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "1000")
    _ins_processed(conn, "1000")
    _ins_annotation(conn, "1000", "Fin utsikt", "A")
    conn.close()

    resp = client.get("/api/listings")
    item = _by_finnkode(resp.json()["listings"], "1000")
    assert item["kommentar"] == "Fin utsikt"
    assert item["tag"] == "A"


# ---------------------------------------------------------------------------
# /api/listings -- DNB
# ---------------------------------------------------------------------------

def test_dnb_row_appears_with_own_travel(db_path, client):
    conn = _conn(db_path)
    _ins_dnb(conn, "https://dnb.no/unmatched", brj=15, mvv=25)
    conn.close()

    resp = client.get("/api/listings")
    listings = resp.json()["listings"]
    dnb_items = [i for i in listings if i["source"] == "dnb"]
    assert len(dnb_items) == 1
    item = dnb_items[0]
    assert item["adresse"] == "DNB Gata 2"
    assert item["url"] == "https://dnb.no/unmatched"
    assert item["travel"] == {"brj": 15, "mvv": 25, "mvv_uni": None}
    assert item["sold"] is False
    assert item["finnkode"].startswith("dnb:")


def test_dnb_matched_row_excluded(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "F")
    _ins_processed(conn, "F")
    _ins_dnb(conn, "https://dnb.no/matched", duplicate_of_finnkode="F")
    _ins_dnb(conn, "https://dnb.no/unmatched")
    conn.close()

    resp = client.get("/api/listings")
    urls = {i["url"] for i in resp.json()["listings"] if i["source"] == "dnb"}
    assert urls == {"https://dnb.no/unmatched"}


# ---------------------------------------------------------------------------
# /api/listings -- sold
# ---------------------------------------------------------------------------

def test_sold_excluded_by_default_included_with_param(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "visible")
    _ins_processed(conn, "visible")
    _ins_eiendom(conn, "sold-one", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-one")
    conn.close()

    resp = client.get("/api/listings")
    finnkoder = {i["finnkode"] for i in resp.json()["listings"]}
    assert finnkoder == {"visible"}

    resp = client.get("/api/listings", params={"sold": 1})
    listings = resp.json()["listings"]
    finnkoder = {i["finnkode"] for i in listings}
    assert finnkoder == {"visible", "sold-one"}
    sold_item = _by_finnkode(listings, "sold-one")
    assert sold_item["sold"] is True
    assert sold_item["source"] == "eie"
    visible_item = _by_finnkode(listings, "visible")
    assert visible_item["sold"] is False


# ---------------------------------------------------------------------------
# /api/listings/{finnkode} -- detail
# ---------------------------------------------------------------------------

def test_listing_detail_200(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "42", boligtype="Leilighet")
    _ins_processed(conn, "42", brj=5, mvv=6, mvv_uni=7)
    _ins_annotation(conn, "42", "Nice", "T")
    conn.close()

    resp = client.get("/api/listings/42")
    assert resp.status_code == 200
    body = resp.json()
    # API-shaped fields present.
    assert body["finnkode"] == "42"
    assert body["source"] == "eie"
    assert body["sold"] is False
    assert body["travel"] == {"brj": 5, "mvv": 6, "mvv_uni": 7}
    assert body["kommentar"] == "Nice"
    # Every raw sheet column also present (full row detail).
    assert body["Finnkode"] == "42"
    assert body["Tilgjengelighet"] == "Til salgs"
    assert body["GOOGLE_MAPS_URL"] == "https://maps/x"
    assert body["SCRAPED_AT"] == "2026-01-01T00:00:00"


def test_listing_detail_sold_row_flagged(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "99", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "99")
    conn.close()

    resp = client.get("/api/listings/99")
    assert resp.status_code == 200
    assert resp.json()["sold"] is True


def test_listing_detail_dnb(db_path, client):
    conn = _conn(db_path)
    _ins_dnb(conn, "https://dnb.no/detail-me", dnb_id="XYZ123", brj=9)
    conn.close()

    listings = client.get("/api/listings").json()["listings"]
    finnkode = next(i["finnkode"] for i in listings if i["source"] == "dnb")
    # Even with dnb_id="XYZ123", finnkode is derived from url hash, not dnb_id.
    assert finnkode.startswith("dnb:")
    assert len(finnkode) == 20  # "dnb:" (4) + 16 hex chars
    assert finnkode != "dnb:XYZ123"

    resp = client.get(f"/api/listings/{finnkode}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "dnb"
    assert body["URL"] == "https://dnb.no/detail-me"


def test_dnb_identifier_url_hash_stable_regardless_of_dnb_id(db_path, client):
    """Verify that a DNB row WITH a populated dnb_id still gets the url-hash
    identifier format, ensuring id stability across scrapes regardless of when
    dnb_id is populated. The identity/upsert-match key is url, not dnb_id."""
    conn = _conn(db_path)
    url1 = "https://dnb.no/stable-url-1"
    url2 = "https://dnb.no/stable-url-2"
    _ins_dnb(conn, url1, dnb_id="has_dnb_id", brj=5)
    _ins_dnb(conn, url2, dnb_id=None, brj=6)
    conn.close()

    listings = client.get("/api/listings").json()["listings"]
    dnb_items = [i for i in listings if i["source"] == "dnb"]
    assert len(dnb_items) == 2

    # Both should have url-hash-derived finnkodes.
    finnkodes = {i["finnkode"] for i in dnb_items}
    for fk in finnkodes:
        assert fk.startswith("dnb:")
        digest_part = fk[4:]  # strip "dnb:" prefix
        assert len(digest_part) == 16
        assert all(c in "0123456789abcdef" for c in digest_part)

    # Lookup detail by url-hash id should work.
    item1 = next(i for i in dnb_items if i["url"] == url1)
    resp = client.get(f"/api/listings/{item1['finnkode']}")
    assert resp.status_code == 200
    assert resp.json()["URL"] == url1


def test_listing_detail_404(db_path, client):
    resp = client.get("/api/listings/does-not-exist")
    assert resp.status_code == 404
    body = resp.json()
    assert "detail" in body
    assert "does-not-exist" in body["detail"]


# ---------------------------------------------------------------------------
# /api/meta
# ---------------------------------------------------------------------------

def test_meta_polygon_matches_domain(client):
    domain = load_domain()
    resp = client.get("/api/meta")
    assert resp.status_code == 200
    body = resp.json()
    assert body["polygon"] == [list(p) for p in domain.polygon_points]
    assert body["filters"] == {
        "sheets_max_price": domain.filters.sheets_max_price,
        "min_bra_i": domain.filters.min_bra_i,
    }
    assert body["destinations"] == [{"key": d.key, "label": d.label} for d in domain.destinations]


def test_meta_boligtyper_distinct_sorted(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "a1", boligtype="Leilighet")
    _ins_processed(conn, "a1")
    _ins_eiendom(conn, "a2", boligtype="Enebolig")
    _ins_processed(conn, "a2")
    _ins_eiendom(conn, "a3", boligtype="Leilighet")  # duplicate value
    _ins_processed(conn, "a3")
    conn.close()

    resp = client.get("/api/meta")
    assert resp.json()["boligtyper"] == ["Enebolig", "Leilighet"]


def test_meta_stations_carry_lines_travel_radius(db_path, client):
    conn = _conn(db_path)
    _ins_station(
        conn, "Sandvika", 59.89, 10.52,
        {
            "L1": {"Sandvika": 0, "Sandvika Transfer": 5},
            "L2": {"Sandvika": 2},
        },
        radius_m=400,
    )
    conn.close()

    resp = client.get("/api/meta")
    stations = resp.json()["stations"]
    assert len(stations) == 1
    st = stations[0]
    assert st["name"] == "Sandvika"
    assert st["lat"] == 59.89
    assert st["lng"] == 10.52
    assert st["radius_m"] == 400
    assert set(st["lines"]) == {"L1", "L2"}
    # L1 gives Sandvika=0, L2 gives Sandvika=2 -> station-level min is 0.
    assert st["travel"]["Sandvika"] == 0
    assert st["travel"]["Sandvika Transfer"] == 5


# ---------------------------------------------------------------------------
# /api/missing-coords
# ---------------------------------------------------------------------------

def test_missing_coords_lists_only_coordless_row(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "has-coords")
    _ins_processed(conn, "has-coords", lat=59.9, lng=10.7)
    _ins_eiendom(conn, "no-coords", adresse="Ingen Koord 3", postnummer="0123")
    _ins_processed(conn, "no-coords", lat=None, lng=None)
    conn.close()

    resp = client.get("/api/missing-coords")
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    assert rows[0] == {"finnkode": "no-coords", "adresse": "Ingen Koord 3", "postnummer": "0123"}


# ---------------------------------------------------------------------------
# _boligtype_raw whitespace regression (deferred from T2)
# ---------------------------------------------------------------------------

def test_boligtype_whitespace_trimmed(db_path, client):
    """DECISION: the API serves TRIMMED boligtype (both per-listing and in
    /api/meta's boligtyper list) for filter-consistency -- a scraped value
    with stray whitespace must not fracture client-side grouping. See
    skannonser/web/api.py's module docstring "BOLIGTYPE TRIM DECISION"."""
    conn = _conn(db_path)
    _ins_eiendom(conn, "ws1", boligtype="Leilighet ")  # trailing space
    _ins_processed(conn, "ws1")
    conn.close()

    resp = client.get("/api/listings")
    item = _by_finnkode(resp.json()["listings"], "ws1")
    assert item["boligtype"] == "Leilighet"

    meta = client.get("/api/meta").json()
    assert meta["boligtyper"] == ["Leilighet"]
