"""Tests for skannonser.web.api (Phase 5 Task 3: listings/meta/missing-coords;
Phase 5 Task 4: annotations CRUD).

Seeds tmp DBs via raw SQL (same helpers/conventions as test_export.py -- full
control over every column, no reliance on ListingsRepo's activation timing).
Note (per the rebuild record, git history): first-appearance activation is LIVE
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
from skannonser.publish.annotations import import_sheet_annotations
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
    # thumbs_dir=None: the generic fixture used by every test in this module
    # deliberately keeps the pre-Task-5 `image = bool(image_url)` fallback
    # (see skannonser.web.api's "IMAGE DECISION" docstring) so the many
    # unrelated tests below don't need to know about the thumbnail cache.
    # The file-existence behavior itself (thumbs_dir configured) has its own
    # dedicated fixture/tests -- see "Image thumb-file existence" below.
    return TestClient(create_app(db_path, thumbs_dir=None))


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


def _seed_details(conn_or_path, finnkode, **cols):
    """Insert a ``listing_details`` row (migration 010) with ``finnkode`` plus
    whatever columns the caller supplies -- unspecified columns stay NULL.
    NOTE: the ``eiendom`` row for ``finnkode`` must already exist (FK,
    ``PRAGMA foreign_keys=ON`` -- see ``connection.connect``)."""
    conn = conn_or_path
    columns = ["finnkode", *cols.keys()]
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO listing_details ({', '.join(columns)}) VALUES ({placeholders})",
        [finnkode, *cols.values()],
    )
    conn.commit()


def _ins_facility(conn, finnkode, facility):
    conn.execute(
        "INSERT INTO listing_facilities (finnkode, facility) VALUES (?, ?)",
        (finnkode, facility),
    )
    conn.commit()


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
        "image", "kommentar", "tag", "scraped_at", "source", "sold",
        # Listing-details enrichment (migration 010; Task 9).
        "soverom", "rom", "etasje", "eieform", "nabolag", "energimerke",
        "energifarge", "totalpris", "omkostninger", "fellesgjeld",
        "felleskost_mnd", "fellesformue", "formuesverdi", "kommunale_avg_aar",
        "facilities", "pris_kvm_totalpris", "maanedskost",
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


def test_bucket_sold_returns_only_sold(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "visible")
    _ins_processed(conn, "visible")
    _ins_eiendom(conn, "sold-one", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-one")
    _ins_dnb(conn, "https://dnb/x")
    conn.close()

    resp = client.get("/api/listings", params={"bucket": "sold"})
    listings = resp.json()["listings"]
    assert {i["finnkode"] for i in listings} == {"sold-one"}
    assert all(i["sold"] is True for i in listings)


def test_bucket_unknown_400(client):
    resp = client.get("/api/listings", params={"bucket": "nope"})
    assert resp.status_code == 400


def _ins_sold_price(
    conn,
    finnkode,
    *,
    sold_price=6_150_000,
    sold_date="2026-05-12",
    price_suggestion=5_750_000,
):
    conn.execute(
        "INSERT INTO sold_prices (finnkode, sold_price, sold_date, price_suggestion) "
        "VALUES (?, ?, ?, ?)",
        (finnkode, sold_price, sold_date, price_suggestion),
    )
    conn.commit()


def test_sold_item_carries_sold_price_fields(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "sold-priced", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-priced")
    _ins_sold_price(conn, "sold-priced")
    _ins_eiendom(conn, "sold-unpriced", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-unpriced")
    conn.close()

    listings = client.get("/api/listings", params={"bucket": "sold"}).json()["listings"]
    priced = _by_finnkode(listings, "sold-priced")
    assert priced["sold_price"] == 6_150_000
    assert priced["sold_date"] == "2026-05-12"
    assert priced["price_suggestion"] == 5_750_000
    unpriced = _by_finnkode(listings, "sold-unpriced")
    assert unpriced["sold_price"] is None
    assert unpriced["sold_date"] is None
    assert unpriced["price_suggestion"] is None


def test_active_item_has_no_sold_price_keys(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "active-one")
    _ins_processed(conn, "active-one")
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "active-one")
    assert "sold_price" not in item


def test_scraped_at_exposed_on_all_buckets(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "active-one", scraped_at="2026-07-01T01:02:03")
    _ins_processed(conn, "active-one")
    _ins_eiendom(
        conn, "sold-one", tilgjengelighet="Solgt", active=0,
        scraped_at="2026-06-01T01:02:03",
    )
    _ins_processed(conn, "sold-one")
    _ins_dnb(conn, "https://dnb/x", scraped_at="2026-07-02T03:04:05")
    conn.close()

    listings = client.get("/api/listings", params={"sold": 1}).json()["listings"]
    assert _by_finnkode(listings, "active-one")["scraped_at"] == "2026-07-01T01:02:03"
    assert _by_finnkode(listings, "sold-one")["scraped_at"] == "2026-06-01T01:02:03"
    dnb = [i for i in listings if i["source"] == "dnb"]
    assert dnb and dnb[0]["scraped_at"] == "2026-07-02T03:04:05"


def test_listing_detail_sold_carries_sold_price_fields(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "sold-priced", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-priced")
    _ins_sold_price(conn, "sold-priced", sold_price=7_000_000)
    conn.close()

    detail = client.get("/api/listings/sold-priced").json()
    assert detail["sold"] is True
    assert detail["sold_price"] == 7_000_000


def test_responses_are_gzipped_when_accepted(db_path, client):
    conn = _conn(db_path)
    for i in range(30):  # enough rows to clear the gzip minimum-size threshold
        _ins_eiendom(conn, f"g{i}")
        _ins_processed(conn, f"g{i}")
    conn.close()

    resp = client.get("/api/listings", headers={"Accept-Encoding": "gzip"})
    assert resp.headers.get("content-encoding") == "gzip"


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


def test_meta_vocabularies(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "m1")
    _ins_processed(conn, "m1")
    _seed_details(conn, "m1", energimerke="C", eieform="Andel")
    _ins_facility(conn, "m1", "Heis")

    _ins_eiendom(conn, "m2")
    _ins_processed(conn, "m2")
    _seed_details(conn, "m2", energimerke="A")
    _ins_facility(conn, "m2", "Heis")
    _ins_facility(conn, "m2", "Peis/Ildsted")
    conn.close()

    meta = client.get("/api/meta").json()
    assert meta["facilities"][0] == {"name": "Heis", "count": 2}
    assert meta["energimerker"] == ["A", "C"]
    assert meta["eieformer"] == ["Andel"]


# ---------------------------------------------------------------------------
# /api/listings + /api/listings/{finnkode} -- listing_details/facilities
# enrichment (migration 010; Task 9)
# ---------------------------------------------------------------------------

def test_listings_carry_details_fields(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "1", bra_i=100)
    _ins_processed(conn, "1")
    _seed_details(
        conn, "1",
        totalpris=5_000_000, felleskost_mnd=4000, kommunale_avg_aar=12000,
        bedrooms=2, eieform="Andel", energimerke="C",
    )
    _ins_facility(conn, "1", "Heis")
    _ins_facility(conn, "1", "Garasje/P-plass")
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "1")
    assert item["soverom"] == 2
    assert item["eieform"] == "Andel"
    assert item["energimerke"] == "C"
    assert item["totalpris"] == 5_000_000
    assert item["felleskost_mnd"] == 4000
    assert item["facilities"] == ["Garasje/P-plass", "Heis"]
    assert item["pris_kvm_totalpris"] == 50_000  # 5_000_000 / 100
    assert item["maanedskost"] == 5000  # 4000 + 12000/12


def test_details_absent_rows_get_nulls_and_empty_facilities(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "2")
    _ins_processed(conn, "2")
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "2")
    assert item["totalpris"] is None
    assert item["facilities"] == []
    assert item["pris_kvm_totalpris"] is None
    assert item["maanedskost"] is None


def test_maanedskost_null_kommunale_avg_contributes_zero(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "3")
    _ins_processed(conn, "3")
    _seed_details(conn, "3", felleskost_mnd=4000)
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "3")
    assert item["maanedskost"] == 4000


def test_sold_bucket_carries_details(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "sold-details", tilgjengelighet="Solgt", active=0)
    _ins_processed(conn, "sold-details")
    _seed_details(conn, "sold-details", totalpris=6_000_000)
    conn.close()

    listings = client.get("/api/listings", params={"bucket": "sold"}).json()["listings"]
    item = _by_finnkode(listings, "sold-details")
    assert item["totalpris"] == 6_000_000


def test_detail_endpoint_exposes_matrikkel(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "111")
    _ins_processed(conn, "111")
    _seed_details(conn, "111", kommunenr="3301", borettslag_navn="X")
    conn.close()

    data = client.get("/api/listings/111").json()
    assert data["KOMMUNENR"] == "3301"
    assert data["BORETTSLAG_NAVN"] == "X"


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


# ---------------------------------------------------------------------------
# `image` bool -- thumbnail-file existence (Phase 5 Task 5)
# ---------------------------------------------------------------------------

@pytest.fixture()
def thumbs_dir(tmp_path):
    d = tmp_path / "thumbs"
    d.mkdir()
    return d


@pytest.fixture()
def thumbs_client(db_path, thumbs_dir):
    return TestClient(create_app(db_path, thumbs_dir=thumbs_dir))


def test_image_false_when_thumbs_dir_configured_but_file_missing(
    db_path, thumbs_client
):
    """A non-empty image_url alone is no longer enough once thumbs_dir is
    configured -- the pre-Task-5 placeholder is retired for this app."""
    conn = _conn(db_path)
    _ins_eiendom(conn, "A", image_url="https://example/img.jpg")
    _ins_processed(conn, "A")
    conn.close()

    item = _by_finnkode(thumbs_client.get("/api/listings").json()["listings"], "A")
    assert item["image"] is False


def test_image_true_when_cached_thumb_file_exists(db_path, thumbs_dir, thumbs_client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A", image_url="https://example/img.jpg")
    _ins_processed(conn, "A")
    conn.close()
    (thumbs_dir / "A.jpg").write_bytes(b"fake-jpeg-bytes")

    item = _by_finnkode(thumbs_client.get("/api/listings").json()["listings"], "A")
    assert item["image"] is True


def test_image_true_for_dnb_row_when_cached_thumb_file_exists(
    db_path, thumbs_dir, thumbs_client
):
    """DNB rows use the SAME synthetic dnb: identifier as the thumbnail
    filename -- proves the two call sites (this API and
    skannonser.enrich.thumbs) share one derivation via skannonser.ids."""
    conn = _conn(db_path)
    _ins_dnb(conn, "https://dnb.no/has-thumb")
    conn.close()

    listings = thumbs_client.get("/api/listings").json()["listings"]
    dnb_item = next(i for i in listings if i["source"] == "dnb")
    assert dnb_item["image"] is False  # no cached file yet

    (thumbs_dir / f"{dnb_item['finnkode']}.jpg").write_bytes(b"fake-jpeg-bytes")

    listings = thumbs_client.get("/api/listings").json()["listings"]
    dnb_item = next(i for i in listings if i["source"] == "dnb")
    assert dnb_item["image"] is True


def test_image_reflects_thumb_existence_on_detail_endpoint_too(
    db_path, thumbs_dir, thumbs_client
):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A", image_url="https://example/img.jpg")
    _ins_processed(conn, "A")
    conn.close()

    assert thumbs_client.get("/api/listings/A").json()["image"] is False

    (thumbs_dir / "A.jpg").write_bytes(b"fake-jpeg-bytes")
    assert thumbs_client.get("/api/listings/A").json()["image"] is True


# ---------------------------------------------------------------------------
# /api/annotations/{finnkode} -- CRUD (Phase 5 Task 4)
# ---------------------------------------------------------------------------

class _FakeSheetsClient:
    """Stands in for SheetsClient in the interplay-lock tests below: `read_tab`
    returns canned rows; any write call is a test failure (mirrors
    tests/rebuild/test_annotations_import.py's FakeClient)."""

    def __init__(self, rows):
        self._rows = rows

    def read_tab(self, tab):
        return self._rows

    def rewrite_tab(self, *a, **kw):
        raise AssertionError("import_sheet_annotations must never write the sheet")


def _annotation_row(conn, finnkode):
    row = conn.execute(
        "SELECT * FROM annotations WHERE finnkode = ?", (finnkode,)
    ).fetchone()
    return dict(row) if row else None


def test_get_annotation_absent_returns_nulls(client):
    resp = client.get("/api/annotations/111")
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "111", "kommentar": None, "tag": None}


def test_put_annotation_create_then_get_roundtrip(db_path, client):
    resp = client.put(
        "/api/annotations/222", json={"kommentar": "Fin utsikt", "tag": "A"}
    )
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "222", "kommentar": "Fin utsikt", "tag": "A"}

    conn = _conn(db_path)
    stored = _annotation_row(conn, "222")
    conn.close()
    assert stored["kommentar"] == "Fin utsikt"
    assert stored["tag"] == "A"
    assert stored["imported_at"] is None
    assert stored["updated_at"] is not None

    resp = client.get("/api/annotations/222")
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "222", "kommentar": "Fin utsikt", "tag": "A"}


def test_put_annotation_update_changes_value_and_bumps_updated_at(db_path, client):
    client.put("/api/annotations/333", json={"kommentar": "First", "tag": "A"})
    conn = _conn(db_path)
    before = _annotation_row(conn, "333")
    conn.close()

    resp = client.put(
        "/api/annotations/333", json={"kommentar": "Second", "tag": "B"}
    )
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "333", "kommentar": "Second", "tag": "B"}

    conn = _conn(db_path)
    after = _annotation_row(conn, "333")
    conn.close()
    assert after["kommentar"] == "Second"
    assert after["tag"] == "B"
    assert after["updated_at"] != before["updated_at"]
    assert after["imported_at"] is None  # fresh insert never had one; untouched on update


def test_put_annotation_both_null_deletes_row(db_path, client):
    """Never-imported row (created only via web PUT, so `imported_at IS
    NULL`) -- nothing for a later import to resurrect, so the both-null PUT
    still physically DELETEs, per the "TOMBSTONE DECISION" in api.py."""
    client.put("/api/annotations/444", json={"kommentar": "Temp", "tag": "T"})
    conn = _conn(db_path)
    assert _annotation_row(conn, "444")["imported_at"] is None
    conn.close()

    resp = client.put("/api/annotations/444", json={"kommentar": None, "tag": None})
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "444", "kommentar": None, "tag": None}

    conn = _conn(db_path)
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM annotations WHERE finnkode = ?", ("444",)
    ).fetchone()["c"]
    conn.close()
    assert count == 0


def test_put_annotation_both_empty_string_deletes_row(db_path, client):
    """Contract treats null and "" interchangeably for the delete trigger."""
    client.put("/api/annotations/445", json={"kommentar": "Temp", "tag": "T"})

    resp = client.put("/api/annotations/445", json={"kommentar": "", "tag": "  "})
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "445", "kommentar": None, "tag": None}

    conn = _conn(db_path)
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM annotations WHERE finnkode = ?", ("445",)
    ).fetchone()["c"]
    conn.close()
    assert count == 0


def test_put_annotation_partial_null_stores_real_sql_null(db_path, client):
    """kommentar set, tag null -> tag round-trips as an actual SQL NULL (not
    the string "None" or an empty string) on direct-SQL readback."""
    resp = client.put(
        "/api/annotations/447", json={"kommentar": "Only kommentar", "tag": None}
    )
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "447", "kommentar": "Only kommentar", "tag": None}

    conn = _conn(db_path)
    row = _annotation_row(conn, "447")
    conn.close()
    assert row["kommentar"] == "Only kommentar"
    assert row["tag"] is None


@pytest.mark.parametrize(
    "bad_finnkode",
    # NOTE: a literal "/" (extra path segment) or "?" (query-string separator)
    # never reaches this route/handler at all -- Starlette's default path
    # converter excludes "/", and "?" splits the URL before path matching --
    # so those aren't exercised here; this covers charset rejection for
    # finnkodes that DO land as a single path segment.
    ["bad kode", "æøå"],
)
def test_put_annotation_bad_finnkode_400(client, bad_finnkode):
    resp = client.put(
        f"/api/annotations/{bad_finnkode}", json={"kommentar": "x", "tag": None}
    )
    assert resp.status_code == 400
    assert "detail" in resp.json()


def test_get_annotation_bad_finnkode_400(client):
    resp = client.get("/api/annotations/bad kode")
    assert resp.status_code == 400
    assert "detail" in resp.json()


def test_put_annotation_missing_body_field_422(client):
    """Both `kommentar` and `tag` are required keys in the body (nullable,
    not omittable) -- an incomplete body is a validation error, not a 400
    from our own finnkode check."""
    resp = client.put("/api/annotations/446", json={"kommentar": "x"})
    assert resp.status_code == 422


# --- interplay lock: web edits vs. the sheet-import protection contract ----

def test_web_created_annotation_survives_subsequent_sheet_import(db_path, client):
    """Create via web PUT, then run the REAL import_sheet_annotations with a
    fake client offering a DIFFERENT kommentar for the same finnkode. The web
    value must survive -- import protection holds because the web PUT bumped
    updated_at without touching imported_at (NULL), so
    `updated_at IS NULL OR updated_at = imported_at` is false."""
    resp = client.put(
        "/api/annotations/12345678", json={"kommentar": "web value", "tag": "W"}
    )
    assert resp.status_code == 200

    conn = _conn(db_path)
    sheet_rows = [
        ["Finnkode", "Kommentar", "Tag"],
        ["12345678", "sheet says something else", "S"],
    ]
    fake_client = _FakeSheetsClient(sheet_rows)
    result = import_sheet_annotations(conn, fake_client, tab="Eie")
    assert result["skipped"] == 1
    assert result["inserted"] == 0
    assert result["updated"] == 0

    row = _annotation_row(conn, "12345678")
    conn.close()
    assert row["kommentar"] == "web value"
    assert row["tag"] == "W"

    resp = client.get("/api/annotations/12345678")
    assert resp.json() == {"finnkode": "12345678", "kommentar": "web value", "tag": "W"}


def test_imported_then_web_edited_row_survives_reimport(db_path, client):
    """Import creates a row (via the real import path), then a web PUT edits
    it -- web wins immediately and a LATER re-import (even with yet another
    differing sheet value) still doesn't clobber it."""
    conn = _conn(db_path)
    sheet_rows = [
        ["Finnkode", "Kommentar", "Tag"],
        ["87654321", "original sheet comment", "O"],
    ]
    fake_client = _FakeSheetsClient(sheet_rows)
    result = import_sheet_annotations(conn, fake_client, tab="Eie")
    assert result["inserted"] == 1
    conn.close()

    resp = client.put(
        "/api/annotations/87654321", json={"kommentar": "web override", "tag": "X"}
    )
    assert resp.status_code == 200

    conn = _conn(db_path)
    row = _annotation_row(conn, "87654321")
    assert row["kommentar"] == "web override"
    assert row["updated_at"] != row["imported_at"]

    # Re-import with yet another differing sheet value -- must still be a no-op.
    sheet_rows_2 = [
        ["Finnkode", "Kommentar", "Tag"],
        ["87654321", "sheet changed again", "Z"],
    ]
    fake_client_2 = _FakeSheetsClient(sheet_rows_2)
    result_2 = import_sheet_annotations(conn, fake_client_2, tab="Eie")
    assert result_2["skipped"] == 1
    assert result_2["updated"] == 0

    row_after = _annotation_row(conn, "87654321")
    conn.close()
    assert row_after["kommentar"] == "web override"
    assert row_after["tag"] == "X"


def test_delete_of_imported_row_tombstones_and_blocks_reimport_of_old_value(
    db_path, client
):
    """TOMBSTONE DECISION regression: an import-created row (`imported_at
    IS NOT NULL`) that a web user then clears (both-null PUT) must NOT be
    physically deleted -- a later re-import offering the SAME old sheet
    value would otherwise resurrect it (an absent row looks identical to a
    never-imported one, so the import's protection WHERE clause has nothing
    to block on). Uses the REAL import_sheet_annotations with a fake client,
    same as the interplay-lock tests above."""
    conn = _conn(db_path)
    sheet_rows = [
        ["Finnkode", "Kommentar", "Tag"],
        ["55555555", "old sheet value", "S"],
    ]
    fake_client = _FakeSheetsClient(sheet_rows)
    result = import_sheet_annotations(conn, fake_client, tab="Eie")
    assert result["inserted"] == 1
    conn.close()

    # Web user clears both fields.
    resp = client.put(
        "/api/annotations/55555555", json={"kommentar": None, "tag": None}
    )
    assert resp.status_code == 200
    assert resp.json() == {"finnkode": "55555555", "kommentar": None, "tag": None}

    conn = _conn(db_path)
    row = _annotation_row(conn, "55555555")
    conn.close()
    # Row SURVIVES as a tombstone -- not physically deleted.
    assert row is not None
    assert row["kommentar"] is None
    assert row["tag"] is None
    assert row["imported_at"] is not None  # untouched
    assert row["updated_at"] != row["imported_at"]  # bumped past it, permanently

    # GET still reads as absent-like nulls even though the row exists.
    resp = client.get("/api/annotations/55555555")
    assert resp.json() == {"finnkode": "55555555", "kommentar": None, "tag": None}

    # A later re-import offering the SAME old sheet value must not resurrect it.
    conn = _conn(db_path)
    fake_client_2 = _FakeSheetsClient(sheet_rows)
    result_2 = import_sheet_annotations(conn, fake_client_2, tab="Eie")
    assert result_2["skipped"] == 1
    assert result_2["updated"] == 0
    row_after = _annotation_row(conn, "55555555")
    conn.close()
    assert row_after["kommentar"] is None
    assert row_after["tag"] is None


# ---------------------------------------------------------------------------
# /api/listings + /api/listings/{finnkode} -- DNB annotations wiring (Fix 2)
# ---------------------------------------------------------------------------

def test_put_annotation_on_dnb_id_surfaces_in_listings_and_detail(db_path, client):
    """A PUT against a `dnb:...` synthetic id was previously accepted and
    stored, but `_dnb_item` hardcoded kommentar/tag to None (the DNB listing
    query has no annotations join) -- so the value never surfaced anywhere.
    Both `/api/listings`'s DNB item and the detail endpoint must reflect it."""
    conn = _conn(db_path)
    _ins_dnb(conn, "https://dnb.no/annotate-me", brj=1)
    conn.close()

    listings = client.get("/api/listings").json()["listings"]
    dnb_item = next(i for i in listings if i["source"] == "dnb")
    finnkode = dnb_item["finnkode"]
    assert finnkode.startswith("dnb:")
    assert dnb_item["kommentar"] is None
    assert dnb_item["tag"] is None

    resp = client.put(
        f"/api/annotations/{finnkode}", json={"kommentar": "DNB note", "tag": "D"}
    )
    assert resp.status_code == 200

    listings = client.get("/api/listings").json()["listings"]
    dnb_item = next(i for i in listings if i["source"] == "dnb")
    assert dnb_item["kommentar"] == "DNB note"
    assert dnb_item["tag"] == "D"

    detail = client.get(f"/api/listings/{finnkode}").json()
    assert detail["source"] == "dnb"
    assert detail["kommentar"] == "DNB note"
    assert detail["tag"] == "D"
