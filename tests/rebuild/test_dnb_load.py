import pytest

from skannonser.config.domain import load_domain
from skannonser.ingest.base import NormalizedListing
from skannonser.ingest.dnb import load
from skannonser.store import connection, migrations
from skannonser.store.repositories.dnb import DnbRepo
from skannonser.store.repositories.listings import ListingsRepo


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def domain():
    return load_domain()


def _dnb_row(url: str, **kw) -> dict:
    """A dict shaped like ``skannonser.ingest.dnb.parse.parse_listing``'s output."""
    row = {
        "URL": url,
        "Title": "Pen leilighet",
        "Description": None,
        "IMAGE_URL": None,
        "StreetAddress": "Storgata 1",
        "Locality": "Oslo",
        "Region": "Oslo",
        "PostalCode": "0155",
        "PropertyType": "Leilighet",
        "Latitude": 59.9139,
        "Longitude": 10.7522,
        "FloorSize": 60,
        "NumberOfRooms": 2,
        "NumberOfBedrooms": 1,
        "Price": 5000000,
    }
    row.update(kw)
    return row


def test_polygon_filter_drops_outside_rows(conn, domain):
    inside = _dnb_row("https://dnbeiendom.no/bolig/inside")
    outside = _dnb_row(
        "https://dnbeiendom.no/bolig/outside", Latitude=58.0, Longitude=3.0
    )
    kept = load.filter_and_match([inside, outside], domain, conn)
    urls = {row["URL"] for row in kept}
    assert urls == {"https://dnbeiendom.no/bolig/inside"}


def test_finn_match_sets_duplicate_of_finnkode(conn, domain):
    # Seed one eiendom row via ListingsRepo with a known address+postcode,
    # then force it inactive via direct SQL (listings now activate on FIRST
    # appearance -- user mandate 2026-07-20 -- so a plain upsert alone would
    # no longer produce an inactive row here). The inactive state is
    # deliberately used to prove the FINN matcher does NOT restrict to
    # active eiendom rows (legacy's CSV-based matcher loaded the whole FINN
    # dataset with no active/inactive filtering at all).
    listings_repo = ListingsRepo(conn)
    listings_repo.upsert(
        [
            NormalizedListing(
                Finnkode="123456",
                URL="https://www.finn.no/realestate/ad.html?finnkode=123456",
                Adresse="Storgata 1",
                Postnummer="0155",
            )
        ]
    )
    conn.execute("UPDATE eiendom SET active = 0 WHERE finnkode = '123456'")
    assert listings_repo.active_finnkodes() == set()  # still inactive

    dnb_row = _dnb_row(
        "https://dnbeiendom.no/bolig/match",
        StreetAddress="Storgata 1",
        PostalCode="0155",
    )
    matched = load.filter_and_match([dnb_row], domain, conn)
    assert len(matched) == 1
    assert matched[0]["duplicate_of_finnkode"] == "123456"


def test_finn_match_leaves_duplicate_of_finnkode_none_when_no_match(conn, domain):
    dnb_row = _dnb_row(
        "https://dnbeiendom.no/bolig/nomatch",
        StreetAddress="Et sted som ikke finnes 99",
        PostalCode="9999",
    )
    matched = load.filter_and_match([dnb_row], domain, conn)
    assert len(matched) == 1
    assert matched[0]["duplicate_of_finnkode"] is None


def test_upsert_inserts_then_updates(conn):
    repo = DnbRepo(conn)
    row = _dnb_row("https://dnbeiendom.no/bolig/1")
    r1 = repo.upsert([row])
    assert r1 == {"inserted": 1, "updated": 0}
    r2 = repo.upsert([row])
    assert r2 == {"inserted": 0, "updated": 1}


def test_insert_inactive_until_second_appearance_live_schema_quirk(conn):
    # The live-migrated dnbeiendom.active column has no schema default (see
    # skannonser/store/migrations/001_adopt_live_schema.sql), so a fresh
    # INSERT (which never mentions `active`) leaves it NULL/falsy -- only the
    # UPDATE branch (hit on a row's second appearance) hard-sets active = 1.
    # This mirrors the eiendom "activate on second appearance" quirk.
    repo = DnbRepo(conn)
    row = _dnb_row("https://dnbeiendom.no/bolig/1")
    repo.upsert([row])
    active = conn.execute(
        "SELECT active FROM dnbeiendom WHERE url = ?", (row["URL"],)
    ).fetchone()["active"]
    assert not active

    repo.upsert([row])
    active = conn.execute(
        "SELECT active FROM dnbeiendom WHERE url = ?", (row["URL"],)
    ).fetchone()["active"]
    assert active == 1


def test_update_coalesce_preserves_old_but_overwrites_addr_pc_pris(conn):
    # Legacy UPDATE semantics (db.py:1605-1613): dnb_id/lat/lng/
    # duplicate_of_finnkode/property_type COALESCE-preserve the old value
    # when the new one is None; adresse/postnummer/pris are unconditionally
    # overwritten even with falsy values.
    repo = DnbRepo(conn)
    first = _dnb_row(
        "https://dnbeiendom.no/bolig/1",
        Latitude=59.9139,
        Longitude=10.7522,
        PropertyType="Leilighet",
        Price=5000000,
    )
    first["duplicate_of_finnkode"] = None
    repo.upsert([first])

    second = _dnb_row(
        "https://dnbeiendom.no/bolig/1",
        Latitude=None,
        Longitude=None,
        PropertyType=None,
        StreetAddress=None,
        PostalCode=None,
        Price=None,
    )
    repo.upsert([second])

    row = conn.execute(
        "SELECT * FROM dnbeiendom WHERE url = 'https://dnbeiendom.no/bolig/1'"
    ).fetchone()
    # COALESCE-preserved:
    assert row["lat"] == 59.9139
    assert row["lng"] == 10.7522
    assert row["property_type"] == "Leilighet"
    # Unconditionally overwritten (falsy new values win):
    assert row["adresse"] == ""
    assert row["postnummer"] == ""
    assert row["pris"] is None


def test_upsert_matches_by_dnb_id_when_url_lookup_misses(conn):
    # Legacy falls back to a dnb_id lookup only when the url lookup found
    # nothing (db.py:1595-1602); the UPDATE never touches the url column, so
    # a row matched via dnb_id keeps its stored url (here: NULL).
    repo = DnbRepo(conn)
    id_only = _dnb_row(None, dnb_id="X-1")
    r1 = repo.upsert([id_only])
    assert r1 == {"inserted": 1, "updated": 0}

    with_url = _dnb_row("https://dnbeiendom.no/bolig/late-url", dnb_id="X-1")
    r2 = repo.upsert([with_url])
    assert r2 == {"inserted": 0, "updated": 1}

    rows = conn.execute("SELECT url, dnb_id FROM dnbeiendom").fetchall()
    assert len(rows) == 1
    assert rows[0]["dnb_id"] == "X-1"
    assert rows[0]["url"] is None  # legacy UPDATE never sets url


def test_deactivate_missing_skips_null_url_rows(conn):
    # Legacy's ``if r[1] and ...`` guard: active rows with a NULL/empty url
    # are never deactivated (filter_and_load_dnbeiendom_no_buffer.py:124-127).
    repo = DnbRepo(conn)
    id_only = _dnb_row(None, dnb_id="X-1")
    repo.upsert([id_only])
    repo.upsert([id_only])  # second appearance activates
    active = conn.execute(
        "SELECT active FROM dnbeiendom WHERE dnb_id = 'X-1'"
    ).fetchone()["active"]
    assert active == 1

    n = repo.deactivate_missing([])
    assert n == 0
    active = conn.execute(
        "SELECT active FROM dnbeiendom WHERE dnb_id = 'X-1'"
    ).fetchone()["active"]
    assert active == 1  # still active, exactly as legacy


def test_deactivate_missing_never_deletes(conn):
    repo = DnbRepo(conn)
    rows = [
        _dnb_row("https://dnbeiendom.no/bolig/a"),
        _dnb_row("https://dnbeiendom.no/bolig/b", StreetAddress="Annen gate 2"),
    ]
    repo.upsert(rows)
    # Second appearance activates both under the live-schema quirk.
    repo.upsert(rows)

    n = repo.deactivate_missing(["https://dnbeiendom.no/bolig/a"])
    assert n == 1

    total = conn.execute("SELECT COUNT(*) FROM dnbeiendom").fetchone()[0]
    assert total == 2  # deactivated, not deleted

    active_urls = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active_urls == {"https://dnbeiendom.no/bolig/a"}


def test_deactivate_missing_empty_list_deactivates_all(conn):
    repo = DnbRepo(conn)
    rows = [_dnb_row("https://dnbeiendom.no/bolig/a")]
    repo.upsert(rows)
    repo.upsert(rows)
    n = repo.deactivate_missing([])
    assert n == 1
    active_urls = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active_urls == set()
