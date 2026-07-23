"""DetailsRepo: full-row REPLACE semantics for the derived listing_details /
listing_facilities cache (2026-07-23 design spec)."""
import pytest

from skannonser.ingest.finn.parse_details import ListingDetails
from skannonser.store import connection, migrations
from skannonser.store.repositories.details import DetailsRepo


@pytest.fixture()
def repo(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    return DetailsRepo(conn)


def _details(finnkode="111", **kw) -> ListingDetails:
    return ListingDetails(finnkode=finnkode, **kw)


def test_migration_created_tables(repo):
    names = {
        r[0]
        for r in repo.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"listing_details", "listing_facilities"} <= names


def test_upsert_inserts_scalar_row_and_facilities(repo):
    repo.conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('111', 'u1')")
    repo.conn.commit()
    d = _details(totalpris=4944646, felleskost_mnd=13813, facilities=["Heis", "Peis/Ildsted"])
    assert repo.upsert_details([d]) == {"upserted": 1}
    row = repo.conn.execute(
        "SELECT totalpris, felleskost_mnd, parsed_at FROM listing_details WHERE finnkode='111'"
    ).fetchone()
    assert row["totalpris"] == 4944646
    assert row["parsed_at"]  # stamped
    facs = [
        r["facility"]
        for r in repo.conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode='111' ORDER BY facility"
        )
    ]
    assert facs == ["Heis", "Peis/Ildsted"]


def test_upsert_is_full_row_replace(repo):
    repo.conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('111', 'u1')")
    repo.conn.commit()
    repo.upsert_details([_details(totalpris=100, fellesgjeld=50, facilities=["Heis"])])
    # Re-parse now lacks fellesgjeld and has different facilities: the old
    # values must be GONE (derived cache -- no fill-only), not preserved.
    repo.upsert_details([_details(totalpris=200, facilities=["Garasje/P-plass"])])
    row = repo.conn.execute(
        "SELECT totalpris, fellesgjeld FROM listing_details WHERE finnkode='111'"
    ).fetchone()
    assert row["totalpris"] == 200
    assert row["fellesgjeld"] is None
    facs = [
        r["facility"]
        for r in repo.conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode='111'"
        )
    ]
    assert facs == ["Garasje/P-plass"]


def test_upsert_empty_list_is_noop(repo):
    assert repo.upsert_details([]) == {"upserted": 0}


def test_wipe(repo):
    repo.conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('111', 'u1')")
    repo.conn.commit()
    repo.upsert_details([_details(facilities=["Heis"])])
    repo.wipe()
    assert repo.conn.execute("SELECT COUNT(*) FROM listing_details").fetchone()[0] == 0
    assert repo.conn.execute("SELECT COUNT(*) FROM listing_facilities").fetchone()[0] == 0


def test_coverage(repo):
    repo.conn.execute(
        "INSERT INTO eiendom (finnkode, url) VALUES ('111', 'u1'), ('222', 'u2')"
    )
    repo.conn.commit()
    repo.upsert_details(
        [_details("111", totalpris=100, felleskost_mnd=10, facilities=["Heis"])]
    )
    cov = repo.coverage()
    assert cov == {
        "eiendom_rows": 2,
        "details_rows": 1,
        "with_totalpris": 1,
        "with_felleskost": 1,
        "facilities_rows": 1,
    }
