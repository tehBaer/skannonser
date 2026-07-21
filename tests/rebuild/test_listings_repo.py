import sqlite3

import pytest

from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo


@pytest.fixture()
def repo(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    return ListingsRepo(conn)


def _listing(finnkode: str, **kw) -> NormalizedListing:
    return NormalizedListing(
        Finnkode=finnkode,
        URL=f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
        **kw,
    )


def test_upsert_inserts_then_updates(repo):
    r1 = repo.upsert([_listing("111")])
    assert r1 == {"inserted": 1, "updated": 0, "excluded": 0}
    r2 = repo.upsert([_listing("111")])
    assert r2["inserted"] == 0


def test_insert_active_on_first_appearance(repo):
    # User mandate 2026-07-20 (STATUS backlog #1, landed with phase-4 cutover):
    # listings are active from FIRST appearance - same-day export/notify.
    repo.upsert([_listing("111")])
    row = repo.conn.execute(
        "SELECT active FROM eiendom WHERE finnkode = '111'"
    ).fetchone()
    assert row["active"] == 1
    assert repo.active_finnkodes() == {"111"}


def test_upsert_updates_only_changed_and_reactivates(repo):
    repo.upsert([_listing("111", Adresse="Gata 1", Pris=100)])
    # A genuine change -> counted as updated.
    r = repo.upsert([_listing("111", Adresse="Gata 2", Pris=100)])
    assert r["updated"] == 1
    row = repo.conn.execute(
        "SELECT adresse, pris FROM eiendom WHERE finnkode = '111'"
    ).fetchone()
    assert row["adresse"] == "Gata 2"
    assert row["pris"] == 100

    # No change -> not counted as updated.
    r2 = repo.upsert([_listing("111", Adresse="Gata 2", Pris=100)])
    assert r2["updated"] == 0

    # Deactivate, then re-appearance (even unchanged) reactivates.
    repo.mark_inactive([])
    assert repo.active_finnkodes() == set()
    r3 = repo.upsert([_listing("111", Adresse="Gata 2", Pris=100)])
    assert r3["updated"] == 1
    assert repo.active_finnkodes() == {"111"}


def test_excluded_urls_are_skipped_and_counted(repo):
    bad = NormalizedListing(
        Finnkode="999",
        URL="https://www.finn.no/realestate/newbuildings/ad.html?finnkode=999",
    )
    r = repo.upsert([bad, _listing("111")])
    assert r == {"inserted": 1, "updated": 0, "excluded": 1}
    # "111" is active from this first upsert; the excluded listing is
    # skipped and never persisted.
    assert repo.active_finnkodes() == {"111"}

    # Re-scan: "111" is unchanged (no update fires), the excluded listing is
    # still skipped both times and never persisted.
    r2 = repo.upsert([bad, _listing("111")])
    assert r2 == {"inserted": 0, "updated": 0, "excluded": 1}
    assert repo.active_finnkodes() == {"111"}


def test_mark_inactive_deactivates_missing_never_deletes(repo):
    repo.upsert([_listing("111"), _listing("222")])  # both active on first upsert
    n = repo.mark_inactive(["111"])
    assert n == 1
    assert repo.active_finnkodes() == {"111"}
    total = repo.conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0]
    assert total == 2  # 222 deactivated, not deleted


def test_mark_inactive_empty_list_deactivates_all(repo):
    repo.upsert([_listing("111"), _listing("222")])  # both active on first upsert
    n = repo.mark_inactive([])
    assert n == 2
    assert repo.active_finnkodes() == set()


def test_overrides_applied_at_upsert(repo):
    repo.conn.execute(
        "INSERT INTO manual_overrides (finnkode, pris, adresse, postnummer, override_reason) "
        "VALUES ('111', 555, 'Override St 9', '0555', 'test')"
    )
    repo.conn.commit()
    repo.upsert([_listing("111", Adresse="Original", Pris=100, Postnummer="1234")])
    row = repo.conn.execute(
        "SELECT pris, adresse, postnummer FROM eiendom WHERE finnkode = '111'"
    ).fetchone()
    assert row["pris"] == 555
    assert row["adresse"] == "Override St 9"
    assert row["postnummer"] == "0555"


def test_upsert_is_one_transaction(repo, monkeypatch):
    # The brief's original seam (monkeypatching ``conn.execute``) is not
    # settable — ``sqlite3.Connection.execute`` is a read-only attribute — so
    # per the brief we use the robust variant: an error while the SECOND
    # listing is being written must roll back the FIRST. We inject the error at
    # a real repo seam that runs per-listing.
    #
    # Reviewed decision: this monkeypatches an internal seam
    # (``repo._apply_overrides``) rather than provoking a genuine SQL
    # failure. That's deliberate, not a shortcut — no genuine SQL-constraint
    # failure is reachable through the public ``upsert()`` path here, because
    # within a single batch/connection, uncommitted reads of rows already
    # written earlier in the same transaction turn intra-batch duplicate
    # finnkodes into UPDATEs (via the ``existing = conn.execute("SELECT * ...")``
    # lookup), not INSERT/PK-constraint failures. So the only way to exercise
    # "an error mid-batch rolls back the whole batch" is to inject a failure
    # at an internal seam that runs per-listing, as done below. A future
    # reader should not treat this as a shortcut that needs upgrading to a
    # "real" SQL error case — there isn't one to provoke here.
    listings = [_listing("111"), _listing("BAD")]
    real_apply = repo._apply_overrides

    def flaky(finnkode, data):
        if finnkode == "BAD":
            raise sqlite3.OperationalError("boom")
        return real_apply(finnkode, data)

    monkeypatch.setattr(repo, "_apply_overrides", flaky)
    with pytest.raises(sqlite3.OperationalError):
        repo.upsert(listings)
    monkeypatch.undo()
    # "111" was inserted before "BAD" failed; the whole batch rolled back.
    assert repo.conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0] == 0
