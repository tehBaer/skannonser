"""Both derived-cache repos write an explicit column tuple; nothing keeps it in
sync with the pydantic model it persists.

`DetailsRepo.upsert_details` and `SalgsoppgaveRepo.upsert` both build their
INSERT from a hand-maintained `_SCALAR_COLS`, reading each name out of
`model_dump()`. Add a field to the model and forget the tuple and the field is
simply never written -- no exception, no warning, just a column that is NULL
forever while the parser cheerfully fills it. That is exactly the failure mode
migration 015 nearly shipped: `eiendomsskatt_kr` and `verditakst` were added to
`ListingDetails` and to `_PRICING_LABELS`, and would have been silently dropped
had `_SCALAR_COLS` not been updated in the same commit.

These tests are the guard. They compare the two sides directly, so the omission
fails at test time rather than months later when someone notices an empty
column.
"""
import sqlite3

import pytest

from skannonser.ingest.finn.parse_details import ListingDetails
from skannonser.ingest.finn.parse_salgsoppgave import Salgsoppgave
from skannonser.store import connection, migrations
from skannonser.store.repositories.details import _SCALAR_COLS as DETAILS_COLS
from skannonser.store.repositories.salgsoppgave import (
    _SCALAR_COLS as SALGSOPPGAVE_COLS,
)

# `finnkode` is the primary key, passed positionally rather than through the
# scalar tuple; `facilities` is a list persisted to its own table.
_DETAILS_NON_SCALAR = {"finnkode", "facilities"}
_SALGSOPPGAVE_NON_SCALAR = {"finnkode"}


@pytest.mark.parametrize(
    "model, cols, non_scalar, repo_name",
    [
        (ListingDetails, DETAILS_COLS, _DETAILS_NON_SCALAR, "DetailsRepo"),
        (Salgsoppgave, SALGSOPPGAVE_COLS, _SALGSOPPGAVE_NON_SCALAR, "SalgsoppgaveRepo"),
    ],
)
def test_scalar_cols_matches_its_model(model, cols, non_scalar, repo_name):
    expected = set(model.model_fields) - non_scalar
    actual = set(cols)
    missing = expected - actual
    extra = actual - expected
    assert not missing, (
        f"{repo_name}._SCALAR_COLS is missing {sorted(missing)} -- those fields "
        f"parse fine but are never written to the DB"
    )
    assert not extra, (
        f"{repo_name}._SCALAR_COLS names {sorted(extra)}, which is not a field "
        f"on {model.__name__}; the INSERT would raise a KeyError"
    )


@pytest.mark.parametrize(
    "cols, table",
    [
        (DETAILS_COLS, "listing_details"),
        (SALGSOPPGAVE_COLS, "listing_salgsoppgave"),
    ],
)
def test_scalar_cols_all_exist_as_db_columns(tmp_path, cols, table):
    """A name can match the model and still not exist in the schema -- e.g. a
    field added to the model and the tuple, with the migration forgotten."""
    conn = connection.connect(tmp_path / "sync.db")
    migrations.migrate(conn)
    db_cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    missing = set(cols) - db_cols
    assert not missing, f"{table} has no column(s) {sorted(missing)}"


def test_migration_015_alter_preserves_existing_listing_details_rows(tmp_path):
    """`ALTER TABLE ADD COLUMN` against a POPULATED table -- the realistic
    upgrade path, and the one the live server actually took. Mirrors migration
    011's precedent test. Nullable ADD COLUMN is metadata-only on SQLite, so
    this is cheap insurance rather than a live risk."""
    conn = connection.connect(tmp_path / "populated.db")
    ran = migrations.migrate(conn)
    assert "015_salgsoppgave" in ran

    # Rewind just 015, the way migration 011's precedent test does: rebuild
    # listing_details without the two columns 015 adds, and drop 015 from the
    # record so `migrate` runs it again -- this time against a populated table.
    conn.execute("DELETE FROM schema_migrations WHERE id = '015_salgsoppgave'")
    pre015 = [
        c[1]
        for c in conn.execute("PRAGMA table_info(listing_details)")
        if c[1] not in ("eiendomsskatt_kr", "verditakst")
    ]
    cols = ", ".join(pre015)
    conn.executescript(
        f"CREATE TABLE ld_pre015 AS SELECT {cols} FROM listing_details;"
        "DROP TABLE listing_details;"
        "ALTER TABLE ld_pre015 RENAME TO listing_details;"
    )
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('1', 'u')")
    conn.execute(
        "INSERT INTO listing_details (finnkode, totalpris) VALUES ('1', 4944646)"
    )
    conn.commit()

    migrations.migrate(conn)  # re-runs 015 against the populated table

    row = conn.execute(
        "SELECT totalpris, eiendomsskatt_kr, verditakst FROM listing_details "
        "WHERE finnkode = '1'"
    ).fetchone()
    assert row["totalpris"] == 4944646, "the pre-existing row must survive"
    assert row["eiendomsskatt_kr"] is None
    assert row["verditakst"] is None
