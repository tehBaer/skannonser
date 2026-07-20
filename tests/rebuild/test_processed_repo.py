import math

import pytest

from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import (
    ProcessedRepo,
    clean_address,
    google_maps_url,
    normalize_coordinates,
)


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def listings(conn):
    return ListingsRepo(conn)


@pytest.fixture()
def repo(conn):
    return ProcessedRepo(conn)


def _seed_eiendom(listings_repo, finnkode: str, *, tilgjengelighet=None, active=True):
    """Seed an ``eiendom`` row (FK target for ``eiendom_processed``).

    Legacy quirk: a finnkode only becomes ``active`` on its SECOND upsert
    appearance (see ``ListingsRepo``/``test_listings_repo.py``), so we upsert
    twice when the caller wants an active row.
    """
    listing = NormalizedListing(
        Finnkode=finnkode,
        URL=f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
        Tilgjengelighet=tilgjengelighet,
    )
    listings_repo.upsert([listing])
    if active:
        listings_repo.upsert([listing])


# -- module-level pure helpers -------------------------------------------


def test_clean_address_strips_dash_suffix():
    assert clean_address("Brynsveien 146 - Prosjekt") == "Brynsveien 146"


def test_clean_address_strips_paren_suffix():
    assert clean_address("Jarenlia 107 (Bolignr. J-02)") == "Jarenlia 107"


def test_clean_address_strips_bracket_and_slash_suffixes():
    assert clean_address("Gata 1 [ny]") == "Gata 1"
    assert clean_address("Gata 1 / Inngang B") == "Gata 1"


def test_clean_address_no_delimiter_unchanged_but_stripped():
    assert clean_address("  Storgata 5  ") == "Storgata 5"


def test_clean_address_falsy_or_nan_returned_unchanged():
    assert clean_address(None) is None
    assert clean_address("") == ""
    nan = float("nan")
    assert clean_address(nan) is nan or math.isnan(clean_address(nan))


def test_google_maps_url_normal_case():
    url = google_maps_url("Storgata 5", "0155")
    assert url == "https://www.google.com/maps/place/Storgata+5+0155"


def test_google_maps_url_empty_when_adresse_missing():
    assert google_maps_url(None, "0155") == ""
    assert google_maps_url(float("nan"), "0155") == ""


def test_google_maps_url_empty_when_postnummer_missing():
    assert google_maps_url("Storgata 5", None) == ""
    assert google_maps_url("Storgata 5", float("nan")) == ""


def test_normalize_coordinates_valid_passthrough():
    lat, lng, swapped = normalize_coordinates(59.91, 10.75)
    assert (lat, lng, swapped) == (59.91, 10.75, False)


def test_normalize_coordinates_swap_corrected():
    # lng/lat reversed (10.75 out of lat-bounds, 59.91 out of lng-bounds).
    lat, lng, swapped = normalize_coordinates(10.75, 59.91)
    assert (lat, lng, swapped) == (59.91, 10.75, True)


def test_normalize_coordinates_invalid_both_ways_returns_none():
    lat, lng, swapped = normalize_coordinates(1000.0, -1000.0)
    assert (lat, lng, swapped) == (None, None, False)


def test_normalize_coordinates_missing_value_returns_none():
    assert normalize_coordinates(None, 10.75) == (None, None, False)
    assert normalize_coordinates(59.91, None) == (None, None, False)


# -- upsert: fill-only vs unconditional columns --------------------------


def test_upsert_insert_writes_all_columns(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert(
        "111",
        "Storgata 5 - Prosjekt",
        "0155",
        lat=59.91,
        lng=10.75,
        travel={"pendl_rush_brj": 30, "pendl_rush_mvv": 40, "pendl_rush_mvv_uni_rush": 50},
        cntr={
            "pendl_morn_cntr": 5,
            "bil_morn_cntr": 6,
            "pendl_dag_cntr": 7,
            "bil_dag_cntr": 8,
        },
        travel_copy_from_finnkode="222",
    )
    row = repo.conn.execute(
        "SELECT * FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["adresse_cleaned"] == "Storgata 5"
    assert row["lat"] == 59.91
    assert row["lng"] == 10.75
    assert row["pendl_rush_brj"] == 30
    assert row["pendl_rush_mvv"] == 40
    assert row["pendl_rush_mvv_uni_rush"] == 50
    assert row["pendl_morn_cntr"] == 5
    assert row["bil_morn_cntr"] == 6
    assert row["pendl_dag_cntr"] == 7
    assert row["bil_dag_cntr"] == 8
    assert row["travel_copy_from_finnkode"] == "222"
    assert row["google_maps_url"] == "https://www.google.com/maps/place/Storgata+5+0155"


def test_upsert_update_travel_columns_are_fill_only(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5", "0155", travel={"pendl_rush_brj": 30})
    # A later write with None must NOT clobber the existing non-null value.
    repo.upsert("111", "Storgata 5", "0155", travel={"pendl_rush_brj": None})
    row = repo.conn.execute(
        "SELECT pendl_rush_brj FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["pendl_rush_brj"] == 30

    # A later write WITH a value still fills in fine.
    repo.upsert("111", "Storgata 5", "0155", travel={"pendl_rush_mvv": 40})
    repo.upsert("111", "Storgata 5", "0155", travel={"pendl_rush_mvv": 99})
    row = repo.conn.execute(
        "SELECT pendl_rush_mvv FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["pendl_rush_mvv"] == 99


def test_upsert_update_lat_lng_are_fill_only(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5", "0155", lat=59.91, lng=10.75)
    repo.upsert("111", "Storgata 5", "0155", lat=None, lng=None)
    row = repo.conn.execute(
        "SELECT lat, lng FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["lat"] == 59.91
    assert row["lng"] == 10.75


def test_upsert_update_cntr_pointer_and_maps_url_are_unconditional(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert(
        "111",
        "Storgata 5",
        "0155",
        cntr={"pendl_morn_cntr": 5},
        travel_copy_from_finnkode="222",
    )
    row = repo.conn.execute(
        "SELECT pendl_morn_cntr, travel_copy_from_finnkode, google_maps_url "
        "FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["pendl_morn_cntr"] == 5
    assert row["travel_copy_from_finnkode"] == "222"
    assert row["google_maps_url"] == "https://www.google.com/maps/place/Storgata+5+0155"

    # A later write with no cntr/pointer values DOES null them out (unconditional).
    repo.upsert("111", "Storgata 5", "0155")
    row = repo.conn.execute(
        "SELECT pendl_morn_cntr, travel_copy_from_finnkode "
        "FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["pendl_morn_cntr"] is None
    assert row["travel_copy_from_finnkode"] is None


def test_upsert_adresse_cleaned_is_unconditional(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5 - Prosjekt", "0155")
    row = repo.conn.execute(
        "SELECT adresse_cleaned FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["adresse_cleaned"] == "Storgata 5"

    # A later write with adresse=None recomputes (and clobbers) adresse_cleaned.
    repo.upsert("111", None, "0155")
    row = repo.conn.execute(
        "SELECT adresse_cleaned FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["adresse_cleaned"] is None


def test_upsert_swapped_coordinates_are_corrected(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5", "0155", lat=10.75, lng=59.91)
    row = repo.conn.execute(
        "SELECT lat, lng FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["lat"] == 59.91
    assert row["lng"] == 10.75


# -- donor_seed ------------------------------------------------------------


def test_donor_seed_key_set(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert(
        "111",
        "Storgata 5",
        "0155",
        lat=59.91,
        lng=10.75,
        travel={"pendl_rush_brj": 30, "pendl_rush_mvv": 40, "pendl_rush_mvv_uni_rush": 50},
        travel_copy_from_finnkode=None,
    )
    seed = repo.donor_seed()
    assert len(seed) == 1
    assert set(seed[0].keys()) == {
        "Finnkode",
        "LAT",
        "LNG",
        "PENDL RUSH BRJ",
        "PENDL RUSH MVV",
        "MVV UNI RUSH",
        "TRAVEL_COPY_FROM_FINNKODE",
    }
    assert seed[0]["Finnkode"] == "111"
    assert seed[0]["PENDL RUSH BRJ"] == 30


def test_donor_seed_orders_by_updated_at_desc(repo, listings):
    _seed_eiendom(listings, "111")
    _seed_eiendom(listings, "222")
    repo.upsert("111", "Storgata 5", "0155")
    repo.conn.execute(
        "UPDATE eiendom_processed SET updated_at = '2020-01-01 00:00:00' WHERE finnkode = '111'"
    )
    repo.upsert("222", "Storgata 6", "0155")
    repo.conn.execute(
        "UPDATE eiendom_processed SET updated_at = '2025-01-01 00:00:00' WHERE finnkode = '222'"
    )
    repo.conn.commit()
    seed = repo.donor_seed()
    assert [row["Finnkode"] for row in seed] == ["222", "111"]


def test_donor_seed_excludes_blank_finnkode(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5", "0155")
    # A synthetic donor row whose finnkode is whitespace-only must be
    # excluded, per legacy's ``TRIM(finnkode) != ''`` guard. Insert the
    # matching eiendom parent row directly (bypassing ListingsRepo, which
    # skips blank finnkoder) so the FK is satisfied.
    repo.conn.execute("INSERT INTO eiendom (finnkode) VALUES ('   ')")
    repo.conn.execute(
        "INSERT INTO eiendom_processed (finnkode, lat, lng) VALUES ('   ', 1.0, 1.0)"
    )
    repo.conn.commit()
    seed = repo.donor_seed()
    assert [row["Finnkode"] for row in seed] == ["111"]


# -- missing_coordinates ----------------------------------------------------


def test_missing_coordinates_includes_active_listing_without_coords(repo, listings):
    _seed_eiendom(listings, "111")
    result = repo.missing_coordinates()
    assert [r["Finnkode"] for r in result] == ["111"]


def test_missing_coordinates_excludes_when_coords_present(repo, listings):
    _seed_eiendom(listings, "111")
    repo.set_coordinates("111", 59.91, 10.75)
    assert repo.missing_coordinates() == []


def test_missing_coordinates_excludes_geocode_failed(repo, listings):
    _seed_eiendom(listings, "111")
    repo.mark_geocode_failed("111")
    assert repo.missing_coordinates() == []


def test_missing_coordinates_excludes_solgt_and_inaktiv_by_default(repo, listings):
    _seed_eiendom(listings, "111", tilgjengelighet="Solgt")
    _seed_eiendom(listings, "222", tilgjengelighet="Inaktiv")
    _seed_eiendom(listings, "333", tilgjengelighet="Aktiv")
    result = repo.missing_coordinates()
    assert [r["Finnkode"] for r in result] == ["333"]


def test_missing_coordinates_excludes_inactive_by_default(repo, listings):
    _seed_eiendom(listings, "111", active=False)
    assert repo.missing_coordinates() == []


def test_missing_coordinates_include_inactive_surfaces_everything(repo, listings):
    _seed_eiendom(listings, "111", active=False)
    _seed_eiendom(listings, "222", tilgjengelighet="Solgt")
    result = repo.missing_coordinates(include_inactive=True)
    assert {r["Finnkode"] for r in result} == {"111", "222"}


def test_missing_coordinates_include_inactive_still_excludes_geocode_failed(repo, listings):
    _seed_eiendom(listings, "111", active=False)
    repo.mark_geocode_failed("111")
    assert repo.missing_coordinates(include_inactive=True) == []


# -- set_coordinates / mark & clear geocode_failed --------------------------


def test_set_coordinates_creates_row_and_returns_true(repo, listings):
    _seed_eiendom(listings, "111")
    assert repo.set_coordinates("111", 59.91, 10.75) is True
    row = repo.conn.execute(
        "SELECT lat, lng, geocode_failed FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert (row["lat"], row["lng"], row["geocode_failed"]) == (59.91, 10.75, 0)


def test_set_coordinates_rejects_invalid_and_writes_nothing(repo, listings):
    _seed_eiendom(listings, "111")
    assert repo.set_coordinates("111", 1000.0, -1000.0) is False
    row = repo.conn.execute(
        "SELECT * FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row is None


def test_set_coordinates_clears_geocode_failed(repo, listings):
    _seed_eiendom(listings, "111")
    repo.mark_geocode_failed("111")
    repo.set_coordinates("111", 59.91, 10.75)
    row = repo.conn.execute(
        "SELECT geocode_failed FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["geocode_failed"] == 0


def test_mark_and_clear_geocode_failed(repo, listings):
    _seed_eiendom(listings, "111")
    repo.mark_geocode_failed("111")
    row = repo.conn.execute(
        "SELECT geocode_failed FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["geocode_failed"] == 1

    repo.clear_geocode_failed("111")
    row = repo.conn.execute(
        "SELECT geocode_failed FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row["geocode_failed"] == 0


def test_mark_geocode_failed_creates_row_if_missing(repo, listings):
    _seed_eiendom(listings, "111")
    repo.mark_geocode_failed("111")
    row = repo.conn.execute(
        "SELECT geocode_failed FROM eiendom_processed WHERE finnkode = '111'"
    ).fetchone()
    assert row is not None
    assert row["geocode_failed"] == 1


# -- sheet_travel_values -----------------------------------------------------


def test_sheet_travel_values_own_when_no_donor(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert("111", "Storgata 5", "0155", travel={"pendl_rush_brj": 30})
    values = repo.sheet_travel_values("111")
    assert values == {"PENDL RUSH BRJ": 30, "PENDL RUSH MVV": None, "MVV UNI RUSH": None}


def test_sheet_travel_values_donor_overrides_own(repo, listings):
    _seed_eiendom(listings, "111")
    _seed_eiendom(listings, "222")
    repo.upsert("222", "Donor Gata 1", "0155", travel={"pendl_rush_brj": 99})
    repo.upsert(
        "111",
        "Storgata 5",
        "0155",
        travel={"pendl_rush_brj": 30},
        travel_copy_from_finnkode="222",
    )
    values = repo.sheet_travel_values("111")
    assert values["PENDL RUSH BRJ"] == 99


def test_sheet_travel_values_donor_null_falls_back_to_own(repo, listings):
    _seed_eiendom(listings, "111")
    _seed_eiendom(listings, "222")
    # Donor row exists but has no pendl_rush_mvv value.
    repo.upsert("222", "Donor Gata 1", "0155", travel={"pendl_rush_brj": 99})
    repo.upsert(
        "111",
        "Storgata 5",
        "0155",
        travel={"pendl_rush_brj": 30, "pendl_rush_mvv": 40},
        travel_copy_from_finnkode="222",
    )
    values = repo.sheet_travel_values("111")
    assert values["PENDL RUSH BRJ"] == 99  # donor has a value -> donor wins
    assert values["PENDL RUSH MVV"] == 40  # donor value is null -> own wins


def test_sheet_travel_values_dangling_pointer_falls_back_to_own(repo, listings):
    _seed_eiendom(listings, "111")
    repo.upsert(
        "111",
        "Storgata 5",
        "0155",
        travel={"pendl_rush_brj": 30},
        travel_copy_from_finnkode="999",  # no such donor row
    )
    values = repo.sheet_travel_values("111")
    assert values["PENDL RUSH BRJ"] == 30


def test_sheet_travel_values_unknown_finnkode_returns_all_none(repo, listings):
    values = repo.sheet_travel_values("does-not-exist")
    assert values == {"PENDL RUSH BRJ": None, "PENDL RUSH MVV": None, "MVV UNI RUSH": None}
