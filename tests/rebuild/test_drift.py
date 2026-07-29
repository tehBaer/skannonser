"""Parser-drift canary: field-presence rates for a parsed batch vs stored rows."""
import pytest

from skannonser.ingest.drift import (
    BASELINE_ROWS,
    MIN_BATCH,
    DriftFinding,
    batch_rates,
    compare,
    is_present,
)


# --- presence ---------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", []])
def test_absent_values(value):
    assert is_present(value) is False


@pytest.mark.parametrize("value", [0, "0", "100", "Selveier", ["Balkong"], 0.0])
def test_present_values(value):
    """0 is a parsed value, not a missing one -- a naive truthiness check
    would drop `bedrooms=0` and understate the rate."""
    assert is_present(value) is True


# --- batch rates ------------------------------------------------------------


def test_batch_rates_keys_by_column_name():
    rows = [{"Primærrom": "100"}, {"Primærrom": ""}, {"Primærrom": "73"}]
    rates = batch_rates(rows, {"Primærrom": "info_primary_area"})
    assert rates == {"info_primary_area": pytest.approx(2 / 3)}


def test_batch_rates_empty_batch_is_zero_not_error():
    assert batch_rates([], {"Primærrom": "info_primary_area"}) == {
        "info_primary_area": 0.0
    }


# --- compare ----------------------------------------------------------------


def _cmp(batch, baseline, n=1000, rows=5000):
    return compare(batch, baseline, n, rows)


def test_collapse_is_reported():
    """The P-ROM shape: a healthy 19% field reading 0% tonight."""
    found = _cmp({"info_primary_area": 0.0}, {"info_primary_area": 0.19})
    assert found == [
        DriftFinding(
            field="info_primary_area",
            baseline_rate=0.19,
            batch_rate=0.0,
            sample_size=1000,
        )
    ]


def test_small_batch_is_skipped():
    """A quiet night must not alert -- 0/40 against a 19% baseline is noise."""
    assert _cmp({"info_primary_area": 0.0}, {"info_primary_area": 0.19}, n=40) == []


def test_small_baseline_table_is_skipped():
    """A freshly wiped listing_details reads every field as 0%."""
    assert (
        _cmp(
            {"energimerke": 0.0},
            {"energimerke": 0.0},
            rows=BASELINE_ROWS - 1,
        )
        == []
    )


def test_rare_field_fluctuating_stays_quiet():
    """BRA-b is legitimately rare (7%); 7% -> 5% is not drift."""
    assert _cmp({"info_usable_b_area": 0.05}, {"info_usable_b_area": 0.07}) == []


def test_field_below_min_baseline_is_not_watched():
    assert _cmp({"info_gross_area": 0.0}, {"info_gross_area": 0.001}) == []


def test_rising_field_never_alerts():
    assert _cmp({"info_primary_area": 0.19}, {"info_primary_area": 0.0}) == []


def test_multiple_collapses_all_reported():
    found = _cmp(
        {"info_primary_area": 0.0, "energimerke": 0.011, "rooms": 0.82},
        {"info_primary_area": 0.19, "energimerke": 0.854, "rooms": 0.81},
    )
    assert sorted(f.field for f in found) == ["energimerke", "info_primary_area"]


# --- baseline + watch lists -------------------------------------------------

from skannonser.ingest.drift import (  # noqa: E402
    DETAIL_FIELD_COLUMNS,
    LISTING_FIELD_COLUMNS,
    baseline_rates,
    check,
)
from skannonser.store import connection, migrations  # noqa: E402


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def test_listing_watch_list_excludes_lifecycle_field():
    """Tilgjengelighet tracks a listing's lifecycle, not FINN's markup --
    watching it would false-alarm on every crawl of live ads."""
    assert "Tilgjengelighet" not in LISTING_FIELD_COLUMNS
    assert LISTING_FIELD_COLUMNS["Primærrom"] == "info_primary_area"
    assert LISTING_FIELD_COLUMNS["Boligtype"] == "info_property_type"


def test_detail_watch_list_is_identity_over_scalar_columns():
    assert DETAIL_FIELD_COLUMNS["energimerke"] == "energimerke"
    assert "facilities" not in DETAIL_FIELD_COLUMNS
    assert "finnkode" not in DETAIL_FIELD_COLUMNS


def test_baseline_rates_counts_all_rows(conn):
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES ('1','u',100)"
    )
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES ('2','u2',NULL)"
    )
    conn.commit()
    rates, rows = baseline_rates(conn, "eiendom", ["info_primary_area"])
    assert rows == 2
    assert rates["info_primary_area"] == pytest.approx(0.5)


def test_baseline_rates_treats_empty_string_as_absent(conn):
    """Fix 3: `eiendom`'s TEXT columns store "" for a missing value (see
    `is_present`'s docstring), but a bare `COUNT("col")` counts "" as
    present -- it only excludes NULL. That mismatch is a latent
    false-negative source for drift (a column reading 100% present here
    while the batch-side `is_present("")` correctly calls it absent)."""
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_plot_ownership) VALUES ('1','u','Selveier')"
    )
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_plot_ownership) VALUES ('2','u2','')"
    )
    conn.commit()
    rates, rows = baseline_rates(conn, "eiendom", ["info_plot_ownership"])
    assert rows == 2
    assert rates["info_plot_ownership"] == pytest.approx(0.5)


def test_baseline_rates_empty_table_no_zero_division(conn):
    rates, rows = baseline_rates(conn, "eiendom", ["info_primary_area"])
    assert rows == 0
    assert rates["info_primary_area"] == 0.0


def test_baseline_rates_missing_table_is_survivable(conn):
    """A pre-migration-010 database has no listing_details. The canary must
    stand down, not crash the ingest it is only observing."""
    conn.execute("DROP TABLE listing_details")
    conn.commit()
    rates, rows = baseline_rates(conn, "listing_details", ["energimerke"])
    assert rates == {}
    assert rows == 0


def test_check_reports_collapse_across_both_tables(conn):
    """Fix 4a: earn the test's name -- seed a trusted baseline and a
    collapsing batch for BOTH tables, not just `eiendom`, and assert both
    findings arrive (sorted by field for determinism)."""
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
        conn.execute(
            "INSERT INTO listing_details (finnkode, energimerke) VALUES (?, 'C')",
            (str(i),),
        )
    conn.commit()
    listing_rows = [{"Primærrom": ""} for _ in range(MIN_BATCH)]
    detail_rows = [{"energimerke": None} for _ in range(MIN_BATCH)]
    found, status = check(conn, listing_rows, detail_rows)
    found = sorted(found, key=lambda f: f.field)
    assert [f.field for f in found] == ["energimerke", "info_primary_area"]
    assert found[0].baseline_rate == pytest.approx(1.0)
    assert found[0].batch_rate == 0.0
    assert found[1].baseline_rate == pytest.approx(1.0)
    assert found[1].batch_rate == 0.0
    assert status == {"eiendom": "checked", "listing_details": "checked"}


def test_check_clean_run_reports_nothing(conn):
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
    conn.commit()
    listing_rows = [{"Primærrom": "100"} for _ in range(MIN_BATCH)]
    found, status = check(conn, listing_rows, [])
    assert found == []
    assert status == {"eiendom": "checked", "listing_details": "skipped:batch"}


def test_each_table_judged_on_its_own_sample_size(conn):
    """Detail parsing is best-effort -- pipeline.py swallows detail failures
    by design -- so the details batch can be smaller than the listings batch.
    Sharing one sample size would misreport both: here the listings batch
    clears MIN_BATCH and must be judged, while the details batch does not and
    must be skipped."""
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
        conn.execute(
            "INSERT INTO listing_details (finnkode, energimerke) VALUES (?, 'C')",
            (str(i),),
        )
    conn.commit()

    found, status = check(
        conn,
        [{"Primærrom": ""} for _ in range(MIN_BATCH)],       # judged
        [{"energimerke": None} for _ in range(MIN_BATCH - 1)],  # too small
    )

    assert [f.field for f in found] == ["info_primary_area"]
    assert found[0].sample_size == MIN_BATCH
    assert status == {"eiendom": "checked", "listing_details": "skipped:batch"}


# --- per-table status (Fix 2) ------------------------------------------------


def test_status_skipped_batch_when_batch_too_small(conn):
    """A batch below MIN_BATCH is reported "skipped:batch" even against a
    fully-trusted baseline -- a quiet night must not be confused with a
    healthy check."""
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
    conn.commit()
    _, status = check(conn, [{"Primærrom": "100"}], [])
    assert status["eiendom"] == "skipped:batch"


def test_status_skipped_baseline_when_stored_table_too_small(conn):
    """A batch that clears MIN_BATCH against an untrusted (too-small)
    baseline is reported "skipped:baseline", not "checked"."""
    listing_rows = [{"Primærrom": "100"} for _ in range(MIN_BATCH)]
    _, status = check(conn, listing_rows, [])
    assert status["eiendom"] == "skipped:baseline"


def test_status_checked_when_batch_and_baseline_both_clear(conn):
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
    conn.commit()
    listing_rows = [{"Primærrom": "100"} for _ in range(MIN_BATCH)]
    _, status = check(conn, listing_rows, [])
    assert status["eiendom"] == "checked"


def test_status_skipped_baseline_for_missing_table(conn):
    """A missing table (pre-migration-010 DB) yields ({}, 0) from
    `baseline_rates`, which the row-count guard reports as "skipped:baseline",
    not a crash."""
    conn.execute("DROP TABLE listing_details")
    conn.commit()
    detail_rows = [{"energimerke": "C"} for _ in range(MIN_BATCH)]
    _, status = check(conn, [], detail_rows)
    assert status["listing_details"] == "skipped:baseline"
