"""Parser-drift canary: field-presence rates for a parsed batch vs stored rows."""
import pytest

from skannonser.ingest.drift import (
    BASELINE_ROWS,
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
