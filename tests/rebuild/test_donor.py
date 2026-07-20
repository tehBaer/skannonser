"""Tests for the donor/reuse system port (`skannonser/enrich/donor.py`).

Pure logic, no DB, no pandas. Synthetic coordinate clusters around Oslo
(59.9139 N, 10.7522 E); `_offset_north` moves a latitude by an approximate
number of meters due north, which is precise enough to place points reliably
inside/outside a given radius without needing to reimplement haversine here.
"""

import pytest

from skannonser.enrich.donor import (
    add_row_as_donor_if_complete,
    assign_donors_prepass,
    build_donor_cache,
    find_nearby_donor,
    maybe_assign_donor,
    resolve_mvv_uni_donor_value,
)

OSLO_LAT = 59.9139
OSLO_LNG = 10.7522
MAX_MINUTES = 60


def _offset_north(lat: float, meters: float) -> float:
    return lat + meters / 111_320.0


def _row(finnkode, lat=OSLO_LAT, lng=OSLO_LNG, values=None, donor_link=None):
    return {
        "finnkode": finnkode,
        "lat": lat,
        "lng": lng,
        "values": values if values is not None else {},
        "donor_link": donor_link,
    }


# ---------------------------------------------------------------------------
# find_nearby_donor
# ---------------------------------------------------------------------------


def test_find_nearby_donor_within_radius_found():
    cache = [(OSLO_LAT, OSLO_LNG, "A1")]
    target_lat = _offset_north(OSLO_LAT, 150)
    assert find_nearby_donor(target_lat, OSLO_LNG, cache, 300) == "A1"


def test_find_nearby_donor_outside_radius_not_found():
    cache = [(OSLO_LAT, OSLO_LNG, "A1")]
    target_lat = _offset_north(OSLO_LAT, 500)
    assert find_nearby_donor(target_lat, OSLO_LNG, cache, 300) is None


def test_find_nearby_donor_self_exclusion():
    cache = [(OSLO_LAT, OSLO_LNG, "A1")]
    # Searching from A1's own coordinates, excluding A1, leaves no candidate.
    assert find_nearby_donor(OSLO_LAT, OSLO_LNG, cache, 300, exclude_finnkode="A1") is None


def test_find_nearby_donor_nearest_wins():
    near_lat = _offset_north(OSLO_LAT, 100)
    far_lat = _offset_north(OSLO_LAT, 250)
    # Deliberately list the farther candidate first to prove distance, not order, wins.
    cache = [(far_lat, OSLO_LNG, "FAR"), (near_lat, OSLO_LNG, "NEAR")]
    assert find_nearby_donor(OSLO_LAT, OSLO_LNG, cache, 300) == "NEAR"


def test_find_nearby_donor_zero_radius_never_matches():
    cache = [(OSLO_LAT, OSLO_LNG, "A1")]
    assert find_nearby_donor(OSLO_LAT, OSLO_LNG, cache, 0) is None


def test_find_nearby_donor_missing_coords_returns_none():
    cache = [(OSLO_LAT, OSLO_LNG, "A1")]
    assert find_nearby_donor(None, OSLO_LNG, cache, 300) is None
    assert find_nearby_donor(OSLO_LAT, None, cache, 300) is None


# ---------------------------------------------------------------------------
# build_donor_cache
# ---------------------------------------------------------------------------

REQUIRED = ["PENDL RUSH BRJ"]


def test_build_donor_cache_includes_only_complete_eligible_rows():
    valid = _row("VALID", values={"PENDL RUSH BRJ": 30})
    no_coords = _row("NOCOORD", lat=None, lng=None, values={"PENDL RUSH BRJ": 30})
    already_linked = _row("LINKED", values={"PENDL RUSH BRJ": 30}, donor_link="OTHER")
    missing_value = _row("MISSING", values={"PENDL RUSH BRJ": None})
    empty_finnkode = _row("", values={"PENDL RUSH BRJ": 30})

    rows = [valid, no_coords, already_linked, missing_value, empty_finnkode]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)

    assert cache == [(OSLO_LAT, OSLO_LNG, "VALID")]


@pytest.mark.parametrize("sentinel", [-1, -2, -3])
def test_build_donor_cache_excludes_sentinel_values(sentinel):
    """Sentinel-validity finding: sentinels do NOT count as valid/complete
    for donor-cache membership -- `_is_valid_travel_value` requires
    `1 <= value <= max_travel_minutes`, which every sentinel fails. A row
    holding a sentinel is treated the same as a row missing the value."""
    row = _row("SENTINEL", values={"PENDL RUSH BRJ": sentinel})
    cache = build_donor_cache([row], REQUIRED, MAX_MINUTES)
    assert cache == []


def test_build_donor_cache_excludes_value_above_max():
    row = _row("TOOFAR", values={"PENDL RUSH BRJ": MAX_MINUTES + 1})
    assert build_donor_cache([row], REQUIRED, MAX_MINUTES) == []


def test_build_donor_cache_requires_all_required_columns():
    row = _row("PARTIAL", values={"PENDL RUSH BRJ": 30, "MVV UNI RUSH": None})
    cache = build_donor_cache([row], ["PENDL RUSH BRJ", "MVV UNI RUSH"], MAX_MINUTES)
    assert cache == []


# ---------------------------------------------------------------------------
# assign_donors_prepass
# ---------------------------------------------------------------------------


def test_prepass_assigns_nearest_root_donor():
    root = _row("root1", values={"PENDL RUSH BRJ": 30})
    acceptor = _row("x1", lat=_offset_north(OSLO_LAT, 100), values={})  # incomplete -> not cache-eligible

    rows = [root, acceptor]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)
    assert cache == [(OSLO_LAT, OSLO_LNG, "root1")]

    caches = {"brj": list(cache), "all": list(cache)}
    assign_donors_prepass(rows, caches, reuse_within_meters=300)

    assert acceptor["donor_link"] == "root1"
    assert root["donor_link"] is None


def test_prepass_ignores_candidates_outside_radius():
    root = _row("root1", values={"PENDL RUSH BRJ": 30})
    far_row = _row("far1", lat=_offset_north(OSLO_LAT, 1000), values={})

    rows = [root, far_row]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)
    caches = {"all": list(cache)}
    assign_donors_prepass(rows, caches, reuse_within_meters=300)

    assert far_row["donor_link"] is None


def test_prepass_cascade_collapses_chain_to_root_no_chains_survive():
    """A (pre-existing) -> X, then X gets assigned X -> root during the
    pre-pass: A's link must collapse to A -> root, never leaving A -> X."""
    root = _row("root1", values={"PENDL RUSH BRJ": 30})
    x = _row("x1", lat=_offset_north(OSLO_LAT, 100), values={})  # incomplete, not in cache
    a = _row("a1", donor_link="x1")  # pre-existing link into x1

    rows = [root, x, a]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)
    caches = {"all": list(cache)}
    assign_donors_prepass(rows, caches, reuse_within_meters=300)

    assert x["donor_link"] == "root1"
    assert a["donor_link"] == "root1"

    # Every link in the whole row-set points directly at a cache root
    # (a row with no donor_link of its own) -- no A->B->C chains survive.
    link_targets = {r["finnkode"] for r in rows if not r["donor_link"]}
    for r in rows:
        if r["donor_link"]:
            assert r["donor_link"] in link_targets


def test_prepass_removes_acceptor_from_all_caches():
    """A row that itself qualified as a donor, once assigned as an acceptor
    to another row, must be evicted from every cache -- it can never be
    picked as someone else's donor again."""
    root = _row("root1", values={"PENDL RUSH BRJ": 30})
    donor2 = _row("donor2", lat=_offset_north(OSLO_LAT, 100), values={"PENDL RUSH BRJ": 45})

    # donor2 listed first so it is processed (and becomes the acceptor)
    # before root1 gets a chance to link to it.
    rows = [donor2, root]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)
    assert {f for _, _, f in cache} == {"root1", "donor2"}

    caches = {"all": cache}
    assign_donors_prepass(rows, caches, reuse_within_meters=300)

    assert donor2["donor_link"] == "root1"
    assert root["donor_link"] is None
    assert [f for _, _, f in caches["all"]] == ["root1"]

    # donor2 is unreachable as a donor even though it is geographically
    # still right there -- a search from its own spot only ever turns up
    # root1 (or nothing), never itself.
    found = find_nearby_donor(donor2["lat"], donor2["lng"], caches["all"], 300)
    assert found in (None, "root1")
    assert not any(f == "donor2" for _, _, f in caches["all"])


def test_prepass_noop_when_radius_not_positive():
    root = _row("root1", values={"PENDL RUSH BRJ": 30})
    other = _row("x1", lat=_offset_north(OSLO_LAT, 100), values={})
    rows = [root, other]
    cache = build_donor_cache(rows, REQUIRED, MAX_MINUTES)
    caches = {"all": cache}
    assign_donors_prepass(rows, caches, reuse_within_meters=0)
    assert other["donor_link"] is None


def test_prepass_noop_when_all_cache_empty():
    row = _row("x1", lat=_offset_north(OSLO_LAT, 100), values={})
    caches = {"all": []}
    assign_donors_prepass([row], caches, reuse_within_meters=300)
    assert row["donor_link"] is None


# ---------------------------------------------------------------------------
# maybe_assign_donor
# ---------------------------------------------------------------------------


def test_maybe_assign_donor_existing_link_wins():
    row = _row("x1", donor_link="already-linked")
    cache = [(OSLO_LAT, OSLO_LNG, "OTHER")]
    assert maybe_assign_donor(row, cache, 300) == "already-linked"


def test_maybe_assign_donor_finds_nearest():
    row = _row("x1", lat=_offset_north(OSLO_LAT, 100))
    cache = [(OSLO_LAT, OSLO_LNG, "DONOR")]
    assert maybe_assign_donor(row, cache, 300) == "DONOR"


def test_maybe_assign_donor_none_when_nothing_in_radius():
    row = _row("x1", lat=_offset_north(OSLO_LAT, 1000))
    cache = [(OSLO_LAT, OSLO_LNG, "DONOR")]
    assert maybe_assign_donor(row, cache, 300) is None


def test_maybe_assign_donor_returns_none_when_nearest_is_self_no_fallback():
    """If the nearest cache entry is the row's own finnkode, legacy returns
    None outright rather than searching for the next-nearest candidate."""
    row = _row("SELF")
    cache = [(OSLO_LAT, OSLO_LNG, "SELF"), (_offset_north(OSLO_LAT, 200), OSLO_LNG, "OTHER")]
    assert maybe_assign_donor(row, cache, 300) is None


# ---------------------------------------------------------------------------
# add_row_as_donor_if_complete
# ---------------------------------------------------------------------------


def test_add_row_as_donor_if_complete_joins_cache_mid_run_and_is_findable():
    row = _row("late1", values={"PENDL RUSH BRJ": None})
    caches = {"brj": []}
    required_by_target = {"brj": ["PENDL RUSH BRJ"]}

    add_row_as_donor_if_complete(row, caches, required_by_target, MAX_MINUTES)
    assert caches["brj"] == []  # still incomplete

    row["values"]["PENDL RUSH BRJ"] = 30  # completes mid-run (e.g. API call succeeded)
    add_row_as_donor_if_complete(row, caches, required_by_target, MAX_MINUTES)
    assert caches["brj"] == [(OSLO_LAT, OSLO_LNG, "late1")]

    seeker_lat = _offset_north(OSLO_LAT, 100)
    assert find_nearby_donor(seeker_lat, OSLO_LNG, caches["brj"], 300) == "late1"


def test_add_row_as_donor_if_complete_skips_row_with_existing_link():
    row = _row("late1", values={"PENDL RUSH BRJ": 30}, donor_link="someone")
    caches = {"brj": []}
    add_row_as_donor_if_complete(row, caches, {"brj": ["PENDL RUSH BRJ"]}, MAX_MINUTES)
    assert caches["brj"] == []


def test_add_row_as_donor_if_complete_only_populates_satisfied_targets():
    row = _row("late1", values={"PENDL RUSH BRJ": 30, "MVV UNI RUSH": None})
    caches = {"brj": [], "all": []}
    required_by_target = {
        "brj": ["PENDL RUSH BRJ"],
        "all": ["PENDL RUSH BRJ", "MVV UNI RUSH"],
    }
    add_row_as_donor_if_complete(row, caches, required_by_target, MAX_MINUTES)
    assert caches["brj"] == [(OSLO_LAT, OSLO_LNG, "late1")]
    assert caches["all"] == []


def test_add_row_as_donor_if_complete_does_not_duplicate():
    row = _row("late1", values={"PENDL RUSH BRJ": 30})
    caches = {"brj": [(OSLO_LAT, OSLO_LNG, "late1")]}
    add_row_as_donor_if_complete(row, caches, {"brj": ["PENDL RUSH BRJ"]}, MAX_MINUTES)
    assert caches["brj"] == [(OSLO_LAT, OSLO_LNG, "late1")]


# ---------------------------------------------------------------------------
# resolve_mvv_uni_donor_value
# ---------------------------------------------------------------------------


def test_resolve_mvv_uni_donor_value_returns_own_value_first():
    links = {"a": "b"}
    values = {"a": 5, "b": 10}
    assert resolve_mvv_uni_donor_value("a", links, values) == 5


def test_resolve_mvv_uni_donor_value_walks_chain_to_first_stored_value():
    links = {"a": "b", "b": "c"}
    values = {"c": 42}
    assert resolve_mvv_uni_donor_value("a", links, values) == 42


def test_resolve_mvv_uni_donor_value_stops_at_first_value_along_chain():
    links = {"a": "b", "b": "c"}
    values = {"b": 10, "c": 42}
    assert resolve_mvv_uni_donor_value("a", links, values) == 10


def test_resolve_mvv_uni_donor_value_cycle_terminates_with_none():
    links = {"a": "b", "b": "c", "c": "a"}
    values = {}
    assert resolve_mvv_uni_donor_value("a", links, values) is None


def test_resolve_mvv_uni_donor_value_missing_finnkode_returns_none():
    assert resolve_mvv_uni_donor_value(None, {}, {}) is None
    assert resolve_mvv_uni_donor_value("", {}, {}) is None


def test_resolve_mvv_uni_donor_value_dead_end_chain_returns_none():
    links = {"a": "b"}  # b has no further link and no value
    values = {}
    assert resolve_mvv_uni_donor_value("a", links, values) is None
