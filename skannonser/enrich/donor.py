"""Travel-time donor/reuse system -- pure logic, no DB, no pandas.

Ports `main/post_process.py`'s donor-cache machinery:

  - `_build_travel_donor_cache` (116-141)   -> `build_donor_cache`
  - `_find_nearby_donor_finnkode` (144-162) -> `find_nearby_donor`
  - the pre-pass (534-587)                  -> `assign_donors_prepass`
  - `_resolve_mvv_uni_donor_value` (491-504) -> `resolve_mvv_uni_donor_value`
  - `_maybe_assign_donor` (818-840)          -> `maybe_assign_donor`
  - `_add_row_as_donor_if_complete` (842-855) -> `add_row_as_donor_if_complete`

Rows are plain dicts: `{"finnkode": str, "lat": float | None,
"lng": float | None, "values": dict[str, int | None], "donor_link": str | None}`.
`values` is keyed by the same df-column names legacy used (e.g.
`"PENDL RUSH BRJ"`), already coerced to `int | None`.

Sentinel-validity finding (see task-7-report.md for detail): legacy's
`_is_valid_travel_value` requires `1 <= value <= max_travel_minutes`, which a
sentinel (-1/-2/-3, see `skannonser.enrich.sentinels`) always fails. A row
holding a sentinel in a required column is therefore NOT "complete" and is
excluded from `build_donor_cache` / `add_row_as_donor_if_complete`, exactly
like a missing value would be. This module mirrors that: sentinels do NOT
count as valid for donor-cache membership. (Legacy's separate MVV-UNI
donor-*value* lookup, `_seed_mvv_uni_lookup` line 485, treats sentinels as a
resolvable value so a known failure isn't retried forever -- but that lookup
is built by the caller, outside this module's `resolve_mvv_uni_donor_value`,
which only walks pre-built `links`/`values` dicts per lines 491-504.)
"""

import math
from typing import Optional


EARTH_RADIUS_M = 6371000.0


def _clean(value) -> str:
    """Trim to a string, treating None as empty. Mirrors `_to_text_or_empty`."""
    if value is None:
        return ""
    return str(value).strip()


def _haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Byte-for-byte port of `_haversine_meters` (post_process.py:81-93)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)

    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _is_valid_travel_value(value: Optional[int], max_travel_minutes: float) -> bool:
    """Port of `_is_valid_travel_value` (post_process.py:105-109).

    Sentinels (-1/-2/-3) fail this on purpose -- see module docstring.
    """
    if value is None:
        return False
    return 1 <= value <= max_travel_minutes


def _row_has_all_valid_values(
    row: dict, columns: list[str], max_travel_minutes: float
) -> bool:
    """Port of `_row_has_all_travel_values` (post_process.py:112-113)."""
    values = row.get("values") or {}
    return all(_is_valid_travel_value(values.get(col), max_travel_minutes) for col in columns)


def build_donor_cache(
    rows: list[dict], required_columns: list[str], max_travel_minutes: float
) -> list[tuple[float, float, str]]:
    """Port of `_build_travel_donor_cache` (post_process.py:116-141).

    `max_travel_minutes` is threaded through explicitly since our row schema
    has no notion of "which columns exist on the df" -- legacy needed that
    only to bail out early when LAT/LNG/Finnkode columns were entirely
    absent, which is not a concept that applies to a list of row dicts.

    Eligible: valid (non-None) lat/lng, no existing donor link, ALL
    `required_columns` hold a valid (non-sentinel) travel value, non-empty
    finnkode.
    """
    cache: list[tuple[float, float, str]] = []
    for row in rows:
        lat, lng = row.get("lat"), row.get("lng")
        if lat is None or lng is None:
            continue
        if _clean(row.get("donor_link")):
            continue
        if not _row_has_all_valid_values(row, required_columns, max_travel_minutes):
            continue
        finnkode = _clean(row.get("finnkode"))
        if not finnkode:
            continue
        cache.append((lat, lng, finnkode))
    return cache


def find_nearby_donor(
    lat: Optional[float],
    lng: Optional[float],
    cache: list[tuple[float, float, str]],
    max_distance_m: float,
    exclude_finnkode: Optional[str] = None,
) -> Optional[str]:
    """Port of `_find_nearby_donor_finnkode` (post_process.py:144-162).

    `exclude_finnkode` mirrors the pre-pass's pre-filter
    (`[... if f != _finnkode]`, line 560): when set, candidates with that
    finnkode are dropped before the nearest search runs, so a *different*
    donor can still be found if the excluded one would otherwise have won.
    `maybe_assign_donor` deliberately does NOT use this (see its docstring).
    """
    if lat is None or lng is None or max_distance_m <= 0:
        return None

    best_finnkode = None
    best_distance = None
    for cand_lat, cand_lng, cand_finnkode in cache:
        if exclude_finnkode and cand_finnkode == exclude_finnkode:
            continue
        distance_m = _haversine_meters(lat, lng, cand_lat, cand_lng)
        if distance_m <= max_distance_m and (best_distance is None or distance_m < best_distance):
            best_distance = distance_m
            best_finnkode = cand_finnkode

    return best_finnkode


def assign_donors_prepass(
    rows: list[dict], caches: dict[str, list[tuple[float, float, str]]], reuse_within_meters: float
) -> None:
    """Port of the pre-pass (post_process.py:534-587). Mutates `rows` in place.

    `caches` mirrors legacy's `_all_caches = [donor_cache_brj, donor_cache_mvv,
    donor_cache_mvv_uni, donor_cache_all]` (line 546): a dict of the parallel
    per-target caches, all built from the same `rows` before this call. The
    nearest search always runs against `caches["all"]` only (line 558-562,
    legacy never searches the per-target caches here) -- the other caches are
    only used for the acceptor-eviction step, so every cache in the dict is
    kept in sync when a row becomes an acceptor.

    For each row without an existing donor link, finds the nearest ROOT donor
    (excluding itself) in `caches["all"]` within `reuse_within_meters`. If
    found:
      - assigns `row["donor_link"] = nearest`
      - cascade-collapses any other row currently pointing at *this* row's
        finnkode so it points at `nearest` instead (one-shot, using
        in-progress mutated state -- exactly legacy's live `df.at[...]`
        mask), guaranteeing no A->B->C chains survive this pass
      - evicts this row's finnkode from every cache in `caches` (it is now
        an acceptor and can never be a donor again)

    No-op if `reuse_within_meters <= 0` or `caches["all"]` starts empty
    (legacy's `if travel_reuse_within_meters > 0: ... if donor_cache_all:`
    guards, lines 534/545 -- the lat_col/lng_col existence check in between
    is a pandas-column concept that doesn't apply to row dicts, so it is
    dropped).
    """
    if reuse_within_meters <= 0:
        return
    all_cache = caches.get("all")
    if not all_cache:
        return

    all_caches = list(caches.values())

    for row in rows:
        if _clean(row.get("donor_link")):
            continue  # already an acceptor
        finnkode = _clean(row.get("finnkode"))
        if not finnkode:
            continue
        lat, lng = row.get("lat"), row.get("lng")
        if lat is None or lng is None:
            continue

        nearest = find_nearby_donor(lat, lng, all_cache, reuse_within_meters, exclude_finnkode=finnkode)
        if not nearest:
            continue

        row["donor_link"] = nearest

        # Cascade: collapse any A->B links (B = this row) to A->nearest.
        for other in rows:
            if other is row:
                continue
            if _clean(other.get("donor_link")) == finnkode:
                other["donor_link"] = nearest

        # This row is now an acceptor -- remove it from every donor cache.
        for cache in all_caches:
            cache[:] = [c for c in cache if c[2] != finnkode]


def maybe_assign_donor(
    row: dict, cache: list[tuple[float, float, str]], max_distance_m: float
) -> Optional[str]:
    """Port of `_maybe_assign_donor` (post_process.py:818-840).

    Legacy's `required_columns` parameter is accepted but never referenced
    in the function body (dead parameter) -- dropped here.

    An existing donor link always wins (listing-wide donor semantics). Else
    finds the nearest donor in `cache` *without* pre-excluding self (unlike
    `find_nearby_donor` calls from the pre-pass); if the nearest result
    happens to equal this row's own finnkode, returns None outright rather
    than searching again for a different candidate -- exactly legacy's
    behavior (no fallback to the second-nearest in this path).
    """
    existing_donor = _clean(row.get("donor_link"))
    if existing_donor:
        return existing_donor

    self_finnkode = _clean(row.get("finnkode"))
    if not self_finnkode:
        return None

    donor_finnkode = find_nearby_donor(row.get("lat"), row.get("lng"), cache, max_distance_m)
    if not donor_finnkode or donor_finnkode == self_finnkode:
        return None
    return donor_finnkode


def add_row_as_donor_if_complete(
    row: dict,
    caches: dict[str, list[tuple[float, float, str]]],
    required_by_target: dict[str, list[str]],
    max_travel_minutes: float,
) -> None:
    """Port of `_add_row_as_donor_if_complete` (post_process.py:842-855).

    Legacy calls this once per (required_columns, cache) pair for each row
    (e.g. once for the BRJ-specific cache, once again for the "all" cache --
    see post_process.py:938-939/1060-1061/1191-1192). This port bundles all
    targets into one call via `required_by_target`/`caches` dicts keyed the
    same way, evaluating each target's completeness independently and
    appending to that target's cache when satisfied -- equivalent to calling
    legacy's function once per target with the shared row-level guards
    (donor link / finnkode / coords) evaluated only once instead of per call.
    """
    if _clean(row.get("donor_link")):
        return
    finnkode = _clean(row.get("finnkode"))
    if not finnkode:
        return
    lat, lng = row.get("lat"), row.get("lng")
    if lat is None or lng is None:
        return

    for target, required_columns in required_by_target.items():
        if not _row_has_all_valid_values(row, required_columns, max_travel_minutes):
            continue
        cache = caches.get(target)
        if cache is None:
            continue
        if not any(c[2] == finnkode for c in cache):
            cache.append((lat, lng, finnkode))


def resolve_mvv_uni_donor_value(
    finnkode: Optional[str], links: dict[str, str], values: dict[str, Optional[int]]
) -> Optional[int]:
    """Port of `_resolve_mvv_uni_donor_value` (post_process.py:491-504).

    Walks the donor-link chain starting at `finnkode`, returning the first
    stored value found along the way. Cycle-guarded with a `seen` set --
    matches legacy exactly, including that a value found on the *starting*
    finnkode itself (if `values` has an entry for it) is returned before any
    chain walk happens.
    """
    current = _clean(finnkode)
    if not current:
        return None

    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        value = values.get(current)
        if value is not None:
            return value
        current = _clean(links.get(current))
    return None
