"""Read-only travel-value outlier scoring.

Ports the SCORING core of ``main/tools/validate_travel_values.py`` (637
lines, read FULLY before this port) into ``validate_travel(conn, domain,
...) -> list[dict]``. The legacy CLI tool's ``main()``/argparse/printing
shell is NOT ported line-for-line; ``skannonser run validate-travel``
(``skannonser/commands/run_cmd.py``) is a thin, always-exit-0 wrapper.

Heuristics ported (legacy line refs):
  * Local/neighbor check (``_score_against_group`` called from
    ``_build_findings`` 507-534): haversine-radius spatial-bucket search
    (``_build_spatial_buckets``/``_candidate_positions``, 396-437) for peers
    within ``radius_m``; median + MAD-robust-threshold scoring; +3 to the
    suspicion score when triggered.
  * Postcode-group check (536-549): same scoring against same-postnummer
    peers (excluding self), with ``min_abs_diff + 5.0`` (legacy's hardcoded
    postcode-strictness bump); +2 when triggered.
  * Donor-distance check (551-562): haversine distance between a row's own
    coordinates and its *direct* donor's (``TRAVEL_COPY_FROM_FINNKODE``)
    coordinates; +3 when farther than ``radius_m``. Donor coordinates come
    from the FULL ``eiendom_processed`` table (``ProcessedRepo.donor_seed``,
    unfiltered by active/price -- matches legacy's
    ``db.get_travel_donor_seed()``), not just the active/valid scan set.
  * Donor-chain group dedup (365-393, 475-483): before scoring, rows that
    resolve (via ``TRAVEL_COPY_FROM_FINNKODE``, walked to the chain's root,
    built from the SAME full/unfiltered donor graph as the distance check)
    to the same representative finnkode are collapsed to ONE row --
    preferring the actual representative row when present -- so a cluster of
    listings sharing one donor's value is never flagged N times over for the
    same underlying number. This applies per travel column independently
    (a row's dedup group membership only matters among rows valid for that
    column).
  * MAD outlier scoring (``_median``/``_mad``/``_score_against_group``,
    105-114, 440-455): ``diff = |value - median|``; ``rel = diff /
    max(|median|, 1.0)``; ``robust_threshold = mad_mult * max(mad, 1.0)``;
    suspicious iff ``diff >= max(min_abs_diff, robust_threshold) and rel >=
    min_rel_diff``. Used identically by both the local and postcode checks
    (only the group and the abs-diff floor differ).

Donor-resolved read (legacy line 315-393, ``_prepare_source_dataframe`` ->
``db.get_eiendom_for_sheets()``): legacy validates the SAME donor-resolved
value sheets/exports show, NOT each row's own raw stored column -- when
``travel_copy_from_finnkode`` is set and the donor's own value is non-null,
the donor's value wins (single hop, ``main/database/db.py:829-852``'s
CASE/COALESCE pattern). This port mirrors that with an equivalent bulk SQL
query (the same CASE pattern ``ProcessedRepo.sheet_travel_values`` already
uses per-finnkode, inlined here for a bulk read across the whole active set).

Legacy CLI argparse defaults -- VERIFIED against
``main/tools/validate_travel_values.py`` on 2026-07-20 (the task brief's
assumed numbers were wrong; corrected here):

    param              legacy default   brief said
    -----              --------------   ----------
    score_threshold    3                2
    min_abs_diff        20.0            15
    min_rel_diff         0.35           0.25
    mad_mult              2.5           2.0
    min_neighbors        5              4
    min_postcode_group   6              5
    max_travel_minutes  360             360   (matches -- MAX_TRAVEL_MINUTES)
    radius_m            SEE BELOW       750

``radius_m``: legacy's argparse default is
``default_radius if default_radius > 0 else 750.0``, i.e. 750 is only a
*fallback* used when ``TRAVEL_REUSE_WITHIN_METERS`` is 0/missing/unimportable.
The repo's actual current value (``main/config/filters.py:54`` AND this
rebuild's own ``config/domain.toml`` -- ``[travel] reuse_within_meters =
300``, ported 1:1 from the same legacy constant) is 300, so 300 -- not the
750 fallback -- is what running the legacy CLI today actually defaults to.
This module's default is 300 to match; ``skannonser run validate-travel``
additionally threads ``domain.travel.reuse_within_meters`` /
``domain.travel.max_travel_minutes`` through explicitly (like every other
enrich-side command does) so a future ``domain.toml`` edit stays authoritative
over this module's static defaults.

Scope simplifications (not present in the ``main/tools`` CLI, noted since
this is not a line-for-line shell port):
  * No ``--live-scope-csv``/``--disable-live-scope`` (FINN-search-scope CSV
    filtering) -- no rebuild equivalent exists; this always validates every
    DB-tracked active row, which is a superset of legacy's default live-scope-
    filtered run.
  * No ``--include-inactive`` -- always scoped to ``active = 1``, matching the
    legacy CLI's own default (``args.include_inactive`` defaults to False).
  * No ``--target`` selector -- always validates the non-exclusive
    destinations (brj, mvv), matching legacy's own default
    ``--target all`` -> ``TARGET_COLUMNS["all"] = ["PENDL RUSH BRJ", "PENDL
    RUSH MVV"]`` (MVV-UNI is intentionally excluded from "all", exactly like
    ``mvv_uni``'s exclusivity elsewhere in this rebuild --
    ``skannonser/enrich/travel.py``'s ``_select_destinations``). A caller
    wanting MVV-UNI coverage can still reach it if ever needed by extending
    the destination filter below; not required by the Phase 3 brief.

The targeted re-request tool (a separate legacy script, not
``validate_travel_values.py``) stays legacy-manual per the Phase 3 brief;
noted in STATUS's Phase 4 backlog.
"""

from __future__ import annotations

import math
import sqlite3
import statistics
from typing import Optional

from skannonser.config.domain import DomainConfig
from skannonser.enrich.donor import _clean, _haversine_meters
from skannonser.store.repositories.processed import ProcessedRepo

# Bulk equivalent of `ProcessedRepo.sheet_travel_values`'s single-finnkode
# CASE/COALESCE donor-resolution query (processed.py:390-416), which is
# itself the port of `main/database/db.py:get_eiendom_for_sheets` (829-852).
# Scoped to active listings only (legacy CLI default: `include_inactive=False`).
_SHEET_QUERY = """
    SELECT
        e.finnkode as finnkode,
        e.postnummer as postnummer,
        ep.lat as lat,
        ep.lng as lng,
        ep.travel_copy_from_finnkode as travel_copy_from_finnkode,
        CASE
            WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                 AND ep_src.pendl_rush_brj IS NOT NULL
            THEN ep_src.pendl_rush_brj
            ELSE ep.pendl_rush_brj
        END as pendl_rush_brj,
        CASE
            WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                 AND ep_src.pendl_rush_mvv IS NOT NULL
            THEN ep_src.pendl_rush_mvv
            ELSE ep.pendl_rush_mvv
        END as pendl_rush_mvv,
        CASE
            WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                 AND ep_src.pendl_rush_mvv_uni_rush IS NOT NULL
            THEN ep_src.pendl_rush_mvv_uni_rush
            ELSE ep.pendl_rush_mvv_uni_rush
        END as pendl_rush_mvv_uni_rush
    FROM eiendom e
    LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
    LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
    WHERE e.active = 1
    ORDER BY e.finnkode
"""


# ---------------------------------------------------------------------------
# Small scalar helpers (ports of validate_travel_values.py's module functions)
# ---------------------------------------------------------------------------


def _to_float_or_none(value) -> Optional[float]:
    """Port of `_to_float_or_none` (53-59), pandas-free."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _normalize_postnummer(value) -> str:
    """Port of `_normalize_postnummer` (62-73)."""
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return raw.lower()
    if len(digits) <= 4:
        return digits.zfill(4)
    return digits


def _is_valid_travel(value, max_travel_minutes: Optional[float]) -> bool:
    """Port of `_is_valid_travel` (117-125): negative sentinels (-1/-2/-3,
    see `skannonser.enrich.sentinels`) always fail this via the `< 1` check,
    same as legacy -- no separate sentinel handling needed."""
    parsed = _to_float_or_none(value)
    if parsed is None:
        return False
    if parsed < 1:
        return False
    if max_travel_minutes is not None and parsed > max_travel_minutes:
        return False
    return True


def _median(values: list[float]) -> Optional[float]:
    """Port of `_median` (105-108); `statistics.median` matches
    `pd.Series.median()` for numeric lists (same even-count averaging)."""
    if not values:
        return None
    return statistics.median(values)


def _mad(values: list[float], center: float) -> float:
    """Port of `_mad` (111-114)."""
    if not values:
        return 0.0
    return statistics.median([abs(v - center) for v in values])


def _format_reason(label: str, value: float, median: float, diff: float, group_size: int) -> str:
    """Port of `_format_reason` (128-140). Note: legacy labels travel
    *minutes* with an "m" suffix here (a pre-existing cosmetic quirk in the
    source tool) -- preserved verbatim for output fidelity."""
    value_i = int(round(value))
    median_i = int(round(median))
    diff_i = int(round(diff))
    direction = "higher" if value >= median else "lower"

    if label == "local":
        return f"Local: {value_i}m ({diff_i}m {direction} vs near med {median_i}, n={group_size})"
    if label == "postcode":
        return f"Postnr: {value_i}m ({diff_i}m {direction} vs med {median_i}, n={group_size})"
    return f"Outlier: {value_i}m ({diff_i}m from med {median_i}, n={group_size})"


def _score_against_group(
    value: float,
    peers: list[float],
    min_abs_diff: float,
    min_rel_diff: float,
    mad_mult: float,
) -> tuple[bool, Optional[float], Optional[float]]:
    """Port of `_score_against_group` (440-455)."""
    median = _median(peers)
    if median is None:
        return False, None, None
    diff = abs(value - median)
    rel = diff / max(abs(median), 1.0)
    mad = _mad(peers, median)
    robust_threshold = mad_mult * max(mad, 1.0)
    suspicious = diff >= max(min_abs_diff, robust_threshold) and rel >= min_rel_diff
    return suspicious, median, diff


# ---------------------------------------------------------------------------
# Spatial bucketing (port of _build_spatial_buckets / _candidate_positions,
# 396-437) -- a radius-search performance optimization; same neighbor RESULTS
# as brute-force haversine over the whole scan set, much cheaper.
# ---------------------------------------------------------------------------


def _build_spatial_buckets(
    rows: list[dict], radius_m: float
) -> tuple[dict[tuple[int, int], list[int]], float, float]:
    if radius_m <= 0:
        return {}, 1.0, 1.0

    lats = [r["lat"] for r in rows if r["lat"] is not None]
    mean_lat = sum(lats) / len(lats) if lats else 60.0
    lat_step = max(radius_m / 111320.0, 0.0001)
    lng_step = max(radius_m / (111320.0 * max(0.1, math.cos(math.radians(mean_lat)))), 0.0001)

    buckets: dict[tuple[int, int], list[int]] = {}
    for pos, row in enumerate(rows):
        lat, lng = row["lat"], row["lng"]
        if lat is None or lng is None:
            continue
        key = (int(lat / lat_step), int(lng / lng_step))
        buckets.setdefault(key, []).append(pos)

    return buckets, lat_step, lng_step


def _candidate_positions(
    lat: Optional[float],
    lng: Optional[float],
    buckets: dict[tuple[int, int], list[int]],
    lat_step: float,
    lng_step: float,
) -> list[int]:
    if lat is None or lng is None or not buckets:
        return []
    lat_bucket = int(lat / lat_step)
    lng_bucket = int(lng / lng_step)
    positions: list[int] = []
    for dlat in (-1, 0, 1):
        for dlng in (-1, 0, 1):
            positions.extend(buckets.get((lat_bucket + dlat, lng_bucket + dlng), []))
    return positions


# ---------------------------------------------------------------------------
# Global donor graph + representative resolution (port of
# _prepare_source_dataframe's donor_coords/donor_links/_resolve_representative,
# 349-393) -- built once from the FULL eiendom_processed table, unfiltered.
# ---------------------------------------------------------------------------


def _global_donor_maps(
    conn: sqlite3.Connection,
) -> tuple[dict[str, str], dict[str, tuple[Optional[float], Optional[float]]]]:
    donor_links: dict[str, str] = {}
    donor_coords: dict[str, tuple[Optional[float], Optional[float]]] = {}
    for row in ProcessedRepo(conn).donor_seed():
        finnkode = _clean(row.get("Finnkode"))
        if not finnkode:
            continue
        donor_coords[finnkode] = (
            _to_float_or_none(row.get("LAT")),
            _to_float_or_none(row.get("LNG")),
        )
        donor_finnkode = _clean(row.get("TRAVEL_COPY_FROM_FINNKODE"))
        if donor_finnkode and donor_finnkode != finnkode:
            donor_links[finnkode] = donor_finnkode
    return donor_links, donor_coords


def _resolve_representative(finnkode: str, donor_links: dict[str, str], cache: dict[str, str]) -> str:
    """Port of `_resolve_representative` (367-388): walk the donor-link chain
    to its root, cycle-guarded, memoizing every finnkode visited along the
    way (not just the starting one)."""
    start = _clean(finnkode)
    if not start:
        return ""
    if start in cache:
        return cache[start]

    current = start
    seen: set[str] = set()
    while True:
        donor = _clean(donor_links.get(current, ""))
        if not donor or donor == current or donor in seen:
            break
        seen.add(current)
        current = donor

    representative = current
    cache[start] = representative
    for item in seen:
        cache[item] = representative
    return representative


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def validate_travel(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    score_threshold: int = 3,
    min_abs_diff: float = 20.0,
    min_rel_diff: float = 0.35,
    mad_mult: float = 2.5,
    radius_m: float = 300.0,
    min_neighbors: int = 5,
    min_postcode_group: int = 6,
    max_travel_minutes: float = 360.0,
) -> list[dict]:
    """Score active listings' donor-resolved travel values for suspicious
    outliers. Read-only -- no writes, no external calls. See the module
    docstring for the full heuristic/default-verification writeup.

    Returns findings sorted worst-first (score desc, column asc, value desc
    -- port of the legacy sort at ``_build_findings`` 592-596), each a dict:
    ``finnkode``, ``column`` (the ``db_column``, e.g. ``"pendl_rush_brj"``),
    ``value`` (int minutes), ``score`` (int), ``reasons`` (list[str]), plus
    ``neighbor_count``/``postcode_group_size`` (informational, for the CLI
    table -- not part of the legacy finding's minimum contract but present
    in its output columns too).
    """
    donor_links, donor_coords = _global_donor_maps(conn)
    rep_cache: dict[str, str] = {}
    sheet_rows = conn.execute(_SHEET_QUERY).fetchall()

    findings: list[dict] = []

    for dest in domain.destinations:
        if dest.exclusive:
            # mvv_uni excluded from the default scope -- matches legacy's
            # own TARGET_COLUMNS["all"], which never includes MVV-UNI either.
            continue
        col = dest.db_column

        candidates: list[dict] = []
        for r in sheet_rows:
            finnkode = _clean(r["finnkode"])
            value = _to_float_or_none(r[col])
            if not finnkode or not _is_valid_travel(value, max_travel_minutes):
                continue
            candidates.append(
                {
                    "finnkode": finnkode,
                    "value": float(value),
                    "lat": _to_float_or_none(r["lat"]),
                    "lng": _to_float_or_none(r["lng"]),
                    "postnummer": _normalize_postnummer(r["postnummer"]),
                    "travel_copy_from_finnkode": _clean(r["travel_copy_from_finnkode"]),
                }
            )

        if not candidates:
            continue

        # Donor-chain group dedup (port of 475-483): one row per resolved
        # representative, preferring the row that IS the representative.
        for c in candidates:
            c["_representative"] = _resolve_representative(c["finnkode"], donor_links, rep_cache)
            c["_is_representative"] = c["finnkode"] == c["_representative"]

        candidates.sort(
            key=lambda c: (c["_representative"], not c["_is_representative"], c["finnkode"])
        )
        deduped: list[dict] = []
        seen_reps: set[str] = set()
        for c in candidates:
            rep = c["_representative"]
            if rep in seen_reps:
                continue
            seen_reps.add(rep)
            deduped.append(c)

        buckets, lat_step, lng_step = _build_spatial_buckets(deduped, radius_m)

        postcode_groups: dict[str, list[tuple[str, float]]] = {}
        for c in deduped:
            if not c["postnummer"]:
                continue
            postcode_groups.setdefault(c["postnummer"], []).append((c["finnkode"], c["value"]))

        for pos, c in enumerate(deduped):
            value = c["value"]
            lat, lng = c["lat"], c["lng"]
            score = 0
            reasons: list[str] = []
            neighbor_count = 0
            postcode_group_size = 0

            if lat is not None and lng is not None and radius_m > 0:
                local_values: list[float] = []
                for peer_pos in _candidate_positions(lat, lng, buckets, lat_step, lng_step):
                    if peer_pos == pos:
                        continue
                    peer = deduped[peer_pos]
                    if peer["finnkode"] == c["finnkode"]:
                        continue
                    p_lat, p_lng = peer["lat"], peer["lng"]
                    if p_lat is None or p_lng is None:
                        continue
                    if _haversine_meters(lat, lng, p_lat, p_lng) <= radius_m:
                        local_values.append(peer["value"])

                neighbor_count = len(local_values)
                if neighbor_count >= min_neighbors:
                    suspicious, median, diff = _score_against_group(
                        value, local_values, min_abs_diff, min_rel_diff, mad_mult
                    )
                    if suspicious:
                        score += 3
                        reasons.append(_format_reason("local", value, median, diff, neighbor_count))

            if c["postnummer"]:
                postcode_values = [
                    v for fk, v in postcode_groups.get(c["postnummer"], []) if fk != c["finnkode"]
                ]
                postcode_group_size = len(postcode_values)
                if postcode_group_size >= min_postcode_group:
                    suspicious, median, diff = _score_against_group(
                        value, postcode_values, min_abs_diff + 5.0, min_rel_diff, mad_mult
                    )
                    if suspicious:
                        score += 2
                        reasons.append(
                            _format_reason("postcode", value, median, diff, postcode_group_size)
                        )

            donor_fk = c["travel_copy_from_finnkode"]
            if donor_fk and lat is not None and lng is not None:
                donor_lat, donor_lng = donor_coords.get(donor_fk, (None, None))
                if donor_lat is not None and donor_lng is not None:
                    donor_distance_m = _haversine_meters(lat, lng, donor_lat, donor_lng)
                    if donor_distance_m > radius_m:
                        score += 3
                        reasons.append(
                            f"Donor: {int(round(donor_distance_m))}m > {int(round(radius_m))}m ({donor_fk})"
                        )

            if score < score_threshold:
                continue

            findings.append(
                {
                    "finnkode": c["finnkode"],
                    "column": col,
                    "value": int(round(value)),
                    "score": score,
                    "reasons": reasons,
                    "neighbor_count": neighbor_count,
                    "postcode_group_size": postcode_group_size,
                }
            )

    findings.sort(key=lambda f: (-f["score"], f["column"], -f["value"]))
    return findings
