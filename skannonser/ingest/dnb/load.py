"""DNB Eiendom row filtering (polygon) and FINN address/postcode matching.

Ports the filter + match block from
``main/extractors/filter_and_load_dnbeiendom_no_buffer.main``
(``main/extractors/filter_and_load_dnbeiendom_no_buffer.py:57-106``). Legacy
read both sides from CSV files (``data/dnbeiendom/A_live_filtered_no_buffer.csv``
strictly-inside-polygon rows, matched against the whole
``data/eiendom/A_live.csv``); this port reads the FINN side from the
``eiendom`` table instead of a CSV, everything else preserved.

Legacy's FINN-side read (``pd.read_csv(finn_path)``) loaded the ENTIRE FINN
dataset with no active/inactive filtering whatsoever -- there is no other
matcher anywhere in the legacy codebase that restricts to active rows either
(confirmed by reading every ``duplicate_of_finnkode`` / ``MatchedFinn_Finnkode``
computation site). This port matches against ALL ``eiendom`` rows regardless
of ``active`` for the same reason. Do not "fix" to active-only without a
controller ruling.
"""

import sqlite3

from skannonser.config.domain import DomainConfig
from skannonser.geo import is_point_in_polygon
from skannonser.textnorm import normalize_addr, normalize_pc


def _row_ok(row: dict, polygon: list[tuple[float, float]]) -> bool:
    """Port of ``main()``'s ``row_ok`` closure (lines 71-79): drop rows with
    missing/non-numeric coordinates or coordinates outside the polygon."""
    lat = row.get("Latitude")
    lng = row.get("Longitude")
    if lat is None or lng is None:
        return False
    try:
        return is_point_in_polygon(float(lat), float(lng), polygon)
    except (TypeError, ValueError):
        return False


def filter_and_match(
    rows: list[dict], domain: DomainConfig, conn: sqlite3.Connection
) -> list[dict]:
    """Polygon-filter DNB rows (strict, no buffer) and annotate survivors with
    ``duplicate_of_finnkode``.

    Direct port of ``main()``'s filter-then-match block (lines 57-106):
    rows outside ``domain.polygon_points`` are dropped, then every surviving
    row is looked up by normalized ``(StreetAddress, PostalCode)`` against
    every ``eiendom`` row's normalized ``(adresse, postnummer)``. First match
    per key wins (mirrors legacy's ``if key not in lookup`` first-wins dict
    build). Rows without a match get ``duplicate_of_finnkode: None`` (legacy's
    CSV-side default was ``''``, which the repository layer treats
    identically to ``None`` -- see ``DnbRepo``).
    """
    polygon = domain.polygon_points
    kept = [row for row in rows if _row_ok(row, polygon)]

    lookup: dict[tuple[str, str], str] = {}
    for eiendom_row in conn.execute(
        "SELECT finnkode, adresse, postnummer FROM eiendom ORDER BY id"
    ):
        key = (
            normalize_addr(eiendom_row["adresse"]),
            normalize_pc(eiendom_row["postnummer"]),
        )
        if key not in lookup:
            lookup[key] = eiendom_row["finnkode"]

    matched: list[dict] = []
    for row in kept:
        key = (
            normalize_addr(row.get("StreetAddress")),
            normalize_pc(row.get("PostalCode")),
        )
        out = dict(row)
        out["duplicate_of_finnkode"] = lookup.get(key)
        matched.append(out)
    return matched
