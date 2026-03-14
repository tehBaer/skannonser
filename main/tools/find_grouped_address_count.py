#!/usr/bin/env python3
"""Count grouped unique address clusters for eligible listings.

Clustering uses the same travel reuse radius concept as donor logic
(TRAVEL_REUSE_WITHIN_METERS). Listings inside the same radius-connected
component are treated as one grouped address cluster.
"""

from __future__ import annotations

import argparse
import math
import os
import sys

import pandas as pd

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


def _load_filters() -> tuple[float, float | None]:
    try:
        from main.config.filters import TRAVEL_REUSE_WITHIN_METERS, SHEETS_MAX_PRICE
    except ImportError:
        try:
            from config.filters import TRAVEL_REUSE_WITHIN_METERS, SHEETS_MAX_PRICE
        except ImportError:
            TRAVEL_REUSE_WITHIN_METERS = 0
            SHEETS_MAX_PRICE = None

    reuse_radius = max(float(TRAVEL_REUSE_WITHIN_METERS or 0), 0.0)
    max_price = float(SHEETS_MAX_PRICE) if SHEETS_MAX_PRICE is not None else None
    return reuse_radius, max_price


def _haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)

    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * radius_m * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _normalize_postnummer(value) -> str:
    if value is None or pd.isna(value):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return raw
    if len(digits) <= 4:
        return digits.zfill(4)
    return digits


def _to_float_or_none(value):
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _find(parent: list[int], x: int) -> int:
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


def _union(parent: list[int], rank: list[int], a: int, b: int) -> None:
    ra = _find(parent, a)
    rb = _find(parent, b)
    if ra == rb:
        return
    if rank[ra] < rank[rb]:
        parent[ra] = rb
    elif rank[ra] > rank[rb]:
        parent[rb] = ra
    else:
        parent[rb] = ra
        rank[ra] += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find grouped unique address count (cluster count)",
    )
    parser.add_argument(
        "--radius-meters",
        type=float,
        default=None,
        help="Override clustering radius in meters (default: TRAVEL_REUSE_WITHIN_METERS)",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive listings in the count",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print details; otherwise prints only the grouped count",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reuse_radius, max_price = _load_filters()
    radius = max(float(args.radius_meters), 0.0) if args.radius_meters is not None else reuse_radius

    db = PropertyDatabase()
    df = db.get_eiendom_for_sheets()
    if df.empty:
        print(0)
        return 0

    if not args.include_inactive and "active" in df.columns:
        active_mask = pd.to_numeric(df["active"], errors="coerce").fillna(1).astype(int) == 1
        df = df.loc[active_mask].copy()

    if max_price is not None and "Pris" in df.columns:
        price_ok = pd.to_numeric(df["Pris"], errors="coerce").fillna(0) <= max_price
        df = df.loc[price_ok].copy()

    if df.empty:
        print(0)
        return 0

    address_col = "Adresse" if "Adresse" in df.columns else ("ADRESSE" if "ADRESSE" in df.columns else None)
    post_col = "Postnummer" if "Postnummer" in df.columns else ("POSTNUMMER" if "POSTNUMMER" in df.columns else None)
    lat_col = "LAT" if "LAT" in df.columns else ("lat" if "lat" in df.columns else None)
    lng_col = "LNG" if "LNG" in df.columns else ("lng" if "lng" in df.columns else None)

    if not address_col:
        print(0)
        return 0

    # One row per normalized address key.
    by_key: dict[str, tuple[float | None, float | None]] = {}
    for _, row in df.iterrows():
        address = str(row.get(address_col, "") or "").strip().lower()
        post = _normalize_postnummer(row.get(post_col)) if post_col else ""
        if not address:
            continue
        key = f"{address}|{post}"

        lat = _to_float_or_none(row.get(lat_col)) if lat_col else None
        lng = _to_float_or_none(row.get(lng_col)) if lng_col else None

        if key not in by_key:
            by_key[key] = (lat, lng)
        else:
            # Prefer coordinates when available.
            old_lat, old_lng = by_key[key]
            if old_lat is None or old_lng is None:
                by_key[key] = (lat, lng)

    keys = list(by_key.keys())
    coords = [by_key[k] for k in keys]
    n = len(keys)

    if n == 0:
        print(0)
        return 0

    # If radius disabled or no coordinates, each unique address key is its own cluster.
    if radius <= 0 or not lat_col or not lng_col:
        grouped_count = n
        if args.verbose:
            print(f"eligible_rows={len(df)}")
            print(f"unique_addresses={n}")
            print(f"radius_m={radius:.0f}")
            print(f"grouped_address_count={grouped_count}")
        else:
            print(grouped_count)
        return 0

    parent = list(range(n))
    rank = [0] * n

    # Build radius-connected components across unique address keys.
    for i in range(n):
        lat_i, lng_i = coords[i]
        if lat_i is None or lng_i is None:
            continue
        for j in range(i + 1, n):
            lat_j, lng_j = coords[j]
            if lat_j is None or lng_j is None:
                continue
            if _haversine_meters(lat_i, lng_i, lat_j, lng_j) <= radius:
                _union(parent, rank, i, j)

    roots = set()
    missing_coord_keys = 0
    for i in range(n):
        lat_i, lng_i = coords[i]
        if lat_i is None or lng_i is None:
            missing_coord_keys += 1
            continue
        roots.add(_find(parent, i))

    grouped_count = len(roots) + missing_coord_keys

    if args.verbose:
        print(f"eligible_rows={len(df)}")
        print(f"unique_addresses={n}")
        print(f"radius_m={radius:.0f}")
        print(f"missing_coord_unique_addresses={missing_coord_keys}")
        print(f"grouped_address_count={grouped_count}")
    else:
        print(grouped_count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
