#!/usr/bin/env python3
"""Filter dnbeiendom A_live.csv to entries within the FINN polygon (with buffer).

Reads polygon defaults from `main/runners/run_eiendom_db.py` using the
`load_defaults_from_source` helper and writes a filtered CSV to
`data/dnbeiendom/A_live_filtered.csv`.
"""
from __future__ import annotations

from pathlib import Path
import math
import sys
import pandas as pd

try:
    from main.tools.finn_polygon_editor import load_defaults_from_source
except Exception:
    # allow running from repo root without package path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from main.tools.finn_polygon_editor import load_defaults_from_source


def is_point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool:
    """Ray-casting point-in-polygon test.

    `polygon` is a list of (lng, lat) tuples.
    """
    inside = False
    n = len(polygon)
    if n < 3:
        return False

    for i in range(n):
        j = (i - 1) % n
        xi, yi = polygon[i][0], polygon[i][1]
        xj, yj = polygon[j][0], polygon[j][1]

        if not (math.isfinite(xi) and math.isfinite(yi) and math.isfinite(xj) and math.isfinite(yj)):
            continue

        intersects = ((yi > lat) != (yj > lat)) and (
            lng < ((xj - xi) * (lat - yi)) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside

    return inside


def distance_point_to_segment_meters(px, py, ax, ay, bx, by):
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    ab_len_sq = abx * abx + aby * aby
    if ab_len_sq <= 0:
        dx = px - ax
        dy = py - ay
        return math.hypot(dx, dy)

    t = (apx * abx + apy * aby) / ab_len_sq
    t = max(0.0, min(1.0, t))
    cx = ax + t * abx
    cy = ay + t * aby
    dx = px - cx
    dy = py - cy
    return math.hypot(dx, dy)


def is_point_within_polygon_buffer(lat: float, lng: float, polygon: list[tuple[float, float]], buffer_km: float) -> bool:
    if not (isinstance(buffer_km, (int, float)) and buffer_km > 0):
        return False

    buffer_m = buffer_km * 1000.0
    meters_per_deg_lat = 111320.0
    meters_per_deg_lng = 111320.0 * max(0.1, math.cos(lat * math.pi / 180.0))

    for i in range(len(polygon)):
        j = (i - 1) % len(polygon)
        xi, yi = polygon[i][0], polygon[i][1]
        xj, yj = polygon[j][0], polygon[j][1]
        if not (math.isfinite(xi) and math.isfinite(yi) and math.isfinite(xj) and math.isfinite(yj)):
            continue

        ax = (xj - lng) * meters_per_deg_lng
        ay = (yj - lat) * meters_per_deg_lat
        bx = (xi - lng) * meters_per_deg_lng
        by = (yi - lat) * meters_per_deg_lat

        if distance_point_to_segment_meters(0, 0, ax, ay, bx, by) <= buffer_m:
            return True

    return False


def is_within_search_area(lat: float, lng: float, polygon: list[tuple[float, float]], buffer_km: float) -> bool:
    if not polygon or len(polygon) < 3:
        return True

    # coarse bounding box with buffer
    lats = [p[1] for p in polygon]
    lngs = [p[0] for p in polygon]
    min_lat = min(lats)
    max_lat = max(lats)
    min_lng = min(lngs)
    max_lng = max(lngs)

    # approximate degree buffers
    buffer_m = buffer_km * 1000.0
    lat_buffer_deg = buffer_m / 111320.0
    center_lat = (min_lat + max_lat) / 2.0
    meters_per_deg_lng_center = 111320.0 * max(0.1, math.cos(center_lat * math.pi / 180.0))
    lng_buffer_deg = buffer_m / meters_per_deg_lng_center

    if not (min_lat - lat_buffer_deg <= lat <= max_lat + lat_buffer_deg and min_lng - lng_buffer_deg <= lng <= max_lng + lng_buffer_deg):
        return False

    if is_point_in_polygon(lat, lng, polygon):
        return True

    return is_point_within_polygon_buffer(lat, lng, polygon, buffer_km)


def main():
    src = Path("data/dnbeiendom/A_live.csv")
    out = Path("data/dnbeiendom/A_live_filtered.csv")
    if not src.exists():
        print(f"Source file not found: {src}")
        raise SystemExit(1)

    # load FINN polygon defaults from run_eiendom_db.py
    _, polygon = load_defaults_from_source(Path("main/runners/run_eiendom_db.py"))
    # polygon is list of (lng, lat) tuples
    buffer_km = 2.0

    df = pd.read_csv(src)
    total = len(df)

    # coerce lat/lng to numeric
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    def row_check(r):
        lat = r['Latitude']
        lng = r['Longitude']
        if not (pd.notna(lat) and pd.notna(lng)):
            return False
        try:
            return is_within_search_area(float(lat), float(lng), polygon, buffer_km)
        except Exception:
            return False

    mask = df.apply(row_check, axis=1)
    filtered = df[mask].copy()
    filtered.to_csv(out, index=False)

    print(f"Total rows: {total}")
    print(f"Kept rows within polygon (+{buffer_km} km buffer): {len(filtered)}")
    print(f"Filtered CSV written to: {out}")


if __name__ == '__main__':
    main()
