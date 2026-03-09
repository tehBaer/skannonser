#!/usr/bin/env python3
"""Filter dnbeiendom A_live.csv to entries strictly inside the FINN polygon (no buffer).

Writes: data/dnbeiendom/A_live_filtered_no_buffer.csv
"""
from __future__ import annotations

from pathlib import Path
import math
import sys
import pandas as pd

try:
    from main.tools.finn_polygon_editor import load_defaults_from_source
except Exception:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from main.tools.finn_polygon_editor import load_defaults_from_source


def is_point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool:
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


def main():
    src = Path("data/dnbeiendom/A_live.csv")
    out = Path("data/dnbeiendom/A_live_filtered_no_buffer.csv")
    if not src.exists():
        print(f"Source file not found: {src}")
        raise SystemExit(1)

    _, polygon = load_defaults_from_source(Path("main/runners/run_eiendom_db.py"))
    buffer_km = 0.0

    df = pd.read_csv(src)
    total = len(df)

    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    def row_check(r):
        lat = r['Latitude']
        lng = r['Longitude']
        if not (pd.notna(lat) and pd.notna(lng)):
            return False
        try:
            return is_point_in_polygon(float(lat), float(lng), polygon)
        except Exception:
            return False

    mask = df.apply(row_check, axis=1)
    filtered = df[mask].copy()
    filtered.to_csv(out, index=False)

    print(f"Total rows: {total}")
    print(f"Kept rows strictly inside polygon: {len(filtered)}")
    print(f"Filtered CSV written to: {out}")
