#!/usr/bin/env python3
"""Filter DNB A_live.csv strictly inside FINN polygon (no buffer), annotate matches, and load into DB."""
from pathlib import Path
import sys
import math
import pandas as pd
import re

# ensure repo root on path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from main.tools.finn_polygon_editor import load_defaults_from_source
from main.database.db import PropertyDatabase


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


def normalize_addr(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[.,©()\"'\\/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_pc(pc) -> str:
    if pd.isna(pc):
        return ""
    s = str(pc).strip()
    s = re.sub(r"\.0$", "", s)
    return s


def main():
    src = Path('data/dnbeiendom/A_live.csv')
    out = Path('data/dnbeiendom/A_live_filtered_no_buffer.csv')
    if not src.exists():
        print(f"Source not found: {src}")
        raise SystemExit(1)

    _, polygon = load_defaults_from_source(Path('main/runners/run_eiendom_db.py'))

    df = pd.read_csv(src)
    total = len(df)
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    def row_ok(r):
        lat = r['Latitude']
        lng = r['Longitude']
        if not (pd.notna(lat) and pd.notna(lng)):
            return False
        try:
            return is_point_in_polygon(float(lat), float(lng), polygon)
        except Exception:
            return False

    mask = df.apply(row_ok, axis=1)
    filtered = df[mask].copy()
    filtered.to_csv(out, index=False)

    print(f"Total rows: {total}")
    print(f"Kept rows strictly inside polygon: {len(filtered)}")
    print(f"Filtered CSV written to: {out}")

    # annotate matches against FINN
    finn_path = Path('data/eiendom/A_live.csv')
    if not finn_path.exists():
        print(f"FINN file not found: {finn_path}")
        raise SystemExit(1)

    finn = pd.read_csv(finn_path)
    filtered['__addr_norm'] = filtered.get('StreetAddress','').fillna('').apply(normalize_addr)
    filtered['__pc_norm'] = filtered.get('PostalCode','').apply(normalize_pc)

    finn['__addr_norm'] = finn.get('Adresse','').fillna('').apply(normalize_addr)
    finn['__pc_norm'] = finn.get('Postnummer','').apply(normalize_pc)

    lookup = {}
    for idx,row in finn.iterrows():
        key = (row['__addr_norm'], row['__pc_norm'])
        if key not in lookup:
            lookup[key] = row.get('Finnkode')

    filtered['MatchedFinn_Finnkode'] = filtered.apply(lambda r: lookup.get((r['__addr_norm'], r['__pc_norm']), ''), axis=1)

    # load into DB
    db = PropertyDatabase()
    inserted, updated = db.insert_or_update_dnbeiendom(filtered)
    print(f"Inserted {inserted}, Updated {updated} into dnbeiendom")

    # Deactivate rows whose URL is no longer listed on DNB Eiendom.
    # Use the full crawled URL list (not just polygon-filtered) so that listings
    # outside the polygon but still live on the site are not prematurely deactivated.
    live_url_path = Path('data/dnbeiendom/0_URLs.csv')
    if live_url_path.exists():
        live_urls = set(
            pd.read_csv(live_url_path)['URL']
            .astype(str).str.strip().str.lower().str.rstrip('/')
        )
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute('SELECT id, url FROM dnbeiendom WHERE active = 1')
        to_deactivate = [
            r[0] for r in cur.fetchall()
            if r[1] and r[1].strip().lower().rstrip('/') not in live_urls
        ]
        if to_deactivate:
            placeholders = ','.join('?' * len(to_deactivate))
            cur.execute(
                f'UPDATE dnbeiendom SET active = 0, updated_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})',
                to_deactivate,
            )
            print(f"Deactivated {len(to_deactivate)} stale rows (sold/removed from site)")
        else:
            print("No stale rows to deactivate")
        conn.commit()
        conn.close()
    else:
        print(f"Warning: {live_url_path} not found; skipping deactivation step")

if __name__ == '__main__':
    main()
