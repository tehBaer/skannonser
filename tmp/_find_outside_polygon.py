#!/usr/bin/env python3
"""
Find all listings in the DB whose coordinates fall outside the FINN search polygon.

Covers:
  - eiendom (active + stale/sold)     → coords in eiendom_processed
  - dnbeiendom                         → coords inline

Prints IDs grouped by table/category and emits DELETE statements ready to copy-paste.
"""
from __future__ import annotations

import math
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Load polygon from run_eiendom_db.py
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from main.tools.finn_polygon_editor import load_defaults_from_source  # noqa: E402

_, POLYGON = load_defaults_from_source(REPO_ROOT / "main/runners/run_eiendom_db.py")
# polygon stores (lng, lat) pairs; is_point_in_polygon expects that convention

DB_PATH = REPO_ROOT / "main/database/properties.db"


# ---------------------------------------------------------------------------
# Geometry helper  (same ray-casting logic as filter scripts)
# ---------------------------------------------------------------------------
def is_point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool:
    """Ray-casting test.  polygon stores (lng, lat) tuples (x=lng, y=lat)."""
    inside = False
    n = len(polygon)
    if n < 3:
        return False
    for i in range(n):
        j = (i - 1) % n
        xi, yi = polygon[i][0], polygon[i][1]   # xi=lng, yi=lat
        xj, yj = polygon[j][0], polygon[j][1]
        if not (math.isfinite(xi) and math.isfinite(yi) and math.isfinite(xj) and math.isfinite(yj)):
            continue
        intersects = ((yi > lat) != (yj > lat)) and (
            lng < ((xj - xi) * (lat - yi)) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
    return inside


def outside(lat, lng) -> bool:
    if lat is None or lng is None:
        return False   # no coords → can't judge, skip
    try:
        return not is_point_in_polygon(float(lat), float(lng), POLYGON)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Query DB
# ---------------------------------------------------------------------------
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ---- eiendom (active, i.e. active=1) ----
active_outside = []
rows = conn.execute(
    "SELECT e.finnkode, ep.lat, ep.lng, e.adresse, e.active "
    "FROM eiendom e "
    "LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode "
    "WHERE e.active = 1"
).fetchall()
for r in rows:
    if outside(r["lat"], r["lng"]):
        active_outside.append(dict(r))

# ---- eiendom sold/stale (active=0, tilgjengelighet solgt/inaktiv) ----
sold_outside = []
rows = conn.execute(
    "SELECT e.finnkode, ep.lat, ep.lng, e.adresse, e.tilgjengelighet "
    "FROM eiendom e "
    "LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode "
    "WHERE e.active = 0 "
    "  AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')"
).fetchall()
for r in rows:
    if outside(r["lat"], r["lng"]):
        sold_outside.append(dict(r))

# ---- dnbeiendom (all active) ----
dnb_outside = []
rows = conn.execute(
    "SELECT id, dnb_id, url, adresse, lat, lng, active FROM dnbeiendom WHERE active = 1"
).fetchall()
for r in rows:
    if outside(r["lat"], r["lng"]):
        dnb_outside.append(dict(r))

conn.close()

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def fmt_eiendom(items, label):
    if not items:
        print(f"\n{label}: none outside polygon ✓")
        return []
    print(f"\n{label}: {len(items)} outside polygon")
    finnkodes = []
    for it in items:
        print(f"  finnkode={it['finnkode']}  lat={it.get('lat')}  lng={it.get('lng')}  addr={it.get('adresse') or it.get('Adresse')}")
        finnkodes.append(it['finnkode'])
    return finnkodes


def fmt_dnb(items, label):
    if not items:
        print(f"\n{label}: none outside polygon ✓")
        return []
    print(f"\n{label}: {len(items)} outside polygon")
    ids = []
    for it in items:
        print(f"  id={it['id']}  dnb_id={it['dnb_id']}  lat={it['lat']}  lng={it['lng']}  addr={it['adresse']}")
        ids.append(it['id'])
    return ids


print("=" * 60)
print(f"DB: {DB_PATH}")
print(f"Polygon: {len(POLYGON)} points")
print("=" * 60)

eie_fks    = fmt_eiendom(active_outside, "eiendom (active/live)")
sold_fks   = fmt_eiendom(sold_outside,   "eiendom (sold/stale)")
dnb_ids    = fmt_dnb(dnb_outside,        "dnbeiendom (active)")

# ---------------------------------------------------------------------------
# Delete statements
# ---------------------------------------------------------------------------
all_bad_fks = eie_fks + sold_fks
print("\n" + "=" * 60)
print("PREPARED DELETE STATEMENTS  (review before executing!)")
print("=" * 60)

if all_bad_fks:
    fk_list = ", ".join(f"'{fk}'" for fk in all_bad_fks)
    print(f"\n-- eiendom + eiendom_processed ({len(all_bad_fks)} rows)")
    print(f"DELETE FROM eiendom_processed WHERE finnkode IN ({fk_list});")
    print(f"DELETE FROM eiendom           WHERE finnkode IN ({fk_list});")
else:
    print("\n-- No eiendom rows to delete")

if dnb_ids:
    id_list = ", ".join(str(i) for i in dnb_ids)
    print(f"\n-- dnbeiendom ({len(dnb_ids)} rows)")
    print(f"DELETE FROM dnbeiendom WHERE id IN ({id_list});")
else:
    print("\n-- No dnbeiendom rows to delete")

print()
