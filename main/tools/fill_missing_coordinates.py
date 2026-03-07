#!/usr/bin/env python3
"""Fill missing LAT/LNG in DB using Google Geocoding API."""
import argparse
import os
import sys
import time
from typing import Optional, Tuple

import requests

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


def resolve_api_key(cli_key: Optional[str]) -> str:
    if cli_key:
        return cli_key.strip()

    env_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if env_key:
        return env_key

    try:
        from main.config.config import GOOGLE_MAPS_API_KEY
        if GOOGLE_MAPS_API_KEY:
            return str(GOOGLE_MAPS_API_KEY).strip()
    except Exception:
        pass

    return ""


def geocode_address(address: str, postal_code: str, api_key: str, timeout_sec: float = 10.0) -> Optional[Tuple[float, float]]:
    query = ", ".join([p for p in [str(address or "").strip(), str(postal_code or "").strip(), "Norway"] if p])
    if not query:
        return None

    resp = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": query, "key": api_key},
        timeout=timeout_sec,
    )

    if resp.status_code != 200:
        return None

    payload = resp.json()
    if payload.get("status") != "OK":
        return None

    results = payload.get("results", [])
    if not results:
        return None

    loc = results[0].get("geometry", {}).get("location", {})
    lat = loc.get("lat")
    lng = loc.get("lng")
    if lat is None or lng is None:
        return None

    return float(lat), float(lng)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fill missing coordinates in DB from Google Geocoding API")
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument("--api-key", help="Override Google Maps API key")
    parser.add_argument("--limit", type=int, default=100, help="Max listings to attempt per run (default: 100)")
    parser.add_argument("--rpm", type=float, default=60.0, help="Request rate limit per minute (default: 60)")
    parser.add_argument("--include-inactive", action="store_true", help="Also geocode inactive listings")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to DB")
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        print("Missing Google Maps API key. Use --api-key or set GOOGLE_MAPS_API_KEY.")
        return 1

    db = PropertyDatabase(args.db)
    df = db.get_eiendom_missing_coordinates()

    if not args.include_inactive and not df.empty:
        df = df[df["IsActive"].fillna(0).astype(int) == 1]

    if args.limit > 0:
        df = df.head(args.limit)

    total = len(df)
    print("=" * 72)
    print("Fill Missing Coordinates")
    print("=" * 72)
    print(f"Candidates: {total}")
    print(f"Dry run: {args.dry_run}")

    if total == 0:
        print("Nothing to geocode.")
        return 0

    if args.rpm <= 0:
        print("--rpm must be > 0")
        return 1

    sleep_sec = 60.0 / args.rpm

    ok = 0
    failed = 0
    skipped = 0

    for i, row in df.iterrows():
        finnkode = str(row.get("Finnkode", "")).strip()
        adresse = str(row.get("ADRESSE", "")).strip()
        postnummer = str(row.get("Postnummer", "")).strip()

        if not finnkode or not adresse:
            skipped += 1
            print(f"- SKIP #{finnkode or '?'} missing finnkode/address")
            continue

        result = geocode_address(adresse, postnummer, api_key)
        if not result:
            failed += 1
            print(f"- FAIL #{finnkode} {adresse}")
            time.sleep(sleep_sec)
            continue

        lat, lng = result
        if args.dry_run:
            print(f"- DRY #{finnkode} -> ({lat:.6f}, {lng:.6f})")
            ok += 1
        else:
            changed = db.set_eiendom_coordinates(finnkode, lat, lng)
            if changed:
                ok += 1
                print(f"- OK  #{finnkode} -> ({lat:.6f}, {lng:.6f})")
            else:
                failed += 1
                print(f"- FAIL #{finnkode} DB update")

        time.sleep(sleep_sec)

    print("\nSummary")
    print(f"  Updated: {ok}")
    print(f"  Failed:  {failed}")
    print(f"  Skipped: {skipped}")

    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
