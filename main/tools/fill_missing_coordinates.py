#!/usr/bin/env python3
"""Fill missing LAT/LNG in DB using Google Geocoding API."""
import argparse
import os
import sys
import time
from typing import Optional, Tuple

import requests
import pandas as pd

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


def normalize_postal_code(postal_code: str) -> str:
    """Normalize Norwegian postal codes while preserving leading zeros."""
    raw = str(postal_code or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    if len(digits) <= 4:
        return digits.zfill(4)
    return digits


def _extract_result_country_and_postal(result: dict) -> Tuple[str, str]:
    """Extract country code and postal code from a geocoder result."""
    country = ""
    postal = ""
    for comp in result.get("address_components", []):
        types = comp.get("types", [])
        if "country" in types:
            country = str(comp.get("short_name") or "").upper()
        if "postal_code" in types:
            postal = normalize_postal_code(comp.get("long_name", ""))
    return country, postal


def _result_has_street_level_signal(result: dict) -> bool:
    """Return True when geocoder result looks like a real street-level address."""
    result_types = set(result.get("types", []))
    if result_types.intersection({"street_address", "premise", "subpremise", "route"}):
        return True

    for comp in result.get("address_components", []):
        comp_types = set(comp.get("types", []))
        if comp_types.intersection({"street_number", "route", "premise", "subpremise"}):
            return True

    return False


def geocode_address(address: str, postal_code: str, api_key: str, timeout_sec: float = 10.0) -> Optional[Tuple[float, float]]:
    normalized_postal = normalize_postal_code(postal_code)
    cleaned_address = str(address or "").strip()
    if not cleaned_address:
        return None

    def _request_and_choose(request_postal: Optional[str], strict_postal: bool) -> Optional[dict]:
        query_parts = [cleaned_address, "Norway"]
        if request_postal:
            query_parts.insert(1, request_postal)

        params = {
            "address": ", ".join(query_parts),
            "key": api_key,
            "language": "no",
            "region": "no",
            "components": (
                f"country:NO|postal_code:{request_postal}"
                if strict_postal and request_postal
                else "country:NO"
            ),
        }

        resp = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params=params,
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

        for result in results:
            country, result_postal = _extract_result_country_and_postal(result)
            if country and country != "NO":
                continue
            # Strict pass: require exact postal match when geocoder returns a postal.
            if strict_postal:
                if request_postal and result_postal and result_postal != request_postal:
                    continue
            else:
                # Relaxed/fallback pass: reject low-quality or clearly wrong-region matches.
                if not _result_has_street_level_signal(result):
                    continue

                # When we know the desired postal code, avoid results from clearly different regions.
                if normalized_postal and result_postal:
                    if len(normalized_postal) >= 2 and len(result_postal) >= 2:
                        if normalized_postal[:2] != result_postal[:2]:
                            continue

                # Broad approximate matches often point to country/area centroids.
                location_type = str(result.get("geometry", {}).get("location_type", "")).upper()
                if location_type == "APPROXIMATE" and normalized_postal != result_postal:
                    continue

            return result

        return None

    # First pass: strict (postal + country + postal component + exact postal).
    chosen = _request_and_choose(normalized_postal, True)
    # Second pass: relaxed (postal in query, country component only).
    if not chosen and normalized_postal:
        chosen = _request_and_choose(normalized_postal, False)
    # Final fallback: address + country only.
    if not chosen:
        chosen = _request_and_choose(None, False)

    if not chosen:
        return None

    loc = chosen.get("geometry", {}).get("location", {})
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
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Only print candidate count after filters; do not call API",
    )
    parser.add_argument(
        "--allow-failures",
        action="store_true",
        help="Exit with status 0 even if some listings fail geocoding",
    )
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        print("Missing Google Maps API key. Use --api-key or set GOOGLE_MAPS_API_KEY.")
        return 1

    db = PropertyDatabase(args.db)
    df = db.get_eiendom_missing_coordinates()

    if not args.include_inactive and not df.empty:
        # Keep geocoding scope aligned with what is visible in Sheets by default.
        visible_statuses = {"solgt", "inaktiv"}

        if "Tilgjengelighet" in df.columns:
            status_normalized = (
                df["Tilgjengelighet"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )
        else:
            status_normalized = pd.Series([""] * len(df), index=df.index)

        df = df[~status_normalized.isin(visible_statuses)]

    if args.limit > 0:
        df = df.head(args.limit)

    total = len(df)
    print("=" * 72)
    print("Fill Missing Coordinates")
    print("=" * 72)
    print(f"Candidates: {total}")
    print(f"Dry run: {args.dry_run}")

    if args.count_only:
        print("Count-only mode: no API requests will be sent.")
        return 0

    if total == 0:
        print("Nothing to geocode.")
        return 0

    if args.rpm <= 0:
        print("--rpm must be > 0")
        return 1

    sleep_sec = 60.0 / args.rpm
    started_at = time.time()

    ok = 0
    failed = 0
    skipped = 0

    def _format_duration(seconds: float) -> str:
        total_sec = max(int(seconds), 0)
        hours, rem = divmod(total_sec, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    interrupted = False
    try:
        for seq, (_, row) in enumerate(df.iterrows(), start=1):
            elapsed = time.time() - started_at
            processed_before = seq - 1
            avg_per_row = elapsed / processed_before if processed_before > 0 else 0.0
            remaining_rows = total - processed_before
            eta = avg_per_row * remaining_rows if avg_per_row > 0 else 0.0
            pct = (processed_before / total) * 100 if total else 100.0

            finnkode = str(row.get("Finnkode", "")).strip()
            adresse = str(row.get("ADRESSE", "")).strip()
            postnummer = str(row.get("Postnummer", "")).strip()

            progress_prefix = (
                f"[{seq}/{total} | {pct:5.1f}% | elapsed {_format_duration(elapsed)} | "
                f"eta {_format_duration(eta)}]"
            )

            if not finnkode or not adresse:
                skipped += 1
                print(f"{progress_prefix} - SKIP #{finnkode or '?'} missing finnkode/address")
                continue

            result = geocode_address(adresse, postnummer, api_key)
            if not result:
                failed += 1
                print(f"{progress_prefix} - FAIL #{finnkode} {adresse}")
                time.sleep(sleep_sec)
                continue

            lat, lng = result
            if args.dry_run:
                print(f"{progress_prefix} - DRY #{finnkode} -> ({lat:.6f}, {lng:.6f})")
                ok += 1
            else:
                # Each successful row is committed immediately in set_eiendom_coordinates().
                changed = db.set_eiendom_coordinates(finnkode, lat, lng)
                if changed:
                    ok += 1
                    print(f"{progress_prefix} - OK  #{finnkode} -> ({lat:.6f}, {lng:.6f})")
                else:
                    failed += 1
                    print(f"{progress_prefix} - FAIL #{finnkode} DB update")

            time.sleep(sleep_sec)
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted by user (Ctrl+C). Already-saved rows are preserved.")

    print("\nSummary")
    print(f"  Updated: {ok}")
    print(f"  Failed:  {failed}")
    print(f"  Skipped: {skipped}")
    print(f"  Elapsed: {_format_duration(time.time() - started_at)}")

    if interrupted:
        return 130

    if failed > 0 and args.allow_failures:
        print("Completed with failures, but --allow-failures is set; returning success.")
        return 0

    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
