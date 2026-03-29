#!/usr/bin/env python3
"""Fill missing station LAT/LNG using Google Geocoding API.

By default reads candidates from the SQLite DB (source of truth) and writes
results back there.  Pass --csv to fall back to the legacy CSV-only workflow.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Allow importing main.* when executed from the scripts folder.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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


def geocode_query(query: str, api_key: str, timeout_sec: float = 20.0) -> Tuple[Optional[float], Optional[float], str]:
    params = urllib.parse.urlencode(
        {
            "address": query,
            "key": api_key,
            "language": "no",
            "region": "no",
        }
    )
    url = "https://maps.googleapis.com/maps/api/geocode/json?" + params

    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None, None, "REQUEST_FAILED"

    status = str(payload.get("status") or "UNKNOWN")
    if status != "OK":
        return None, None, status

    results = payload.get("results") or []
    if not results:
        return None, None, "NO_RESULTS"

    loc = (results[0].get("geometry") or {}).get("location") or {}
    lat = loc.get("lat")
    lng = loc.get("lng")

    if lat is None or lng is None:
        return None, None, "MISSING_LOCATION"

    return float(lat), float(lng), "OK"


def candidate_queries(station_name: str) -> List[str]:
    base = station_name.strip()
    queries = [
        f"{base} stasjon, Norway",
        f"{base} station, Norway",
        f"{base} train station",
    ]

    # This name is explicitly Swedish and needs a Sweden fallback.
    if base.casefold() == "g\u00f6teborg c":
        queries.append(f"{base} station, Sweden")

    return queries


def main() -> int:
    parser = argparse.ArgumentParser(description="Fill missing station coordinates from Google Geocoding API")
    parser.add_argument(
        "--csv",
        default=None,
        help=(
            "Legacy: path to stations CSV. When supplied, reads/writes that CSV "
            "instead of the DB. Defaults to DB mode."
        ),
    )
    parser.add_argument("--api-key", help="Override Google Maps API key")
    parser.add_argument("--rpm", type=float, default=60.0, help="Rate limit requests per minute")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist any changes")
    parser.add_argument(
        "--report",
        default="tmp/stations_geocode_report.csv",
        help="Write per-station geocode status report",
    )
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        print("Missing Google Maps API key. Use --api-key or set GOOGLE_MAPS_API_KEY.")
        return 1

    report_path = Path(args.report)
    delay_sec = 60.0 / max(args.rpm, 1.0)

    # ------------------------------------------------------------------ #
    # DB mode (default)                                                    #
    # ------------------------------------------------------------------ #
    if args.csv is None:
        from main.database.stations import StationDatabase

        db = StationDatabase()
        candidates_db = db.get_stations_missing_coords()
        print(f"Missing station coordinates (DB): {len(candidates_db)}")

        report_rows: List[Dict[str, str]] = []
        updated = 0
        failed = 0

        for idx, (station_id, name) in enumerate(candidates_db, start=1):
            lat = None
            lng = None
            final_status = "NO_RESULTS"
            chosen_query = ""

            for query in candidate_queries(name):
                q_lat, q_lng, status = geocode_query(query, api_key)
                chosen_query = query
                final_status = status
                if status == "OK" and q_lat is not None and q_lng is not None:
                    lat, lng = q_lat, q_lng
                    break
                time.sleep(delay_sec)

            if lat is not None and lng is not None:
                updated += 1
                report_rows.append({"Name": name, "Status": "OK", "Query": chosen_query})
                print(f"[{idx}/{len(candidates_db)}] OK    {name} -> {lat:.6f}, {lng:.6f}")
                if not args.dry_run:
                    db.set_station_coords(name, lat, lng)
            else:
                failed += 1
                report_rows.append({"Name": name, "Status": final_status, "Query": chosen_query})
                print(f"[{idx}/{len(candidates_db)}] FAIL  {name} -> {final_status}")

            time.sleep(delay_sec)

        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["Name", "Status", "Query"])
            writer.writeheader()
            writer.writerows(report_rows)

        if args.dry_run:
            print("Dry run complete. DB not modified.")
        print(f"Updated: {updated} | Failed: {failed} | Report: {report_path}")
        return 0

    # ------------------------------------------------------------------ #
    # Legacy CSV mode                                                      #
    # ------------------------------------------------------------------ #
    csv_path = Path(args.csv)

    if not csv_path.exists():
        print(f"Stations CSV not found: {csv_path}")
        return 1

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows: List[Dict[str, str]] = list(reader)

    required = {"Name", "LAT", "LNG"}
    if not required.issubset(set(fieldnames)):
        print(f"CSV must contain headers: {sorted(required)}")
        return 1

    report_rows_csv: List[Dict[str, str]] = []
    candidates = [r for r in rows if not (str(r.get("LAT", "")).strip() and str(r.get("LNG", "")).strip())]
    print(f"Missing station coordinates (CSV): {len(candidates)}")

    updated_csv = 0
    failed_csv = 0

    for idx, row in enumerate(candidates, start=1):
        name = str(row.get("Name") or "").strip()
        if not name:
            failed_csv += 1
            report_rows_csv.append({"Name": "", "Status": "MISSING_NAME", "Query": ""})
            continue

        lat = None
        lng = None
        final_status = "NO_RESULTS"
        chosen_query = ""

        for query in candidate_queries(name):
            q_lat, q_lng, status = geocode_query(query, api_key)
            chosen_query = query
            final_status = status
            if status == "OK" and q_lat is not None and q_lng is not None:
                lat, lng = q_lat, q_lng
                break
            time.sleep(delay_sec)

        if lat is not None and lng is not None:
            row["LAT"] = str(lat)
            row["LNG"] = str(lng)
            updated_csv += 1
            report_rows_csv.append({"Name": name, "Status": "OK", "Query": chosen_query})
            print(f"[{idx}/{len(candidates)}] OK    {name} -> {lat:.6f}, {lng:.6f}")
        else:
            failed_csv += 1
            report_rows_csv.append({"Name": name, "Status": final_status, "Query": chosen_query})
            print(f"[{idx}/{len(candidates)}] FAIL  {name} -> {final_status}")

        time.sleep(delay_sec)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "Status", "Query"])
        writer.writeheader()
        writer.writerows(report_rows_csv)

    if args.dry_run:
        print("Dry run complete. CSV not modified.")
    else:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Updated CSV: {csv_path}")

    print(f"Updated: {updated_csv} | Failed: {failed_csv} | Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
