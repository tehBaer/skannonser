"""Google Geocoding API client for filling missing lat/lng, and the
`run geocode` batch driver.

Ports `main/tools/fill_missing_coordinates.py`'s three-pass Norway geocoding
strategy (`geocode_address`, lines 78-164, plus its private helpers) and the
per-row driver loop (`main`, lines 167-353), routed through the shared
`Gateway` for rate limiting, monthly budget enforcement, and the `api_usage`
ledger -- exactly like `skannonser.enrich.travel_api.TransitCommute` does for
the Routes API.

Three-pass strategy, in order, stopping at the first accepted result:

  1. strict   -- postal code in the query string AND in
                 `components=country:NO|postal_code:{postal}`; only a result
                 whose own postal component exactly matches is accepted.
  2. relaxed  -- postal code still in the query string, but
                 `components=country:NO` only (no postal filter at the API
                 level); the result must show a street-level signal (a
                 street_address/premise/subpremise/route result type, or a
                 street_number/route/premise/subpremise address component),
                 must not fall in a clearly different postal region (first
                 two digits of the normalized postal code), and must not be
                 an APPROXIMATE geometry (unless it happens to match the
                 postal exactly).
  3. fallback -- address + "Norway" only, `components=country:NO`, same
                 relaxed acceptance rules (no postal code to check against).

A result whose country component resolves to something other than "NO" is
rejected in every pass. `geocode_address` returns `None` only once all three
passes are exhausted -- that is the "definitive miss" `run_geocode` maps to
`ProcessedRepo.mark_geocode_failed`.
"""

import sqlite3
from typing import Optional

import requests

from skannonser.config.domain import DomainConfig
from skannonser.gateway import Gateway
from skannonser.store.repositories.processed import ProcessedRepo

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def normalize_postal_code(postal_code) -> str:
    """Normalize Norwegian postal codes while preserving leading zeros.

    Port of `fill_missing_coordinates.py:normalize_postal_code` (40-48).
    """
    raw = str(postal_code or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    if len(digits) <= 4:
        return digits.zfill(4)
    return digits


def _extract_result_country_and_postal(result: dict) -> tuple[str, str]:
    """Extract country code and postal code from a geocoder result.

    Port of `fill_missing_coordinates.py:_extract_result_country_and_postal`
    (51-61).
    """
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
    """Return True when geocoder result looks like a real street-level address.

    Port of `fill_missing_coordinates.py:_result_has_street_level_signal`
    (64-75).
    """
    result_types = set(result.get("types", []))
    if result_types.intersection({"street_address", "premise", "subpremise", "route"}):
        return True

    for comp in result.get("address_components", []):
        comp_types = set(comp.get("types", []))
        if comp_types.intersection({"street_number", "route", "premise", "subpremise"}):
            return True

    return False


def geocode_address(
    address: str,
    postal_code: str,
    api_key: str,
    gateway: Gateway,
    get=requests.get,
) -> Optional[tuple[float, float]]:
    """Geocode a Norwegian address via the three-pass strategy described in
    the module docstring. Returns `(lat, lng)` or `None` on a definitive miss.

    Every HTTP GET is issued through `gateway.call("geocode", fn)` -- rate
    limiting and monthly budget enforcement live there. `BudgetExceeded`
    propagates out untouched (it is an administrative stop, not a per-row
    geocoding failure).

    Port of `fill_missing_coordinates.py:geocode_address` (78-164).
    """
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

        def fn():
            return get(GEOCODE_URL, params=params, timeout=10.0)

        resp = gateway.call("geocode", fn)

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


def run_geocode(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    gateway: Gateway,
    api_key: str,
    limit: int = 0,
    include_inactive: bool = False,
    get=requests.get,
) -> dict:
    """Batch-geocode listings missing coordinates.

    Candidates come from `ProcessedRepo.missing_coordinates(include_inactive)`
    (already excludes rows previously marked `geocode_failed`). `limit > 0`
    caps how many candidates are attempted this run (0 = no cap). A
    successful geocode is written via `ProcessedRepo.set_coordinates`
    (which itself does the out-of-bounds lat/lng swap-and-recheck); a
    definitive three-pass miss is recorded via
    `ProcessedRepo.mark_geocode_failed`, mirroring the nightly
    `--allow-failures` behavior in the legacy CLI tool.

    Rate limiting and monthly budget enforcement come entirely from
    `gateway` (`geocode_rpm` / `geocode_monthly_cap`) -- no second sleep is
    added here. `BudgetExceeded` propagates out untouched: the row being
    processed when the budget is hit is left as-is (not marked failed), and
    the caller (the CLI) is responsible for exiting non-zero.

    `domain` is accepted for symmetry with the other `run_*` pipeline entry
    points (e.g. `run_finn_ingest`); geocoding itself needs only the
    candidate rows, the api key, and the gateway.
    """
    repo = ProcessedRepo(conn)
    candidates = repo.missing_coordinates(include_inactive)
    if limit > 0:
        candidates = candidates[:limit]

    stats = {"candidates": len(candidates), "geocoded": 0, "failed": 0}

    for row in candidates:
        finnkode = str(row.get("Finnkode") or "").strip()
        address = str(row.get("ADRESSE") or "").strip()
        postal = str(row.get("Postnummer") or "").strip()

        if not finnkode or not address:
            stats["failed"] += 1
            continue

        result = geocode_address(address, postal, api_key, gateway, get=get)
        if result is None:
            repo.mark_geocode_failed(finnkode)
            stats["failed"] += 1
            continue

        lat, lng = result
        repo.set_coordinates(finnkode, lat, lng)
        stats["geocoded"] += 1

    return stats
