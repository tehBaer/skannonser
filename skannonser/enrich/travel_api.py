"""Google Routes API client for public-transit commute times.

Ports `main.location_features._next_monday_iso` and the request-construction /
response-parsing behavior of `main.location_features.PublicTransitCommuteTime`
(TRANSIT-mode only), routed through the shared `Gateway` for rate limiting,
budget enforcement, and the `api_usage` ledger.
"""
from datetime import datetime, timedelta
from typing import Any, Optional

import requests

from skannonser.enrich.sentinels import TRAVEL_API_ERROR, TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC
from skannonser.gateway import BudgetExceeded, Gateway

ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"


def next_monday_iso(hour: int, minute: int = 0) -> str:
    """Return ISO timestamp (UTC-formatted) for next Monday at given hour/minute."""
    now = datetime.now()
    days_ahead = 0 - now.weekday()  # Monday is 0
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    next_monday = now + timedelta(days=days_ahead)
    departure_time = next_monday.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return departure_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_duration_minutes(duration_value: Any) -> Optional[int]:
    if duration_value is None:
        return None
    text = str(duration_value).strip().lower()
    if not text:
        return None
    if text.endswith("s"):
        text = text[:-1]
    try:
        seconds = float(text)
        if seconds <= 0:
            return None
        return int(seconds / 60)
    except Exception:
        return None


def _is_reasonable(minutes: Optional[int], max_minutes: int) -> bool:
    if minutes is None:
        return False
    return 1 <= int(minutes) <= max_minutes


class TransitCommute:
    """Public-transit commute time to a fixed destination, via the Routes API."""

    def __init__(
        self,
        destination_address: str,
        gateway: Gateway,
        api_key: str,
        post=requests.post,
        max_minutes: int = 360,
    ):
        self.destination_address = destination_address
        self.gateway = gateway
        self.api_key = api_key
        self.post = post
        self.max_minutes = max_minutes

    def build_request(self, address: str, postnummer: Optional[str] = None) -> tuple[str, dict, dict]:
        origin = f"{address}, {postnummer}, Norway" if postnummer else f"{address}, Norway"

        destination = self.destination_address
        if destination and "Norway" not in destination and "Norge" not in destination:
            destination = f"{destination}, Norway"

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "routes.duration,routes.distanceMeters",
        }
        body = {
            "origin": {"address": origin},
            "destination": {"address": destination},
            "travelMode": "TRANSIT",
            "departureTime": next_monday_iso(8),
        }
        return ROUTES_URL, headers, body

    def minutes(self, address: str, postnummer: Optional[str] = None) -> Optional[int]:
        """Return the transit commute time in minutes, or a sentinel/None.

        Raises BudgetExceeded when the monthly routes budget is exhausted —
        callers must halt and leave rows untouched.
        """
        if not self.api_key:
            return None

        url, headers, body = self.build_request(address, postnummer)

        def fn():
            return self.post(url, headers=headers, json=body, timeout=10)

        try:
            response = self.gateway.call("routes", fn)

            if response.status_code != 200:
                return None

            data = response.json()
            routes = data.get("routes")
            if not routes:
                return TRAVEL_NO_ROUTES

            route = routes[0]
            if "duration" not in route:
                return TRAVEL_NO_ROUTES

            m = _parse_duration_minutes(route["duration"])
            if not _is_reasonable(m, self.max_minutes):
                return TRAVEL_UNREALISTIC
            return m
        except BudgetExceeded:
            raise
        except Exception:
            return TRAVEL_API_ERROR
