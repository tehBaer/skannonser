"""FINN ad HTML -> ListingDetails: the group-A/B/C enrichment fields
(2026-07-23 listing-details design spec).

Deliberately SEPARATE from `parse.py`/`NormalizedListing`: that model is a
frozen legacy contract (AST-pinned by test). This module owns everything
new. Every field is optional and every extractor is null-tolerant -- a
parse failure on any field yields None for that field, and `parse_details`
itself never raises on arbitrary HTML (worst case: an all-NULL row).
"""
import json
import re

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field


class ListingDetails(BaseModel):
    finnkode: str
    # Group A -- rooms / location (GAM targeting JSON)
    bedrooms: int | None = None
    rooms: int | None = None
    floor: int | None = None
    eieform: str | None = None
    nabolag: str | None = None
    # Group B -- money (pricing-details <dl>)
    totalpris: int | None = None
    omkostninger: int | None = None
    fellesgjeld: int | None = None
    felleskost_mnd: int | None = None
    fellesformue: int | None = None
    formuesverdi: int | None = None
    kommunale_avg_aar: int | None = None
    # Group C -- condition / identity
    energimerke: str | None = None
    energifarge: str | None = None
    kommunenr: str | None = None
    gardsnr: str | None = None
    bruksnr: str | None = None
    seksjonsnr: str | None = None
    borettslag_navn: str | None = None
    borettslag_orgnr: str | None = None
    borettslag_andelsnr: str | None = None
    facilities: list[str] = Field(default_factory=list)


def _gam_targeting(soup) -> dict[str, list]:
    """The GAM ad-targeting key/value pairs from the
    `advertising-initial-state` JSON blob -- typed data FINN itself ships on
    every ad page. `{}` on any structural surprise."""
    script = soup.find("script", {"id": "advertising-initial-state"})
    if script is None or not script.string:
        return {}
    try:
        data = json.loads(script.string)
        targeting = data["config"]["adServer"]["gam"]["targeting"]
        return {
            t["key"]: t["value"]
            for t in targeting
            if isinstance(t, dict) and "key" in t and isinstance(t.get("value"), list)
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}


def _first_int(targeting: dict, key: str) -> int | None:
    values = targeting.get(key) or []
    try:
        return int(str(values[0]))
    except (IndexError, ValueError, TypeError):
        return None


def parse_details(html: str, finnkode: str) -> ListingDetails:
    soup = BeautifulSoup(html, "html.parser")
    targeting = _gam_targeting(soup)
    return ListingDetails(
        finnkode=finnkode,
        bedrooms=_first_int(targeting, "bedrooms"),
        rooms=_first_int(targeting, "rooms"),
        floor=_first_int(targeting, "floor"),
    )
