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


# GAM ownership_type enum -> Norwegian display value, used only when the
# key-info <dd> is absent. An unknown enum is stored raw rather than lost.
_OWNERSHIP_ENUM = {
    "FREEHOLD": "Eier (selveier)",
    "PART_OWNERSHIP": "Andel",
    "STOCK": "Aksje",
}


def _first_int(targeting: dict, key: str) -> int | None:
    values = targeting.get(key) or []
    try:
        return int(str(values[0]))
    except (IndexError, ValueError, TypeError):
        return None


def _eieform(soup, targeting: dict) -> str | None:
    element = soup.find(attrs={"data-testid": "info-ownership-type"})
    if element is not None:
        dd = element.find("dd")
        if dd is not None:
            text = dd.get_text(strip=True)
            if text:
                return text
    values = targeting.get("ownership_type") or []
    if values:
        raw = str(values[0])
        return _OWNERSHIP_ENUM.get(raw, raw)
    return None


def _nabolag(soup) -> str | None:
    element = soup.find(attrs={"data-testid": "local-area-name"})
    if element is None:
        return None
    return element.get_text(strip=True) or None


def _energy(soup) -> tuple[str | None, str | None]:
    """'Energimerking A - Mørkegrønn' -> ('A', 'Mørkegrønn'). A bare
    'Energimerking' heading (grade missing on the ad) -> (None, None)."""
    element = soup.find(attrs={"data-testid": "energy-label"})
    if element is None:
        return None, None
    text = element.get_text(" ", strip=True)
    text = re.sub(r"^Energimerking\s*", "", text).strip()
    if not text:
        return None, None
    if " - " in text:
        letter, colour = text.split(" - ", 1)
        return letter.strip() or None, colour.strip() or None
    return text, None


# dt label -> ListingDetails field, exactly as they appear in the
# pricing-details <dl> (verified against the 12 golden fixtures).
_PRICING_LABELS = {
    "Totalpris": "totalpris",
    "Omkostninger": "omkostninger",
    "Fellesgjeld": "fellesgjeld",
    "Felleskost/mnd.": "felleskost_mnd",
    "Fellesformue": "fellesformue",
    "Formuesverdi": "formuesverdi",
    "Kommunale avg.": "kommunale_avg_aar",
}


def _parse_kr(text: str | None) -> int | None:
    """'1\xa0945\xa0000 kr' -> 1945000. Tolerates a trailing 'per år'
    (kommunale avg.). None when no kr-amount is found."""
    match = re.search(r"([\d\xa0\s]+)\s*kr", text or "")
    if not match:
        return None
    digits = match.group(1).replace("\xa0", "").replace(" ", "")
    try:
        return int(digits)
    except ValueError:
        return None


def _pricing_details(soup) -> dict:
    out: dict = {}
    section = soup.find(attrs={"data-testid": "pricing-details"})
    if section is None:
        return out
    for dt in section.find_all("dt"):
        field = _PRICING_LABELS.get(dt.get_text(strip=True))
        if field is None:
            continue
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        value = _parse_kr(dd.get_text())
        if value is not None:
            out[field] = value
    return out


def parse_details(html: str, finnkode: str) -> ListingDetails:
    soup = BeautifulSoup(html, "html.parser")
    targeting = _gam_targeting(soup)
    energimerke, energifarge = _energy(soup)
    return ListingDetails(
        finnkode=finnkode,
        bedrooms=_first_int(targeting, "bedrooms"),
        rooms=_first_int(targeting, "rooms"),
        floor=_first_int(targeting, "floor"),
        eieform=_eieform(soup, targeting),
        nabolag=_nabolag(soup),
        energimerke=energimerke,
        energifarge=energifarge,
        **_pricing_details(soup),
    )
