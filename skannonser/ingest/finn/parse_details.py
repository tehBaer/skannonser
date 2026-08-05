"""FINN ad HTML -> ListingDetails: the group-A/B/C enrichment fields
(2026-07-23 listing-details design spec).

Deliberately SEPARATE from `parse.py`/`NormalizedListing`: that model is a
frozen legacy contract (AST-pinned by test). This module owns everything
new. Every field is optional and every extractor is null-tolerant -- a
parse failure on any field yields None for that field, and `parse_details`
itself never raises on arbitrary HTML (worst case: an all-NULL row).
"""
import re

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from skannonser.ingest.finn.gam import gam_targeting


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
    eiendomsskatt_kr: int | None = None
    verditakst: int | None = None
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


# GAM ownership_type enum -> Norwegian display value, used only when the
# key-info <dd> is absent. An unknown enum is stored raw rather than lost.
_OWNERSHIP_ENUM = {
    "FREEHOLD": "Selveier",
    "PART_OWNERSHIP": "Andel",
    "STOCK": "Aksje",
}


def _first_int(targeting: dict, key: str) -> int | None:
    values = targeting.get(key) or []
    try:
        return int(str(values[0]))
    except (IndexError, ValueError, TypeError):
        return None


def _canonicalize_eieform(text: str) -> str:
    """Real ads spell freehold three ways -- 'Selveier', 'Eier (Selveier)',
    and our own GAM-enum fallback 'Eier (selveier)' -- all mean the same
    thing and are canonicalized to 'Selveier'. Everything else ('Andel',
    'Aksje', ...) passes through unchanged."""
    if text.casefold() in ("eier (selveier)", "selveier"):
        return "Selveier"
    return text


def _eieform(soup, targeting: dict) -> str | None:
    element = soup.find(attrs={"data-testid": "info-ownership-type"})
    if element is not None:
        dd = element.find("dd")
        if dd is not None:
            text = dd.get_text(strip=True)
            if text:
                return _canonicalize_eieform(text)
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


# The svg badge's aria-label, e.g. 'Energimerke E'. Anchored and limited to the
# seven real Norwegian grades: an unrecognized shape must stay None rather than
# be stored as a grade -- a bare 'Energimerke' (no letter) is a real case.
_ENERGY_ARIA_RE = re.compile(r"^Energimerke\s+([A-G])$")


def _energy_from_svg(element) -> str | None:
    """FINN renders the grade two ways. The second draws it as an <svg> badge
    whose only textual trace is ``aria-label="Energimerke E"`` -- ``get_text()``
    returns the bare 'Energimerking' heading, so the letter is invisible to the
    text path in ``_energy``. 47 % of the crawled corpus (3712 of 7867 ads) uses
    this variant; until it was read those ads stored a NULL ``energimerke`` and
    then passed every Energimerking filter as "unknown" (``includeUnknown``
    defaults true), so picking 'A' returned C/D/E/G listings."""
    for svg in element.find_all("svg"):
        match = _ENERGY_ARIA_RE.match((svg.get("aria-label") or "").strip())
        if match:
            return match.group(1)
    return None


def _energy(soup) -> tuple[str | None, str | None]:
    """'Energimerking A - Mørkegrønn' -> ('A', 'Mørkegrønn'). A bare
    'Energimerking' heading (grade missing on the ad) -> (None, None).
    Some ads carry a colour with no letter -- 'Energimerking - Oransje'
    (leading dash after stripping the prefix) -> (None, 'Oransje').
    When the visible text yields no letter, the svg badge's aria-label is the
    fallback (see ``_energy_from_svg``), mirroring how ``_eieform`` falls back
    to the GAM blob."""
    element = soup.find(attrs={"data-testid": "energy-label"})
    if element is None:
        return None, None
    text = element.get_text(" ", strip=True)
    text = re.sub(r"^Energimerking\s*", "", text).strip()
    letter: str | None = None
    colour: str | None = None
    if text.startswith("-"):
        colour = text.lstrip("-").strip() or None
    elif " - " in text:
        head, tail = text.split(" - ", 1)
        letter, colour = head.strip() or None, tail.strip() or None
    elif text:
        letter = text
    # Fallback only -- the visible text is the richer source (it is the one that
    # also carries the colour), and the badge supplies no Norwegian colour name
    # of its own, only raw hex fills. So the colour is left unknown rather than
    # inferred from the letter: FINN itself pairs them inconsistently (fixture
    # 466043223 ships 'G - Mørkegrønn').
    if letter is None:
        letter = _energy_from_svg(element)
    return letter, colour


# dt label -> ListingDetails field, exactly as they appear in the
# pricing-details <dl>. The first seven were verified against the 12 golden
# fixtures; `Eiendomsskatt` (8% of ads) and `Verditakst` (3%) were added later
# and are covered by fixtures 463763329 and 447401579, since no ad among the
# original 12 carries either label.
_PRICING_LABELS = {
    "Totalpris": "totalpris",
    "Omkostninger": "omkostninger",
    "Fellesgjeld": "fellesgjeld",
    "Felleskost/mnd.": "felleskost_mnd",
    "Fellesformue": "fellesformue",
    "Formuesverdi": "formuesverdi",
    "Kommunale avg.": "kommunale_avg_aar",
    "Eiendomsskatt": "eiendomsskatt_kr",
    "Verditakst": "verditakst",
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


def _facilities(soup) -> list[str]:
    """The Fasiliteter grid: leaf <div>s inside the section, deduped,
    document order preserved (a bounded controlled vocabulary -- 26 distinct
    values across the 12 fixtures)."""
    section = soup.find(attrs={"data-testid": "object-facilities"})
    if section is None:
        return []
    out: list[str] = []
    for div in section.find_all("div"):
        if div.find("div") is not None:  # container, not a facility cell
            continue
        text = div.get_text(strip=True)
        if text and text not in out:
            out.append(text)
    return out


# cadastre-info row label -> ListingDetails field. Values stay TEXT --
# matrikkel numbers are identity keys, not quantities.
_CADASTRE_LABELS = {
    "Kommunenr": "kommunenr",
    "Gårdsnr": "gardsnr",
    "Bruksnr": "bruksnr",
    "Seksjonsnr": "seksjonsnr",
    "Borettslag-navn": "borettslag_navn",
    "Borettslag-orgnummer": "borettslag_orgnr",
    "Borettslag-andelsnummer": "borettslag_andelsnr",
}


def _cadastre(soup) -> dict:
    out: dict = {}
    section = soup.find(attrs={"data-testid": "cadastre-info"})
    if section is None:
        return out
    for div in section.find_all("div"):
        if div.find("div") is not None:  # only leaf rows carry one label:value
            continue
        match = re.match(r"([^:]+?)\s*:\s*(\S.*)$", div.get_text(" ", strip=True))
        if not match:
            continue
        field = _CADASTRE_LABELS.get(match.group(1).strip())
        if field and field not in out:
            out[field] = match.group(2).strip()
    return out


def parse_details(html: str, finnkode: str) -> ListingDetails:
    soup = BeautifulSoup(html, "html.parser")
    targeting = gam_targeting(soup)
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
        facilities=_facilities(soup),
        **_pricing_details(soup),
        **_cadastre(soup),
    )
