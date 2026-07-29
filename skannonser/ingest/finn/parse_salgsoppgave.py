"""Salgsoppgave prose -> typed scalars (2026-07-27 design spec).

Deliberately separate from `parse_details.py`: that module scrapes the ad's
DOM, this one reads the embedded app-state payload. Different source,
different failure modes.

Every field is optional and every extractor is null-tolerant --
`parse_salgsoppgave` never raises on arbitrary HTML; worst case is an
all-NULL row. Enum fields emit a member of their vocabulary or None, never
free text.
"""
import re

from pydantic import BaseModel

from skannonser.ingest.finn.payload import Section, decode_ad, sections


class Salgsoppgave(BaseModel):
    finnkode: str
    boligselgerforsikring: bool | None = None
    eiendomsskatt_kr: int | None = None
    ferdigattest: str | None = None      # 'ferdigattest' | 'midlertidig' | 'ingen'
    radon_omtalt: bool | None = None
    utleie: str | None = None            # 'tillatt' | 'ikke_tillatt' | 'egen_enhet'
    husdyr: str | None = None            # 'tillatt' | 'krever_godkjenning' | 'ikke_tillatt'
    heftelser: bool | None = None


_KR = r"(?:kr\.?\s*)?([\d][\d\s .]*)"


def _kr_int(raw: str | None) -> int | None:
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    try:
        value = int(digits)
    except ValueError:
        return None
    return value if 0 < value < 100_000_000 else None


_EIENDOMSSKATT = re.compile(
    r"eiendomsskatt\w*[^.]{0,80}?" + _KR, re.I
)


def _eiendomsskatt(text: str) -> int | None:
    """'Eiendomsskatten er kr. 1 827,-' -> 1827. The <dl> source in
    listing_details is preferred where present; this covers the ~32% of ads
    that only state it in prose."""
    match = _EIENDOMSSKATT.search(text)
    return _kr_int(match.group(1)) if match else None


_SELGERFORSIKRING_NOT = re.compile(
    r"har ikke tegnet\s+(?:bolig)?selgerforsikring|ingen boligselgerforsikring", re.I
)
_SELGERFORSIKRING_HAS = re.compile(
    r"(?:selger|det)\s+har tegnet\s+(?:bolig)?selgerforsikring"
    r"|boligselgerforsikring er tegnet",
    re.I,
)


def _boligselgerforsikring(text: str) -> bool | None:
    """From prose, NOT from ad.changeOfOwnershipInsurance -- that flag reads
    False on ~96% of ads regardless of what the prose says (verified over 300
    ads, 2026-07-27), so it would be wrong far more often than right.
    Negative pattern first: 'har ikke tegnet' contains 'har tegnet'."""
    if _SELGERFORSIKRING_NOT.search(text):
        return False
    if _SELGERFORSIKRING_HAS.search(text):
        return True
    return None


_FERDIGATTEST_NONE = re.compile(
    r"foreligger ikke ferdigattest|ingen ferdigattest|ferdigattest foreligger ikke", re.I
)
_MIDLERTIDIG = re.compile(r"midlertidig brukstillatelse", re.I)
_FERDIGATTEST = re.compile(r"ferdigattest", re.I)


def _ferdigattest(text: str) -> str | None:
    """Order matters: many ads say 'foreligger ikke ferdigattest, men
    midlertidig brukstillatelse', which is 'midlertidig', not 'ingen'."""
    if _FERDIGATTEST_NONE.search(text):
        return "midlertidig" if _MIDLERTIDIG.search(text) else "ingen"
    if _MIDLERTIDIG.search(text):
        return "midlertidig"
    if _FERDIGATTEST.search(text):
        return "ferdigattest"
    return None


_UTLEIE_EGEN = re.compile(r"egen (?:utleie|hybel)|utleiedel|hybelleilighet", re.I)
_UTLEIE_NOT = re.compile(r"(?:ikke|ei) (?:anledning|tillatt|lov).{0,30}(?:leie ut|utleie)", re.I)
_UTLEIE_OK = re.compile(r"anledning til å leie ut|kan leies ut|utleie er tillatt", re.I)


def _utleie(text: str) -> str | None:
    if _UTLEIE_EGEN.search(text):
        return "egen_enhet"
    if _UTLEIE_NOT.search(text):
        return "ikke_tillatt"
    if _UTLEIE_OK.search(text):
        return "tillatt"
    return None


_HUSDYR_GODKJENNING = re.compile(
    r"(?:dyrehold|husdyr)[^.]{0,80}?(?:godkjenn|samtykke|søknad|styret)", re.I
)
_HUSDYR_NOT = re.compile(
    r"(?:dyrehold|husdyr)[^.]{0,60}?ikke tillatt|forbud mot (?:dyrehold|husdyr)", re.I
)
_HUSDYR_OK = re.compile(r"(?:dyrehold|husdyr)[^.]{0,60}?(?:er )?tillatt", re.I)


def _husdyr(text: str) -> str | None:
    if _HUSDYR_NOT.search(text):
        return "ikke_tillatt"
    if _HUSDYR_GODKJENNING.search(text):
        return "krever_godkjenning"
    if _HUSDYR_OK.search(text):
        return "tillatt"
    return None


_HEFTELSER = re.compile(r"servitutt|heftelse|pengeheftelse", re.I)
_RADON = re.compile(r"\bradon\b", re.I)


def _flat_text(secs: list[Section]) -> str:
    return "\n".join(f"{s.heading}\n{s.text}" for s in secs)


def parse_salgsoppgave(html: str, finnkode: str) -> Salgsoppgave:
    """Never raises. An unrecognisable page yields an all-NULL row."""
    ad = decode_ad(html)
    if ad is None:
        return Salgsoppgave(finnkode=finnkode)
    secs = sections(ad)
    text = _flat_text(secs)
    if not text.strip():
        return Salgsoppgave(finnkode=finnkode)
    return Salgsoppgave(
        finnkode=finnkode,
        boligselgerforsikring=_boligselgerforsikring(text),
        eiendomsskatt_kr=_eiendomsskatt(text),
        ferdigattest=_ferdigattest(text),
        # bool(...), NOT `bool(...) or None`: reaching this branch means we
        # DID read a salgsoppgave, so "radon not mentioned" is False, not
        # unknown. NULL stays reserved for "no salgsoppgave text at all",
        # which the early return above handles.
        radon_omtalt=bool(_RADON.search(text)),
        utleie=_utleie(text),
        husdyr=_husdyr(text),
        heftelser=bool(_HEFTELSER.search(text)),
    )
