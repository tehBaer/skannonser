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


_MIDLERTIDIG = re.compile(r"midlertidig brukstillatelse", re.I)
_FERDIGATTEST = re.compile(r"ferdigattest", re.I)

# Structural negation, not an allowlist of exact phrasings: a fixed set of
# phrases ("foreligger ikke ferdigattest" and two others) missed most real
# negations -- "ikke mottatt ferdigattest", "ferdigattest gis ikke", "det
# utstedes ikke ferdigattest" all fell through the allowlist to the bare
# `_FERDIGATTEST` fallback below and came out as "ferdigattest": the exact
# opposite of what the text said. Instead, look for a negation word sharing
# a sentence with "ferdigattest" in either order. "ingen" is only allowed
# immediately before the term (bidirectional would catch boilerplate like
# "ferdigattest eksisterer, gir ingen garanti for ...", which is unrelated).
_FERDIGATTEST_NEG = re.compile(
    r"\bikke\b[^.!?\n]{0,50}\bferdigattest\w*\b"
    r"|\bferdigattest\w*\b[^.!?\n]{0,35}\bikke\b"
    r"|\buten\b[^.!?\n]{0,25}\bferdigattest\w*\b"
    r"|\bferdigattest\w*\b[^.!?\n]{0,25}\buten\b"
    r"|\bmangler\b[^.!?\n]{0,25}\bferdigattest\w*\b"
    r"|\bingen\b[^.!?\n]{0,15}\bferdigattest\w*\b",
    re.I,
)

# Boilerplate patterns that mention "ferdigattest" and "ikke"/"ingen" in the
# same sentence without the sentence being *about* whether one exists,
# verified against the cached corpus (data/eiendom/html_extracted):
#   - the standard disclaimer "At ferdigattest eksisterer, gir ingen garanti
#     for at det ikke er utført ..." -- ferdigattest here is asserted to
#     exist; "ingen"/"ikke" both refer to something else entirely.
#   - "Rommet er ikke godkjent ..., se punktet Ferdigattest for mer
#     informasjon" -- a cross-reference to a document heading, not an
#     assertion about the room or the property.
#   - "Ferdigattesten omfatter ikke <tiltak X>" -- almost always preceded a
#     few sentences earlier by "Det foreligger ferdigattest ... datert ...":
#     a certificate exists, it just doesn't cover a later, separate addition.
#   - "Ferdigattest utstedes ikke lenger for tiltak det er søkt om før
#     01.01.1998, jf. plan- og bygningsloven § 21-10 femte ledd" -- a fixed
#     statutory-citation disclaimer repeated near-verbatim across ads
#     regardless of the property's own certificate status (frequently right
#     after an explicit "Ferdigattest er utstedt: <date>" for this specific
#     property, or in the mirrored order "På bygg ... ikke lenger utstedes
#     ferdigattest"). Distinguished from a genuine property-specific
#     negation ("Eiendommen ... har ikke ferdigattest") by its grammar: the
#     negated object is the generic class ("tiltak"/"bygg(ninger)"/
#     "byggesak") dated by a "søkt/omsøkt/oppført ... før <year>" clause,
#     not "eiendommen" or "boligen".
#   - "... uansett om det foreligger ferdigattest eller ikke" -- explicitly
#     indifferent to whether one exists, not an assertion either way.
# All recur across many ads (same boilerplate text), so left unhandled they
# were a systematic false-inversion source, not a rare edge case.
_FERDIGATTEST_NON_ASSERTION = re.compile(
    r"ferdigattest\w*\s+(?:eksisterer|foreligger\s+er\s+likevel)\b[^.!?\n]*\bingen\s+garanti"
    r"|\bse\b[^.!?\n]{0,20}\bpunktet?\b[^.!?\n]{0,15}[\"']?ferdigattest"
    r"|ferdigattest\w*\s+omfatter\s+ikke"
    r"|ferdigattest\w*[^.!?\n]{0,60}\b(?:for|på)\s+(?:[\w-]+\s+){0,2}"
    r"(?:tiltak\w*|bygg\w*|bygning\w*|byggesak\w*|byggemelding\w*)\b[^.!?\n]{0,50}"
    r"(?:\b(?:søkt|omsøkt|oppført|bygget|etablert)\b|\bfør\s+(?:19|20)?\d{2}\b)"
    r"|\b(?:tiltak\w*|bygg\w*|bygning\w*|byggesak\w*|byggemelding\w*)\b[^.!?\n]{0,60}"
    r"\bikke\s+(?:lenger\s+)?(?:utsted\w*|bli\w*\s+gitt|gis)\s+ferdigattest\w*"
    r"|ferdigattest\w*\s+eller\s+ikke\b",
    re.I,
)


def _ferdigattest(text: str) -> str | None:
    """Order matters: many ads say 'foreligger ikke ferdigattest, men
    midlertidig brukstillatelse', which is 'midlertidig', not 'ingen'."""
    negation_text = _FERDIGATTEST_NON_ASSERTION.sub(" ", text)
    if _FERDIGATTEST_NEG.search(negation_text):
        return "midlertidig" if _MIDLERTIDIG.search(text) else "ingen"
    if _MIDLERTIDIG.search(text):
        return "midlertidig"
    if _FERDIGATTEST.search(text):
        return "ferdigattest"
    return None


_UTLEIE_EGEN = re.compile(r"egen (?:utleie|hybel)|utleiedel|hybelleilighet", re.I)
# 'ei' is dropped: in modern Norwegian it is overwhelmingly the feminine
# indefinite article ("a/an"), not a negation, and matched sentences like
# "Det er ei tillatt utleie ..." (permits it) as if they forbade it.
_UTLEIE_NOT = re.compile(r"ikke (?:anledning|tillatt|lov).{0,30}(?:leie ut|utleie)", re.I)
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
    # Godkjenning checked first: "ikke tillatt uten styrets samtykke" is a
    # conditional permit (permitted with board consent), and also matches
    # the bare-prohibition pattern below ("ikke tillatt"). Testing the
    # prohibition first collapsed that common co-op phrasing to
    # ikke_tillatt, discarding the "uten styrets samtykke" qualifier.
    if _HUSDYR_GODKJENNING.search(text):
        return "krever_godkjenning"
    if _HUSDYR_NOT.search(text):
        return "ikke_tillatt"
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
