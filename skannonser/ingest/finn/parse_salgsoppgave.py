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
#     assertion about the room or the property. The heading itself is
#     often the compound "Ferdigattest/midlertidig brukstillatelse", so the
#     exclusion also swallows an immediately-following "/midlertidig
#     brukstillatelse" (or the mirrored "Midlertidig brukstillatelse/
#     ferdigattest") -- otherwise a bare cross-reference like "Se punkt
#     ferdigattest/midlertidig brukstillatelse for mer info" left
#     "midlertidig brukstillatelse" unstripped and got misread as an
#     assertion that a temporary permit exists.
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
#     negation ("Eiendommen ... har ikke ferdigattest") NOT by the wording
#     of the negated noun ("tiltak"/"bygg(ninger)"/"byggesak" vs.
#     "eiendommen"/"boligen") -- "bygget" is ambiguous between that generic
#     class and the ordinary definite form of "building" ("the building"),
#     so a word-class heuristic here swallowed genuine property-specific
#     negations too (e.g. "Ferdigattest foreligger ikke for bygget, som ble
#     oppført i 2015" was misread as boilerplate and inverted to
#     "ferdigattest"). Instead, key off the statutory citation itself
#     ("plan- og bygningsloven" / "§ 21-10"), which per this same comment is
#     present in every real occurrence of this specific boilerplate --
#     narrower (a property-specific sentence never cites the statute) and
#     more robust (survives rewording of the surrounding prose) than
#     guessing from the negated noun. The gap to the citation is matched
#     with a plain `.` (any char but newline), not `[^.!?\n]`, because the
#     boilerplate's own date ("før 01.01.1998") and abbreviation ("jf.")
#     periods sit between "ferdigattest" and the citation -- a
#     sentence-bounded gap would stop at the first one and never reach it.
#     Still bounded (200 chars) and still confined to one section (no `\n`
#     crossing), so it can't reach past the boilerplate's own sentence(s).
#   - "... uansett om det foreligger ferdigattest eller ikke" -- explicitly
#     indifferent to whether one exists, not an assertion either way.
# All recur across many ads (same boilerplate text), so left unhandled they
# were a systematic false-inversion source, not a rare edge case.
_FERDIGATTEST_NON_ASSERTION = re.compile(
    r"ferdigattest\w*\s+eksisterer\b[^.!?\n]*\bingen\s+garanti"
    r"|\bse\b[^.!?\n]{0,20}\bpunkt(?:et)?\b[^.!?\n]{0,15}[\"']?"
    r"(?:ferdigattest\w*(?:\s*(?:/|,|og)\s*midlertidig brukstillatelse)?"
    r"|midlertidig brukstillatelse(?:\s*(?:/|,|og)\s*ferdigattest\w*)?)"
    r"|ferdigattest\w*\s+omfatter\s+ikke"
    r"|ferdigattest\w*.{0,200}?(?:plan-?\s*og\s*bygningsloven|§\s*21-10)"
    r"|(?:plan-?\s*og\s*bygningsloven|§\s*21-10).{0,200}?ferdigattest\w*"
    r"|ferdigattest\w*\s+eller\s+ikke\b",
    re.I,
)


# Second-round fix (2026-07-29, corpus-verified over 300 ads, seed 77): the
# un-negated branch used to return "midlertidig" the instant "midlertidig
# brukstillatelse" appeared anywhere in the text, even when the SAME text
# also affirmatively asserted a full certificate elsewhere ("Det foreligger
# ferdigattest ... datert 2025", with "midlertidig brukstillatelse" only
# naming a superseded, historical permit from years earlier). 68% of ads
# that landed on "midlertidig" in the sample had a body that explicitly
# asserted a certificate exists. Per the spec: an un-negated certificate
# assertion always wins ("does not negate it -> ferdigattest"); "midlertidig"
# only applies when nothing affirms a certificate. Both directions of
# "foreligger" are covered since Norwegian word order varies ("Ferdigattest
# foreligger ..." / "... foreligger ferdigattest").
_FERDIGATTEST_ASSERT = re.compile(
    r"\bforeligger\b[^.!?\n]{0,20}\bferdigattest\w*\b"
    r"|\bferdigattest\w*\b[^.!?\n]{0,20}\bforeligger\b"
    r"|\bferdigattest\w*\b[^.!?\n]{0,20}\b(?:er\s+)?(?:utstedt|datert|gitt)\b"
    r"|\bferdigattest\w*\s+kom\b"
    r"|\bferdigattest\w*[^.!?\n]{0,20}\bp[åa]\s+eiendommen\b",
    re.I,
)


def _ferdigattest(text: str) -> str | None:
    """Order matters, and not only for the reason below: `_FERDIGATTEST_NEG`
    must be checked before `_FERDIGATTEST_ASSERT`, because ASSERT's
    'foreligger...ferdigattest' pattern also matches negated text --
    'foreligger ikke ferdigattest' satisfies both regexes ('ikke' within
    NEG's window of 'ferdigattest', and 'foreligger' within 20 chars of
    'ferdigattest' for ASSERT). Checking ASSERT first would read every such
    sentence as an assertion and silently flip real negations to
    'ferdigattest'. NEG must win when both match.

    Given that ordering, many ads then say 'foreligger ikke ferdigattest,
    men midlertidig brukstillatelse', which is 'midlertidig', not 'ingen' --
    handled inside the NEG branch below rather than by ASSERT.

    `negation_text` (boilerplate/cross-reference spans blanked out) feeds
    every check except the last: a bare "ferdigattest" mention surviving
    only in the *unstripped* text is the deliberate fallback for boilerplate
    that talks *around* the topic without asserting anything either way
    (e.g. "se punktet Ferdigattest for mer informasjon") -- there is no
    positive signal to prefer, so the plain mention wins by default. But
    "midlertidig brukstillatelse" appearing *only* inside one of those same
    blanked spans (e.g. a cross-reference literally named "Ferdigattest/
    midlertidig brukstillatelse") must not be credited as an assertion that
    a temporary permit exists -- corpus case: an ad whose only mention was
    "Se punkt ferdigattest/midlertidig brukstillatelse for mer info" wrongly
    came out "midlertidig" when the bare presence check ran on unstripped
    text.
    """
    negation_text = _FERDIGATTEST_NON_ASSERTION.sub(" ", text)
    if _FERDIGATTEST_NEG.search(negation_text):
        return "midlertidig" if _MIDLERTIDIG.search(negation_text) else "ingen"
    if _FERDIGATTEST_ASSERT.search(negation_text):
        return "ferdigattest"
    if _MIDLERTIDIG.search(negation_text):
        return "midlertidig"
    if _FERDIGATTEST.search(text):
        return "ferdigattest"
    return None


# Root cause of the heading/list misclassification (corpus-verified, same
# sample): `_flat_text` glues EVERY section's heading onto its body with no
# distinguishing marker. Norwegian brokers use a fixed section heading that
# names both documents regardless of which the property actually has --
# "Midlertidig brukstillatelse og ferdigattest", "Ferdigattest/midlertidig
# brukstillatelse" -- so scanning headings as if they were assertions made
# 59/132 sampled ads misclassify as "midlertidig" purely because of their
# own section heading, while the body underneath asserted a full
# ferdigattest. A further 24/132 came from "Vedlegg til salgsoppgaven"
# (attachment list) sections, which enumerate document *names* one per
# line -- "Ferdigattest/midlertidig brukstillatelse", "Arealbekreftelse" --
# not a statement about which the property has.
#
# Fix: classify from the BODY of the section(s) actually about the topic
# (heading contains "ferdigattest" or "brukstillatelse"), never from any
# heading text. This also incidentally fixed two more corpus-found sources
# of noise for free, since neither is topically headed: cross-references to
# the section by name from unrelated sections ("... se punktet Midlertidig
# brukstillatelse og ferdigattest", differently phrased from the reference
# `_FERDIGATTEST_NON_ASSERTION` already excludes), and mentions of
# "midlertidig brukstillatelse" in a "Regulering"/zoning section describing
# an unrelated municipal construction project in the area, not the
# property's own permit status.
#
# Falls back to every section's body (still never headings, still never a
# "Vedlegg..." list) when no dedicated section exists, so ads that only
# mention it in passing (e.g. within "Innhold") aren't silently dropped.
_FERDIGATTEST_TOPIC_HEADING = re.compile(r"ferdigattest|brukstillatelse", re.I)
_VEDLEGG_HEADING = re.compile(r"vedlegg", re.I)


def _ferdigattest_scope(secs: list[Section]) -> str:
    """Body-only text to classify `ferdigattest` from -- never headings."""
    topic = [s.text for s in secs if _FERDIGATTEST_TOPIC_HEADING.search(s.heading)]
    if topic:
        return "\n".join(topic)
    return "\n".join(s.text for s in secs if not _VEDLEGG_HEADING.search(s.heading))


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
        ferdigattest=_ferdigattest(_ferdigattest_scope(secs)),
        # bool(...), NOT `bool(...) or None`: reaching this branch means we
        # DID read a salgsoppgave, so "radon not mentioned" is False, not
        # unknown. NULL stays reserved for "no salgsoppgave text at all",
        # which the early return above handles.
        radon_omtalt=bool(_RADON.search(text)),
        utleie=_utleie(text),
        husdyr=_husdyr(text),
        heftelser=bool(_HEFTELSER.search(text)),
    )
