"""Tilstand classifier (2026-08-05 design spec): condition-section selection,
the strict output schema, the Claude call seam, the response cache, and the
per-listing rollup math. The API call itself lives behind an injected `_call`
so tests never touch the network and never import `anthropic`.
"""
import hashlib
import re

from skannonser.ingest.finn.payload import Section, decode_ad, sections

# The only legal kostnad values (design spec: coarse grid; 1_000_000 = "1M+").
GRID = (0, 10_000, 20_000, 50_000, 100_000, 200_000, 300_000, 500_000, 1_000_000)

BYGNINGSDEL = (
    "vatrom", "kjokken", "tak", "vinduer_dorer", "yttervegg", "etasjeskille",
    "grunn_drenering", "vvs", "elektrisk", "ventilasjon", "overflater",
    "balkong_terrasse", "trapp", "radon", "vaskerom", "utvendig_annet",
    "helhet", "annet",
)
TILTAK = ("lokal_utbedring", "utskiftning", "videre_undersokelse", "overvaking", "estetisk")
ALVORLIGHET = ("kosmetisk", "mindre", "vesentlig", "alvorlig")
FORHOLD = (
    "vannskade", "fuktskade", "soppskade", "brannskade", "skadedyr",
    "ufaglaert_arbeid", "manglende_dokumentasjon", "tvist", "palegg_offentlig",
    "annet",
)
UTSTEDER = ("anticimex", "norsk_takst", "takstinstituttet", "nito_takst", "annet")

# Measured 2026-08-05 (see design spec): this selection yields mean 8.7k
# chars/ad vs 24k for the full text, and misses ~0 condition content.
_KEEP_HEADING = re.compile(
    r"tilstand|tg\b|avvik|bygningssakkyndig|takst|egenerkl|vedlikehold"
    r"|bygningsdel|boligsalgsrapport",
    re.I,
)
_BODY_MARKER = re.compile(r"\bTG\s?-?\s?[23]\b|tilstandsgrad|egenerkl", re.I)

# Below this the "selection" is stray keyword hits, not a condition report.
_MIN_INPUT_CHARS = 200


def select_sections(secs: list[Section]) -> list[Section]:
    return [s for s in secs if _KEEP_HEADING.search(s.heading) or _BODY_MARKER.search(s.text)]


def classify_input(html: str) -> str | None:
    """The text one classification call operates on, or None when the ad has
    nothing to classify (new-builds, undecodable payloads)."""
    ad = decode_ad(html)
    if not ad:
        return None
    sel = select_sections(sections(ad))
    text = "\n\n".join(f"## {s.heading}\n{s.text}" for s in sel).strip()
    return text if len(text) >= _MIN_INPUT_CHARS else None


def content_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
