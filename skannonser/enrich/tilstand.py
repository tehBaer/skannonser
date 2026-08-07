"""Tilstand classifier (2026-08-05 design spec): condition-section selection,
the strict output schema, the Claude call seam, the response cache, and the
per-listing rollup math. The API call itself lives behind an injected `_call`
so tests never touch the network and never import `anthropic`.
"""
import hashlib
import math
import re
import sqlite3

from pydantic import BaseModel, field_validator

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
TG_GRADES = (2, 3)
KOSTNAD_KILDE = ("takst", "estimat")

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


_MODEL = "claude-opus-5"


def _enum_check(allowed):
    def check(cls, v):
        if v is not None and v not in allowed:
            raise ValueError(f"{v!r} not in vocabulary")
        return v
    return check


class TgFinding(BaseModel):
    tg: int
    bygningsdel: str
    tiltak: str | None
    alvorlighet: str
    kostnad_lav: int | None
    kostnad_hoy: int | None
    kostnad_kilde: str | None

    _v_tg = field_validator("tg")(_enum_check(TG_GRADES))
    _v_del = field_validator("bygningsdel")(_enum_check(BYGNINGSDEL))
    _v_tiltak = field_validator("tiltak")(_enum_check(TILTAK))
    _v_alv = field_validator("alvorlighet")(_enum_check(ALVORLIGHET))
    _v_lav = field_validator("kostnad_lav")(_enum_check(GRID))
    _v_hoy = field_validator("kostnad_hoy")(_enum_check(GRID))
    _v_kilde = field_validator("kostnad_kilde")(_enum_check(KOSTNAD_KILDE))


class TilstandResponse(BaseModel):
    findings: list[TgFinding]
    egenerklaering_present: bool
    egenerklaering: list[str]
    tilstandsrapport_dato: str | None
    tilstandsrapport_utsteder: str | None

    @field_validator("egenerklaering")
    @classmethod
    def _v_egen(cls, v):
        for x in v:
            if x not in FORHOLD:
                raise ValueError(f"{x!r} not in vocabulary")
        return v

    _v_utst = field_validator("tilstandsrapport_utsteder")(_enum_check(UTSTEDER))


_COST_ENUM = list(GRID)

TILSTAND_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "findings", "egenerklaering_present", "egenerklaering",
        "tilstandsrapport_dato", "tilstandsrapport_utsteder",
    ],
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "tg", "bygningsdel", "tiltak", "alvorlighet",
                    "kostnad_lav", "kostnad_hoy", "kostnad_kilde",
                ],
                "properties": {
                    "tg": {"type": "integer", "enum": list(TG_GRADES)},
                    "bygningsdel": {"type": "string", "enum": list(BYGNINGSDEL)},
                    "tiltak": {"anyOf": [
                        {"type": "string", "enum": list(TILTAK)}, {"type": "null"}]},
                    "alvorlighet": {"type": "string", "enum": list(ALVORLIGHET)},
                    "kostnad_lav": {"anyOf": [
                        {"type": "integer", "enum": _COST_ENUM}, {"type": "null"}]},
                    "kostnad_hoy": {"anyOf": [
                        {"type": "integer", "enum": _COST_ENUM}, {"type": "null"}]},
                    "kostnad_kilde": {"anyOf": [
                        {"type": "string", "enum": list(KOSTNAD_KILDE)}, {"type": "null"}]},
                },
            },
        },
        "egenerklaering_present": {"type": "boolean"},
        "egenerklaering": {"type": "array",
                           "items": {"type": "string", "enum": list(FORHOLD)}},
        "tilstandsrapport_dato": {"anyOf": [
            {"type": "string", "format": "date"}, {"type": "null"}]},
        "tilstandsrapport_utsteder": {"anyOf": [
            {"type": "string", "enum": list(UTSTEDER)}, {"type": "null"}]},
    },
}

_SYSTEM_PROMPT = """\
You classify the condition sections of a Norwegian real-estate prospectus
(salgsoppgave). Extract every TG2 and TG3 finding from the tilstandsrapport,
the seller's egenerklaering disclosures, and report metadata.

Rules:
- One finding per distinct defect. A single header like "Boligen har fatt
  folgende TG2:" followed by six building parts is six findings; "TG2 -
  Taktekking" is one. Never let broker formatting inflate or deflate counts.
- Classify each finding's bygningsdel from the defect BODY text, not from
  section headings (headings name topics, not facts). Structural boilerplate
  ("Vurdering av avvik", "Tiltak", "Konsekvens") is never a building part --
  discard it. A real building part that fits no other enum value goes to
  "annet" and still counts.
- alvorlighet is your judgment of how serious the defect is, from the defect
  and consequence text (kosmetisk < mindre < vesentlig < alvorlig). The TG
  grade alone does not determine it: a missing handrail and a bathroom
  needing full renovation are both TG3 but differ in severity.
- Costs: if the text states a cost for the finding (Kostnadsestimat,
  Utbedringskostnader, Kostnadsoverslag, prisanslag, ...), snap it OUTWARD
  onto the allowed values (floor down, ceiling up -- never narrower than
  stated) and set kostnad_kilde to "takst". Otherwise estimate a realistic
  Norwegian repair-cost band for the defect and set kostnad_kilde to
  "estimat". Use null for both bounds only when even a rough estimate is
  impossible.
- egenerklaering_present is true only if the text contains the seller's own
  egenerklaering disclosures. egenerklaering lists one entry per distinct
  disclosed condition; a seller disclosing nothing yields an empty list with
  egenerklaering_present true. Beware Norwegian negation: "har ikke tegnet"
  contains "har tegnet" -- read the sentence, not the keyword.
- tilstandsrapport_dato is the report's own date as YYYY-MM-DD if stated.
"""


def _response_text(response) -> str:
    """Refusal/truncation checks plus text extraction, split out from
    `_anthropic_call` so it's unit-testable with plain stub objects --
    no `anthropic` import needed to exercise these paths."""
    if response.stop_reason == "refusal":
        raise RuntimeError("classification request refused")
    if response.stop_reason == "max_tokens":
        raise RuntimeError("classification truncated at max_tokens")
    return next(b.text for b in response.content if b.type == "text")


def _anthropic_call(text: str) -> str:
    """Default `_call` seam: one classification request. Imported lazily so
    the `anthropic` package is only needed where classification actually runs
    (the [llm] extra), never by tests or the server."""
    import anthropic

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=_MODEL,
        max_tokens=32000,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": text}],
        output_config={"format": {"type": "json_schema", "schema": TILSTAND_SCHEMA}},
    )
    return _response_text(response)


def classify_one(text: str, *, _call=_anthropic_call) -> TilstandResponse:
    return TilstandResponse.model_validate_json(_call(text))


def cache_get(conn: sqlite3.Connection, sha: str) -> str | None:
    row = conn.execute(
        "SELECT response_json FROM salgsoppgave_llm_cache WHERE content_sha256 = ?",
        (sha,),
    ).fetchone()
    return row[0] if row else None


def cache_put(
    conn: sqlite3.Connection,
    sha: str,
    response_json: str,
    model: str = _MODEL,
    effort: str | None = None,
) -> None:
    """`effort` defaults to None = NOT RECORDED, which is the honest value: the
    API seam specifies no reasoning effort, so nothing has ever set one. Pass it
    only when the producing run actually had one."""
    conn.execute(
        "INSERT OR REPLACE INTO salgsoppgave_llm_cache "
        "(content_sha256, response_json, model, effort, created_at) "
        "VALUES (?, ?, ?, ?, datetime('now'))",
        (sha, response_json, model, effort),
    )
    conn.commit()


_CACHE_COLS = ("content_sha256", "response_json", "model", "effort", "created_at")

# `effort` arrived with migration 017, so export files written before it lack
# the key entirely. Requiring it on import would reject every cache file
# exported earlier -- including ones already copied to the server. Missing
# means NOT RECORDED, which is exactly what NULL already means here.
_CACHE_COLS_REQUIRED = tuple(c for c in _CACHE_COLS if c != "effort")


def export_cache(conn: sqlite3.Connection) -> list[dict]:
    """Every cached classifier response, as plain dicts.

    The cache is the ONLY thing worth moving between machines: findings and
    rollups are derived from it, so a receiving DB replays them for free with
    `classify-tilstand --limit 0`. Classification runs locally (it needs an
    API key); the server only ever reads the results.
    """
    return [
        dict(zip(_CACHE_COLS, row))
        for row in conn.execute(
            f"SELECT {', '.join(_CACHE_COLS)} FROM salgsoppgave_llm_cache "
            "ORDER BY content_sha256"
        )
    ]


def import_cache(conn: sqlite3.Connection, rows: list[dict]) -> dict:
    """Insert exported rows, returning {"imported": n, "replaced": n}.

    `created_at` travels with the row rather than being re-stamped: it records
    when the response was paid for, which keeps a re-export byte-identical.
    Validates the whole batch before writing anything, so a malformed export
    file cannot leave the cache half-populated.
    """
    for row in rows:
        missing = [c for c in _CACHE_COLS_REQUIRED if c not in row]
        if missing:
            raise ValueError(f"cache row missing {missing}: {row!r}")

    existing = {r[0] for r in conn.execute("SELECT content_sha256 FROM salgsoppgave_llm_cache")}
    replaced = sum(1 for r in rows if r["content_sha256"] in existing)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.executemany(
            f"INSERT OR REPLACE INTO salgsoppgave_llm_cache ({', '.join(_CACHE_COLS)}) "
            f"VALUES ({', '.join('?' * len(_CACHE_COLS))})",
            [tuple(r.get(c) for c in _CACHE_COLS) for r in rows],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"imported": len(rows) - replaced, "replaced": replaced}


_SEVERITY_ORDER = {"kosmetisk": 0, "mindre": 1, "vesentlig": 2, "alvorlig": 3}


def compute_rollup(resp: TilstandResponse) -> dict:
    """Per-listing rollups (design spec 'Rollup semantics'). Pure function so
    the max/sum/tie-break rules are testable without a DB or an API call."""
    findings = resp.findings
    costed = [f for f in findings
              if f.kostnad_lav is not None and f.kostnad_hoy is not None]
    if costed:
        lav = sum(f.kostnad_lav for f in costed)
        hoy = sum(f.kostnad_hoy for f in costed)
        est = math.floor(sum((f.kostnad_lav + f.kostnad_hoy) / 2 for f in costed) / 10_000 + 0.5) * 10_000
        kilder = {f.kostnad_kilde for f in costed}
        kilde = "takst" if kilder == {"takst"} else (
            "estimat" if kilder == {"estimat"} else "blandet")
    else:
        lav = hoy = est = kilde = None
    if findings:
        worst = max(findings,
                    key=lambda f: (_SEVERITY_ORDER[f.alvorlighet], f.kostnad_hoy or 0))
        alvorlighet, verste = worst.alvorlighet, worst.bygningsdel
    else:
        alvorlighet = verste = None
    return {
        "tg2_count": sum(1 for f in findings if f.tg == 2),
        "tg3_count": sum(1 for f in findings if f.tg == 3),
        "reparasjon_lav": lav,
        "reparasjon_hoy": hoy,
        "reparasjon_est": est,
        "alvorlighet": alvorlighet,
        "verste_bygningsdel": verste,
        "reparasjon_kilde": kilde,
        "tilstandsrapport_dato": resp.tilstandsrapport_dato,
        "tilstandsrapport_utsteder": resp.tilstandsrapport_utsteder,
        # NULL = no egenerklaering section existed; 0 = section existed, seller
        # disclosed nothing. Same discipline as Phase 1's null-vs-false rule.
        # Counts the model's raw list, not stored rows: TilstandRepo dedups via
        # INSERT OR IGNORE on (finnkode, forhold), so two disclosures mapping to
        # the same forhold make this exceed listing_egenerklaering's row count.
        # Known and harmless -- those extra rows are never surfaced anywhere.
        "egenerklaering_antall": (
            len(resp.egenerklaering) if resp.egenerklaering_present else None),
    }
