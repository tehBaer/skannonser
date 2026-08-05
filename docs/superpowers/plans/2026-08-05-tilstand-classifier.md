# Tilstand Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** LLM classification of TG2/TG3 condition findings — with severity and repair-cost bands — plus egenerklæring, from the salgsoppgave text already cached on disk, surfaced in the web app.

**Architecture:** Migration 016 moves Phase-2 storage into classifier-owned tables (`listing_tilstand`, rebuilt `listing_tg_findings`) so Phase-1's `INSERT OR REPLACE`/`wipe()` can never touch it. A new `skannonser/enrich/tilstand*.py` module family selects the condition sections from cached ad HTML, sends them to Claude Opus 5 with a strict JSON schema (enums enforced at the wire), caches responses by content hash in the existing `salgsoppgave_llm_cache`, and upserts findings + rollups. CLI `skannonser tools classify-tilstand` drives sync, Batch-API, and validation modes. The web app joins `listing_tilstand` and shows tier/cost with provenance markers.

**Tech Stack:** Python 3.11, sqlite3, pydantic v2, typer, `anthropic` SDK (new optional dep), FastAPI, vanilla-JS frontend, pytest + `node --test`.

**Spec:** `docs/superpowers/specs/2026-08-05-tilstand-classifier-design.md` — the authority on schema, vocabularies, rollup semantics, and prompt contract. Read it before starting.

## Global Constraints

- **Worktree + env**: work in a worktree (`EnterWorktree` + `./ops/setup-worktree.sh`). Tests: `PYTHONPATH=. ./.venv/bin/pytest` — bare `pytest` in a worktree silently tests the MAIN clone. JS: `node --test tests/web/*.test.mjs` (bare directory form fails).
- **Baseline**: 799 Python tests pass before this work (handoff, 2026-07-29). Anything less was already broken.
- **Migration number**: 016. Re-verify with `git fetch && ls skannonser/store/migrations/` before writing the file — another session works in this repo.
- **Model**: `claude-opus-5` exactly. Never a date-suffixed variant.
- **Cost grid** (kr, the only legal `kostnad_lav`/`kostnad_hoy` values): `0, 10000, 20000, 50000, 100000, 200000, 300000, 500000, 1000000`.
- **No network in tests**: every API call sits behind an injected `_call`/`_client` seam; pytest never touches the network and never imports `anthropic` (the import is lazy, inside the default seam only).
- **Null discipline**: no `listing_tilstand` row = never classified; row with counts 0 = classified, nothing found. Never collapse.
- **Subagents**: start in the main clone. Every dispatch needs `cd <worktree>` + `git branch --show-current` check + absolute paths.
- **Norwegian chars in .py/.js**: plain UTF-8 (this repo is UTF-8 throughout; the Mac Roman rule is for `.hal` files elsewhere). Enum *values* are ASCII (`vatrom`, not `våtrom`); UI *labels* use proper Norwegian.

---

### Task 1: Migration 016

**Files:**
- Create: `skannonser/store/migrations/016_tilstand.sql`
- Modify: `tests/rebuild/test_migrations.py` (ALL_MIGRATIONS list at line ~16; also grep for `EXPECTED_TABLES` and add the new table)

**Interfaces:**
- Produces: tables `listing_tilstand` (new), `listing_tg_findings` (rebuilt: `id` PK, no UNIQUE, +`alvorlighet`, `kostnad_lav`, `kostnad_hoy`, `kostnad_kilde`); `listing_salgsoppgave` loses its five dead Phase-2 columns.

- [ ] **Step 1: Update the migration test first**

In `tests/rebuild/test_migrations.py`, append `"016_tilstand"` to `ALL_MIGRATIONS` (keep numeric order). Find `EXPECTED_TABLES` (grep the file; it may live in `conftest.py`) and add `"listing_tilstand"`. Add a shape test to `test_migrations.py`:

```python
def test_016_reshapes_phase2_tables(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    tg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_tg_findings)")}
    assert {"id", "alvorlighet", "kostnad_lav", "kostnad_hoy", "kostnad_kilde"} <= tg_cols
    # the UNIQUE collapse is gone: two TG3 vatrom rows must coexist
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('1')")
    for _ in range(2):
        conn.execute(
            "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel, alvorlighet) "
            "VALUES ('1', 3, 'vatrom', 'alvorlig')"
        )
    n = conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0]
    assert n == 2
    # phase-2 columns left listing_salgsoppgave
    so_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_salgsoppgave)")}
    assert not ({"tg2_count", "tg3_count", "tilstandsrapport_dato",
                 "tilstandsrapport_utsteder", "egenerklaering_antall"} & so_cols)
    rollup_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_tilstand)")}
    assert {"finnkode", "tg2_count", "tg3_count", "reparasjon_lav", "reparasjon_hoy",
            "reparasjon_est", "alvorlighet", "verste_bygningsdel", "reparasjon_kilde",
            "tilstandsrapport_dato", "tilstandsrapport_utsteder",
            "egenerklaering_antall", "classified_at"} <= rollup_cols
```

Note: if `eiendom` has NOT NULL columns beyond finnkode, adapt the INSERT to satisfy them (check `PRAGMA table_info(eiendom)` output in the 001 migration SQL).

- [ ] **Step 2: Run, verify failure**

`PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_migrations.py -x -q` — expect FAIL (016 not found / assertions fail).

- [ ] **Step 3: Write the migration**

`skannonser/store/migrations/016_tilstand.sql` (verify 016 is still free first):

```sql
-- 016_tilstand.sql
-- Tilstand classifier storage (2026-08-05 design spec). Classifier-owned,
-- separate from Phase-1's listing_salgsoppgave so a routine Phase-1 re-parse
-- or `backfill-salgsoppgave --wipe` can never touch classifier output.
-- Derived + disposable: rebuildable via `tools classify-tilstand --wipe`,
-- with salgsoppgave_llm_cache replaying paid responses for free.

-- Rebuild listing_tg_findings: the old UNIQUE (finnkode, tg, bygningsdel)
-- collapsed two TG3 bathrooms into one row -- harmless for counting parts,
-- silently halves the repair bill once findings carry costs. Table is empty
-- everywhere (0 rows, by design), so DROP is safe.
DROP TABLE IF EXISTS listing_tg_findings;
CREATE TABLE listing_tg_findings (
    id            INTEGER PRIMARY KEY,
    finnkode      TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg            INTEGER NOT NULL,        -- 2 | 3
    bygningsdel   TEXT NOT NULL,           -- 18-value enum, or 'annet'
    tiltak        TEXT,
    alvorlighet   TEXT NOT NULL,           -- kosmetisk|mindre|vesentlig|alvorlig
    kostnad_lav   INTEGER,                 -- kr, grid value (see design spec)
    kostnad_hoy   INTEGER,                 -- kr, grid value; 1000000 = "1M+"
    kostnad_kilde TEXT                     -- 'takst' | 'estimat'
);
CREATE INDEX IF NOT EXISTS idx_tg_findings_finnkode
    ON listing_tg_findings (finnkode);

-- Per-listing rollups, denormalised so the web app can sort/filter.
CREATE TABLE IF NOT EXISTS listing_tilstand (
    finnkode              TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    tg2_count             INTEGER NOT NULL,
    tg3_count             INTEGER NOT NULL,
    reparasjon_lav        INTEGER,   -- SUM(kostnad_lav)   -> filter floor
    reparasjon_hoy        INTEGER,   -- SUM(kostnad_hoy)   -> filter ceiling
    reparasjon_est        INTEGER,   -- SUM(midpoints)     -> sort key
    alvorlighet           TEXT,      -- worst tier across findings
    verste_bygningsdel    TEXT,      -- bygningsdel of the worst finding
    reparasjon_kilde      TEXT,      -- 'takst' | 'blandet' | 'estimat'
    tilstandsrapport_dato TEXT,
    tilstandsrapport_utsteder TEXT,
    egenerklaering_antall INTEGER,
    classified_at         TEXT NOT NULL
);

-- Phase-2 columns on listing_salgsoppgave: NULL on every live row, now dead.
ALTER TABLE listing_salgsoppgave DROP COLUMN tg2_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tg3_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_dato;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_utsteder;
ALTER TABLE listing_salgsoppgave DROP COLUMN egenerklaering_antall;
```

- [ ] **Step 4: Run the full migration test file**

`PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_migrations.py -q` — expect PASS (including the adopt-preexisting-schema test).

- [ ] **Step 5: Commit**

```bash
git add skannonser/store/migrations/016_tilstand.sql tests/rebuild/test_migrations.py
git commit -m "feat(store): migration 016 - classifier-owned tilstand tables"
```

---

### Task 2: SalgsoppgaveRepo stops touching Phase-2 tables

**Files:**
- Modify: `skannonser/store/repositories/salgsoppgave.py` (`wipe()` at ~line 51, `coverage()` at ~line 61, module docstring)
- Test: `tests/rebuild/test_salgsoppgave_repo.py` (find via `grep -rln "SalgsoppgaveRepo" tests/`; if the repo tests live in `test_backfill_salgsoppgave.py`, add there)

**Interfaces:**
- Consumes: migration 016 (Task 1).
- Produces: `SalgsoppgaveRepo.wipe()` deletes ONLY `listing_salgsoppgave`. `coverage()` no longer reports `tg_findings_rows`.

- [ ] **Step 1: Write the failing test**

```python
def test_phase1_wipe_spares_classifier_tables(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('1')")
    conn.execute(
        "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel, alvorlighet) "
        "VALUES ('1', 3, 'vatrom', 'alvorlig')"
    )
    conn.execute("INSERT INTO listing_egenerklaering (finnkode, forhold) VALUES ('1', 'vannskade')")
    conn.commit()
    SalgsoppgaveRepo(conn).wipe()
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 1
```

(Match the existing test file's connection/migration fixtures — reuse its helpers.)

- [ ] **Step 2: Run, verify failure** — the wipe currently deletes both tables.

- [ ] **Step 3: Implement**

In `wipe()`, delete the two lines `conn.execute("DELETE FROM listing_tg_findings")` and `conn.execute("DELETE FROM listing_egenerklaering")`. In `coverage()`, delete the `"tg_findings_rows"` entry. Update the module docstring: Phase-2 tables are now owned by `TilstandRepo` (migration 016); this repo touches only `listing_salgsoppgave`.

- [ ] **Step 4: Run** `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/ -q -k "salgsoppgave"` — expect PASS. If an existing test asserted `tg_findings_rows` in coverage, update it.

- [ ] **Step 5: Commit** — `git commit -m "fix(store): phase-1 wipe no longer destroys classifier tables"`

---

### Task 3: TilstandRepo

**Files:**
- Create: `skannonser/store/repositories/tilstand.py`
- Test: `tests/rebuild/test_tilstand_repo.py`

**Interfaces:**
- Consumes: migration 016.
- Produces:
  - `TilstandRepo(conn)` with:
    - `upsert_ad(finnkode: str, findings: list[dict], egenerklaering: list[str], rollup: dict) -> None` — full per-ad replace, one transaction. `findings` dicts carry keys `tg, bygningsdel, tiltak, alvorlighet, kostnad_lav, kostnad_hoy, kostnad_kilde`. `rollup` carries every `_ROLLUP_COLS` key.
    - `wipe() -> None` — clears the three classifier tables, spares `salgsoppgave_llm_cache`.
    - `coverage() -> dict`.

- [ ] **Step 1: Write failing tests**

```python
# tests/rebuild/test_tilstand_repo.py
from skannonser.store import connection, migrations
from skannonser.store.repositories.tilstand import TilstandRepo

ROLLUP = {
    "tg2_count": 1, "tg3_count": 1,
    "reparasjon_lav": 210_000, "reparasjon_hoy": 550_000, "reparasjon_est": 380_000,
    "alvorlighet": "alvorlig", "verste_bygningsdel": "vatrom",
    "reparasjon_kilde": "blandet",
    "tilstandsrapport_dato": "2026-05-01", "tilstandsrapport_utsteder": "anticimex",
    "egenerklaering_antall": 1,
}
FINDINGS = [
    {"tg": 3, "bygningsdel": "vatrom", "tiltak": "utskiftning", "alvorlighet": "alvorlig",
     "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "takst"},
    {"tg": 2, "bygningsdel": "tak", "tiltak": None, "alvorlighet": "mindre",
     "kostnad_lav": 10_000, "kostnad_hoy": 50_000, "kostnad_kilde": "estimat"},
]


def _db(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('42')")
    conn.commit()
    return conn


def test_upsert_ad_writes_all_three_tables(tmp_path):
    conn = _db(tmp_path)
    TilstandRepo(conn).upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 2
    assert conn.execute("SELECT forhold FROM listing_egenerklaering").fetchone()[0] == "vannskade"
    row = conn.execute("SELECT * FROM listing_tilstand WHERE finnkode='42'").fetchone()
    assert row["reparasjon_est"] == 380_000
    assert row["classified_at"] is not None


def test_upsert_ad_is_a_full_replace(tmp_path):
    conn = _db(tmp_path)
    repo = TilstandRepo(conn)
    repo.upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    repo.upsert_ad("42", FINDINGS[:1], [], {**ROLLUP, "tg2_count": 0, "egenerklaering_antall": None})
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 1


def test_wipe_spares_llm_cache(tmp_path):
    conn = _db(tmp_path)
    conn.execute(
        "INSERT INTO salgsoppgave_llm_cache (content_sha256, response_json, model, created_at) "
        "VALUES ('abc', '{}', 'm', datetime('now'))"
    )
    repo = TilstandRepo(conn)
    repo.upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    repo.wipe()
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM salgsoppgave_llm_cache").fetchone()[0] == 1
```

- [ ] **Step 2: Run, verify failure** — `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_tilstand_repo.py -q` → import error.

- [ ] **Step 3: Implement**

```python
# skannonser/store/repositories/tilstand.py
"""Classifier-owned tables (migration 016): listing_tilstand,
listing_tg_findings, listing_egenerklaering.

Full per-ad REPLACE semantics like the other derived caches: these are LLM
classifier output over cached ad HTML, rebuildable via
`tools classify-tilstand --wipe`. `wipe()` spares salgsoppgave_llm_cache --
that cache is what makes a rebuild free instead of a paid re-run.
Phase-1's SalgsoppgaveRepo must never touch these tables (and since 016, it
structurally cannot: they are not in its SQL).
"""
import sqlite3

_FINDING_COLS = (
    "tg", "bygningsdel", "tiltak", "alvorlighet",
    "kostnad_lav", "kostnad_hoy", "kostnad_kilde",
)
_ROLLUP_COLS = (
    "tg2_count", "tg3_count", "reparasjon_lav", "reparasjon_hoy",
    "reparasjon_est", "alvorlighet", "verste_bygningsdel", "reparasjon_kilde",
    "tilstandsrapport_dato", "tilstandsrapport_utsteder", "egenerklaering_antall",
)


class TilstandRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_ad(
        self,
        finnkode: str,
        findings: list[dict],
        egenerklaering: list[str],
        rollup: dict,
    ) -> None:
        """Replace one ad's classifier output atomically."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings WHERE finnkode = ?", (finnkode,))
            conn.execute("DELETE FROM listing_egenerklaering WHERE finnkode = ?", (finnkode,))
            for f in findings:
                conn.execute(
                    "INSERT INTO listing_tg_findings "
                    f"(finnkode, {', '.join(_FINDING_COLS)}) "
                    f"VALUES (?, {', '.join('?' * len(_FINDING_COLS))})",
                    [finnkode] + [f[c] for c in _FINDING_COLS],
                )
            for forhold in egenerklaering:
                conn.execute(
                    "INSERT OR IGNORE INTO listing_egenerklaering (finnkode, forhold) "
                    "VALUES (?, ?)",
                    (finnkode, forhold),
                )
            conn.execute(
                "INSERT OR REPLACE INTO listing_tilstand "
                f"(finnkode, {', '.join(_ROLLUP_COLS)}, classified_at) "
                f"VALUES (?, {', '.join('?' * len(_ROLLUP_COLS))}, datetime('now'))",
                [finnkode] + [rollup[c] for c in _ROLLUP_COLS],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def wipe(self) -> None:
        """Clear the classifier tables. Leaves salgsoppgave_llm_cache intact."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings")
            conn.execute("DELETE FROM listing_egenerklaering")
            conn.execute("DELETE FROM listing_tilstand")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "tilstand_rows": one("SELECT COUNT(*) FROM listing_tilstand"),
            "tg_findings_rows": one("SELECT COUNT(*) FROM listing_tg_findings"),
            "egenerklaering_rows": one("SELECT COUNT(*) FROM listing_egenerklaering"),
            "llm_cache_rows": one("SELECT COUNT(*) FROM salgsoppgave_llm_cache"),
            "with_tg3": one("SELECT COUNT(*) FROM listing_tilstand WHERE tg3_count > 0"),
        }
```

- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(store): TilstandRepo for classifier-owned tables"`

---

### Task 4: Section selection and classifier input

**Files:**
- Create: `skannonser/enrich/tilstand.py` (this task starts it; Tasks 5–6 extend it)
- Test: `tests/rebuild/test_tilstand.py`

**Interfaces:**
- Consumes: `skannonser.ingest.finn.payload.decode_ad(html) -> dict | None`, `sections(ad) -> list[Section]` where `Section = NamedTuple(heading: str, text: str)`. Both fail soft.
- Produces:
  - `select_sections(secs: list[Section]) -> list[Section]`
  - `classify_input(html: str) -> str | None` — condition-section text, `None` when nothing to classify (< 200 chars selected)
  - `content_sha(text: str) -> str` — sha256 hexdigest
  - Vocabulary constants: `GRID`, `BYGNINGSDEL`, `TILTAK`, `ALVORLIGHET`, `FORHOLD`, `UTSTEDER` (values verbatim from the design spec)

- [ ] **Step 1: Write failing tests**

```python
# tests/rebuild/test_tilstand.py
from pathlib import Path

from skannonser.ingest.finn.payload import Section
from skannonser.enrich.tilstand import (
    GRID, BYGNINGSDEL, classify_input, content_sha, select_sections,
)

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def test_select_sections_keeps_condition_headings_and_tg_bodies():
    secs = [
        Section("Tilstandsrapport", "Rapport fra bygningssakkyndig."),
        Section("Beliggenhet", "Kort vei til butikk."),
        Section("Standard", "Bad har TG3 og krever utbedring."),  # body marker
        Section("Egenerklæring", "Selger opplyser om fuktskade."),
    ]
    kept = select_sections(secs)
    assert [s.heading for s in kept] == ["Tilstandsrapport", "Standard", "Egenerklæring"]


def test_classify_input_none_when_nothing_selected():
    # fixture 424071751 decodes to 38 sections, none condition-related
    html = (FIXTURES / "424071751.html").read_text(encoding="utf-8", errors="replace")
    assert classify_input(html) is None


def test_classify_input_selects_condition_text_from_real_ad():
    # fixture 432672475: 15 sections, 2 condition sections, ~9.5k chars
    html = (FIXTURES / "432672475.html").read_text(encoding="utf-8", errors="replace")
    text = classify_input(html)
    assert text is not None
    assert "TG" in text or "tilstand" in text.lower()
    assert len(text) < len(html)


def test_classify_input_never_raises_on_junk():
    for junk in ("", "<html></html>", "\x00\xff", "a" * 10):
        assert classify_input(junk) is None


def test_content_sha_is_stable_hex():
    assert content_sha("abc") == content_sha("abc")
    assert len(content_sha("abc")) == 64


def test_grid_is_the_spec_grid():
    assert GRID == (0, 10_000, 20_000, 50_000, 100_000, 200_000, 300_000, 500_000, 1_000_000)
    assert len(BYGNINGSDEL) == 18 and "annet" in BYGNINGSDEL
```

- [ ] **Step 2: Run, verify failure** (module doesn't exist).

- [ ] **Step 3: Implement**

```python
# skannonser/enrich/tilstand.py
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
```

- [ ] **Step 4: Run** `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_tilstand.py -q` — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(enrich): tilstand section selection and input builder"`

---

### Task 5: Response schema, prompt, classify seam, cache

**Files:**
- Modify: `skannonser/enrich/tilstand.py`
- Modify: `pyproject.toml` (add optional dep group)
- Test: `tests/rebuild/test_tilstand.py` (extend)

**Interfaces:**
- Produces:
  - `TgFinding` (pydantic): `tg: int`, `bygningsdel: str`, `tiltak: str | None`, `alvorlighet: str`, `kostnad_lav: int | None`, `kostnad_hoy: int | None`, `kostnad_kilde: str | None`
  - `TilstandResponse` (pydantic): `findings: list[TgFinding]`, `egenerklaering_present: bool`, `egenerklaering: list[str]`, `tilstandsrapport_dato: str | None`, `tilstandsrapport_utsteder: str | None`
  - `TILSTAND_SCHEMA: dict` — the wire schema
  - `classify_one(text: str, *, _call=None) -> TilstandResponse`
  - `_anthropic_call(text: str) -> str` — the default seam (lazy `import anthropic`)
  - `cache_get(conn, sha: str) -> str | None`, `cache_put(conn, sha: str, response_json: str, model: str = _MODEL) -> None`
  - `_MODEL = "claude-opus-5"`, `_SYSTEM_PROMPT`

- [ ] **Step 1: Add the optional dependency**

In `pyproject.toml` under `[project.optional-dependencies]`:

```toml
llm = ["anthropic>=0.40"]
```

Do NOT add it to core `dependencies` — the server never needs it (design decision: backfill runs locally). Do not install it in this task; tests never import it.

- [ ] **Step 2: Write failing tests**

Append to `tests/rebuild/test_tilstand.py`:

```python
import json
import pytest
from pydantic import ValidationError

from skannonser.enrich.tilstand import (
    TILSTAND_SCHEMA, TilstandResponse, cache_get, cache_put, classify_one,
)

GOOD_RESPONSE = {
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": "utskiftning",
         "alvorlighet": "alvorlig", "kostnad_lav": 200_000, "kostnad_hoy": 500_000,
         "kostnad_kilde": "takst"},
    ],
    "egenerklaering_present": True,
    "egenerklaering": ["fuktskade"],
    "tilstandsrapport_dato": "2026-05-01",
    "tilstandsrapport_utsteder": "anticimex",
}


def test_classify_one_parses_via_injected_call():
    resp = classify_one("some text", _call=lambda text: json.dumps(GOOD_RESPONSE))
    assert resp.findings[0].bygningsdel == "vatrom"
    assert resp.egenerklaering == ["fuktskade"]


def test_response_model_rejects_off_vocab_and_off_grid():
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate(
            {**GOOD_RESPONSE, "findings": [{**GOOD_RESPONSE["findings"][0], "bygningsdel": "badekar"}]}
        )
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate(
            {**GOOD_RESPONSE, "findings": [{**GOOD_RESPONSE["findings"][0], "kostnad_lav": 137_500}]}
        )
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate({**GOOD_RESPONSE, "egenerklaering": ["badekar"]})


def test_schema_declares_enums_at_the_wire():
    f = TILSTAND_SCHEMA["properties"]["findings"]["items"]
    assert f["properties"]["bygningsdel"]["enum"][0] == "vatrom"
    assert 137_500 not in f["properties"]["kostnad_lav"]["anyOf"][0]["enum"]
    assert TILSTAND_SCHEMA["additionalProperties"] is False


def test_cache_roundtrip(tmp_path):
    from skannonser.store import connection, migrations
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    assert cache_get(conn, "deadbeef") is None
    cache_put(conn, "deadbeef", json.dumps(GOOD_RESPONSE))
    assert json.loads(cache_get(conn, "deadbeef")) == GOOD_RESPONSE
```

- [ ] **Step 3: Run, verify failure.**

- [ ] **Step 4: Implement**

Append to `skannonser/enrich/tilstand.py`:

```python
import sqlite3
from pydantic import BaseModel, field_validator

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

    _v_tg = field_validator("tg")(_enum_check((2, 3)))
    _v_del = field_validator("bygningsdel")(_enum_check(BYGNINGSDEL))
    _v_tiltak = field_validator("tiltak")(_enum_check(TILTAK))
    _v_alv = field_validator("alvorlighet")(_enum_check(ALVORLIGHET))
    _v_lav = field_validator("kostnad_lav")(_enum_check(GRID))
    _v_hoy = field_validator("kostnad_hoy")(_enum_check(GRID))
    _v_kilde = field_validator("kostnad_kilde")(_enum_check(("takst", "estimat")))


class TilstandResponse(BaseModel):
    findings: list[TgFinding]
    egenerklaering_present: bool
    egenerklaering: list[str]
    tilstandsrapport_dato: str | None
    tilstandsrapport_utsteder: str | None

    _v_egen = field_validator("egenerklaering")(
        lambda cls, v: [x for x in v if _require(x in FORHOLD, x)]
    )
    _v_utst = field_validator("tilstandsrapport_utsteder")(_enum_check(UTSTEDER))


def _require(ok: bool, value):
    if not ok:
        raise ValueError(f"{value!r} not in vocabulary")
    return True
```

(If the lambda-validator reads poorly, use a plain `@field_validator("egenerklaering")` method that loops and raises — either is fine; keep vocabularies single-sourced from the tuples.)

The wire schema:

```python
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
                    "tg": {"type": "integer", "enum": [2, 3]},
                    "bygningsdel": {"type": "string", "enum": list(BYGNINGSDEL)},
                    "tiltak": {"anyOf": [
                        {"type": "string", "enum": list(TILTAK)}, {"type": "null"}]},
                    "alvorlighet": {"type": "string", "enum": list(ALVORLIGHET)},
                    "kostnad_lav": {"anyOf": [
                        {"type": "integer", "enum": _COST_ENUM}, {"type": "null"}]},
                    "kostnad_hoy": {"anyOf": [
                        {"type": "integer", "enum": _COST_ENUM}, {"type": "null"}]},
                    "kostnad_kilde": {"anyOf": [
                        {"type": "string", "enum": ["takst", "estimat"]}, {"type": "null"}]},
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
```

The prompt (module constant — the prompt contract from the design spec, verbatim rules):

```python
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


def _anthropic_call(text: str) -> str:
    """Default `_call` seam: one classification request. Imported lazily so
    the `anthropic` package is only needed where classification actually runs
    (the [llm] extra), never by tests or the server."""
    import anthropic

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=_MODEL,
        max_tokens=16000,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": text}],
        output_config={"format": {"type": "json_schema", "schema": TILSTAND_SCHEMA}},
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("classification request refused")
    return next(b.text for b in response.content if b.type == "text")


def classify_one(text: str, *, _call=_anthropic_call) -> TilstandResponse:
    return TilstandResponse.model_validate_json(_call(text))


def cache_get(conn: sqlite3.Connection, sha: str) -> str | None:
    row = conn.execute(
        "SELECT response_json FROM salgsoppgave_llm_cache WHERE content_sha256 = ?",
        (sha,),
    ).fetchone()
    return row[0] if row else None


def cache_put(conn: sqlite3.Connection, sha: str, response_json: str, model: str = _MODEL) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO salgsoppgave_llm_cache "
        "(content_sha256, response_json, model, created_at) "
        "VALUES (?, ?, ?, datetime('now'))",
        (sha, response_json, model),
    )
    conn.commit()
```

- [ ] **Step 5: Run** — expect PASS. Also run the whole suite: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild -q` (no `anthropic` import error must appear anywhere).
- [ ] **Step 6: Commit** — `git commit -m "feat(enrich): tilstand schema, prompt, classify seam and cache"`

---

### Task 6: Rollup computation

**Files:**
- Modify: `skannonser/enrich/tilstand.py`
- Test: `tests/rebuild/test_tilstand.py` (extend)

**Interfaces:**
- Produces: `compute_rollup(resp: TilstandResponse) -> dict` with exactly the `_ROLLUP_COLS` keys from Task 3 (`tg2_count, tg3_count, reparasjon_lav, reparasjon_hoy, reparasjon_est, alvorlighet, verste_bygningsdel, reparasjon_kilde, tilstandsrapport_dato, tilstandsrapport_utsteder, egenerklaering_antall`).

- [ ] **Step 1: Write failing tests**

```python
from skannonser.enrich.tilstand import compute_rollup


def _resp(findings, egen_present=True, egen=()):
    return TilstandResponse.model_validate({
        "findings": findings,
        "egenerklaering_present": egen_present,
        "egenerklaering": list(egen),
        "tilstandsrapport_dato": None,
        "tilstandsrapport_utsteder": None,
    })

F_BAD = {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "takst"}
F_TAK = {"tg": 2, "bygningsdel": "tak", "tiltak": None, "alvorlighet": "mindre",
         "kostnad_lav": 10_000, "kostnad_hoy": 50_000, "kostnad_kilde": "estimat"}


def test_rollup_sums_and_worst():
    r = compute_rollup(_resp([F_BAD, F_TAK], egen=["fuktskade"]))
    assert (r["tg2_count"], r["tg3_count"]) == (1, 1)
    assert (r["reparasjon_lav"], r["reparasjon_hoy"]) == (210_000, 550_000)
    # midpoints 350k + 30k = 380k, rounded to nearest 10k
    assert r["reparasjon_est"] == 380_000
    assert r["alvorlighet"] == "alvorlig"
    assert r["verste_bygningsdel"] == "vatrom"
    assert r["reparasjon_kilde"] == "blandet"
    assert r["egenerklaering_antall"] == 1


def test_rollup_severity_tie_broken_by_kostnad_hoy():
    a = {**F_BAD, "bygningsdel": "tak", "kostnad_hoy": 300_000}
    b = {**F_BAD, "bygningsdel": "vatrom", "kostnad_hoy": 500_000}
    assert compute_rollup(_resp([a, b]))["verste_bygningsdel"] == "vatrom"


def test_rollup_zero_findings_is_counts_zero_not_null():
    r = compute_rollup(_resp([]))
    assert (r["tg2_count"], r["tg3_count"]) == (0, 0)
    assert r["reparasjon_lav"] is None and r["reparasjon_est"] is None
    assert r["alvorlighet"] is None and r["reparasjon_kilde"] is None


def test_rollup_egen_absent_section_is_null_not_zero():
    assert compute_rollup(_resp([], egen_present=False))["egenerklaering_antall"] is None
    assert compute_rollup(_resp([], egen_present=True))["egenerklaering_antall"] == 0


def test_rollup_kilde_uniform():
    assert compute_rollup(_resp([F_BAD]))["reparasjon_kilde"] == "takst"
    assert compute_rollup(_resp([F_TAK]))["reparasjon_kilde"] == "estimat"
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

```python
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
        est = round(sum((f.kostnad_lav + f.kostnad_hoy) / 2 for f in costed) / 10_000) * 10_000
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
        "egenerklaering_antall": (
            len(resp.egenerklaering) if resp.egenerklaering_present else None),
    }
```

- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(enrich): tilstand rollup computation"`

---

### Task 7: Sync classification driver

**Files:**
- Create: `skannonser/enrich/tilstand_backfill.py`
- Test: `tests/rebuild/test_classify_tilstand.py`

**Interfaces:**
- Consumes: `classify_input`, `content_sha`, `cache_get`, `cache_put`, `TilstandResponse`, `compute_rollup`, `_anthropic_call` (from `tilstand.py`); `TilstandRepo` (Task 3). Mirrors the walk in `skannonser/ingest/finn/backfill_salgsoppgave.py` (read it first).
- Produces: `classify_tilstand(conn, project_dir, *, limit=None, wipe=False, cache_only=False, _call=None, _input_fn=None) -> dict` returning counters `{eiendom_rows, missing_html, empty_input, cached, called, limit_skipped, uncached_skipped, errors, upserted}`.
  - `limit` bounds **new API calls only** — cache replays are unlimited (this is the spend control).
  - `cache_only=True` never calls the API (batch mode's derive pass).
  - `_input_fn` defaults to `classify_input`; tests inject `lambda html: html.strip() or None`.

- [ ] **Step 1: Write failing tests**

```python
# tests/rebuild/test_classify_tilstand.py
import json

from skannonser.store import connection, migrations
from skannonser.enrich.tilstand import cache_get, content_sha
from skannonser.enrich.tilstand_backfill import classify_tilstand

RESPONSE = json.dumps({
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "estimat"},
    ],
    "egenerklaering_present": True,
    "egenerklaering": [],
    "tilstandsrapport_dato": None,
    "tilstandsrapport_utsteder": None,
})

FAKE_INPUT = lambda html: html.strip() or None  # noqa: E731


def _env(tmp_path, ads: dict[str, str]):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    html_dir = tmp_path / "html_extracted"
    html_dir.mkdir()
    for finnkode, text in ads.items():
        conn.execute("INSERT INTO eiendom (finnkode) VALUES (?)", (finnkode,))
        html_dir.write_text if False else (html_dir / f"{finnkode}.html").write_text(text)
    conn.commit()
    return conn


def test_classifies_and_upserts(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    calls = []
    result = classify_tilstand(
        conn, tmp_path, _call=lambda t: calls.append(t) or RESPONSE, _input_fn=FAKE_INPUT
    )
    assert result["called"] == 1 and result["upserted"] == 1
    assert conn.execute("SELECT tg3_count FROM listing_tilstand WHERE finnkode='1'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1


def test_second_run_replays_from_cache_without_api(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    classify_tilstand(conn, tmp_path, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT)

    def explode(text):
        raise AssertionError("API called on cached input")

    result = classify_tilstand(conn, tmp_path, _call=explode, _input_fn=FAKE_INPUT)
    assert result["cached"] == 1 and result["called"] == 0 and result["upserted"] == 1


def test_limit_bounds_api_calls_not_cache_replays(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60, "3": "TG3 c " * 60})
    result = classify_tilstand(
        conn, tmp_path, limit=2, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT
    )
    assert result["called"] == 2 and result["limit_skipped"] == 1


def test_cache_only_never_calls(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})

    def explode(text):
        raise AssertionError("cache_only must not call the API")

    result = classify_tilstand(conn, tmp_path, cache_only=True, _call=explode, _input_fn=FAKE_INPUT)
    assert result["uncached_skipped"] == 1 and result["upserted"] == 0


def test_empty_input_and_missing_html_are_counted_not_fatal(tmp_path):
    conn = _env(tmp_path, {"1": "   "})
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('2')")  # no html file
    conn.commit()
    result = classify_tilstand(conn, tmp_path, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT)
    assert result["empty_input"] == 1 and result["missing_html"] == 1


def test_bad_api_response_is_error_not_cached(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    result = classify_tilstand(conn, tmp_path, _call=lambda t: "not json", _input_fn=FAKE_INPUT)
    assert result["errors"] == 1 and result["upserted"] == 0
    assert cache_get(conn, content_sha(("TG3 bad " * 50).strip())) is None
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

```python
# skannonser/enrich/tilstand_backfill.py
"""Local classification driver over cached ad HTML (2026-08-05 design spec).

Mirrors `backfill_salgsoppgave`'s walk, plus the cache/spend logic:
- `salgsoppgave_llm_cache` hit -> free replay, always processed.
- Miss -> one API call, bounded by `limit` (the spend control; the Batch API
  has none of its own) unless `cache_only`.
Responses are validated BEFORE caching so a malformed response never poisons
the cache. Purely local: reads the on-disk HTML cache, never FINN.
"""
import sqlite3
from pathlib import Path

from skannonser.enrich.tilstand import (
    TilstandResponse, _anthropic_call, cache_get, cache_put, classify_input,
    compute_rollup, content_sha,
)
from skannonser.store.repositories.tilstand import TilstandRepo


def classify_tilstand(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int | None = None,
    wipe: bool = False,
    cache_only: bool = False,
    _call=None,
    _input_fn=None,
) -> dict:
    call = _call or _anthropic_call
    input_fn = _input_fn or classify_input
    repo = TilstandRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")]
    counts = {
        "eiendom_rows": len(finnkodes), "missing_html": 0, "empty_input": 0,
        "cached": 0, "called": 0, "limit_skipped": 0, "uncached_skipped": 0,
        "errors": 0, "upserted": 0,
    }
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            counts["missing_html"] += 1
            continue
        try:
            text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            counts["errors"] += 1
            continue
        if text is None:
            counts["empty_input"] += 1
            continue
        sha = content_sha(text)
        raw = cache_get(conn, sha)
        if raw is None:
            if cache_only:
                counts["uncached_skipped"] += 1
                continue
            if limit is not None and counts["called"] >= limit:
                counts["limit_skipped"] += 1
                continue
            try:
                raw = call(text)
                counts["called"] += 1
                resp = TilstandResponse.model_validate_json(raw)
            except Exception:
                counts["errors"] += 1
                continue
            cache_put(conn, sha, raw)
        else:
            counts["cached"] += 1
            try:
                resp = TilstandResponse.model_validate_json(raw)
            except Exception:
                counts["errors"] += 1
                continue
        repo.upsert_ad(
            finnkode,
            [f.model_dump() for f in resp.findings],
            resp.egenerklaering,
            compute_rollup(resp),
        )
        counts["upserted"] += 1
    return counts
```

- [ ] **Step 4: Run** `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_classify_tilstand.py -q` — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(enrich): sync tilstand classification driver"`

---

### Task 8: Batch API mode

**Files:**
- Modify: `skannonser/enrich/tilstand_backfill.py`
- Test: `tests/rebuild/test_classify_tilstand.py` (extend)

**Interfaces:**
- Produces: `classify_tilstand_batch(conn, project_dir, *, limit=None, _client=None, _sleep=None, _input_fn=None) -> dict` — gathers uncached inputs (dedup by sha, bounded by `limit`), submits ONE batch, polls until `processing_status == "ended"`, caches validated successes keyed by sha, then runs `classify_tilstand(..., cache_only=True)` to derive+upsert. Returns `{submitted, succeeded, failed, ...derive counters prefixed derive_}`.
- `custom_id` per request = the input's `content_sha` (64 hex chars, exactly the Batch API's 64-char cap; results arrive unordered, keyed by custom_id, never by position).

- [ ] **Step 1: Write failing tests**

```python
from types import SimpleNamespace

from skannonser.enrich.tilstand_backfill import classify_tilstand_batch


class FakeBatchClient:
    """Stands in for anthropic.Anthropic(): create -> poll twice -> results."""

    def __init__(self, response_json):
        self.response_json = response_json
        self.submitted = None
        self.polls = 0
        outer = self

        class _Batches:
            def create(self, requests):
                outer.submitted = requests
                return SimpleNamespace(id="b1", processing_status="in_progress")

            def retrieve(self, batch_id):
                outer.polls += 1
                status = "ended" if outer.polls >= 2 else "in_progress"
                return SimpleNamespace(id=batch_id, processing_status=status)

            def results(self, batch_id):
                for req in outer.submitted:
                    yield SimpleNamespace(
                        custom_id=req["custom_id"],
                        result=SimpleNamespace(
                            type="succeeded",
                            message=SimpleNamespace(
                                stop_reason="end_turn",
                                content=[SimpleNamespace(type="text", text=outer.response_json)],
                            ),
                        ),
                    )

        self.messages = SimpleNamespace(batches=_Batches())


def test_batch_submits_polls_caches_and_derives(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60})
    client = FakeBatchClient(RESPONSE)
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["submitted"] == 2 and result["succeeded"] == 2
    assert client.polls >= 2
    assert result["derive_upserted"] == 2
    # requests keyed by content sha, params carry the strict schema
    req = client.submitted[0]
    assert len(req["custom_id"]) == 64
    assert req["params"]["output_config"]["format"]["type"] == "json_schema"


def test_batch_skips_cached_and_dedups_identical_inputs(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 same " * 40, "2": "TG3 same " * 40})
    client = FakeBatchClient(RESPONSE)
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["submitted"] == 1          # identical text -> one request
    assert result["derive_upserted"] == 2    # ...but both ads get rows


def test_batch_failed_result_is_counted_not_cached(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60})
    client = FakeBatchClient("not json")
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["failed"] == 1 and result["succeeded"] == 0
    assert result["derive_upserted"] == 0


def test_batch_nothing_to_do(tmp_path):
    conn = _env(tmp_path, {"1": "   "})
    result = classify_tilstand_batch(
        conn, tmp_path, _client=FakeBatchClient(RESPONSE),
        _sleep=lambda s: None, _input_fn=FAKE_INPUT,
    )
    assert result["submitted"] == 0
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

Append to `tilstand_backfill.py`:

```python
import time

from skannonser.enrich.tilstand import _MODEL, _SYSTEM_PROMPT, TILSTAND_SCHEMA


def _default_client():
    import anthropic  # lazy: only where classification actually runs

    return anthropic.Anthropic()


def _pending_inputs(conn, project_dir, input_fn, limit) -> dict[str, str]:
    """sha -> input text for every ad whose input is not yet cached.
    Dedup by sha is automatic (dict key); `limit` bounds the request count."""
    pending: dict[str, str] = {}
    for (finnkode,) in conn.execute("SELECT finnkode FROM eiendom"):
        if limit is not None and len(pending) >= limit:
            break
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            continue
        try:
            text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
        if text is None:
            continue
        sha = content_sha(text)
        if sha not in pending and cache_get(conn, sha) is None:
            pending[sha] = text
    return pending


def classify_tilstand_batch(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int | None = None,
    _client=None,
    _sleep=None,
    _input_fn=None,
) -> dict:
    """Backfill via the Batch API (50% cheaper). Fills the cache, then the
    sync driver derives rows from it -- so an interrupted run loses nothing
    already paid for."""
    input_fn = _input_fn or classify_input
    sleep = _sleep or time.sleep
    pending = _pending_inputs(conn, project_dir, input_fn, limit)
    counts = {"submitted": len(pending), "succeeded": 0, "failed": 0}
    if pending:
        client = _client or _default_client()
        requests = [
            {
                "custom_id": sha,  # sha256 hex = 64 chars = the API's cap, exactly
                "params": {
                    "model": _MODEL,
                    "max_tokens": 16000,
                    "system": _SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": text}],
                    "output_config": {
                        "format": {"type": "json_schema", "schema": TILSTAND_SCHEMA}
                    },
                },
            }
            for sha, text in pending.items()
        ]
        batch = client.messages.batches.create(requests=requests)
        while True:
            batch = client.messages.batches.retrieve(batch.id)
            if batch.processing_status == "ended":
                break
            sleep(60)
        for result in client.messages.batches.results(batch.id):
            ok = (
                result.result.type == "succeeded"
                and getattr(result.result.message, "stop_reason", None) != "refusal"
            )
            raw = None
            if ok:
                raw = next(
                    (b.text for b in result.result.message.content if b.type == "text"),
                    None,
                )
            if raw is not None:
                try:
                    TilstandResponse.model_validate_json(raw)
                except Exception:
                    raw = None
            if raw is None:
                counts["failed"] += 1
                continue
            cache_put(conn, result.custom_id, raw)
            counts["succeeded"] += 1
    derive = classify_tilstand(conn, project_dir, cache_only=True, _input_fn=_input_fn)
    counts.update({f"derive_{k}": v for k, v in derive.items()})
    return counts
```

- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(enrich): Batch API mode for the tilstand backfill"`

---

### Task 9: Validation harness (stage-1 ground truth)

**Files:**
- Create: `skannonser/enrich/tilstand_validate.py`
- Test: `tests/rebuild/test_tilstand_validate.py`

**Interfaces:**
- Consumes: `GRID`, `classify_input`, `classify_one` from `tilstand.py`.
- Produces:
  - `stated_bands(text: str) -> list[tuple[int, int]]` — surveyor-stated cost bands, snapped to the grid
  - `strip_stated_costs(text: str) -> str` — same text with the label+value spans removed
  - `snap_band(lav: int, hoy: int) -> tuple[int, int]` — outward snap onto `GRID`
  - `validate_estimates(conn, project_dir, *, limit=50, _call=None, _input_fn=None) -> dict` — report with keys `ads, pairs, exact, within_one, model_higher, model_lower, stated_unmatched, model_unmatched`

- [ ] **Step 1: Write failing tests**

```python
# tests/rebuild/test_tilstand_validate.py
from skannonser.enrich.tilstand_validate import snap_band, stated_bands, strip_stated_costs


def test_snap_band_outward():
    assert snap_band(15_000, 60_000) == (10_000, 100_000)
    assert snap_band(10_000, 50_000) == (10_000, 50_000)   # already on grid
    assert snap_band(700_000, 2_000_000) == (500_000, 1_000_000)  # ceiling caps at 1M+


def test_stated_bands_covers_the_observed_phrasings():
    text = (
        "Badet er ikke tett. Kostnadsestimat: 200 000 - 500 000,-. "
        "Vinduer med trekarmer. Utbedringskostnader: Under 10 000. "
        "Taket har mose. Estimert prisanslag Kr 100 000 - 300 000."
    )
    assert stated_bands(text) == [(200_000, 500_000), (0, 10_000), (100_000, 300_000)]


def test_stated_bands_ignores_prose_mentions_without_figures():
    # boilerplate: "angir ... kostnadsoverslag for eventuelle oppgraderinger"
    assert stated_bands("Rapporten angir kostnadsoverslag for oppgraderinger.") == []


def test_strip_removes_figures_but_keeps_defect_text():
    text = "Badet er ikke tett. Kostnadsestimat: 200 000 - 500 000. Må utbedres."
    stripped = strip_stated_costs(text)
    assert "200 000" not in stripped
    assert "Badet er ikke tett" in stripped and "Må utbedres" in stripped
```

Plus one driver test (reuses `_env`/`RESPONSE`/`FAKE_INPUT` — import them from `test_classify_tilstand.py` or duplicate the three definitions locally; duplication is fine here):

```python
import json
from skannonser.store import connection, migrations
from skannonser.enrich.tilstand_validate import validate_estimates

AD_TEXT = ("Badet er ikke tett og må renoveres. TG3. "
           "Kostnadsestimat: 200 000 - 500 000. ") * 5

ESTIMATE_RESPONSE = json.dumps({
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 100_000, "kostnad_hoy": 300_000, "kostnad_kilde": "estimat"},
    ],
    "egenerklaering_present": False,
    "egenerklaering": [],
    "tilstandsrapport_dato": None,
    "tilstandsrapport_utsteder": None,
})


def test_validate_pairs_and_scores(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    (tmp_path / "html_extracted").mkdir()
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('1')")
    (tmp_path / "html_extracted" / "1.html").write_text(AD_TEXT)
    conn.commit()

    seen = []
    report = validate_estimates(
        conn, tmp_path, limit=10,
        _call=lambda t: seen.append(t) or ESTIMATE_RESPONSE,
        _input_fn=lambda html: html.strip() or None,
    )
    # the model was shown the STRIPPED text
    assert "200 000" not in seen[0]
    assert report["ads"] == 1
    # AD_TEXT repeats the stated band 5x -> 5 stated bands, model gave 1
    assert report["pairs"] == 1 and report["stated_unmatched"] == 4
    # (100k, 300k) vs stated (200k, 500k): one grid step off on both bounds
    assert report["exact"] == 0 and report["within_one"] == 1
    assert report["model_lower"] == 1
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

```python
# skannonser/enrich/tilstand_validate.py
"""Stage-1 validation harness (design spec 'Validation: the free ground
truth'). The ~12% of ads with surveyor-stated cost bands are labelled data
for the estimation task: strip the stated figures, let the model estimate
blind, compare. Acceptance gate: >=70% within one band, no direction bias.
Never used in production classification -- there, stated costs stay in.
"""
import re
import sqlite3
from pathlib import Path

from skannonser.enrich.tilstand import GRID, classify_input, classify_one

# The label vocabulary measured over 500 ads (design spec 'Measurements').
_COST_LABEL = re.compile(
    r"(kostnadsestimat|utbedringskostnad\w*|kostnadsoverslag"
    r"|(?:sjablongmessig\w*\s+|estimert\w*\s+)?prisanslag"
    r"|oppgraderingskostnad\w*|estimert\w*\s+kostnad\w*)"
    r"(?:\s+gjelder(?:\s+er)?)?\s*:?\s*",
    re.I,
)
# A band range or an open-ended bound, right after a label.
_COST_VALUE = re.compile(
    r"(?:under|over)\s*(?:kr\.?\s*)?[\d][\d\s.]{2,}"
    r"|(?:kr\.?\s*)?[\d][\d\s.]{2,}\s*(?:-|–|til)\s*(?:kr\.?\s*)?[\d][\d\s.]{2,}",
    re.I,
)


def snap_band(lav: int, hoy: int) -> tuple[int, int]:
    """Outward snap: never narrower than the surveyor said."""
    lo = max((g for g in GRID if g <= lav), default=0)
    hi = min((g for g in GRID if g >= hoy), default=GRID[-1])
    return lo, hi


def _amounts(raw: str) -> list[int]:
    out = []
    for m in re.findall(r"[\d][\d\s.]{2,}", raw):
        digits = re.sub(r"\D", "", m)
        if digits and int(digits) >= 1000:
            out.append(int(digits))
    return out


def _label_value_spans(text: str) -> list[tuple[int, int, str]]:
    """(start, end, value_text) for each stated label+figure occurrence."""
    spans = []
    for m in _COST_LABEL.finditer(text):
        v = _COST_VALUE.match(text[m.end():m.end() + 60])
        if v:
            spans.append((m.start(), m.end() + v.end(), v.group(0)))
    return spans


def stated_bands(text: str) -> list[tuple[int, int]]:
    bands = []
    for _, _, raw in _label_value_spans(text):
        nums = _amounts(raw)
        if not nums:
            continue
        low = raw.lower()
        if len(nums) == 1:
            lav, hoy = ((0, nums[0]) if "under" in low
                        else (nums[0], GRID[-1]) if "over" in low
                        else (nums[0], nums[0]))
        else:
            lav, hoy = nums[0], nums[1]
        if lav <= hoy:
            bands.append(snap_band(lav, hoy))
    return bands


def strip_stated_costs(text: str) -> str:
    out, prev = [], 0
    for a, b, _ in _label_value_spans(text):
        out.append(text[prev:a])
        prev = b
    out.append(text[prev:])
    return "".join(out)


def validate_estimates(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int = 50,
    _call=None,
    _input_fn=None,
) -> dict:
    input_fn = _input_fn or classify_input
    report = {"ads": 0, "pairs": 0, "exact": 0, "within_one": 0,
              "model_higher": 0, "model_lower": 0,
              "stated_unmatched": 0, "model_unmatched": 0}
    for (finnkode,) in conn.execute("SELECT finnkode FROM eiendom"):
        if report["ads"] >= limit:
            break
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            continue
        text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        if text is None:
            continue
        stated = sorted(stated_bands(text), key=lambda b: b[0] + b[1])
        if not stated:
            continue
        try:
            resp = (classify_one(strip_stated_costs(text), _call=_call)
                    if _call else classify_one(strip_stated_costs(text)))
        except Exception:
            continue
        model = sorted(
            ((f.kostnad_lav, f.kostnad_hoy) for f in resp.findings
             if f.kostnad_lav is not None and f.kostnad_kilde == "estimat"),
            key=lambda b: b[0] + b[1],
        )
        report["ads"] += 1
        n = min(len(stated), len(model))
        report["stated_unmatched"] += len(stated) - n
        report["model_unmatched"] += len(model) - n
        for (slav, shoy), (mlav, mhoy) in zip(stated[:n], model[:n]):
            report["pairs"] += 1
            if (mlav, mhoy) == (slav, shoy):
                report["exact"] += 1
            if (abs(GRID.index(mlav) - GRID.index(slav)) <= 1
                    and abs(GRID.index(mhoy) - GRID.index(shoy)) <= 1):
                report["within_one"] += 1
            mmid, smid = (mlav + mhoy) / 2, (slav + shoy) / 2
            if mmid > smid:
                report["model_higher"] += 1
            elif mmid < smid:
                report["model_lower"] += 1
    return report
```

Note the boilerplate test: `_COST_LABEL` matches "kostnadsoverslag" in prose, but `_COST_VALUE.match` right after it finds no figure, so no band — that's the mechanism keeping mention-boilerplate out.

- [ ] **Step 4: Run** — expect PASS. Tune `_COST_LABEL`'s optional `gjelder`-tail only if the fixture tests demand it.
- [ ] **Step 5: Commit** — `git commit -m "feat(enrich): stage-1 validation harness with surveyor ground truth"`

---

### Task 10: CLI command

**Files:**
- Modify: `skannonser/commands/tools_cmd.py` (mirror `backfill_salgsoppgave_cmd` at line ~87 exactly — db guard, pending-migrations guard, typer patterns)
- Test: `tests/rebuild/test_cli.py` (extend; read its existing salgsoppgave CLI tests first and mirror their invocation style)

**Interfaces:**
- Consumes: `classify_tilstand`, `classify_tilstand_batch` (Tasks 7–8), `validate_estimates` (Task 9), `TilstandRepo` (Task 3).
- Produces: `skannonser tools classify-tilstand` with `--db`, `--project-dir`, `--limit`, `--wipe`, `--batch`, `--validate`, `--status`.

- [ ] **Step 1: Write failing tests**

Mirror the existing `test_cli.py` style (CliRunner or subprocess — copy whichever the file uses). Cover at minimum:

```python
def test_classify_tilstand_status_prints_coverage(...):
    # create migrated DB at tmp path, invoke
    #   tools classify-tilstand --db <path> --status
    # assert exit code 0 and "tilstand_rows" in output

def test_classify_tilstand_refuses_pending_migrations(...):
    # DB with schema but unmigrated bookkeeping -> exit code 1,
    # "pending migrations" in stderr  (copy the existing backfill test for this)
```

The classification paths themselves are covered by Tasks 7–9; the CLI tests only guard wiring. Do not invoke the API paths from CLI tests (no seam injection through typer — keep those paths untested at CLI level, tested at driver level).

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

Append to `tools_cmd.py`:

```python
@app.command(name="classify-tilstand")
def classify_tilstand_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir",
        help="FINN cache root (html_extracted/ lives here)"),
    limit: int | None = typer.Option(
        None, "--limit",
        help="Max NEW API classifications this run -- the spend control. "
             "Cache replays are unlimited and free."),
    wipe: bool = typer.Option(
        False, "--wipe",
        help="Clear the tilstand tables first. The LLM cache survives, so the "
             "rebuild replays paid responses for free."),
    batch: bool = typer.Option(
        False, "--batch",
        help="Submit uncached ads via the Batch API (50%% cheaper), poll until "
             "done, then derive rows from the cache."),
    validate: bool = typer.Option(
        False, "--validate",
        help="Stage-1 harness: blind-estimate ads that carry surveyor-stated "
             "costs and score against them. Calls the API; respects --limit."),
    status: bool = typer.Option(False, "--status", help="Print coverage only"),
) -> None:
    """Classify TG2/TG3 condition findings from cached salgsoppgave text
    (Claude Opus 5). COSTS MONEY on uncached ads -- run staged: --limit 200,
    check, --limit 1000, check, then the rest with --batch. Requires
    `pip install -e .[llm]` and ANTHROPIC_API_KEY locally; the server never
    needs either."""
    from skannonser.enrich.tilstand_backfill import (
        classify_tilstand, classify_tilstand_batch,
    )
    from skannonser.enrich.tilstand_validate import validate_estimates
    from skannonser.store.repositories.tilstand import TilstandRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    repo = TilstandRepo(conn)
    if status:
        typer.echo(f"classify-tilstand status: {repo.coverage()}")
        return
    if validate:
        report = validate_estimates(conn, project_dir, limit=limit or 50)
        typer.echo(f"validate: {report}")
        return
    if batch:
        result = classify_tilstand_batch(conn, project_dir, limit=limit)
    else:
        result = classify_tilstand(conn, project_dir, limit=limit, wipe=wipe)
    typer.echo(f"classify-tilstand: {result}")
    typer.echo(f"coverage: {repo.coverage()}")
```

- [ ] **Step 4: Run** `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_cli.py -q` — expect PASS.
- [ ] **Step 5: Run the FULL Python suite** — `PYTHONPATH=. ./.venv/bin/pytest -q`. Expect ≥ baseline (799 + this plan's additions), zero failures.
- [ ] **Step 6: Commit** — `git commit -m "feat(cli): tools classify-tilstand (sync/batch/validate/status)"`

---

### Task 11: Web API plumbing

**Files:**
- Modify: `skannonser/publish/rows.py` (`_EIE_SELECT_TAIL` at ~line 99, `_EIE_JOINS` at ~line 135; check whether `_SOLD_API_SQL`/sold-records SQL uses the same fragments — if it has its own joins, mirror there too)
- Modify: `skannonser/web/api.py` (`_eie_item` at ~line 304, its four call sites — grep `_eie_item(` — and the filters-meta endpoint at ~line 684)
- Test: `tests/rebuild/test_web_api.py` (extend, mirroring its existing salgsoppgave-field tests)

**Interfaces:**
- Consumes: `listing_tilstand`, `listing_tg_findings` (migration 016).
- Produces (API item keys, all `None`/`[]` when unclassified):
  - `tg2_count, tg3_count, reparasjon_lav, reparasjon_hoy, reparasjon_est, alvorlighet, verste_bygningsdel, reparasjon_kilde` (flat, like the migration-015 keys)
  - `tg_findings`: list of `{tg, bygningsdel, alvorlighet, kostnad_lav, kostnad_hoy, kostnad_kilde}` dicts, cost-descending
  - Filters meta gains `"alvorligheter"`: distinct non-null values in severity order

- [ ] **Step 1: Write failing tests**

In `test_web_api.py`, find how existing tests seed `listing_salgsoppgave` and assert item keys; mirror:

```python
def test_tilstand_rollup_flows_into_item(...):
    # seed listing_tilstand + two listing_tg_findings rows for a finnkode,
    # request the listings endpoint, assert:
    #   item["tg3_count"] == 1
    #   item["reparasjon_est"] == 380000
    #   item["alvorlighet"] == "alvorlig"
    #   item["reparasjon_kilde"] == "blandet"
    #   item["tg_findings"][0]["bygningsdel"] == "vatrom"   # cost-descending

def test_unclassified_listing_has_null_tilstand_keys(...):
    # no listing_tilstand row -> keys present, values None; tg_findings == []

def test_filters_meta_lists_alvorligheter(...):
    # seed two rows with alvorlighet 'mindre' and 'alvorlig'
    # meta endpoint -> "alvorligheter" == ["mindre", "alvorlig"]  (severity order)
```

Write these as real tests using the file's existing fixtures/client helpers — copy an adjacent salgsoppgave test and adapt.

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

`rows.py` — append to `_EIE_SELECT_TAIL` (after the `s.heftelser` line):

```sql
    ,t.tg2_count AS "TG2_COUNT",
    t.tg3_count AS "TG3_COUNT",
    t.reparasjon_lav AS "REPARASJON_LAV",
    t.reparasjon_hoy AS "REPARASJON_HOY",
    t.reparasjon_est AS "REPARASJON_EST",
    t.alvorlighet AS "ALVORLIGHET",
    t.verste_bygningsdel AS "VERSTE_BYGNINGSDEL",
    t.reparasjon_kilde AS "REPARASJON_KILDE"
```

and to `_EIE_JOINS`:

```sql
    LEFT JOIN listing_tilstand t ON t.finnkode = e.finnkode
```

Update the module docstring's join inventory. If the sold-records SQL builds its own SELECT (check `_SOLD_API_SQL` in api.py), add the same columns/join there so sold listings carry tilstand too.

`api.py` — next to `_facilities_by_finnkode`:

```python
def _tg_findings_by_finnkode(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Every listing's TG findings in one query, worst-cost first -- same
    group-in-Python pattern as _facilities_by_finnkode."""
    out: dict[str, list[dict]] = {}
    for row in conn.execute(
        "SELECT finnkode, tg, bygningsdel, alvorlighet, "
        "       kostnad_lav, kostnad_hoy, kostnad_kilde "
        "FROM listing_tg_findings "
        "ORDER BY finnkode, kostnad_hoy DESC NULLS LAST, tg DESC"
    ):
        out.setdefault(str(row["finnkode"]), []).append({
            "tg": row["tg"],
            "bygningsdel": row["bygningsdel"],
            "alvorlighet": row["alvorlighet"],
            "kostnad_lav": row["kostnad_lav"],
            "kostnad_hoy": row["kostnad_hoy"],
            "kostnad_kilde": row["kostnad_kilde"],
        })
    return out
```

(SQLite pre-3.30 lacks `NULLS LAST`; this repo runs 3.51 — fine.)

`_eie_item`: add parameter `tg_findings: list[dict] | None = None` and, next to the migration-015 keys:

```python
        # Tilstand classifier (migration 016; None/[] when unclassified).
        "tg2_count": rec.get("TG2_COUNT"),
        "tg3_count": rec.get("TG3_COUNT"),
        "reparasjon_lav": rec.get("REPARASJON_LAV"),
        "reparasjon_hoy": rec.get("REPARASJON_HOY"),
        "reparasjon_est": rec.get("REPARASJON_EST"),
        "alvorlighet": rec.get("ALVORLIGHET"),
        "verste_bygningsdel": rec.get("VERSTE_BYGNINGSDEL"),
        "reparasjon_kilde": rec.get("REPARASJON_KILDE"),
        "tg_findings": tg_findings or [],
```

At every `_eie_item(` call site (grep — there are four), compute `tgf = _tg_findings_by_finnkode(conn)` once per request alongside `facs` and pass `tg_findings=tgf.get(rec.get("_finnkode"))`.

Filters-meta endpoint — next to `"energimerker"`:

```python
        "alvorligheter": [
            v for v in ("kosmetisk", "mindre", "vesentlig", "alvorlig")
            if conn.execute(
                "SELECT 1 FROM listing_tilstand WHERE alvorlighet = ? LIMIT 1", (v,)
            ).fetchone()
        ],
```

(Fixed severity order, not alphabetical — `ORDER BY` on the text values would shuffle the scale.)

- [ ] **Step 4: Run** `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_web_api.py -q` — expect PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(web): expose tilstand rollups and findings through the API"`

---

### Task 12: Frontend — formatters and popup

**Files:**
- Modify: `skannonser/web/static/listingmeta.js` (formatters + label vocab; read the whole file first — the vocab/formatter conventions at lines ~140–215 are the template)
- Modify: `skannonser/web/static/popup.js` (new "Tilstand" block after the "Fra salgsoppgaven" block at ~line 265)
- Test: `tests/web/tilstand.test.mjs` (new; mirror `tests/web/salgsoppgave.test.mjs`)

**Interfaces:**
- Consumes: item keys from Task 11.
- Produces (exported from `listingmeta.js`):
  - `BYGNINGSDEL_LABELS` — enum→Norwegian label map, all 18 values
  - `fmtAlvorlighet(value) -> string | null`
  - `fmtBygningsdel(value) -> string | null`
  - `fmtKostnadBand(lav, hoy, kilde) -> string | null` — `"200 000 – 500 000 kr"`, `estimat`/`blandet` prefixed `"~"`, `(0, x)` → `"under x kr"`, `(x, 1000000)` where x < 1000000 → `"over x kr"`

- [ ] **Step 1: Write failing tests**

```javascript
// tests/web/tilstand.test.mjs
// Display formatting for the tilstand classifier (migration 016). Same two
// guards as salgsoppgave.test.mjs: booleans/nulls must not leak as JS
// literals, and enum keys must never reach a Norwegian reader verbatim --
// but an UNMAPPED key passes through as-is (parser grew a value the UI
// hasn't caught up with; ugly beats silent).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  fmtAlvorlighet,
  fmtBygningsdel,
  fmtKostnadBand,
  BYGNINGSDEL_LABELS,
} from "../../skannonser/web/static/listingmeta.js";

test("fmtAlvorlighet maps the four tiers and passes null through", () => {
  assert.equal(fmtAlvorlighet("alvorlig"), "Alvorlig");
  assert.equal(fmtAlvorlighet("kosmetisk"), "Kosmetisk");
  assert.equal(fmtAlvorlighet(null), null);
  assert.equal(fmtAlvorlighet("ukjent_verdi"), "ukjent_verdi"); // unmapped passes through
});

test("fmtBygningsdel covers the whole enum", () => {
  assert.equal(Object.keys(BYGNINGSDEL_LABELS).length, 18);
  assert.equal(fmtBygningsdel("vatrom"), "Våtrom");
  assert.equal(fmtBygningsdel("vinduer_dorer"), "Vinduer/dører");
  assert.equal(fmtBygningsdel(null), null);
});

test("fmtKostnadBand renders bands with provenance", () => {
  // takst: the surveyor said it -- no hedge marker
  assert.equal(fmtKostnadBand(200000, 500000, "takst"), "200 000 – 500 000 kr");
  // estimat/blandet: model judgment -- hedged with ~
  assert.equal(fmtKostnadBand(200000, 500000, "estimat"), "~200 000 – 500 000 kr");
  assert.equal(fmtKostnadBand(200000, 500000, "blandet"), "~200 000 – 500 000 kr");
  assert.equal(fmtKostnadBand(0, 10000, "takst"), "under 10 000 kr");
  assert.equal(fmtKostnadBand(500000, 1000000, "estimat"), "~over 500 000 kr");
  assert.equal(fmtKostnadBand(null, null, null), null);
  assert.equal(fmtKostnadBand(1000000, 1000000, "takst"), "over 1 000 000 kr");
});
```

- [ ] **Step 2: Run** `node --test tests/web/tilstand.test.mjs` — expect FAIL (exports missing).

- [ ] **Step 3: Implement in `listingmeta.js`**

Follow the file's `fromVocab` convention (unmapped keys pass through). Use the existing `fmtPris`-style number formatting if a helper exists — read the file; otherwise:

```javascript
// --- Tilstand classifier (migration 016) ------------------------------------
// Same discipline as the salgsoppgave fields: null means "never classified",
// which addRow must skip -- never render as "0 kr" or "Nei".

const ALVORLIGHET_LABELS = {
  kosmetisk: "Kosmetisk",
  mindre: "Mindre",
  vesentlig: "Vesentlig",
  alvorlig: "Alvorlig",
};

export const BYGNINGSDEL_LABELS = {
  vatrom: "Våtrom",
  kjokken: "Kjøkken",
  tak: "Tak",
  vinduer_dorer: "Vinduer/dører",
  yttervegg: "Yttervegg",
  etasjeskille: "Etasjeskille",
  grunn_drenering: "Grunn/drenering",
  vvs: "VVS",
  elektrisk: "Elektrisk",
  ventilasjon: "Ventilasjon",
  overflater: "Overflater",
  balkong_terrasse: "Balkong/terrasse",
  trapp: "Trapp",
  radon: "Radon",
  vaskerom: "Vaskerom",
  utvendig_annet: "Utvendig annet",
  helhet: "Helhet",
  annet: "Annet",
};

export function fmtAlvorlighet(value) {
  return fromVocab(ALVORLIGHET_LABELS, value);
}

export function fmtBygningsdel(value) {
  return fromVocab(BYGNINGSDEL_LABELS, value);
}

function fmtKr(n) {
  return n.toLocaleString("nb-NO").replace(/ /g, " ");
}

// Cost band with provenance: `takst` is the surveyor's own figure and renders
// plain; `estimat`/`blandet` carry model judgment and are hedged with "~".
// The grid's 1 000 000 ceiling means "1M+", so a band touching it is open.
export function fmtKostnadBand(lav, hoy, kilde) {
  if (lav === null || lav === undefined || hoy === null || hoy === undefined) return null;
  const hedge = kilde === "takst" ? "" : "~";
  if (lav === 0) return hedge + "under " + fmtKr(hoy) + " kr";
  if (hoy === 1000000 && lav < 1000000) return hedge + "over " + fmtKr(lav) + " kr";
  if (hoy === 1000000) return hedge.replace("~", "") + "over " + fmtKr(lav) + " kr";
  if (lav === hoy) return hedge + fmtKr(lav) + " kr";
  return hedge + fmtKr(lav) + " – " + fmtKr(hoy) + " kr";
}
```

Wait — the `(1000000, 1000000, "takst")` test expects `"over 1 000 000 kr"` (no hedge, takst). Order the branches: `lav === 0` first, then `hoy === 1000000` (covers both the `lav < 1000000` and `lav === 1000000` cases identically) → `hedge + "over " + fmtKr(lav) + " kr"`, then `lav === hoy`, then the range. Delete the duplicate `hoy === 1000000` line above — one branch suffices:

```javascript
export function fmtKostnadBand(lav, hoy, kilde) {
  if (lav === null || lav === undefined || hoy === null || hoy === undefined) return null;
  const hedge = kilde === "takst" ? "" : "~";
  if (lav === 0) return hedge + "under " + fmtKr(hoy) + " kr";
  if (hoy === 1000000) return hedge + "over " + fmtKr(lav) + " kr";
  if (lav === hoy) return hedge + fmtKr(lav) + " kr";
  return hedge + fmtKr(lav) + " – " + fmtKr(hoy) + " kr";
}
```

Also export a hint constant next to `SALGSOPPGAVE_HINT`:

```javascript
export const TILSTAND_HINT =
  "Fra tilstandsrapporten, KI-klassifisert. ~ = kostnadsanslag fra modellen, " +
  "ikke takstmannens tall. Tomt felt betyr at ingen tilstandsrapport ble lest.";
```

- [ ] **Step 4: Run** `node --test tests/web/tilstand.test.mjs` — expect PASS.

- [ ] **Step 5: Popup block**

In `popup.js`, import the new formatters, then after the "Fra salgsoppgaven" block (mirror its shape exactly — conditional `dl`, heading with `title` hint):

```javascript
  // Tilstand classifier (migration 016), under its own heading. Absent
  // entirely when the listing was never classified.
  const tdl = el("dl");
  if (item.tg2_count !== null && item.tg2_count !== undefined) {
    addRow(tdl, "TG2 / TG3", item.tg2_count + " / " + item.tg3_count);
  }
  addRow(tdl, "Alvorlighet",
    item.alvorlighet
      ? fmtAlvorlighet(item.alvorlighet)
        + (item.verste_bygningsdel ? " – " + fmtBygningsdel(item.verste_bygningsdel) : "")
      : null);
  addRow(tdl, "Utbedring",
    fmtKostnadBand(item.reparasjon_lav, item.reparasjon_hoy, item.reparasjon_kilde));
  if (tdl.childNodes.length) {
    const thead = el("p", "sk-dl-head", "Tilstand");
    thead.title = TILSTAND_HINT;
    body.appendChild(thead);
    body.appendChild(tdl);
    // Findings list, worst first (API pre-sorts by kostnad_hoy DESC)
    if (item.tg_findings && item.tg_findings.length) {
      const ul = el("ul", "sk-tg-findings");
      for (const f of item.tg_findings) {
        const band = fmtKostnadBand(f.kostnad_lav, f.kostnad_hoy, f.kostnad_kilde);
        ul.appendChild(el(
          "li", null,
          "TG" + f.tg + " " + fmtBygningsdel(f.bygningsdel)
            + (band ? " – " + band : "")
        ));
      }
      body.appendChild(ul);
    }
  }
```

Check `el(tag, className, text)`'s actual signature in popup.js before using; adjust to match. Add minimal CSS for `.sk-tg-findings` in `style.css` (small font, tight margins — mirror an existing compact list class if one exists).

- [ ] **Step 6: Run all JS tests** `node --test tests/web/*.test.mjs` — expect PASS (157 baseline + new).
- [ ] **Step 7: Commit** — `git commit -m "feat(web): tilstand formatters and popup block"`

---

### Task 13: Frontend — table columns and filters

**Files:**
- Modify: `skannonser/web/static/table.js` (COLUMNS list ~line 80, DEFAULT_HIDDEN/migration lists ~line 107, cell-render switch ~line 440)
- Modify: `skannonser/web/static/tablefilters.js` (FILTERS descriptor map, ~line 32)
- Modify: `skannonser/web/static/filterstate.js`, `skannonser/web/static/filters.js` (read both COMPLETELY first — state keys, `selectionExcludes`, chip-row mounting, and the map-side filter application all follow one pattern; copy the `energimerke` wiring end-to-end)
- Test: extend `tests/web/tilstand.test.mjs` or the pattern used by `tests/web/salgsoppgavefilters.test.mjs` (read it — it tests filter predicates; mirror for alvorlighet)

**Interfaces:**
- Consumes: item keys + `alvorligheter` meta from Task 11, formatters from Task 12.
- Produces: three new table columns (`tg3_count` "TG3", `reparasjon_est` "Utbedring", `alvorlighet` "Alvorlighet"), default-hidden like the migration-015 columns; an `alvorlighet` chip filter (with Ukjent bucket, following the energimerke pattern); a `reparasjon_est` slider-max filter.

- [ ] **Step 1: Read the pattern files.** `filterstate.js` in full, the `energimerke` chip wiring in `filters.js` (state key, `selectionExcludes` call at ~line 129, chip-row mount), the `verditakst` column wiring in `table.js`, and one `slider-max` descriptor in `tablefilters.js`. Also `tests/web/salgsoppgavefilters.test.mjs` for how filter predicates are tested. Everything below adapts those patterns — where this plan and the codebase pattern disagree, the codebase wins.

- [ ] **Step 2: Write failing tests** for the filter predicate (mirroring `salgsoppgavefilters.test.mjs`'s import and call style): an item with `alvorlighet: "alvorlig"` is excluded when the selection excludes `"alvorlig"`; `alvorlighet: null` maps to the Ukjent bucket (`""`); `reparasjon_est` above the slider value is excluded, `null` passes (unknown never excluded by a max-slider — verify against how `verditakst`/`totalpris` sliders treat null, and match).

- [ ] **Step 3: Run, verify failure.**

- [ ] **Step 4: Implement**

`table.js` — in COLUMNS after the salgsoppgave block:

```javascript
  // Tilstand classifier (migration 016). Default-hidden like the
  // salgsoppgave columns, and listed in their own migration array so stored
  // column preferences that predate them hide them once without re-hiding.
  { key: "tg3_count", label: "TG3", sortable: true },
  { key: "reparasjon_est", label: "Utbedring", sortable: true },
  { key: "alvorlighet", label: "Alvorlighet", sortable: true },
```

Add `const TILSTAND_COLUMNS = ["tg3_count", "reparasjon_est", "alvorlighet"];`, spread into `DEFAULT_HIDDEN_COLUMNS`, and pass through the same `resolveHiddenColumns` migration mechanism as `SALGSOPPGAVE_COLUMNS` (read `loadHiddenColumns`/`resolveHiddenColumns` and extend the same way 015 did).

Cell rendering, in the switch:

```javascript
      case "tg3_count": {
        td.textContent = item.tg3_count ?? "";
        td.classList.add("num");
        break;
      }
      case "reparasjon_est": {
        const v = fmtPris(item.reparasjon_est);
        td.textContent = v
          ? (item.reparasjon_kilde === "takst" ? v : "~" + v)
          : "";
        td.classList.add("num");
        break;
      }
      case "alvorlighet": {
        td.textContent = fmtAlvorlighet(item.alvorlighet) || "";
        break;
      }
```

Sorting: check how the table sorts string columns; `alvorlighet` must sort by severity order, not alphabetically — find the sort-accessor mechanism and give it `{kosmetisk: 0, mindre: 1, vesentlig: 2, alvorlig: 3}[value]`. If the table has no per-column sort accessor, sort on the raw value and accept alphabetical for now, noting it in the commit message.

`tablefilters.js` — descriptors:

```javascript
  reparasjon_est: { kind: "slider-max", stateKey: "reparasjonMax", bound: () => REPARASJON_MAX, step: 50000, fmt: "kr" },
  alvorlighet: { /* chip row — copy the energimerke descriptor shape exactly */ },
```

with `const REPARASJON_MAX = 2_000_000;` (rollup sums can exceed the 1M grid ceiling). Wire `reparasjonMax` and the alvorlighet selection into `filterstate.js` and the map-side filter in `filters.js` exactly as `energiSelected`/`priceMax` are wired — same defaulting, same URL/localStorage persistence if those exist, same `selectionExcludes(f.alvorlighetSelected, item.alvorlighet || "")` exclusion with the trailing `""` Ukjent bucket. Chip options come from meta `alvorligheter` + `""`, labels via `fmtAlvorlighet`.

- [ ] **Step 5: Run** `node --test tests/web/*.test.mjs` — expect PASS.

- [ ] **Step 6: Serve-and-curl sanity check** (browser preview is unreliable from a worktree — curl instead):

```bash
PYTHONPATH=. ./.venv/bin/python -c "from skannonser.cli import main; main()" --help >/dev/null && echo cli-ok
grep -n "reparasjon_est" skannonser/web/static/table.js skannonser/web/static/tablefilters.js
```

(Static files are baked into the server image at deploy and served at `/`, not `/static/` — remember when verifying on the server.)

- [ ] **Step 7: Run BOTH full suites** — `PYTHONPATH=. ./.venv/bin/pytest -q` and `node --test tests/web/*.test.mjs`.
- [ ] **Step 8: Commit** — `git commit -m "feat(web): tilstand table columns and filters"`

---

### Task 14: Operator runbook (staged backfill — human-in-the-loop)

This task is documentation + the operator's checklist; no code. It exists so the spend gates from the spec are not lost at execution time.

**Files:**
- Modify: `docs/salgsoppgave-handoff.md` (append a "Phase 2 shipped" section) or create `docs/tilstand-runbook.md`

- [ ] **Step 1: Write the runbook**

Content (complete, not a placeholder — adjust paths if they moved):

```markdown
# Tilstand classifier — backfill runbook

Prereqs (local machine only; the server never needs any of this):
    ./.venv/bin/pip install -e ".[llm]"
    export ANTHROPIC_API_KEY=...        # or `ant auth login`
    # optional but recommended: set a spend limit in the Anthropic Console

Stage 0 — verify the token estimate (~free):
    Run `count_tokens` over ~50 classify_input() texts and compare against
    the spec's ~25M-token corpus estimate. If it's >2x off, recompute the
    cost table before proceeding.

Stage 1 — ~200 ads + validation (~$3):
    skannonser tools classify-tilstand --limit 200
    skannonser tools classify-tilstand --validate --limit 50
    Gates (design spec): >=70% of blind estimates within one band of the
    surveyor's figure, no systematic direction bias (model_higher vs
    model_lower roughly balanced). Also hand-check ~30 ads for alvorlighet
    and finding counts against the live FINN pages.
    If the estimate gate FAILS: stop. The estimat path drops to NULL
    (surveyor figures only) -- that is a prompt change + re-run, not a
    schema change.

Stage 2 — ~1,000 ads (~$17):
    skannonser tools classify-tilstand --limit 1000
    Watch the `annet` share of bygningsdel (SELECT bygningsdel, COUNT(*) ...):
    >20% means the enum needs new values before the full run.

Stage 3 — remainder (~$110 via Batch):
    skannonser tools classify-tilstand --batch
    Polls until the batch ends (typically <1h), then derives rows from the
    cache. Safe to interrupt and re-run: paid responses are cached by
    content hash.

Deploy: merge -> push -> server pull -> `skannonser db backup` ->
`skannonser db migrate` (016 DROPs columns -- the backup matters) ->
sync/replay locally-built rows via the normal pipeline-write path ->
`docker compose up -d --build` (the --build is load-bearing).

Ongoing: new listings are classified by re-running
`skannonser tools classify-tilstand` locally (incremental: cache hits are
free, only genuinely new salgsoppgave text is billed -- a few ads/day).
```

- [ ] **Step 2: Commit** — `git commit -m "docs: tilstand backfill runbook"`

---

## Self-Review Notes (already applied)

- Spec coverage: schema→T1, trap-fixes→T1/T2, repo→T3, selection→T4, schema/prompt/cache→T5, rollups→T6, driver→T7, batch→T8, validation gate→T9, CLI→T10, API→T11, UI→T12/T13, staged backfill + deploy→T14. The spec's "pipeline classifies new listings on next local run" is satisfied by T7's incremental cache logic + T14's ongoing note — no pipeline-code wiring, by design (classification costs money; it stays a deliberate local command).
- Type consistency: `_ROLLUP_COLS` (T3) == `compute_rollup` keys (T6) == migration 016 columns (T1); `TgFinding.model_dump()` keys (T5) == `_FINDING_COLS` (T3); counter names in T7 match T7/T8 tests; API keys (T11) match the JS consumers (T12/T13).
- Known judgment calls an implementer may hit: `field_validator` lambda style in T5 (use plain methods if pydantic complains — behavior over form); `el()` signature in popup.js; sort accessor for alvorlighet; how null interacts with existing max-sliders. In each case: read the neighboring code, follow it, and keep the test green.
