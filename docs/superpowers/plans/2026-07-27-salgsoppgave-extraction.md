# Salgsoppgave Extraction — Phase 1 (deterministic) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract typed, filterable salgsoppgave fields from the ad HTML the scanner already caches — with no new crawling, no API dependency, and no free-text columns.

**Architecture:** A payload decoder reads the serialised app state FINN ships on every ad page (two formats), yielding `objectData.ad` and its `generalText` sections. Rules extract typed scalars from those sections into `listing_salgsoppgave`; two labels the existing pricing-`<dl>` parser was silently dropping are added to `listing_details`. Everything runs offline from `data/eiendom/html_extracted/`, following the established `parse_details` → `backfill-details` → `listing_details` pattern.

**Tech Stack:** Python 3.11+, pydantic v2, BeautifulSoup4, typer, sqlite3, pytest. **No new runtime dependencies in this phase.**

**Spec:** `docs/superpowers/specs/2026-07-27-salgsoppgave-extraction-design.md`

## Scope

This plan covers **Phase 1 only: the deterministic path.** It ships working, independently useful software — boligselgerforsikring, eiendomsskatt, verditakst, ferdigattest, radon, utleie, husdyr, heftelser — with zero spend and zero new dependencies.

**Phase 2 (the tilstandsrapport/egenerklæring classifier) is deliberately a separate plan**, written after Phase 1 ships. Its model choice is resolved by evidence at backfill stage 1, so planning it in detail now would be guesswork. Migration 015 creates its tables in this phase so the schema lands once; they simply stay empty until Phase 2.

## Global Constraints

- **No free-text columns.** Every column is `INTEGER`, `BOOLEAN`, `DATE`, or enum-constrained `TEXT`. Raw prose stays in the on-disk HTML cache and is never copied into SQLite.
- **Parsers never raise on arbitrary input.** Every field is optional; a parse failure yields `None` for that field only. This mirrors `parse_details.py`.
- **Migration 015 is the only migration number available to this work.** A parallel session has claimed 016+.
- **Fully offline.** No network access in parsing, backfill, or tests.
- **Use ABSOLUTE paths for every file operation.** A relative path resolves
  against the main clone `/Users/tehbaer/kode/skannonser`, not this worktree —
  verified 2026-07-27, when a subagent silently read an unrelated task brief
  left over in the main clone's `.superpowers/sdd/`. Confirm `pwd` before any
  relative-path write.
- **The ad-HTML cache lives ONLY in the main clone**, at
  `/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted` (7,731 files).
  It is gitignored, so this worktree does not have it. Read from that absolute
  path; never write to it, and never `cd` into the main clone.
- **Baseline is 662 passing tests.** No task may reduce it.
- **Always run tests as `PYTHONPATH=. ./.venv/bin/pytest`.** A bare `pytest`
  silently tests the MAIN CLONE's code, not this worktree's: `tests/rebuild/`
  is a package but `tests/` is not, so pytest puts `tests/` on `sys.path`
  rather than the repo root, and the venv's editable-install finder then
  resolves `skannonser` to `/Users/tehbaer/kode/skannonser`. Verified
  2026-07-27 — this contradicts CLAUDE.md's claim that cwd wins. A green bare
  `pytest` run is NOT evidence your change works.
- `listing_details` keeps migration 010's full-row REPLACE semantics — **no fill-only columns, ever.**

---

### Task 1: Migration 015 — schema

**Files:**
- Create: `skannonser/store/migrations/015_salgsoppgave.sql`
- Modify: `tests/rebuild/test_migrations.py:8-22` (`EXPECTED_TABLES`, `ALL_MIGRATIONS`)

**Interfaces:**
- Consumes: nothing (first task)
- Produces: tables `listing_salgsoppgave`, `listing_tg_findings`, `listing_egenerklaering`, `salgsoppgave_llm_cache`; new columns `listing_details.eiendomsskatt_kr` and `listing_details.verditakst`

- [ ] **Step 1: Write the failing test**

In `tests/rebuild/test_migrations.py`, add the four table names to `EXPECTED_TABLES` and the migration stem to `ALL_MIGRATIONS`:

```python
EXPECTED_TABLES = {
    "eiendom", "eiendom_processed", "dnbeiendom", "manual_overrides",
    "listing_comments", "stations", "station_lines", "station_travel",
    "annotations", "sold_prices", "sold_sweep_state", "sold_price_attempts",
    "listing_details", "listing_facilities",
    "listing_salgsoppgave", "listing_tg_findings", "listing_egenerklaering",
    "salgsoppgave_llm_cache",
}

ALL_MIGRATIONS = [
    "001_adopt_live_schema", "002_notify_tables", "003_api_usage",
    "004_dnb_travel", "005_annotations", "006_sold_prices",
    "007_sold_sweep_state", "008_postnummer_pad", "009_sold_attempts",
    "010_listing_details", "011_neighbour_sold", "012_neighbour_sold_index",
    "013_gjovikbanen_missing_stations", "014_r31_north_of_jaren",
    "015_salgsoppgave",
]
```

Append a new test to the same file:

```python
def test_migration_015_adds_dl_columns_to_listing_details(tmp_path):
    """The two pricing-<dl> labels parse_details was dropping need columns."""
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_details)")}
    assert {"eiendomsskatt_kr", "verditakst"} <= cols


def test_migration_015_tg_findings_dedupes(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('1', 'u')")
    for _ in range(2):
        conn.execute(
            "INSERT OR IGNORE INTO listing_tg_findings "
            "(finnkode, tg, bygningsdel) VALUES ('1', 2, 'vatrom')"
        )
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_migrations.py -v`
Expected: FAIL — `test_migrate_fresh_db_creates_full_schema` asserts `ran == ALL_MIGRATIONS` and `015_salgsoppgave` does not exist yet.

- [ ] **Step 3: Write the migration**

Create `skannonser/store/migrations/015_salgsoppgave.sql`:

```sql
-- 015_salgsoppgave.sql
-- Salgsoppgave extraction (2026-07-27 design spec). Like migration 010 these
-- are a DERIVED, DISPOSABLE cache -- fully rebuildable from
-- data/eiendom/html_extracted/ via `skannonser tools backfill-salgsoppgave --wipe`.
-- Every column is typed or enum-constrained; no free-text columns by design.

-- No byggeaar column on purpose: eiendom.info_construction_year already
-- carries it on 99% of live rows and the API already exposes it as `byggeaar`.
CREATE TABLE IF NOT EXISTS listing_salgsoppgave (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    boligselgerforsikring BOOLEAN,
    eiendomsskatt_kr INTEGER,
    ferdigattest TEXT,          -- 'ferdigattest' | 'midlertidig' | 'ingen'
    radon_omtalt BOOLEAN,
    utleie TEXT,                -- 'tillatt' | 'ikke_tillatt' | 'egen_enhet'
    husdyr TEXT,                -- 'tillatt' | 'krever_godkjenning' | 'ikke_tillatt'
    heftelser BOOLEAN,
    -- Phase 2 (classifier) fills these; NULL until then.
    tg2_count INTEGER,
    tg3_count INTEGER,
    tilstandsrapport_dato TEXT,
    tilstandsrapport_utsteder TEXT,
    egenerklaering_antall INTEGER,
    parsed_at TEXT
);

CREATE TABLE IF NOT EXISTS listing_tg_findings (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg INTEGER NOT NULL,
    bygningsdel TEXT NOT NULL,
    tiltak TEXT,
    UNIQUE (finnkode, tg, bygningsdel)
);

CREATE TABLE IF NOT EXISTS listing_egenerklaering (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    forhold TEXT NOT NULL,
    UNIQUE (finnkode, forhold)
);

CREATE TABLE IF NOT EXISTS salgsoppgave_llm_cache (
    content_sha256 TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- The two pricing-<dl> labels parse_details.py currently drops. Deterministic
-- DOM parsing, so they belong alongside the other <dl> money fields.
ALTER TABLE listing_details ADD COLUMN eiendomsskatt_kr INTEGER;
ALTER TABLE listing_details ADD COLUMN verditakst INTEGER;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_migrations.py -v`
Expected: PASS, all tests in the file.

- [ ] **Step 5: Run the full suite**

Run: `PYTHONPATH=. ./.venv/bin/pytest`
Expected: `664 passed` (662 baseline + 2 new).

- [ ] **Step 6: Commit**

```bash
git add skannonser/store/migrations/015_salgsoppgave.sql tests/rebuild/test_migrations.py
git commit -m "feat(store): migration 015 — salgsoppgave tables + two dropped <dl> columns"
```

---

### Task 2: Capture the two dropped pricing-`<dl>` labels

`_PRICING_LABELS` silently discards `Eiendomsskatt` (8% of ads) and `Verditakst` (3%). Both are already handled by the existing `_parse_kr`; only the label map and the column lists need extending.

**Files:**
- Modify: `skannonser/ingest/finn/parse_details.py:17-43` (`ListingDetails`), `:137-145` (`_PRICING_LABELS`)
- Modify: `skannonser/store/repositories/details.py:13-20` (`_SCALAR_COLS`)
- Create (fixtures): `tests/rebuild/fixtures/finn/463763329.html`, `tests/rebuild/fixtures/finn/447401579.html`
- Test: `tests/rebuild/test_parse_details.py`

**Interfaces:**
- Consumes: Task 1's `listing_details.eiendomsskatt_kr` / `verditakst` columns
- Produces: `ListingDetails.eiendomsskatt_kr: int | None`, `ListingDetails.verditakst: int | None`

- [ ] **Step 1: Copy the two fixtures**

None of the 12 existing fixtures carries either label (verified 2026-07-27), so the parser change would otherwise ship untested.

```bash
CACHE=/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted
cp "$CACHE"/463763329.html tests/rebuild/fixtures/finn/463763329.html
cp "$CACHE"/447401579.html tests/rebuild/fixtures/finn/447401579.html
```

Confirm the labels are present:

```bash
grep -c Eiendomsskatt tests/rebuild/fixtures/finn/463763329.html
grep -c Verditakst tests/rebuild/fixtures/finn/447401579.html
```

Expected: both print a non-zero count.

- [ ] **Step 2: Write the failing test**

Append to `tests/rebuild/test_parse_details.py`:

```python
def test_eiendomsskatt_from_pricing_dl():
    html = (FIXTURES / "463763329.html").read_text(encoding="utf-8")
    d = parse_details(html, "463763329")
    assert d.eiendomsskatt_kr is not None
    assert d.eiendomsskatt_kr > 0


def test_verditakst_from_pricing_dl():
    html = (FIXTURES / "447401579.html").read_text(encoding="utf-8")
    d = parse_details(html, "447401579")
    assert d.verditakst == 4200000


def test_missing_dl_labels_stay_none():
    """An ad without either label yields None, not 0."""
    html = (FIXTURES / "448347467.html").read_text(encoding="utf-8")
    d = parse_details(html, "448347467")
    assert d.eiendomsskatt_kr is None
    assert d.verditakst is None
```

If `FIXTURES` is not already defined in that file, add at the top:

```python
from pathlib import Path
FIXTURES = Path(__file__).parent / "fixtures" / "finn"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_parse_details.py -k "eiendomsskatt or verditakst or missing_dl" -v`
Expected: FAIL with `AttributeError: 'ListingDetails' object has no attribute 'eiendomsskatt_kr'`.

- [ ] **Step 4: Add the fields and labels**

In `skannonser/ingest/finn/parse_details.py`, add to the Group B block of `ListingDetails` (after `kommunale_avg_aar`):

```python
    kommunale_avg_aar: int | None = None
    eiendomsskatt_kr: int | None = None
    verditakst: int | None = None
```

And extend `_PRICING_LABELS`:

```python
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
```

- [ ] **Step 5: Add the columns to the repo's write list**

In `skannonser/store/repositories/details.py`, extend `_SCALAR_COLS`:

```python
_SCALAR_COLS = (
    "bedrooms", "rooms", "floor", "eieform", "nabolag",
    "totalpris", "omkostninger", "fellesgjeld", "felleskost_mnd",
    "fellesformue", "formuesverdi", "kommunale_avg_aar",
    "eiendomsskatt_kr", "verditakst",
    "energimerke", "energifarge",
    "kommunenr", "gardsnr", "bruksnr", "seksjonsnr",
    "borettslag_navn", "borettslag_orgnr", "borettslag_andelsnr",
)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_parse_details.py tests/rebuild/test_details_repo.py tests/rebuild/test_backfill_details.py -v`
Expected: PASS.

- [ ] **Step 7: Run the full suite**

Run: `PYTHONPATH=. ./.venv/bin/pytest`
Expected: `667 passed`.

- [ ] **Step 8: Commit**

```bash
git add skannonser/ingest/finn/parse_details.py skannonser/store/repositories/details.py \
        tests/rebuild/test_parse_details.py tests/rebuild/fixtures/finn/463763329.html \
        tests/rebuild/fixtures/finn/447401579.html
git commit -m "feat(finn): capture Eiendomsskatt and Verditakst from the pricing <dl>"
```

---

### Task 3: Payload decoder

**Files:**
- Create: `skannonser/ingest/finn/payload.py`
- Create (fixtures): `tests/rebuild/fixtures/finn/432672475.html` (Remix format), `tests/rebuild/fixtures/finn/445242445.html` (new-build project, empty `generalText`)
- Test: `tests/rebuild/test_payload.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `decode_ad(html: str) -> dict | None` — the `objectData.ad` mapping, or `None`
  - `class Section(NamedTuple): heading: str; text: str`
  - `sections(ad: dict) -> list[Section]` — plaintext salgsoppgave sections

- [ ] **Step 1: Copy the two fixtures**

`432672475` is the only Remix-format ad among the fixtures — without it the second decoder branch ships untested. `445242445` is a `realestate-development-single` ad, the documented empty-`generalText` case.

```bash
CACHE=/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted
cp "$CACHE"/432672475.html tests/rebuild/fixtures/finn/432672475.html
cp "$CACHE"/445242445.html tests/rebuild/fixtures/finn/445242445.html
grep -c __remixContext tests/rebuild/fixtures/finn/432672475.html
```

Expected: the grep prints `1`.

- [ ] **Step 2: Write the failing test**

Create `tests/rebuild/test_payload.py`:

```python
"""Decoding the app-state payload FINN ships on every ad page, both formats."""
from pathlib import Path

import pytest

from skannonser.ingest.finn.payload import Section, decode_ad, sections

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def _load(name):
    return (FIXTURES / f"{name}.html").read_text(encoding="utf-8", errors="replace")


def test_decodes_turbostream_format():
    ad = decode_ad(_load("448347467"))
    assert ad is not None
    assert ad["constructionYear"]
    assert isinstance(ad["generalText"], list)


def test_decodes_remix_format():
    """Older cached pages use window.__remixContext = {...} instead."""
    ad = decode_ad(_load("432672475"))
    assert ad is not None
    assert isinstance(ad["generalText"], list)
    assert len(ad["generalText"]) > 0


def test_sections_are_plaintext_with_headings():
    secs = sections(decode_ad(_load("448347467")))
    assert secs
    assert all(isinstance(s, Section) for s in secs)
    assert all("<" not in s.text for s in secs), "HTML tags must be stripped"
    assert any(s.heading for s in secs)


def test_sections_unescape_entities():
    """Labels like 'Utvendig &gt; Veggkonstruksjon' must come back as '>'."""
    for name in ("448347467", "432672475"):
        for s in sections(decode_ad(_load(name))):
            assert "&gt;" not in s.text
            assert "&amp;" not in s.text


def test_development_project_has_no_sections():
    """New-build projects genuinely carry no salgsoppgave; not a parse failure."""
    ad = decode_ad(_load("445242445"))
    assert ad is not None
    assert sections(ad) == []


def test_page_without_payload_returns_none():
    assert decode_ad(_load("211471492")) is None


@pytest.mark.parametrize(
    "junk",
    ["", "<html></html>", "<script>window.__remixContext = {broken</script>",
     '<script>enqueue("[not json")</script>', "<script>enqueue(</script>"],
)
def test_malformed_input_returns_none_never_raises(junk):
    assert decode_ad(junk) is None


def test_sections_tolerates_garbage_ad():
    assert sections({}) == []
    assert sections({"generalText": None}) == []
    assert sections({"generalText": ["not-a-dict"]}) == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_payload.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.ingest.finn.payload'`.

- [ ] **Step 4: Write the implementation**

Create `skannonser/ingest/finn/payload.py`:

```python
"""Decode the serialised app state a FINN ad page ships.

Two formats live side by side in the on-disk cache:

- current pages: ``window.__reactRouterContext.streamController.enqueue("…")``
  -- React Router's turbo-stream. The payload is a JSON *string* containing a
  FLAT array; objects inside it are ``{"_<keyIdx>": <valueIdx>}`` where BOTH
  sides are indices into that same array. It therefore needs a resolver pass,
  not a plain ``json.loads``. The root value is index 0.
- older pages: ``window.__remixContext = {…}`` -- ordinary nested JSON, with
  the payload one level deeper under ``state``.

Both land at ``objectData.ad``. Every entry point returns ``None`` rather than
raising, so an unfamiliar third format degrades to "no data" instead of
breaking the backfill.
"""
import html as html_mod
import json
import re
from typing import NamedTuple

from bs4 import BeautifulSoup

# Turbo-stream encodes a few JS values as negative pseudo-indices. Only the
# two booleans carry meaning for us; everything else collapses to None.
_NEGATIVE = {-7: False, -8: True}

_ENQUEUE = re.compile(r'enqueue\((".*")\)', re.S)
_REMIX = re.compile(r"window\.__remixContext\s*=\s*(\{.*\})", re.S)

_BREAK = re.compile(r"<br\s*/?>|</p>|</li>|</h\d>", re.I)
_TAG = re.compile(r"<[^>]+>")


class Section(NamedTuple):
    heading: str
    text: str


def _largest_script(html: str) -> str:
    """The payload always lives in the page's biggest inline <script>."""
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return ""
    bodies = [s.string or "" for s in soup.find_all("script")]
    return max(bodies, key=len) if bodies else ""


def _resolve(arr: list, index, seen: frozenset) -> object:
    """Walk the turbo-stream index graph into ordinary Python values."""
    if not isinstance(index, int):
        return None
    if index < 0:
        return _NEGATIVE.get(index)
    if index in seen or index >= len(arr):
        return None
    value = arr[index]
    if isinstance(value, dict):
        seen = seen | {index}
        out = {}
        for raw_key, raw_val in value.items():
            try:
                key_index = int(str(raw_key).lstrip("_"))
            except ValueError:
                continue
            key = _resolve(arr, key_index, seen)
            if key is not None:
                out[str(key)] = _resolve(arr, raw_val, seen)
        return out
    if isinstance(value, list):
        seen = seen | {index}
        return [_resolve(arr, i, seen) for i in value]
    return value


def _from_turbostream(script: str) -> dict | None:
    match = _ENQUEUE.search(script)
    if not match:
        return None
    try:
        arr = json.loads(json.loads(match.group(1)))
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    if not isinstance(arr, list) or not arr:
        return None
    root = _resolve(arr, 0, frozenset())
    return root if isinstance(root, dict) else None


def _from_remix(script: str) -> dict | None:
    match = _REMIX.search(script)
    if not match:
        return None
    try:
        root = json.loads(match.group(1).rstrip().rstrip(";"))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(root, dict):
        return None
    state = root.get("state")
    return state if isinstance(state, dict) else root


def decode_ad(html: str) -> dict | None:
    """The ad's ``objectData.ad`` mapping, or None if the page has no
    recognisable payload."""
    if not html:
        return None
    script = _largest_script(html)
    if not script:
        return None
    root = _from_turbostream(script) or _from_remix(script)
    if not isinstance(root, dict):
        return None
    loader = root.get("loaderData")
    if not isinstance(loader, dict):
        return None
    for key, value in loader.items():
        if "ad[.html]" not in key and "homes.ad" not in key:
            continue
        if not isinstance(value, dict):
            continue
        object_data = value.get("objectData")
        if not isinstance(object_data, dict):
            continue
        ad = object_data.get("ad")
        if isinstance(ad, dict):
            return ad
    return None


def sections(ad: dict | None) -> list[Section]:
    """The salgsoppgave's ``generalText`` as plaintext (heading, text) pairs.

    Block-level tags become newlines so label structure survives; everything
    else is stripped and HTML entities are unescaped.
    """
    if not isinstance(ad, dict):
        return []
    raw_sections = ad.get("generalText")
    if not isinstance(raw_sections, list):
        return []
    out: list[Section] = []
    for item in raw_sections:
        if not isinstance(item, dict):
            continue
        body = item.get("textUnsafe") or ""
        body = _BREAK.sub("\n", body)
        body = _TAG.sub(" ", body)
        body = html_mod.unescape(body)
        body = re.sub(r"[ \t]+", " ", body)
        body = re.sub(r"\n\s*\n+", "\n", body).strip()
        heading = html_mod.unescape(str(item.get("heading") or "")).strip()
        out.append(Section(heading, body))
    return out
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_payload.py -v`
Expected: PASS, all 12 tests (5 of them parametrized).

- [ ] **Step 6: Sanity-check decoder reach across the real cache**

This is the claim the whole feature rests on; verify it against real data, not just fixtures.

```bash
./.venv/bin/python - <<'PY'
import random
from pathlib import Path
from skannonser.ingest.finn.payload import decode_ad, sections
CACHE = "/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted"
files = sorted(Path(CACHE).glob("*.html"))
random.seed(1)
ok = empty = fail = 0
for f in random.sample(files, 200):
    ad = decode_ad(f.read_text(encoding="utf-8", errors="replace"))
    if ad is None:
        fail += 1
    elif sections(ad):
        ok += 1
    else:
        empty += 1
print(f"with sections={ok}  decoded-but-empty={empty}  undecodable={fail}")
PY
```

Expected: roughly `with sections=190+`, `undecodable` in low single digits. If `undecodable` exceeds ~10, a third payload format exists — stop and investigate before continuing.

- [ ] **Step 7: Commit**

```bash
git add skannonser/ingest/finn/payload.py tests/rebuild/test_payload.py \
        tests/rebuild/fixtures/finn/432672475.html tests/rebuild/fixtures/finn/445242445.html
git commit -m "feat(finn): decode the ad app-state payload in both formats"
```

---

### Task 4: Rules extraction into a typed model

**Files:**
- Create: `skannonser/ingest/finn/parse_salgsoppgave.py`
- Test: `tests/rebuild/test_parse_salgsoppgave.py`

**Interfaces:**
- Consumes: `decode_ad`, `sections`, `Section` from Task 3
- Produces:
  - `class Salgsoppgave(BaseModel)` with fields `finnkode: str`, `boligselgerforsikring: bool | None`, `eiendomsskatt_kr: int | None`, `ferdigattest: str | None`, `radon_omtalt: bool | None`, `utleie: str | None`, `husdyr: str | None`, `heftelser: bool | None`
  - `parse_salgsoppgave(html: str, finnkode: str) -> Salgsoppgave`

- [ ] **Step 1: Write the failing test**

Create `tests/rebuild/test_parse_salgsoppgave.py`:

```python
"""Rules-extracted salgsoppgave fields. Every field is optional and typed;
a parse failure yields None for that field only, never an exception."""
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse_salgsoppgave import (
    Salgsoppgave,
    parse_salgsoppgave,
)

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def _parse(name):
    html = (FIXTURES / f"{name}.html").read_text(encoding="utf-8", errors="replace")
    return parse_salgsoppgave(html, name)


def test_extracts_at_least_one_field_from_a_real_ad():
    parsed = _parse("448347467")
    populated = [
        k for k, v in parsed.model_dump().items() if k != "finnkode" and v is not None
    ]
    assert populated, "no rule fired on a real salgsoppgave — regexes are dead"


def test_returns_model_with_finnkode_even_for_junk():
    result = parse_salgsoppgave("<html></html>", "999")
    assert isinstance(result, Salgsoppgave)
    assert result.finnkode == "999"
    assert result.ferdigattest is None
    assert result.eiendomsskatt_kr is None


def test_absent_topic_is_false_not_null_when_a_salgsoppgave_was_read():
    """NULL means 'no salgsoppgave text'; False means 'read it, not mentioned'.
    Conflating the two would make an unparsed listing indistinguishable from
    one that simply never discusses radon."""
    parsed = _parse("448347467")
    assert parsed.radon_omtalt in (True, False)
    assert parsed.heftelser in (True, False)

    unparsed = parse_salgsoppgave("<html></html>", "999")
    assert unparsed.radon_omtalt is None
    assert unparsed.heftelser is None


@pytest.mark.parametrize(
    "junk", ["", "<html>", "<script>enqueue(</script>", "not html at all"]
)
def test_never_raises_on_arbitrary_input(junk):
    assert parse_salgsoppgave(junk, "1").finnkode == "1"


def test_enum_fields_only_emit_known_values():
    """No free text: every enum column is a member of its vocabulary or None."""
    allowed = {
        "ferdigattest": {"ferdigattest", "midlertidig", "ingen"},
        "utleie": {"tillatt", "ikke_tillatt", "egen_enhet"},
        "husdyr": {"tillatt", "krever_godkjenning", "ikke_tillatt"},
    }
    for name in ("448347467", "432672475", "451631591", "466043223"):
        parsed = _parse(name)
        for field, vocabulary in allowed.items():
            value = getattr(parsed, field)
            assert value is None or value in vocabulary, (name, field, value)


@pytest.mark.parametrize(
    "prose, expected",
    [
        ("Selger har tegnet Boligselgerforsikring levert av Gjensidige.", True),
        ("Det har tegnet boligselgerforsikring.", True),
        ("Selger har ikke tegnet boligselgerforsikring.", False),
        ("Ingen boligselgerforsikring er tegnet for eiendommen.", False),
        ("Ingenting om forsikring her.", None),
    ],
)
def test_boligselgerforsikring_from_prose(prose, expected):
    """Comes from prose, never from ad.changeOfOwnershipInsurance -- that flag
    reads False on ~96% of ads regardless of what the prose says (verified over
    300 ads, 2026-07-27).

    The negative pattern must be checked first: 'har ikke tegnet' contains
    'har tegnet', so testing the positive first would invert every negative.
    """
    from skannonser.ingest.finn.parse_salgsoppgave import _boligselgerforsikring

    assert _boligselgerforsikring(prose) is expected


def test_kr_amounts_are_ints_not_strings():
    for name in ("448347467", "432672475"):
        value = _parse(name).eiendomsskatt_kr
        assert value is None or isinstance(value, int)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_parse_salgsoppgave.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.ingest.finn.parse_salgsoppgave'`.

- [ ] **Step 3: Write the implementation**

Create `skannonser/ingest/finn/parse_salgsoppgave.py`:

```python
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


_KR = r"(?:kr\.?\s*)?([\d][\d\s .]*)"


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_parse_salgsoppgave.py -v`
Expected: PASS.

- [ ] **Step 5: Measure real-world field coverage**

The spec's coverage figures are predictions; confirm the regexes actually fire.

```bash
./.venv/bin/python - <<'PY'
import collections, random
from pathlib import Path
from skannonser.ingest.finn.parse_salgsoppgave import parse_salgsoppgave
CACHE = "/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted"
files = sorted(Path(CACHE).glob("*.html"))
random.seed(2)
hits, n = collections.Counter(), 0
for f in random.sample(files, 200):
    p = parse_salgsoppgave(f.read_text(encoding="utf-8", errors="replace"), f.stem)
    n += 1
    for field, value in p.model_dump().items():
        if field != "finnkode" and value is not None:
            hits[field] += 1
for field, count in hits.most_common():
    print(f"{count*100//n:3}%  {field}")
PY
```

Measured on 2026-07-27 after implementation (200 ads, seed=2):
`ferdigattest` 65%, `utleie` 41%, `boligselgerforsikring` 39%, `husdyr` 17%,
`eiendomsskatt_kr` 11%; `radon_omtalt` and `heftelser` are True on 26% / 52%
(their not-None rate is ~98% by design — NULL means "no salgsoppgave text at
all", so use the True rate for those two, not not-None).

**The spec's percentages are MENTION rates, not extraction rates** — the share
of ads whose prose contains the topic at all. Extraction is necessarily lower,
because a mention is often not a rule: `husdyr` mentions include a neighbouring
farm's animals and passing list items like "husdyrhold, dugnader, trappevask",
neither of which states a pet policy. Verified: of 51 husdyr mentions in 200
ads, 34 classify and most of the 17 misses are correct non-classifications. The
one genuinely-missed form, a `Husdyr: Ja/Nei` key-value line, occurs in 1 of 400
ads — not worth a regex.

Only a field at or near **0%** indicates a dead regex worth fixing.

- [ ] **Step 6: Commit**

```bash
git add skannonser/ingest/finn/parse_salgsoppgave.py tests/rebuild/test_parse_salgsoppgave.py
git commit -m "feat(finn): rules-extract typed salgsoppgave fields from prose"
```

---

### Task 5: Repository

**Files:**
- Create: `skannonser/store/repositories/salgsoppgave.py`
- Test: `tests/rebuild/test_salgsoppgave_repo.py`

**Interfaces:**
- Consumes: `Salgsoppgave` from Task 4; Task 1's tables
- Produces: `class SalgsoppgaveRepo` with `__init__(conn: sqlite3.Connection)`, `upsert(items: list[Salgsoppgave]) -> dict` (returns `{"upserted": int}`), `wipe() -> None`, `coverage() -> dict`

- [ ] **Step 1: Write the failing test**

Create `tests/rebuild/test_salgsoppgave_repo.py`:

```python
import pytest

from skannonser.ingest.finn.parse_salgsoppgave import Salgsoppgave
from skannonser.store import connection, migrations
from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    c.execute("INSERT INTO eiendom (finnkode, url) VALUES ('1', 'u')")
    c.commit()
    return c


def test_upsert_writes_a_row(conn):
    repo = SalgsoppgaveRepo(conn)
    item = Salgsoppgave(finnkode="1", ferdigattest="midlertidig")
    assert repo.upsert([item]) == {"upserted": 1}
    row = conn.execute(
        "SELECT ferdigattest, parsed_at FROM listing_salgsoppgave WHERE finnkode='1'"
    ).fetchone()
    assert row["ferdigattest"] == "midlertidig"
    assert row["parsed_at"] is not None


def test_upsert_is_idempotent_and_replaces(conn):
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen", radon_omtalt=True)])
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ferdigattest")])
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 1
    row = conn.execute(
        "SELECT ferdigattest, radon_omtalt FROM listing_salgsoppgave WHERE finnkode='1'"
    ).fetchone()
    assert row["ferdigattest"] == "ferdigattest"
    assert row["radon_omtalt"] is None, "full-row REPLACE, not fill-only"


def test_upsert_empty_list_is_a_noop(conn):
    assert SalgsoppgaveRepo(conn).upsert([]) == {"upserted": 0}


def test_wipe_clears_all_four_tables(conn):
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen")])
    conn.execute(
        "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel) VALUES ('1',2,'vatrom')"
    )
    conn.execute("INSERT INTO listing_egenerklaering (finnkode, forhold) VALUES ('1','tvist')")
    conn.commit()
    repo.wipe()
    for table in ("listing_salgsoppgave", "listing_tg_findings", "listing_egenerklaering"):
        assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def test_wipe_preserves_the_llm_cache(conn):
    """The cache is what makes a --wipe rebuild free; wiping it defeats the point."""
    conn.execute(
        "INSERT INTO salgsoppgave_llm_cache (content_sha256, response_json, model, created_at)"
        " VALUES ('abc', '{}', 'm', datetime('now'))"
    )
    conn.commit()
    SalgsoppgaveRepo(conn).wipe()
    assert conn.execute("SELECT COUNT(*) FROM salgsoppgave_llm_cache").fetchone()[0] == 1


def test_coverage_counts(conn):
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen")])
    stats = repo.coverage()
    assert stats["salgsoppgave_rows"] == 1
    assert stats["with_ferdigattest"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_salgsoppgave_repo.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.store.repositories.salgsoppgave'`.

- [ ] **Step 3: Write the implementation**

Create `skannonser/store/repositories/salgsoppgave.py`:

```python
"""``listing_salgsoppgave`` repository (migration 015).

Full-row REPLACE semantics, matching DetailsRepo: these tables are a DERIVED
cache of parser output over cached ad HTML, never hand-curated data. The
rebuild path is `tools backfill-salgsoppgave --wipe`, so there is deliberately
no fill-only or partial-update logic.

`wipe()` deliberately spares `salgsoppgave_llm_cache` -- that cache is keyed by
content hash and is precisely what lets a rebuild replay Phase 2's classifier
results for free. Clearing it would turn every rebuild back into a paid run.
"""
import sqlite3

from skannonser.ingest.finn.parse_salgsoppgave import Salgsoppgave

_SCALAR_COLS = (
    "boligselgerforsikring", "eiendomsskatt_kr",
    "ferdigattest", "radon_omtalt", "utleie", "husdyr", "heftelser",
)


class SalgsoppgaveRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(self, items: list[Salgsoppgave]) -> dict:
        """REPLACE each item's row (parsed_at stamped now) in one transaction.
        Returns {"upserted": n}."""
        if not items:
            return {"upserted": 0}
        cols = ("finnkode",) + _SCALAR_COLS + ("parsed_at",)
        placeholders = ", ".join("?" * (len(cols) - 1))
        sql = (
            f"INSERT OR REPLACE INTO listing_salgsoppgave ({', '.join(cols)}) "
            f"VALUES ({placeholders}, datetime('now'))"
        )
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            for item in items:
                data = item.model_dump()
                conn.execute(
                    sql, [item.finnkode] + [data[c] for c in _SCALAR_COLS]
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"upserted": len(items)}

    def wipe(self) -> None:
        """Clear the derived tables. Leaves salgsoppgave_llm_cache intact."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings")
            conn.execute("DELETE FROM listing_egenerklaering")
            conn.execute("DELETE FROM listing_salgsoppgave")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "salgsoppgave_rows": one("SELECT COUNT(*) FROM listing_salgsoppgave"),
            "with_eiendomsskatt": one(
                "SELECT COUNT(*) FROM listing_salgsoppgave WHERE eiendomsskatt_kr IS NOT NULL"
            ),
            "with_ferdigattest": one(
                "SELECT COUNT(*) FROM listing_salgsoppgave WHERE ferdigattest IS NOT NULL"
            ),
            "tg_findings_rows": one("SELECT COUNT(*) FROM listing_tg_findings"),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_salgsoppgave_repo.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add skannonser/store/repositories/salgsoppgave.py tests/rebuild/test_salgsoppgave_repo.py
git commit -m "feat(store): SalgsoppgaveRepo with full-row REPLACE semantics"
```

---

### Task 6: Offline backfill + CLI command

**Files:**
- Create: `skannonser/ingest/finn/backfill_salgsoppgave.py`
- Modify: `skannonser/commands/tools_cmd.py` (append a new command)
- Test: `tests/rebuild/test_backfill_salgsoppgave.py`

**Interfaces:**
- Consumes: `parse_salgsoppgave` (Task 4), `SalgsoppgaveRepo` (Task 5)
- Produces: `backfill_salgsoppgave(conn: sqlite3.Connection, project_dir: Path, wipe: bool = False) -> dict` returning `{"eiendom_rows": int, "parsed": int, "missing_html": int, "upserted": int}`; CLI `skannonser tools backfill-salgsoppgave`

- [ ] **Step 1: Write the failing test**

Create `tests/rebuild/test_backfill_salgsoppgave.py`:

```python
"""tools backfill-salgsoppgave: local re-parse of cached ad HTML. Purely
offline -- the whole point is zero FINN traffic."""
import shutil
from pathlib import Path

import pytest

from skannonser.ingest.finn.backfill_salgsoppgave import backfill_salgsoppgave
from skannonser.store import connection, migrations

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def _seed(conn, finnkode):
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES (?, ?)", (finnkode, "u"))
    conn.commit()


def _project(tmp_path, *finnkodes):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    for code in finnkodes:
        shutil.copy(FIXTURES / f"{code}.html", project / "html_extracted" / f"{code}.html")
    return project


def test_backfill_parses_cached_html(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    _seed(conn, "999999999")  # no cached HTML

    stats = backfill_salgsoppgave(conn, project)
    assert stats == {
        "eiendom_rows": 2,
        "parsed": 1,
        "missing_html": 1,
        "upserted": 1,
    }
    row = conn.execute(
        "SELECT finnkode, parsed_at FROM listing_salgsoppgave WHERE finnkode='448347467'"
    ).fetchone()
    assert row["parsed_at"] is not None


def test_backfill_is_idempotent(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    backfill_salgsoppgave(conn, project)
    backfill_salgsoppgave(conn, project)
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 1


def test_backfill_handles_both_payload_formats(conn, tmp_path):
    project = _project(tmp_path, "448347467", "432672475")
    _seed(conn, "448347467")
    _seed(conn, "432672475")
    stats = backfill_salgsoppgave(conn, project)
    assert stats["parsed"] == 2
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 2


def test_wipe_clears_before_rebuilding(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    backfill_salgsoppgave(conn, project)
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('stale', 'u')")
    conn.execute("INSERT INTO listing_salgsoppgave (finnkode) VALUES ('stale')")
    conn.commit()
    backfill_salgsoppgave(conn, project, wipe=True)
    codes = {r[0] for r in conn.execute("SELECT finnkode FROM listing_salgsoppgave")}
    assert codes == {"448347467"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_backfill_salgsoppgave.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.ingest.finn.backfill_salgsoppgave'`.

- [ ] **Step 3: Write the backfill**

Create `skannonser/ingest/finn/backfill_salgsoppgave.py`:

```python
"""Local re-parse of cached ad HTML into listing_salgsoppgave.

The recovery/bootstrap path for the salgsoppgave cache (2026-07-27 design
spec), mirroring `backfill_details`: iterate every `eiendom` finnkode, read
`{project_dir}/html_extracted/{finnkode}.html` where present,
`parse_salgsoppgave` it, upsert. Purely offline -- reads only the on-disk
cache, never FINN, and makes no API calls.
"""
import sqlite3
from pathlib import Path

from skannonser.ingest.finn.parse_salgsoppgave import parse_salgsoppgave
from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo

_BATCH_SIZE = 200


def backfill_salgsoppgave(
    conn: sqlite3.Connection, project_dir: Path, wipe: bool = False
) -> dict:
    repo = SalgsoppgaveRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")]
    parsed = missing = upserted = 0
    batch = []
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            missing += 1
            continue
        html = path.read_text(encoding="utf-8", errors="replace")
        batch.append(parse_salgsoppgave(html, finnkode))
        parsed += 1
        if len(batch) >= _BATCH_SIZE:
            upserted += repo.upsert(batch)["upserted"]
            batch = []
    if batch:
        upserted += repo.upsert(batch)["upserted"]

    return {
        "eiendom_rows": len(finnkodes),
        "parsed": parsed,
        "missing_html": missing,
        "upserted": upserted,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_backfill_salgsoppgave.py -v`
Expected: PASS.

- [ ] **Step 5: Add the CLI command**

Append to `skannonser/commands/tools_cmd.py`, following the shape of `backfill_details_cmd`:

```python
@app.command(name="backfill-salgsoppgave")
def backfill_salgsoppgave_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir", help="FINN cache root (html_extracted/ lives here)"
    ),
    wipe: bool = typer.Option(False, "--wipe", help="Clear the salgsoppgave tables first, then rebuild"),
    status: bool = typer.Option(False, "--status", help="Print coverage only; parse nothing"),
) -> None:
    """(Re)build the listing_salgsoppgave derived cache from already-downloaded
    ad HTML. Purely local -- zero FINN traffic, zero API calls. Safe to re-run
    any time; use --wipe after a parser change."""
    from skannonser.ingest.finn.backfill_salgsoppgave import backfill_salgsoppgave
    from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)

    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    repo = SalgsoppgaveRepo(conn)
    if status:
        typer.echo(f"backfill-salgsoppgave status: {repo.coverage()}")
        return

    result = backfill_salgsoppgave(conn, project_dir, wipe=wipe)
    typer.echo(f"backfill-salgsoppgave: {result}")
    typer.echo(f"coverage: {repo.coverage()}")
```

- [ ] **Step 6: Verify the command is wired up**

The installed `skannonser` console script points at the **main clone**, so it will not see this code. Run it as a module with `PYTHONPATH` set:

```bash
PYTHONPATH=. ./.venv/bin/python -c "from skannonser.cli import main; main()" tools backfill-salgsoppgave --help
```

Expected: the command's help text, including `--wipe` and `--status`.

- [ ] **Step 7: Run the full suite**

Run: `PYTHONPATH=. ./.venv/bin/pytest`
Expected: all green, count increased by this task's new tests.

- [ ] **Step 8: Commit**

```bash
git add skannonser/ingest/finn/backfill_salgsoppgave.py skannonser/commands/tools_cmd.py \
        tests/rebuild/test_backfill_salgsoppgave.py
git commit -m "feat(cli): tools backfill-salgsoppgave — offline rebuild from cached HTML"
```

---

### Task 7: Expose the fields in the web API

**Files:**
- Modify: `skannonser/web/api.py` (the migration-010 enrichment block, around `:192` and `:324`)
- Test: `tests/rebuild/test_web_api.py`

**Interfaces:**
- Consumes: `listing_salgsoppgave` + `listing_details.eiendomsskatt_kr` from Tasks 1–2
- Produces: new listing-record keys `boligselgerforsikring`, `eiendomsskatt_kr`, `ferdigattest`, `radon_omtalt`, `utleie`, `husdyr`, `heftelser`. **`byggeaar` already exists** (from `eiendom.info_construction_year`, 99% populated) — do not add it again.

- [ ] **Step 1: Read the existing enrichment block**

```bash
sed -n 185,215p skannonser/web/api.py
grep -n "SOVEROM\|listing_details" skannonser/web/api.py | head -20
```

Follow whatever join/lookup shape is already used for the migration-010 fields — do not invent a second mechanism.

**Two facts about the test file, already verified — do not rediscover them:**

- The Python API tests live in **`tests/rebuild/test_web_api.py`**. (`tests/web/` is JavaScript, and `pyproject.toml` sets `testpaths = ["tests/rebuild"]`, so pytest never collects it.) Its fixtures are `db_path` (a `Path`) and `client` (a `TestClient`); tests seed via the module's own `_conn(db_path)` and `_ins_eiendom(conn, finnkode, ...)` helpers and read results with `_by_finnkode(body["listings"], code)`.
- **`test_listing_shape_and_donor_resolved_travel` asserts an exact key set** (`set(item.keys()) == {...}`). Adding record keys *will* break it. Updating that assertion is part of this task, not an unrelated failure.

- [ ] **Step 2: Write the failing test**

Append to `tests/rebuild/test_web_api.py`, using that module's existing `_conn` / `_ins_eiendom` / `_by_finnkode` helpers:

```python
def test_listing_exposes_salgsoppgave_fields(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.execute(
        "INSERT INTO listing_salgsoppgave "
        "(finnkode, ferdigattest, radon_omtalt, eiendomsskatt_kr, utleie) "
        "VALUES ('A', 'midlertidig', 1, 1827, 'tillatt')"
    )
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["ferdigattest"] == "midlertidig"
    assert item["radon_omtalt"] is True
    assert item["eiendomsskatt_kr"] == 1827
    assert item["utleie"] == "tillatt"


def test_eiendomsskatt_prefers_the_deterministic_dl_source(db_path, client):
    """listing_details comes from the pricing <dl>; listing_salgsoppgave from
    prose. Where both fire, the <dl> wins."""
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.execute(
        "INSERT INTO listing_details (finnkode, eiendomsskatt_kr) VALUES ('A', 5000)"
    )
    conn.execute(
        "INSERT INTO listing_salgsoppgave (finnkode, eiendomsskatt_kr) VALUES ('A', 1827)"
    )
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["eiendomsskatt_kr"] == 5000


def test_eiendomsskatt_falls_back_to_prose_when_dl_is_absent(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.execute(
        "INSERT INTO listing_salgsoppgave (finnkode, eiendomsskatt_kr) VALUES ('A', 1827)"
    )
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["eiendomsskatt_kr"] == 1827


def test_unparsed_listing_has_null_salgsoppgave_fields(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["ferdigattest"] is None
    assert item["radon_omtalt"] is None
    assert item["eiendomsskatt_kr"] is None
```

Then extend the exact key set in `test_listing_shape_and_donor_resolved_travel` with the new keys:

```python
        # Salgsoppgave enrichment (migration 015).
        "boligselgerforsikring", "eiendomsskatt_kr", "ferdigattest",
        "radon_omtalt", "utleie", "husdyr", "heftelser",
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_web_api.py -k salgsoppgave -v`
Expected: FAIL with `KeyError: 'ferdigattest'` (and the exact-key-set assertion in `test_listing_shape_and_donor_resolved_travel` failing too).

- [ ] **Step 4: Add the fields to the query and the record**

Extend the SQL that loads the migration-010 enrichment to LEFT JOIN `listing_salgsoppgave` and select its columns, coalescing eiendomsskatt so the deterministic `<dl>` source wins:

```sql
LEFT JOIN listing_salgsoppgave s ON s.finnkode = e.finnkode
```

```sql
COALESCE(d.eiendomsskatt_kr, s.eiendomsskatt_kr) AS EIENDOMSSKATT_KR,
s.boligselgerforsikring AS BOLIGSELGERFORSIKRING,
s.ferdigattest AS FERDIGATTEST,
s.radon_omtalt AS RADON_OMTALT,
s.utleie     AS UTLEIE,
s.husdyr     AS HUSDYR,
s.heftelser  AS HEFTELSER,
```

Then add to the record dict, alongside the existing migration-010 keys:

```python
        # Salgsoppgave enrichment (migration 015; None when unparsed).
        "boligselgerforsikring": _as_bool(rec.get("BOLIGSELGERFORSIKRING")),
        "eiendomsskatt_kr": rec.get("EIENDOMSSKATT_KR"),
        "ferdigattest": rec.get("FERDIGATTEST"),
        "radon_omtalt": _as_bool(rec.get("RADON_OMTALT")),
        "utleie": rec.get("UTLEIE"),
        "husdyr": rec.get("HUSDYR"),
        "heftelser": _as_bool(rec.get("HEFTELSER")),
```

SQLite has no native boolean, so add this helper near the other module-level helpers in `api.py` (reuse an existing equivalent if one is already defined):

```python
def _as_bool(value):
    """SQLite stores BOOLEAN as 0/1/NULL; the API contract is True/False/None."""
    return None if value is None else bool(value)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_web_api.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `PYTHONPATH=. ./.venv/bin/pytest`
Expected: all green.

- [ ] **Step 7: Verify in the browser**

```bash
PYTHONPATH=. ./.venv/bin/python -c "from skannonser.cli import main; main()" web --help
```

Then start the dev server per the project's usual method and confirm a listing record carries the new keys — check the network response rather than only the rendered page.

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/api.py tests/rebuild/test_web_api.py
git commit -m "feat(web): expose salgsoppgave fields, preferring the <dl> eiendomsskatt"
```

---

## Done criteria

- [ ] `PYTHONPATH=. ./.venv/bin/pytest` green, count ≥ 662 + new tests
- [ ] `tools backfill-salgsoppgave --status` reports non-zero `with_ferdigattest` after a real run
- [ ] Field-coverage script (Task 4 Step 5) shows no field stuck at 0%
- [ ] Decoder reach (Task 3 Step 6) shows `undecodable` in low single digits
- [ ] No column in `listing_salgsoppgave` holds prose

## Deploy

Committing a migration does not deploy it: merge → pull on the server → `skannonser db migrate` → container restart. The backfill runs locally, so enriched rows reach the server the same way any other local pipeline write does.

## Follow-on

> ⚠️ **Hazard Phase 2 must handle, found in Task 5's review.**
> `SalgsoppgaveRepo.upsert` uses `INSERT OR REPLACE`, which deletes and
> reinserts the whole row — so any column not in `_SCALAR_COLS` reverts to its
> default. The five Phase-2 columns (`tg2_count`, `tg3_count`,
> `tilstandsrapport_dato`, `tilstandsrapport_utsteder`,
> `egenerklaering_antall`) are not in that list, so a routine Phase-1 re-parse
> of an already-classified listing will silently null out its classifier
> results. Phase 2 must either write those columns in the same statement or
> guarantee ordering within a wipe cycle. This is not a Task 5 defect — it is
> correct for Phase 1's own fields — but it is a live trap for Phase 2.

**Phase 2 — the classifier** gets its own plan once this ships and stage 1 has settled the model choice. It fills `listing_tg_findings`, `listing_egenerklaering`, and the five currently-NULL rollup columns on `listing_salgsoppgave` (`tg2_count`, `tg3_count`, `tilstandsrapport_dato`, `tilstandsrapport_utsteder`, `egenerklaering_antall`), using `salgsoppgave_llm_cache` so rebuilds stay free.
