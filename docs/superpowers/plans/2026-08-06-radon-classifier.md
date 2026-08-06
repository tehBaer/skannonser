# Radon Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the useless `radon_omtalt` mention flag with real radon facts — measured / not measured / above the limit, radon barrier present or absent, and the property's own Bq/m³ value where one is genuinely stated.

**Architecture:** Three nullable fields ride along in the existing per-ad classification request (no extra API calls). The condition-section selector widens so the classifier can actually see radon text. The response cache gains a `schema_version` column so changing the output schema invalidates old responses explicitly instead of silently returning NULLs forever.

**Tech Stack:** Python 3.11, sqlite3, pydantic v2, typer, FastAPI, vanilla-JS frontend, pytest + `node --test`.

**Spec:** `docs/superpowers/specs/2026-08-06-radon-classifier-design.md` — the authority on the two traps, the measured coverage numbers, and the cache-versioning decision. Read it before starting.

## Coordinates with: `2026-08-06-table-toolbar-redesign.md`

That plan rebuilds the table's filter toolbar and unifies the status filter
across both pages. It is JS-only and changes no Python; this plan is 7/8
Python. They were kept separate so this plan's migration and paid
re-classification decisions stay out of a CSS change, and so that plan keeps
its "pytest stays at 858" invariant.

There are **no overlapping hunks**; the two can be developed in parallel and
merged in either order.

| File | This plan touches | Toolbar plan touches |
|---|---|---|
| `listingmeta.js` | radon formatters (~260), `TILSTAND_DERIVED` (~318) | `premiumPct` (~106), new `TILGJENGELIGHET_OPTIONS` (~226) |
| `table.js` | `COLUMNS` (~106-115), `TILSTAND_COLUMNS` (~145), cell switch (~535) | `state` (~185), `render` (~637), `wireToolbar` (~675) |

Task 7 here adds a `radon_status` column and relabels the Phase-1 one to
"Radon nevnt". The toolbar plan's Kolonner picker reads `COLUMNS` dynamically,
so both columns appear in it automatically — no coordination beyond rerunning
`node --test tests/web/*.test.mjs` after the second of the two merges.

One difference worth not tripping over: that plan invokes pytest as
`PYTHONPATH=. ./.venv/bin/pytest`, this one as `./.venv/bin/python -m pytest`.
Both put the worktree on `sys.path`; neither is the bare `pytest` that would
silently test the main clone.

## Global Constraints

- **Worktree + env**: work in a worktree (`EnterWorktree` + `./ops/setup-worktree.sh`). Tests: `./.venv/bin/python -m pytest` (bare `pytest` in a worktree silently tests the MAIN clone; the `python -m` form puts the worktree on `sys.path`). JS: `node --test tests/web/*.test.mjs` (the bare directory form fails).
- **Baseline before this work**: 858 Python, 183 JS, zero failures.
- **Migration number**: 017. Re-verify with `git fetch && ls skannonser/store/migrations/` before writing the file — another session works in this repo. `ALL_MIGRATIONS` in `tests/rebuild/test_migrations.py` gains a line and conflicts on merge; resolve by keeping both in numeric order.
- **No network in tests**: `import anthropic` stays inside `_anthropic_call` / `_default_client` only. `./.venv/bin/python -c "import skannonser.enrich.tilstand"` must succeed with the package absent.
- **Enum values are ASCII** (`ikke_malt`, not `ikke_målt`); UI labels use proper Norwegian.
- **Null discipline**: `radon_status` NULL means "the document said nothing substantive". Generic "vi anbefaler radonmåling" advice is NOT substantive and must leave it NULL — that boilerplate is exactly what makes `radon_omtalt` useless.
- **`radon_bq` is a free integer**, not a grid value: it is a measured quantity whose worth is its position relative to the 100 and 200 thresholds.
- **Model**: `claude-opus-5` exactly, unchanged.

---

### Task 1: Cache schema versioning

Do this first and alone. Every later task depends on a version bump invalidating old responses, and this is the one change that is dangerous to get wrong: without it, the radon fields silently read NULL forever on every already-classified ad.

**Files:**
- Create: `skannonser/store/migrations/017_radon.sql`
- Modify: `skannonser/enrich/tilstand.py` (`cache_get` ~line 224, `cache_put` ~line 232, `_CACHE_COLS` ~line 242)
- Modify: `tests/rebuild/test_migrations.py` (`ALL_MIGRATIONS`)
- Test: `tests/rebuild/test_tilstand.py`

**Interfaces:**
- Produces:
  - `_SCHEMA_VERSION: int = 2` in `tilstand.py` (1 was the pre-radon schema)
  - `cache_get(conn, sha, *, version=_SCHEMA_VERSION) -> str | None` — matches on sha AND version
  - `cache_put(conn, sha, response_json, model=_MODEL, *, version=_SCHEMA_VERSION) -> None`
  - `_CACHE_COLS` gains `"schema_version"` (so `export_cache`/`import_cache` carry it)
  - migration 017 adds `schema_version INTEGER NOT NULL DEFAULT 1` to `salgsoppgave_llm_cache`, plus the three `listing_tilstand` columns used from Task 4 on

- [ ] **Step 1: Write the failing tests**

Append to `tests/rebuild/test_tilstand.py`:

```python
# --- cache schema versioning ------------------------------------------------
# Adding fields to the output schema makes every cached response incomplete.
# Version the cache so that shows up as a MISS (re-classify) rather than as
# silent NULLs that are indistinguishable from "the document said nothing".

def test_cache_get_ignores_rows_from_an_older_schema(tmp_path):
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_get, cache_put

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"old": true}', version=_SCHEMA_VERSION - 1)
    assert cache_get(conn, "a" * 64) is None                       # current version: miss
    assert cache_get(conn, "a" * 64, version=_SCHEMA_VERSION - 1) == '{"old": true}'


def test_cache_put_stamps_the_current_version(tmp_path):
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_put, export_cache

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"x": 1}')
    assert export_cache(conn)[0]["schema_version"] == _SCHEMA_VERSION


def test_a_version_bump_keeps_the_old_row(tmp_path):
    """Superseded rows stay on disk: they cost nothing and they document what
    was actually paid for."""
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_put, export_cache

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"old": true}', version=_SCHEMA_VERSION - 1)
    cache_put(conn, "b" * 64, '{"new": true}')
    assert len(export_cache(conn)) == 2


def test_export_import_carries_the_version(tmp_path):
    from skannonser.enrich.tilstand import (
        _SCHEMA_VERSION, cache_get, cache_put, export_cache, import_cache,
    )

    src = _cache_db(tmp_path, "src.db")
    cache_put(src, "a" * 64, '{"x": 1}', version=_SCHEMA_VERSION - 1)
    dst = _cache_db(tmp_path, "dst.db")
    import_cache(dst, export_cache(src))
    # a stale row must still read as stale on the receiving side, or a
    # server import would resurrect responses the local side had retired
    assert cache_get(dst, "a" * 64) is None
    assert export_cache(dst)[0]["schema_version"] == _SCHEMA_VERSION - 1
```

- [ ] **Step 2: Run, verify failure**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py -q -k cache`
Expected: FAIL — `ImportError: cannot import name '_SCHEMA_VERSION'`

- [ ] **Step 3: Write migration 017**

Verify the number is free first: `git fetch && ls skannonser/store/migrations/`.

`skannonser/store/migrations/017_radon.sql`:

```sql
-- 017_radon.sql
-- Radon facts in the tilstand classifier (2026-08-06 design spec), plus the
-- cache versioning that makes changing the output schema safe.
--
-- `radon_omtalt` on listing_salgsoppgave stays: it is regex over prose, free,
-- and covers every parsed ad including ones classification has not reached.
-- These columns answer a different, much stronger question.

ALTER TABLE listing_tilstand ADD COLUMN radon_status TEXT;
-- 'ikke_malt' | 'malt_under_grense' | 'malt_over_grense' | 'malt_ukjent_verdi'
ALTER TABLE listing_tilstand ADD COLUMN radonsperre TEXT;   -- 'finnes' | 'mangler'
ALTER TABLE listing_tilstand ADD COLUMN radon_bq INTEGER;   -- the property's OWN measurement

-- The cache is keyed by sha256 of the INPUT text; the prompt and output schema
-- are not in the key. Without this column, adding a field means every cached
-- response silently yields NULL for it forever -- indistinguishable from "the
-- document said nothing". Version mismatch is treated as a cache miss.
-- Existing rows predate radon, so they default to version 1.
ALTER TABLE salgsoppgave_llm_cache ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
```

Add `"017_radon"` to `ALL_MIGRATIONS` in `tests/rebuild/test_migrations.py`, in numeric order.

- [ ] **Step 4: Implement the versioned cache**

In `skannonser/enrich/tilstand.py`, next to `_MODEL`:

```python
# Bump when the output schema or the prompt changes what the model produces.
# A mismatch is a cache MISS, so a bump re-classifies the corpus at full
# price -- batch schema changes rather than trickling them out.
_SCHEMA_VERSION = 2
```

Replace `cache_get` / `cache_put` / `_CACHE_COLS`:

```python
def cache_get(
    conn: sqlite3.Connection, sha: str, *, version: int = _SCHEMA_VERSION
) -> str | None:
    row = conn.execute(
        "SELECT response_json FROM salgsoppgave_llm_cache "
        "WHERE content_sha256 = ? AND schema_version = ?",
        (sha, version),
    ).fetchone()
    return row[0] if row else None


def cache_put(
    conn: sqlite3.Connection,
    sha: str,
    response_json: str,
    model: str = _MODEL,
    *,
    version: int = _SCHEMA_VERSION,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO salgsoppgave_llm_cache "
        "(content_sha256, response_json, model, created_at, schema_version) "
        "VALUES (?, ?, ?, datetime('now'), ?)",
        (sha, response_json, model, version),
    )
    conn.commit()


_CACHE_COLS = ("content_sha256", "response_json", "model", "created_at", "schema_version")
```

Note: `content_sha256` is the table's PRIMARY KEY, so one sha holds one row and a `cache_put` at a new version REPLACES the old row rather than adding a second. That is why `test_a_version_bump_keeps_the_old_row` uses two different shas — it asserts the version bump does not wipe unrelated rows, not that one sha keeps a history.

- [ ] **Step 5: Run the tests**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py tests/rebuild/test_migrations.py -q`
Expected: PASS.

- [ ] **Step 6: Full suite, then commit**

Run: `./.venv/bin/python -m pytest -q` — expect 858 + your additions, zero failures.

```bash
git add skannonser/store/migrations/017_radon.sql skannonser/enrich/tilstand.py \
        tests/rebuild/test_migrations.py tests/rebuild/test_tilstand.py
git commit -m "feat(enrich): version the classifier cache so schema changes invalidate"
```

---

### Task 2: Widen section selection to reach radon text

**Files:**
- Modify: `skannonser/enrich/tilstand.py` (`_KEEP_HEADING` ~line 106)
- Test: `tests/rebuild/test_tilstand.py`

**Interfaces:**
- Consumes: `select_sections(secs)`, `classify_input(html)` (unchanged signatures).
- Produces: `_KEEP_HEADING` additionally matching `radon` and `helse.{0,3}milj`.

Measured over 400 ads: substantive radon statements visible to the classifier go from 59/75 (79%) to 70/75 (93%), for +0.7% input size. The lost text sits under headings literally named `Radonmåling` (9 ads) and `Radon` (2).

- [ ] **Step 1: Write the failing test**

```python
def test_select_sections_keeps_radon_and_hms_headings():
    """Radon statements often sit under their own heading or under "Helse,
    miljø og sikkerhet" -- neither matches the condition-report vocabulary, so
    without this the classifier is asked about text it was never shown."""
    secs = [
        Section("Radonmåling", "Det er ikke foretatt radonmålinger."),
        Section("Helse, miljø og sikkerhet", "Bygget er ikke oppført med radonsperre."),
        Section("Beliggenhet", "Kort vei til butikk."),
    ]
    kept = [s.heading for s in select_sections(secs)]
    assert kept == ["Radonmåling", "Helse, miljø og sikkerhet"]
```

- [ ] **Step 2: Run, verify failure**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py -q -k radon_and_hms`
Expected: FAIL — `assert [] == ['Radonmåling', 'Helse, miljø og sikkerhet']`

- [ ] **Step 3: Widen the regex**

```python
# `radon` and the HMS heading were added 2026-08-06: measured over 400 ads,
# they lift substantive-radon visibility from 79% to 93% of the ads that have
# it, for +0.7% input size. Without them the classifier is asked about radon
# text that section selection dropped.
_KEEP_HEADING = re.compile(
    r"tilstand|tg\b|avvik|bygningssakkyndig|takst|egenerkl|vedlikehold"
    r"|bygningsdel|boligsalgsrapport|radon|helse.{0,3}milj",
    re.I,
)
```

- [ ] **Step 4: Verify, and check the size cost on real fixtures**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py -q` — expect PASS.

Then confirm the widening did not balloon the input on a real ad:

```bash
./.venv/bin/python -c "
from pathlib import Path
from skannonser.enrich.tilstand import classify_input
p = Path('tests/rebuild/fixtures/finn/432672475.html')
print(len(classify_input(p.read_text(encoding='utf-8', errors='replace'))), 'chars')
"
```
Expected: a number in the 9,000–11,000 range (it was ~9,460 before). If it is
above 15,000 the regex is matching far more than intended — stop and report.

- [ ] **Step 5: Commit**

```bash
git add skannonser/enrich/tilstand.py tests/rebuild/test_tilstand.py
git commit -m "feat(enrich): select radon and HMS sections for classification"
```

---

### Task 3: Radon fields in the response schema and prompt

**Files:**
- Modify: `skannonser/enrich/tilstand.py` (vocabularies ~line 30, `TilstandResponse` ~line 96, `TILSTAND_SCHEMA` ~line 113, `_SYSTEM_PROMPT` ~line 160)
- Test: `tests/rebuild/test_tilstand.py`

**Interfaces:**
- Consumes: `_enum_check(allowed)`, `TilstandResponse`, `TILSTAND_SCHEMA`, `_SYSTEM_PROMPT`.
- Produces:
  - `RADON_STATUS = ("ikke_malt", "malt_under_grense", "malt_over_grense", "malt_ukjent_verdi")`
  - `RADONSPERRE = ("finnes", "mangler")`
  - `TilstandResponse` gains `radon_status: str | None`, `radonsperre: str | None`, `radon_bq: int | None`
  - `TILSTAND_SCHEMA` declares all three (the two enums; `radon_bq` a plain nullable integer)

- [ ] **Step 1: Write the failing tests**

```python
def test_radon_fields_parse_and_reject_off_vocab():
    from skannonser.enrich.tilstand import RADON_STATUS, RADONSPERRE

    base = {**GOOD_RESPONSE, "radon_status": "malt_over_grense",
            "radonsperre": "mangler", "radon_bq": 280}
    resp = TilstandResponse.model_validate(base)
    assert resp.radon_status == "malt_over_grense"
    assert resp.radonsperre == "mangler"
    assert resp.radon_bq == 280

    for bad in ({"radon_status": "kanskje"}, {"radonsperre": "delvis"}):
        with pytest.raises(ValidationError):
            TilstandResponse.model_validate({**base, **bad})

    assert len(RADON_STATUS) == 4 and len(RADONSPERRE) == 2


def test_radon_fields_are_nullable():
    """NULL is the common case: most ads carry only generic advice, which is
    not a statement about this property."""
    resp = TilstandResponse.model_validate(
        {**GOOD_RESPONSE, "radon_status": None, "radonsperre": None, "radon_bq": None})
    assert resp.radon_status is None and resp.radon_bq is None


def test_radon_bq_is_a_free_integer_not_a_grid_value():
    """Unlike repair costs, this is a measured quantity whose whole value is
    its position relative to the 100 and 200 Bq/m3 thresholds -- snapping it
    to the cost grid would destroy that."""
    resp = TilstandResponse.model_validate({**GOOD_RESPONSE, "radon_bq": 137})
    assert resp.radon_bq == 137


def test_schema_declares_the_radon_enums_at_the_wire():
    from skannonser.enrich.tilstand import RADON_STATUS

    props = TILSTAND_SCHEMA["properties"]
    assert props["radon_status"]["anyOf"][0]["enum"] == list(RADON_STATUS)
    assert props["radon_bq"]["anyOf"][0]["type"] == "integer"
    for key in ("radon_status", "radonsperre", "radon_bq"):
        assert key in TILSTAND_SCHEMA["required"]


def test_prompt_names_both_documented_traps():
    """The two failure modes measured in the spec. If these instructions are
    ever dropped the model reverts to extracting the statutory limit as the
    property's radon level."""
    from skannonser.enrich.tilstand import _SYSTEM_PROMPT

    p = _SYSTEM_PROMPT.lower()
    assert "grenseverdi" in p          # trap 1: quoted threshold, not a measurement
    assert "radonsperre" in p          # trap 2: negation around the barrier
    assert "aktsomhet" in p            # the risk-map paragraphs, also not a measurement
```

Also extend the existing `GOOD_RESPONSE` dict at the top of the file with the
three new keys so the other tests keep validating:

```python
GOOD_RESPONSE = {
    ...,
    "radon_status": None,
    "radonsperre": None,
    "radon_bq": None,
}
```

- [ ] **Step 2: Run, verify failure**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py -q -k radon`
Expected: FAIL — `ImportError: cannot import name 'RADON_STATUS'`

- [ ] **Step 3: Add the vocabularies and model fields**

Next to `UTSTEDER` in `tilstand.py`:

```python
RADON_STATUS = ("ikke_malt", "malt_under_grense", "malt_over_grense", "malt_ukjent_verdi")
RADONSPERRE = ("finnes", "mangler")
```

On `TilstandResponse`:

```python
    radon_status: str | None
    radonsperre: str | None
    radon_bq: int | None

    _v_radon = field_validator("radon_status")(_enum_check(RADON_STATUS))
    _v_sperre = field_validator("radonsperre")(_enum_check(RADONSPERRE))
```

In `TILSTAND_SCHEMA["properties"]`:

```python
        "radon_status": {"anyOf": [
            {"type": "string", "enum": list(RADON_STATUS)}, {"type": "null"}]},
        "radonsperre": {"anyOf": [
            {"type": "string", "enum": list(RADONSPERRE)}, {"type": "null"}]},
        "radon_bq": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
```

and add `"radon_status", "radonsperre", "radon_bq"` to `TILSTAND_SCHEMA["required"]`.

- [ ] **Step 4: Extend the prompt**

Append to `_SYSTEM_PROMPT`, before the closing `"""`:

```
- Radon. radon_status describes THIS property, and is null unless the text
  says something substantive about it. Generic advice ("vi anbefaler
  radonmaling", "interessenter gjores oppmerksom pa...") is boilerplate that
  appears in most prospectuses and is NOT a statement about this property --
  leave it null. Use "ikke_malt" when the text says no measurement was taken,
  "malt_under_grense" / "malt_over_grense" when a measurement is reported with
  its result, and "malt_ukjent_verdi" when a measurement is confirmed but no
  result is given.
- radon_bq is the property's OWN measured value in Bq/m3, and null otherwise.
  Most numbers near the word radon are the statutory thresholds quoted in
  advisory text -- "ovre anbefalte grenseverdi pa 200 Bq/m3", "dersom det
  overstiger 100 Bq/m3" -- and those are NOT this property's value. Numbers
  from the aktsomhetskart risk-area paragraphs are not measurements either.
  If in doubt, leave it null: no value is fine, a wrong one is not.
- radonsperre: read the negation. "bygget er heller ikke utfort med
  radonsperre" means "mangler", not "finnes" -- the phrase contains the word
  either way.
```

- [ ] **Step 5: Verify, including that nothing else broke**

Run: `./.venv/bin/python -m pytest tests/rebuild/ -q` — expect PASS. If the
recorded-response fixtures in `test_classify_tilstand.py` / `test_tilstand_validate.py`
fail on the now-required radon keys, add `"radon_status": None, "radonsperre":
None, "radon_bq": None` to their `RESPONSE` / `ESTIMATE_RESPONSE` constants.

- [ ] **Step 6: Commit**

```bash
git add skannonser/enrich/tilstand.py tests/rebuild/
git commit -m "feat(enrich): radon status, barrier and Bq value in the output schema"
```

---

### Task 4: Rollup and persistence

**Files:**
- Modify: `skannonser/enrich/tilstand.py` (`compute_rollup` ~line 250)
- Modify: `skannonser/store/repositories/tilstand.py` (`_ROLLUP_COLS` line 17)
- Test: `tests/rebuild/test_tilstand.py`, `tests/rebuild/test_tilstand_repo.py`

**Interfaces:**
- Consumes: `TilstandResponse` with radon fields (Task 3); migration 017's three `listing_tilstand` columns (Task 1).
- Produces: `compute_rollup(resp)` returns three more keys — `radon_status`, `radonsperre`, `radon_bq` — matching `_ROLLUP_COLS` exactly.

- [ ] **Step 1: Write the failing tests**

In `tests/rebuild/test_tilstand.py`, extend the `_resp` helper to accept radon
values and add:

```python
def test_rollup_passes_radon_through_unchanged():
    """Radon is a per-listing fact, not an aggregate: it is copied, not summed.
    Kept in the rollup (not listing_tg_findings) because a TG grade on radon
    and a radon measurement are different claims -- see the spec."""
    resp = TilstandResponse.model_validate({
        **GOOD_RESPONSE,
        "radon_status": "malt_over_grense", "radonsperre": "mangler", "radon_bq": 280,
    })
    r = compute_rollup(resp)
    assert r["radon_status"] == "malt_over_grense"
    assert r["radonsperre"] == "mangler"
    assert r["radon_bq"] == 280


def test_rollup_radon_defaults_to_none():
    r = compute_rollup(_resp([]))
    assert r["radon_status"] is None
    assert r["radonsperre"] is None
    assert r["radon_bq"] is None
```

In `tests/rebuild/test_tilstand_repo.py`, extend the module-level `ROLLUP`
dict with `"radon_status": "ikke_malt", "radonsperre": "mangler",
"radon_bq": None` and add:

```python
def test_upsert_ad_persists_radon(tmp_path):
    conn = _db(tmp_path)
    TilstandRepo(conn).upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    row = conn.execute("SELECT * FROM listing_tilstand WHERE finnkode='42'").fetchone()
    assert row["radon_status"] == "ikke_malt"
    assert row["radonsperre"] == "mangler"
    assert row["radon_bq"] is None
```

- [ ] **Step 2: Run, verify failure**

Run: `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py tests/rebuild/test_tilstand_repo.py -q -k radon`
Expected: FAIL — `KeyError: 'radon_status'`

- [ ] **Step 3: Implement**

In `compute_rollup`'s returned dict, alongside `tilstandsrapport_dato`:

```python
        # Per-listing facts, copied rather than aggregated.
        "radon_status": resp.radon_status,
        "radonsperre": resp.radonsperre,
        "radon_bq": resp.radon_bq,
```

In `skannonser/store/repositories/tilstand.py`:

```python
_ROLLUP_COLS = (
    "tg2_count", "tg3_count", "reparasjon_lav", "reparasjon_hoy",
    "reparasjon_est", "alvorlighet", "verste_bygningsdel", "reparasjon_kilde",
    "tilstandsrapport_dato", "tilstandsrapport_utsteder", "egenerklaering_antall",
    "radon_status", "radonsperre", "radon_bq",
)
```

- [ ] **Step 4: Run** — `./.venv/bin/python -m pytest tests/rebuild/ -q`, expect PASS.

- [ ] **Step 5: Commit**

```bash
git add skannonser/enrich/tilstand.py skannonser/store/repositories/tilstand.py tests/rebuild/
git commit -m "feat(store): persist radon status, barrier and Bq value"
```

---

### Task 5: The two traps, as tests

This task adds no production code. It exists because the traps are the entire
reason radon is in the classifier rather than in another regex, and a plan
that does not pin them lets a future prompt edit silently reintroduce them.

**Files:**
- Test: `tests/rebuild/test_tilstand.py`

**Interfaces:**
- Consumes: `classify_one(text, *, _call=...)` — the injected seam; no network.

- [ ] **Step 1: Write the trap tests**

```python
# --- the two measured radon traps -------------------------------------------
# Both were reproduced against the real corpus (2026-08-06 spec). They are
# pinned here as CONTRACT tests on the parsed shape: a recorded response that
# fell for either trap must not validate into something the UI would show as
# fact.

_TRAP_BOILERPLATE = json.dumps({
    **GOOD_RESPONSE,
    # An ad whose only radon text is: "ovre anbefalte grenseverdi pa 200
    # Bq/m3 ... dersom det overstiger 100 Bq/m3". Nothing about THIS property.
    "radon_status": None, "radonsperre": None, "radon_bq": None,
})

_TRAP_NEGATED = json.dumps({
    **GOOD_RESPONSE,
    # "Det er ikke foretatt radonmalinger, og bygget er heller ikke utfort
    # med radonsperre." Both clauses are negative.
    "radon_status": "ikke_malt", "radonsperre": "mangler", "radon_bq": None,
})


def test_quoted_threshold_yields_no_bq_value():
    resp = classify_one("...", _call=lambda _: _TRAP_BOILERPLATE)
    assert resp.radon_bq is None, "200/100 Bq/m3 are the statutory limits, not a measurement"
    assert resp.radon_status is None, "generic advice is not a statement about this property"


def test_negated_barrier_reads_as_missing_not_present():
    resp = classify_one("...", _call=lambda _: _TRAP_NEGATED)
    assert resp.radonsperre == "mangler"
    assert resp.radon_status == "ikke_malt"


def test_a_threshold_valued_bq_would_still_validate_so_the_prompt_carries_the_rule():
    """Documents a real limit: 200 is a legal integer, so the SCHEMA cannot
    reject a threshold mistakenly extracted as a measurement. The only defence
    is the prompt rule asserted in test_prompt_names_both_documented_traps,
    plus the stage-1 spot check in the runbook. If validation ever needs to
    catch this, it needs a rule beyond the type."""
    resp = classify_one("...", _call=lambda _: json.dumps({**GOOD_RESPONSE, "radon_bq": 200}))
    assert resp.radon_bq == 200
```

- [ ] **Step 2: Run** — `./.venv/bin/python -m pytest tests/rebuild/test_tilstand.py -q -k trap`. These should pass immediately (Tasks 1–4 did the work); if any fails, the schema or validators are wrong, not the test.

- [ ] **Step 3: Commit**

```bash
git add tests/rebuild/test_tilstand.py
git commit -m "test: pin the two measured radon extraction traps"
```

---

### Task 6: API exposure

**Files:**
- Modify: `skannonser/publish/rows.py` (`_EIE_SELECT_TAIL` ~line 130)
- Modify: `skannonser/web/api.py` (`_eie_item` ~line 380)
- Test: `tests/rebuild/test_web_api.py`

**Interfaces:**
- Consumes: `listing_tilstand.radon_status / radonsperre / radon_bq`.
- Produces: API item keys `radon_status`, `radonsperre`, `radon_bq` — `None` when unclassified. `radon_omtalt` is unchanged and still served.

- [ ] **Step 1: Write the failing tests**

```python
def test_radon_classifier_fields_flow_into_item(db_path, client):
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.execute(
        "INSERT INTO listing_tilstand "
        "(finnkode, tg2_count, tg3_count, radon_status, radonsperre, radon_bq, "
        " classified_at) "
        "VALUES ('A', 0, 0, 'malt_over_grense', 'mangler', 280, '2026-08-01T00:00:00')"
    )
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["radon_status"] == "malt_over_grense"
    assert item["radonsperre"] == "mangler"
    assert item["radon_bq"] == 280


def test_radon_classifier_fields_are_none_when_unclassified(db_path, client):
    """And radon_omtalt keeps working independently: it covers every parsed
    ad, including ones classification has not reached."""
    conn = _conn(db_path)
    _ins_eiendom(conn, "A")
    conn.execute(
        "INSERT INTO listing_salgsoppgave (finnkode, radon_omtalt, parsed_at) "
        "VALUES ('A', 1, '2026-08-01T00:00:00')"
    )
    conn.commit()
    conn.close()

    item = _by_finnkode(client.get("/api/listings").json()["listings"], "A")
    assert item["radon_status"] is None
    assert item["radon_bq"] is None
    assert item["radon_omtalt"] is True
```

- [ ] **Step 2: Run, verify failure** — `KeyError: 'radon_status'`.

- [ ] **Step 3: Implement**

In `skannonser/publish/rows.py`, append to `_EIE_SELECT_TAIL` after
`t.reparasjon_kilde AS "REPARASJON_KILDE"`:

```sql
    ,t.radon_status AS "RADON_STATUS",
    t.radonsperre AS "RADONSPERRE",
    t.radon_bq AS "RADON_BQ"
```

In `skannonser/web/api.py`'s `_eie_item`, next to the other tilstand keys:

```python
        # Radon from the classifier (migration 017). Distinct from
        # `radon_omtalt` above, which is a regex mention-detector: that says
        # the document discusses radon, these say what it discussed.
        "radon_status": rec.get("RADON_STATUS"),
        "radonsperre": rec.get("RADONSPERRE"),
        "radon_bq": rec.get("RADON_BQ"),
```

- [ ] **Step 4: Run** — `./.venv/bin/python -m pytest tests/rebuild/test_web_api.py -q`.

The exhaustive key-set test `test_listing_shape_and_donor_resolved_travel`
(~line 248) WILL fail; add the three new keys to its expected set.

- [ ] **Step 5: Commit**

```bash
git add skannonser/publish/rows.py skannonser/web/api.py tests/rebuild/test_web_api.py
git commit -m "feat(web): expose classifier radon fields through the API"
```

---

### Task 7: Frontend — formatters, column, popup

**Files:**
- Modify: `skannonser/web/static/listingmeta.js` (vocab maps ~line 260, `TILSTAND_DERIVED` ~line 310)
- Modify: `skannonser/web/static/table.js` (COLUMNS ~line 106 and ~line 115, `TILSTAND_COLUMNS` ~line 144, cell switch ~line 517)
- Modify: `skannonser/web/static/popup.js` (Tilstand block ~line 300)
- Test: `tests/web/tilstand.test.mjs`

**Interfaces:**
- Consumes: item keys from Task 6; `fromVocab`, `TILSTAND_DERIVED`, `TILSTAND_HINT`.
- Produces: `fmtRadonStatus(value)`, `fmtRadonsperre(value)`, `fmtRadon(item)`; a `radon_status` table column labelled **"Radon"**; the Phase-1 column relabelled **"Radon nevnt"**.

- [ ] **Step 1: Write the failing tests**

```javascript
import { fmtRadonStatus, fmtRadonsperre, fmtRadon } from "../../skannonser/web/static/listingmeta.js";

test("fmtRadonStatus maps the four states and passes null through", () => {
  assert.equal(fmtRadonStatus("ikke_malt"), "Ikke målt");
  assert.equal(fmtRadonStatus("malt_under_grense"), "Målt, under grense");
  assert.equal(fmtRadonStatus("malt_over_grense"), "Målt, OVER grense");
  assert.equal(fmtRadonStatus("malt_ukjent_verdi"), "Målt, verdi ikke oppgitt");
  assert.equal(fmtRadonStatus(null), null);
  assert.equal(fmtRadonStatus("nytt_svar"), "nytt_svar"); // unmapped passes through
});

test("fmtRadonsperre reads as a fact, not a yes/no", () => {
  assert.equal(fmtRadonsperre("finnes"), "Radonsperre");
  assert.equal(fmtRadonsperre("mangler"), "Ingen radonsperre");
  assert.equal(fmtRadonsperre(null), null);
});

test("fmtRadon appends the measured value only when there is one", () => {
  assert.equal(
    fmtRadon({ radon_status: "malt_over_grense", radon_bq: 280 }),
    "Målt, OVER grense (280 Bq/m³)");
  assert.equal(fmtRadon({ radon_status: "ikke_malt", radon_bq: null }), "Ikke målt");
  assert.equal(fmtRadon({ radon_status: null, radon_bq: null }), null);
});

test("radon_status is LLM-derived; radon_omtalt is not", () => {
  assert.ok(TILSTAND_DERIVED.has("radon_status"));
  assert.ok(!TILSTAND_DERIVED.has("radon_omtalt"));
  assert.ok(SALGSOPPGAVE_DERIVED.has("radon_omtalt"));
});
```

- [ ] **Step 2: Run** — `node --test tests/web/tilstand.test.mjs`. Expected: FAIL (exports missing).

- [ ] **Step 3: Implement the formatters**

In `listingmeta.js`, next to `BYGNINGSDEL_LABELS`:

```javascript
const RADON_STATUS_LABELS = {
  ikke_malt: "Ikke målt",
  malt_under_grense: "Målt, under grense",
  // Shouted deliberately: this is the one radon state that costs money.
  malt_over_grense: "Målt, OVER grense",
  malt_ukjent_verdi: "Målt, verdi ikke oppgitt",
};

const RADONSPERRE_LABELS = { finnes: "Radonsperre", mangler: "Ingen radonsperre" };

export function fmtRadonStatus(value) {
  return fromVocab(RADON_STATUS_LABELS, value);
}

export function fmtRadonsperre(value) {
  return fromVocab(RADONSPERRE_LABELS, value);
}

// Status plus the measured value when one was stated. radon_bq is populated on
// only ~2% of ads by design -- most Bq figures in a prospectus are the
// statutory thresholds, and the classifier is told not to extract those.
export function fmtRadon(item) {
  const s = fmtRadonStatus(item.radon_status);
  if (!s) return null;
  const bq = item.radon_bq;
  return bq === null || bq === undefined ? s : s + " (" + bq + " Bq/m³)";
}
```

Add `"radon_status"` to `TILSTAND_DERIVED`.

- [ ] **Step 4: Wire the table column**

In `table.js`: relabel the existing Phase-1 column so the two do not collide,
and add the new one beside the other tilstand columns:

```javascript
  { key: "radon_omtalt", label: "Radon nevnt", sortable: true },
```

```javascript
  { key: "alvorlighet", label: "Alvorlighet", sortable: true },
  { key: "radon_status", label: "Radon", sortable: true },
```

Add `"radon_status"` to `TILSTAND_COLUMNS`. In the cell switch:

```javascript
      case "radon_status": {
        td.textContent = fmtRadon(item) || "";
        break;
      }
```

Import `fmtRadon` from `./listingmeta.js`.

- [ ] **Step 5: Add the popup row**

In `popup.js`'s Tilstand block, after the Utbedring row:

```javascript
  addRow(tdl, "Radon", fmtRadon(item));
```

Import `fmtRadon`. Leave the existing `addRow(sdl, "Radon", fmtOmtalt(...))`
in the salgsoppgave block alone but relabel it to `"Radon nevnt"`, so the two
blocks do not both claim the row name "Radon".

- [ ] **Step 6: Run both suites**

Run: `node --test tests/web/*.test.mjs` — expect PASS (183 + your additions).
Run: `./.venv/bin/python -m pytest -q` — unchanged, zero failures.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/ tests/web/
git commit -m "feat(web): show classifier radon status, barrier and value"
```

---

### Task 8: Runbook update

**Files:**
- Modify: `docs/tilstand-runbook.md`

- [ ] **Step 1: Document the version bump and the radon spot-check**

Add to the runbook, after the Prereqs block:

```markdown
## Before a run: check the cache schema version

`_SCHEMA_VERSION` in `skannonser/enrich/tilstand.py` is part of the cache key.
Bumping it makes every cached response a MISS, so the next run re-classifies
the whole corpus at full price. It was bumped to 2 on 2026-08-06 to add radon.

Check what a run would actually cost before starting it:

    skannonser tools classify-tilstand --db main/database/properties.db --status

Rows whose `schema_version` is below the current one will be re-billed. Batch
schema changes rather than trickling them out.
```

And to the Stage 1 gates:

```markdown
Radon spot-check (stage 1, ~10 ads): of the ads where `radon_bq` is non-NULL,
confirm none is 100 or 200 — those are the statutory thresholds quoted in
advisory boilerplate, not measurements. Per the design spec, if any
threshold-valued extraction appears, DROP the field rather than tune it:

    SELECT finnkode, radon_status, radon_bq FROM listing_tilstand
    WHERE radon_bq IS NOT NULL;

Also confirm `radon_status` is non-NULL on materially less than ~40% of
classified ads. Higher than that means the prompt is treating generic advice
as a statement about the property — the exact failure that makes the old
`radon_omtalt` field useless.
```

- [ ] **Step 2: Commit**

```bash
git add docs/tilstand-runbook.md
git commit -m "docs(runbook): cache version bump and the radon spot-check"
```

---

## Self-Review Notes (already applied)

- **Spec coverage**: cache versioning → T1; section widening → T2; schema/prompt/enums → T3; storage + rollup → T1 (DDL) & T4; the two traps → T3 (prompt) & T5 (contract tests); `radon_omtalt` coexistence → T6 & T7; UI relabelling → T7; testing → T3/T5/T6/T7; cost + spot-check → T8.
- **Type consistency**: `RADON_STATUS`/`RADONSPERRE` tuples single-source the pydantic validators (T3), the wire schema (T3), and nothing else duplicates the literals. `compute_rollup`'s three new keys (T4) match `_ROLLUP_COLS` (T4) match migration 017's columns (T1) match the API's `RADON_*` aliases (T6) match the JS item keys (T7).
- **Known judgment call**: `test_a_threshold_valued_bq_would_still_validate...` (T5) asserts a *limitation* rather than a behaviour. It is deliberate — the schema cannot distinguish 200-the-threshold from 200-the-measurement, so the defence lives in the prompt and the runbook check, and the test says so out loud rather than leaving a reader to assume validation covers it.
- **Migration 017 does double duty** (radon columns + cache version) rather than taking two numbers, because they must land together: the columns are useless without the version bump forcing re-classification.
