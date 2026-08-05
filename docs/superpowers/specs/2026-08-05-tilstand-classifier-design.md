# Tilstand classifier (salgsoppgave Phase 2) — design

**Date:** 2026-08-05
**Status:** approved, pending implementation plan
**Builds on:** `2026-07-27-salgsoppgave-extraction-design.md` (Phase 1, shipped) and
`docs/salgsoppgave-handoff.md`. Read both first; this spec does not repeat them.

## Problem

Phase 1 shipped the rules-extracted fields. The original motivation — "which
flats have a TG3 bathroom" — is still unanswerable, and the user's actual
question is one step past it: **how bad is it, and roughly what would it cost
to fix?** TG3 alone is a bad severity signal: "utvendig trapp mangler
rekkverk" and "badet må totalrenoveres" are both TG3; one is a 10k fix, the
other 500k.

This spec covers the LLM classification pass over the condition sections:
TG2/TG3 findings with per-finding severity and repair-cost bands, the
egenerklæring, and per-listing rollups the web app can sort and filter on.

## Measurements this design rests on

All measured 2026-08-05 against `data/eiendom/html_extracted/` (7,731 files),
random samples of 400–500 ads. Probe scripts were session scratchpad; rerun
from the numbers below if needed.

- **Surveyor cost estimates ARE in the embedded text** — the Phase-1 spec's
  claim that they only live in the broker PDF is wrong. Labels:
  `Kostnadsestimat`, `Utbedringskostnader`, `Kostnadsoverslag`,
  `(Sjablongmessig|Estimert) prisanslag`. Coverage: **11.8%** of ads carry ≥1
  costed finding; **17.9%** of TG3-mentioning ads. So stated figures are a
  minority — but a genuine ground truth (see Validation).
- **The bands are semi-standard**: `Under 10 000`, `10 000–50 000`,
  `50 000–100 000`, `100 000–300 000`, `200 000–500 000`, `Over 300 000` and
  ~10 rarer variants. All snap onto a coarse grid (below).
- **Condition-section input size**: selecting sections whose heading matches
  `tilstand|tg\b|avvik|bygningssakkyndig|takst|egenerkl|vedlikehold|bygningsdel|boligsalgsrapport`
  or whose body contains TG2/TG3/egenerklæring markers yields mean **8,715
  chars/ad** (median 7,633, p90 16,362), ~3 sections/ad. 11.5% of ads have an
  empty selection (mostly new-builds) — skip those, no API call.
- **Corpus input estimate**: ~25M tokens at 2.7 chars/token (Norwegian) plus
  prompt/schema overhead. **Chars-derived, not `count_tokens`-verified** — no
  API key exists in the dev environment. Verify at stage 1.

## Decisions already made (with the user, 2026-08-05)

| Decision | Choice |
| --- | --- |
| Cost source | Extract surveyor band where stated (`takst`); model estimates a band otherwise (`estimat`). Provenance is a column, never dropped. |
| Per-listing rollups | All three: cost range (filter), midpoint sum (sort), severity tier + worst finding (glance). Cheap — all derive from the findings table. |
| Model | **Opus 5 via Batch API**, ~$133 for the full corpus. Chosen by the user. Staged backfill is the spend control — the API has none. |
| Phase-2 storage | **Separate tables** (`listing_tilstand`, rebuilt `listing_tg_findings`), not columns on `listing_salgsoppgave`. Dissolves all three handoff traps structurally. |

## Why separate tables (the contentious call)

The handoff documents two traps: `SalgsoppgaveRepo.upsert` uses
`INSERT OR REPLACE` and would null Phase-2 columns on every Phase-1 re-parse,
and `wipe()` deletes the Phase-2 tables even though `--wipe` is routine
Phase-1 maintenance. A third was found in review: `listing_tg_findings` has
`UNIQUE (finnkode, tg, bygningsdel)`, which collapses two TG3 bathrooms into
one row — harmless for counting parts, **silently halves the repair bill**
once findings carry costs.

All three share a root cause: Phase-2 data living inside Phase-1's storage.
Moving it out fixes them structurally instead of by discipline:

- `upsert` cannot touch what isn't in its table.
- `wipe()` drops its two Phase-2 `DELETE`s; `TilstandRepo` gets its own wipe.
- Each table is independently rebuildable from its own source — the
  migration-010 invariant Phase 1 deliberately preserved.

Cost of the call: migration 015's five Phase-2 columns on
`listing_salgsoppgave` become dead. They are NULL on all 6,217 live rows, so
dropping them loses nothing.

## Schema (migration 016)

**016 is free as of 2026-08-05** (`ls migrations/` + `git log`), but another
session works in this repo — re-check before writing the file, and expect an
`ALL_MIGRATIONS` merge conflict in `tests/rebuild/test_migrations.py`
(resolve by keeping both lines in numeric order).

```sql
-- Rebuild: drop the UNIQUE collapse, add per-finding judgment columns.
-- Table is empty on the server (0 rows), so DROP is safe.
DROP TABLE listing_tg_findings;
CREATE TABLE listing_tg_findings (
    id            INTEGER PRIMARY KEY,
    finnkode      TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg            INTEGER NOT NULL,        -- 2 | 3
    bygningsdel   TEXT NOT NULL,           -- 18-value enum (Phase-1 spec, unchanged)
    tiltak        TEXT,                    -- enum (Phase-1 spec, unchanged)
    alvorlighet   TEXT NOT NULL,           -- 'kosmetisk'|'mindre'|'vesentlig'|'alvorlig'
    kostnad_lav   INTEGER,                 -- kr, value from the grid below
    kostnad_hoy   INTEGER,                 -- kr, value from the grid below
    kostnad_kilde TEXT                     -- 'takst' | 'estimat'
);
CREATE INDEX idx_tg_findings_finnkode ON listing_tg_findings (finnkode);

-- Rollups: own table so Phase-1 re-parse/wipe cannot touch them.
CREATE TABLE listing_tilstand (
    finnkode              TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    tg2_count             INTEGER NOT NULL,
    tg3_count             INTEGER NOT NULL,
    reparasjon_lav        INTEGER,   -- SUM(kostnad_lav)      → filter floor
    reparasjon_hoy        INTEGER,   -- SUM(kostnad_hoy)      → filter ceiling
    reparasjon_est        INTEGER,   -- SUM(midpoints)        → sort key
    alvorlighet           TEXT,      -- worst tier across findings
    verste_bygningsdel    TEXT,      -- bygningsdel of the worst finding
    reparasjon_kilde      TEXT,      -- 'takst' | 'blandet' | 'estimat'
    tilstandsrapport_dato TEXT,
    tilstandsrapport_utsteder TEXT,  -- enum + 'annet' (Phase-1 spec)
    egenerklaering_antall INTEGER,   -- COUNT(*) of listing_egenerklaering rows
    classified_at         TEXT NOT NULL
);

-- listing_egenerklaering keeps its shape and stays classifier-owned; move its
-- wipe from SalgsoppgaveRepo to TilstandRepo.

-- Phase-2 columns on listing_salgsoppgave: NULL on all rows, now dead.
ALTER TABLE listing_salgsoppgave DROP COLUMN tg2_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tg3_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_dato;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_utsteder;
ALTER TABLE listing_salgsoppgave DROP COLUMN egenerklaering_antall;
```

`salgsoppgave_llm_cache` (migration 015) is reused as-is: keyed by
`content_sha256` of the classifier *input* text, so `--wipe` rebuilds replay
cached responses at zero cost and an unchanged re-crawled listing is free.

### The cost grid

`kostnad_lav`/`kostnad_hoy` take values only from:

```
0, 10_000, 20_000, 50_000, 100_000, 200_000, 300_000, 500_000, 1_000_000
```

with `kostnad_hoy = 1_000_000` meaning "1M+". Enforced as JSON-schema enums
at the wire. Rationale: every observed surveyor band snaps onto this grid,
and it stops the model inventing "kr 137 500" precision it does not have.
A stated band that straddles grid points snaps **outward** (floor down,
ceiling up) — never narrower than the surveyor said.

### Rollup semantics

- `reparasjon_est` = Σ((lav + hoy) / 2), rounded to nearest 10k.
- `alvorlighet` = max over findings (ordering: kosmetisk < mindre <
  vesentlig < alvorlig).
- `verste_bygningsdel` = bygningsdel of the finding driving that max; ties
  broken by higher `kostnad_hoy`.
- `reparasjon_kilde`: `takst` if all findings are takst, `estimat` if all
  are estimat, else `blandet`.
- A listing whose classification ran but found zero findings gets a
  `listing_tilstand` row with counts 0 and NULL costs — distinct from "never
  classified" (no row). Same null-vs-false discipline as Phase 1.

## The classification call

- **Input**: the condition-section selection only (heading regex + body
  markers above), not the full salgsoppgave. Skip ads with an empty
  selection.
- **Model**: `claude-opus-5`, Batch API for backfill (50% off; results keyed
  by `custom_id` = finnkode, never by position), single calls for daily new
  listings.
- **Output**: strict JSON schema via `output_config.format` —
  `additionalProperties: false`, enums for `bygningsdel`, `tiltak`,
  `alvorlighet`, `kostnad_kilde`, `kostnad_lav`, `kostnad_hoy`,
  `tilstandsrapport_utsteder`, `forhold`. One response carries findings,
  egenerklæring rows, and report metadata for one ad.
- **Prompt contract** (per finding):
  1. If the text states a cost for this finding, snap it outward onto the
     grid and set `kostnad_kilde: "takst"`.
  2. Otherwise estimate a band from the defect description, consequence
     text, and typical Norwegian repair costs; set `"estimat"`.
  3. `alvorlighet` is always the model's judgment from the defect and
     consequence text (TG grade alone does not determine it).
  4. Structural boilerplate headings (`Vurdering av avvik`, `Tiltak`, …) are
     not building parts — discard, don't bucket. Unmappable real parts go to
     `annet` and still count.
- **Norwegian negation and heading traps** from the Phase-1 lessons apply to
  the prompt too: instruct the model to classify from finding bodies, not
  section headings.

## Cost & spend control

~25M input + ~4–5M output tokens ≈ **$265 straight / $133 via Batch** at
Opus 5 pricing ($5/$25 per MTok). **The API has no self-limiting** — a batch
runs to completion and bills what it consumes; `max_tokens` caps one
response, not the job. Controls:

1. **Staged backfill**: ~200 ads (≈$3) → validate → ~1,000 (≈$17) →
   validate → remainder in batches. Abort points before bulk spend.
2. Run `count_tokens` on a 50-ad sample at stage 1 to replace the
   chars-derived estimate before committing to the remainder.
3. Optionally set a Console spend limit on the API account first.

The backfill runs **locally**; the server never needs the API key (Phase-1
decision, unchanged). Daily new listings are classified on the next local
pipeline run.

## Validation: the free ground truth

The ~12% of ads with a surveyor-stated cost are labelled data for the
*estimation* task. At stage 1:

1. For those ads, **strip the stated cost figures from the input**, let the
   model estimate blind.
2. Compare its band to the surveyor's band (after snapping both to the grid).
3. Report: exact-band match rate, within-one-band rate, direction bias.

Acceptance gate for proceeding past stage 1: ≥70% within one band and no
systematic direction bias. If it fails, the `estimat` path gets dropped to
NULL (surveyor figures only) rather than shipping confident nonsense — the
schema supports that without change. Severity (`alvorlighet`) has no ground
truth; spot-check ~30 ads by hand at stage 1.

In production runs (not the validation harness), stated costs are of course
left in the input.

## New modules & CLI

- `skannonser/enrich/tilstand.py` — section selection, prompt, schema, cache
  lookup, response→rows mapping. API call behind an injected transport seam
  (convention: `_transport` in `skannonser/http.py`).
- `skannonser/store/repositories/tilstand.py` — `TilstandRepo` with
  `upsert_findings`, `upsert_rollup`, `wipe()`. Rollups computed in Python
  from the findings, written in the same transaction.
- `SalgsoppgaveRepo.wipe()` **stops deleting** `listing_tg_findings` and
  `listing_egenerklaering` (they move to `TilstandRepo.wipe()`).
- `skannonser tools classify-tilstand` — mirrors `backfill-salgsoppgave`:
  `--limit N`, `--wipe` (Phase-2 tables only), `--batch/--no-batch`,
  `--validate` (the stage-1 ground-truth harness above).

## Web app

- API: join `listing_tilstand` into the listing-details response; findings
  list (`listing_tg_findings` rows) exposed for the popup. Same
  None-when-unclassified convention.
- Table/filter: sort on `reparasjon_est`; range filter on
  `reparasjon_lav`/`reparasjon_hoy`; chips for `alvorlighet` following the
  existing distinct-value pattern.
- Popup: tier + worst finding ("alvorlig — våtrom"), cost range, findings
  list. Everywhere a cost shows, `reparasjon_kilde`/`kostnad_kilde` drives a
  visible marker (the Phase-1 `(s)` convention extended: e.g. `~` or muted
  styling for `estimat`).

## Testing

- **Section selection**: golden fixtures (both payload formats, a new-build
  with empty selection, malformed input → no raise).
- **Classifier**: recorded responses behind the transport seam; `pytest`
  never touches the network. Cases: takst extraction incl. outward snapping,
  estimat fallback, boilerplate discard, `annet` bucketing, zero-findings ad.
- **Cache**: second run with unchanged input issues no API call.
- **Rollups**: pure-function tests for the max/sum/tie-break semantics.
- **Migration**: 016 in `ALL_MIGRATIONS`; rebuild test passes.
- Environment: always `PYTHONPATH=. ./.venv/bin/pytest` in a worktree; JS
  untouched by this phase.

## Deploy

Merge → push → server pull → `db backup` → `db migrate` → run the backfill
**locally** against the synced DB per the existing pipeline-write path →
`docker compose up -d --build` (the `--build` is load-bearing). Migration
016 drops columns — take the backup seriously.

## Risks

- **Estimate quality is the whole bet.** Mitigated by the stage-1
  ground-truth harness and the drop-to-NULL fallback.
- **Token estimate unverified** (no API key in dev env). Verified at stage 1
  before >$20 is committed.
- **`annet` bucket growth** — same signal and remedy as Phase 1: extend the
  enum, re-classify affected content hashes (cheap, cache-scoped).
- **Concurrent session takes 016.** Re-check migration numbers at
  implementation time.
- **FINN payload format drift** — unchanged from Phase 1; decoder fails to
  NULL, classifier just sees fewer ads.
