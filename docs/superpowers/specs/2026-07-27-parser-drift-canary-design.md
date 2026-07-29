# Parser drift canary — design

**Date:** 2026-07-27
**Status:** Designed

## Goal

Fail loudly, on the same night, when a FINN markup change stops a parser field
from extracting — instead of discovering it months later by accident.

## The problem

`eiendom.info_primary_area` sat at 0% filled for months. `parse.py` read
P-ROM from `data-testid="info-primary-area"`, FINN stopped emitting that block
under NS 3940:2023, and the parser did what it was told: found nothing,
returned `""`, wrote NULL. No test failed. No log line appeared. `pris_kvm`
quietly moved onto a larger denominator for every affected listing.

That is not an isolated slip. Within the same week, `7c66d1b` fixed the energy
grade after it moved into an SVG variant, and `a40f25c` added an "Ukjent" chip
so the UI could absorb the resulting gap. Three pieces of work, one root cause:
**FINN changed markup, a parser silently returned empty, and nothing noticed.**

`pipeline.py` already names this threat. Guard 2 protects against "a FINN/DNB
layout change that lets the crawl phase succeed while nearly every ad fails to
parse". It catches the loud failure mode — parses that *throw*. It cannot see
the quiet one — parses that *succeed, emptily*. This canary is its sibling.

### Baseline dilution after a parser fix

A freshly fixed field is not watched immediately. `baseline_rates` measures
every stored row, but only rows re-parsed after the fix carry the value, so
the baseline starts diluted: for P-ROM, roughly `0.20 × 1142/5863 ≈ 3.9%` on
night one — under `MIN_BASELINE` — crossing the 5% threshold after ~2 weeks
of listing turnover. This is deliberate, conservative behaviour: a diluted
baseline can only suppress alerts, never manufacture them.

### What this does not catch

It detects **regressions from a working state**. A field that has read 0%
since before the baseline existed looks perfectly stable, so this would not
have found the P-ROM bug retroactively — only on the night the selector died.

Finding already-dead fields is a different job: compare what the parser
extracts against what FINN actually ships (the GAM targeting blob, the raw
`data-testid` inventory). That audit found the P-ROM bug and is worth
re-running by hand occasionally. It is explicitly out of scope here.

## Decisions

Every decision below was made explicitly during brainstorming.

1. **Measure the parsed batch inside `run_finn_ingest`, before upsert.**
   Rejected: a pure-SQL check comparing rows scraped today against older rows.
   It looks cheaper and needs no parser coupling, but `upsert` *updates*
   existing listings rather than re-inserting them, so "scraped today" is only
   the handful of genuinely new listings (~10–40/night). Against a 19%
   baseline that cohort cannot distinguish drift from a quiet Tuesday. The
   parsed batch includes every re-parsed existing ad — order of a thousand —
   which is enough to alert the same night.
   Also rejected: a separate nightly step that re-parses recently-cached HTML.
   Decoupled, but pays to re-parse what the pipeline just parsed.

2. **The existing DB rows are the baseline.** `eiendom` and `listing_details`
   already record what fraction of past ads carried each field, so no new
   schema, no per-field thresholds to hand-tune, and it self-calibrates as the
   corpus grows. Rejected: static floors committed in code (every floor is a
   guess that rots as FINN's listing mix shifts) and a per-run history table
   (a migration and retention policy to catch gradual drift this does not
   need yet).

3. **Read the baseline before `upsert`.** Otherwise tonight's rows are already
   folded into the baseline the check compares them against.

4. **Alert only — no behaviour change.** Guard 2 skips deactivation because
   acting on bad data destroys the active set. Silent field loss only degrades
   data; there is nothing to protect by halting, and a canary that can abort
   the nightly is a canary that eventually gets muted.

5. **Blunt arithmetic, no statistics library.** At n=100 against a 19%
   baseline, observing zero has probability ~1e-9. A binomial test buys
   nothing over a ratio comparison and adds a dependency.

6. **Derive the watch list from the existing column maps**, not a new
   hand-maintained list that can rot out of sync with the parsers.

7. **Exclude `Tilgjengelighet`.** Its presence tracks a listing's lifecycle,
   not FINN's markup — 70.9% across all stored rows but 6.7% among active
   ones. A batch of freshly-crawled live ads would look like a collapse every
   single night. The rule: watch fields whose presence is a property of the
   markup.

## Architecture

New module `skannonser/ingest/drift.py` — pure functions, no I/O, no DB
writes. The pipeline calls it exactly as it calls `_failure_rate_too_high`
today.

```
batch_rates(objects, fields)        -> dict[str, float]
baseline_rates(conn, table, cols)   -> tuple[dict[str, float], int]
compare(batch, baseline, n)         -> list[DriftFinding]
```

`DriftFinding` carries `field`, `baseline_rate`, `batch_rate`, `sample_size`.

`baseline_rates` measures **every row in the table**, not just active ones —
markup presence is not lifecycle-dependent for the watched fields (which is
precisely why `Tilgjengelighet` is excluded), and all rows is the larger
sample. It returns the row count alongside the rates so the caller can tell an
empty table from a genuinely 0% field.

**`eiendom` and `listing_details` are compared independently**, each with its
own batch and its own baseline. Their sample sizes differ: detail parsing is
best-effort (`pipeline.py` swallows detail failures so a parser bug there can
never fail the listing), so `len(details)` can be smaller than
`len(listings)`. Sharing one `n` between them would misreport both.

### Presence

A field counts as present when it is not `None`, not `""`, and not `[]`.
`NormalizedListing` signals absence with empty strings; `ListingDetails` uses
`None`; `facilities` uses `[]`. One predicate covers all three.

### Data flow

```
run_finn_ingest
  parse batch ──> listings[], details[]   (already in memory)
  baseline_rates(conn, ...)               (BEFORE upsert)
  batch_rates(listings), batch_rates(details)
  compare(...) ──> findings
  stats["drift"] = findings
  upsert(...)                             (unchanged)
```

### Detection rule

```python
MIN_BATCH    = 100   # below this, skip the check entirely
MIN_BASELINE = 0.05  # fields rarer than this aren't watched -- too noisy
DROP_FACTOR  = 0.4   # alert when batch_rate < baseline_rate * DROP_FACTOR
BASELINE_ROWS = 200  # below this many stored rows, the baseline isn't trusted
```

A field is reported when all of these hold:

- the batch is at least `MIN_BATCH`
- the baseline table holds at least `BASELINE_ROWS` rows
- the field's baseline rate is at least `MIN_BASELINE`
- its batch rate is below `baseline_rate * DROP_FACTOR`

Verified against real corpus numbers: P-ROM collapsing 19% → 0% fires; BRA-b
fluctuating 7% → 5% does not. A field whose rate *rises* never fires.

`MIN_BATCH = 100` was checked against real runs rather than guessed. Every
nightly since the pipeline cut over on 2026-07-22 (`~/skannonser-logs/
full_*.log` on the server):

| Date | parsed | failed |
|---|---|---|
| 07-22 | 967 | 0 |
| 07-23 | 960 | 0 |
| 07-24 | 947 | 0 |
| 07-25 | 953 | 0 |
| 07-26 | 1008 | 0 |
| 07-27 | 1009 | 0 |

947–1009, roughly ten times the floor. The batch tracks the size of the active
listing set — the crawl re-walks the whole polygon nightly and most of the
result is updates (07-27: 1009 parsed, 231 upserted) — so it is structurally
stable rather than dependent on how many new listings appeared. A night that
falls below 100 means something is already badly wrong, and guards 1 and 2
own that case; the canary correctly stands down instead of adding noise.

Detail parsing matched listing parsing exactly on every run
(`details_upserted: 1009`), so the separate-sample-size rule is currently
non-binding — but it stays, because detail failures are swallowed by design
and nothing guarantees the two stay equal.

The `BASELINE_ROWS` guard matters on a fresh or newly-migrated database.
`listing_details` is a derived, disposable cache — `backfill-details --wipe`
empties it by design — and a near-empty baseline would otherwise read every
field as 0% and either divide by zero or alert on everything at once.

When any guard fails the check is **skipped and says so** in its result. It
does not silently pass, and it does not alert.

### Watch list

| Source | Fields |
|---|---|
| `NormalizedListing` → `eiendom` | `_TEXT_COLUMNS` + `_INT_COLUMNS` from `store/repositories/listings.py`, minus `Tilgjengelighet` |
| `ListingDetails` → `listing_details` | model field names, which already match the column names |

Deriving from those maps means a field added to either parser is watched
automatically.

## Surfacing

- `run_finn_ingest` returns findings in its stats dict under `drift`, plus a
  per-table `drift_status` (`checked` / `skipped:batch` / `skipped:baseline`,
  or `{"error": ...}` when the canary itself raised) so an empty findings
  list is never ambiguous between healthy, skipped, and crashed — the canary
  must not itself be able to fail silently for months.
- `run_cmd.py` exits non-zero when findings are non-empty, mirroring how guard
  2's operational alert is layered on top of the pipeline. **Both** CLI entry
  points carry the check: `run ingest` AND `run nightly` — the production
  cron runs `run nightly`, so wiring only `ingest` would mean the alert never
  fires on the path that actually executes at 01:00. (The original draft of
  this section missed that; the final whole-branch review caught it.)
- `nightly.py` itself stays untouched: its `_run_step` records each step's
  whole stats dict, and the nightly CLI command reads
  `steps["ingest_finn"]["stats"]["drift"]` out of the result.
- `notifications.default_send(title, message, priority=1)` sends the alert.
  Priority 1 rather than the daily summary's 0: rare, and actionable.

Message shape:

```
Parser drift: 2 field(s)
info_primary_area  19.2% -> 0.0%  (n=1043)
energimerke  85.4% -> 1.1%  (n=1043)
```

One line per field, unpadded — the field names vary enough in length that
column alignment would cost a max-width pass for no gain in a phone alert.

## Testing

Pure functions, so the tests are direct:

- a P-ROM-shaped collapse (19% → 0%) is reported
- a batch below `MIN_BATCH` is skipped, not passed and not alerted
- a baseline table below `BASELINE_ROWS` is skipped, not alerted
- an empty baseline table does not raise `ZeroDivisionError`
- a rare field fluctuating within noise (7% → 5%) stays quiet
- a field whose rate rises never alerts
- a field with a baseline below `MIN_BASELINE` is not watched
- presence handles `None`, `""` and `[]` uniformly
- `eiendom` and `listing_details` are judged on their own sample sizes when
  the details batch is smaller than the listings batch
- `run_finn_ingest` folds findings into its stats without altering upsert
  behaviour or the existing guard-2 path
- the baseline is read before `upsert`, so a collapse is still detected on the
  same run that caused it

The existing 12 golden fixtures are frozen and cannot exercise drift — they
parse identically forever by construction. Drift tests use synthetic batches.

## Out of scope

- DNB ingest. A separate parser against a different site, 176 active rows.
- Detecting fields that are already dead (see "What this does not catch").
- A per-run history table for gradual-drift trends. Revisit if single-night
  comparison proves too blunt.
- Any migration. This design adds no schema.
