# Priority ordering for the tilstand classifier — design

**Date:** 2026-08-06
**Status:** approved, pending implementation plan
**Builds on:** `2026-08-05-tilstand-classifier-design.md` (shipped, migration 016).
Read that first — this changes only the *order* of the walk it defines.

## Problem

Both classifier walks iterate the corpus in raw insertion order:

- `tilstand_backfill.py:37` — the sync driver, `SELECT finnkode FROM eiendom`
- `tilstand_backfill.py:100` — `_pending_inputs`, feeding the Batch API

`--limit` is the spend control, and it cuts that walk wherever it happens to
be. So the ads a bounded run pays for are whichever ones were scraped
earliest — a property that sold in 2025 and one that is on the market today
have exactly the same claim on the budget.

The corpus is 5863 rows: **3434 sold, 1364 inactive, 1065 active**. Sold ads
are 59% of the cost and the least actionable, and there is no way to steer
spend toward the places actually worth reading.

## What changes

One ordered query replaces both unordered walks. Nothing is filtered out —
`--all` still covers all 5863 rows — and cached responses still replay free
regardless of order, so this costs nothing retroactively and does not
invalidate batch 1.

What changes is that `--limit 500` now spends on the active tier, best-fit
first, instead of on insertion order.

## Ordering

```
ORDER BY status_tier, match_band, e.finnkode
```

Status is the outer key: every active ad outranks every inactive one, which
outranks every sold one, regardless of how good the property is. Fit only
breaks ties within a tier. `e.finnkode` last makes the order total and stable
across runs.

### status_tier

Reuses the active-listing rule already canonical in `publish/rows.py:205`, so
"active" means the same thing here as in the sheets export.

| tier | definition | rows |
| --- | --- | --- |
| 0 — active | `active = 1` AND `LOWER(TRIM(COALESCE(tilgjengelighet,'')))` not in (`solgt`, `inaktiv`) | 1065 |
| 1 — inactive | everything not active and not sold | 1364 |
| 2 — sold | `LOWER(TRIM(COALESCE(tilgjengelighet,''))) = 'solgt'` | 3434 |

`active` and `tilgjengelighet` disagree on 77 rows (`Inaktiv` with `active=1`).
The rows.py rule resolves those to inactive, and this spec inherits that
resolution rather than inventing a second definition.

### match_band

Computed within each tier from two inputs:

- **Area** — `COALESCE(info_usable_area, info_primary_area, areal)`. Plain
  `areal` is NULL on 5776 of 5863 rows; `info_usable_area` (BRA) is populated
  on 5827. The coalesce is the fallback chain, not the primary source.
- **Travel** — donor-resolved `pendl_rush_brj` and `pendl_rush_mvv`, reusing
  the `_DONOR_TRAVEL_SQL` CASE from `publish/rows.py`. A listing that inherits
  a donor's travel times is ranked on the inherited values, matching what the
  web UI displays for it.

| band | condition |
| --- | --- |
| 0 — match | area ≥ 80 AND `pendl_rush_brj` ≤ 70 AND `pendl_rush_mvv` ≤ 70 |
| 1 — unknown | no known disqualifier, but some input missing or sentinel |
| 2 — miss | any known value fails its threshold |

Both commutes must qualify — a place that works for one destination and not
the other sorts below one that works for both.

Band 1 exists so missing data is not read as failure. Travel is NULL on 4-8%
of rows per tier; an ad missing a geocode is *unrated*, not *too far*, and
sorting it above known misses keeps a genuinely good listing from waiting
behind ads measured at 100 minutes.

**Travel sentinels count as unknown.** `-1` (no routes), `-2` (unrealistic)
and `-3` (API error) are negative integers stored in the travel columns —
20 rows in total. Each is numerically below 70 and would otherwise be read as
an excellent commute. `sentinels.is_travel_sentinel` already names them; the
SQL must exclude them from the `≤ 70` test rather than comparing raw values.

## Implementation

`candidate_finnkodes(conn)` on `TilstandRepo`, returning the ordered finnkode
list. Both walks in `tilstand_backfill.py` call it. The repo layer is its home
because it is a query over `eiendom` and `eiendom_processed`; the enrich
modules stay SQL-free, as they are today.

`tilstand_validate.py:94` keeps its own walk. It is the surveyor ground-truth
harness, deliberately scoped to ads carrying stated costs, and reordering it
would change what calibration measures.

## Testing

Repo-level, against a fixture DB:

- Tier ordering: an active, an inactive and a sold ad come back in that order,
  including a row where `active=1` and `tilgjengelighet='Inaktiv'` (must sort
  as inactive).
- Band ordering within one tier: match before unknown before miss.
- A sentinel row (`pendl_rush_brj = -1`) lands in band 1, not band 0.
- A donor-linked row is banded on the donor's travel times.
- The order is total and stable — equal tier and band tie-break on finnkode.

Driver-level:

- A `--limit 2` sync run classifies the two top-ranked ads, not the first two
  by insertion order.
- `_pending_inputs` collects in the same order, so the batch path's `--limit`
  cut matches the sync path's.

## Out of scope

- Any hard filter. Every ad stays eligible; this is ordering only.
- Changing what the classifier extracts, the prompt, the cache, or the schema.
- A configurable threshold. 70 minutes and 80 m² are constants in this
  revision; if they need to vary, that is a later change.
