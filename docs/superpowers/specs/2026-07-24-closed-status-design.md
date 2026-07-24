# Closed-listing status: Solgt / Inaktiv / Trukket — design

**Date:** 2026-07-24
**Status:** Approved (design), not yet implemented

## Problem

The UI labels every closed listing "Solgt", but the closed bucket
(`active = 0 AND tilgjengelighet IN ('solgt','inaktiv')`) conflates two
different things: listings FINN marked **Solgt** (a sale happened) and
listings that merely went **Inaktiv** (withdrawn/expired — *or* sold and then
de-listed without the Solgt status). Example: 450301771 is Inaktiv on FINN,
never sold, but is badged "Solgt" in our UI.

Additionally, the sold-price sweep (`enrich/sold.py`) only targets raw-status
`'solgt'` listings — an Inaktiv listing that actually sold is never checked
for a tinglyst price, and incidental card matches for non-targets are
discarded. So "inaktiv but actually sold" is undetectable today.

## Decision record (user choices, 2026-07-24)

- Sweep targets **widen to include Inaktiv**, but **Solgt targets always take
  priority** (higher chance of a tinglyst price appearing).
- Grace period **G = 180 days**: an Inaktiv listing with no price is swept
  and labelled "Inaktiv" while younger than G; at ≥ G it becomes **"Trukket"**
  and is dropped from the sweep's target set. Rationale: tinglysing lags the
  sale by ~100 days median with a long tail; 180 covers the large majority
  of real sales while resolving withdrawals within ~6 months. G lives in
  `config/domain.toml`, not code.
- **Display-only promotion**: stored `tilgjengelighet` is never mutated; the
  presence of a `sold_prices.sold_price` is what promotes an Inaktiv listing
  to "Solgt" in the UI.
- Inaktiv/Trukket get their **own muted style** distinct from Solgt.
- New **"Inaktiv/Trukket" layer toggle** alongside the others; **"Eie" is
  renamed "Finn.no"** in the Lag panel (and the "Eie" badge becomes "Finn").
- The **Tilgjengelighet filter field and table column switch to the derived
  status** so filter options, badges, and layers all speak the same words.

## 1. Derived status (the one rule)

For a closed listing (`active = 0`, raw status in {solgt, inaktiv}):

| Raw status | sold_price | Age since closed | Derived |
|---|---|---|---|
| solgt | any | any | **Solgt** (FINN said sold; price may lag or never register) |
| inaktiv | non-null | any | **Solgt** (promoted by tinglyst evidence) |
| inaktiv | null | < G days | **Inaktiv** (pending — might still register) |
| inaktiv | null | ≥ G days | **Trukket** (resolved: withdrawn, not sold) |

Age proxy: `eiendom.updated_at` — closed rows are never re-touched by the
stale-open refresh, so `updated_at` marks the transition to closed (same
proxy `select_sold_targets` already uses for `min_age_days`).

Open (active) listings keep their raw status untouched.

Computed **in the web API only** (one helper), from columns already available
per record; nothing stored. `G` comes from a new `[sold]` section in
`config/domain.toml`: `trukket_grace_days = 180` (loaded in
`skannonser/config/domain.py`; used by the API and passed to the sweep by
the CLI).

## 2. Sweep changes (`skannonser/enrich/sold.py`)

- `select_sold_targets` widens its WHERE to:
  `raw = 'solgt'` **OR** (`raw = 'inaktiv'` AND `updated_at >=
  datetime('now', '-<G> days')`) — Trukket-aged inaktiv rows are excluded
  (stop looking). Each returned target gains a `status` field
  (`"solgt"`/`"inaktiv"`). The existing `min_age_days` parameter keeps its
  current meaning and applies to both tiers.
- `run_sold_sweep` ordering becomes **two-tier**: all `solgt` targets before
  any `inaktiv` target; within each tier the existing
  fewest-attempts-then-density ordering is unchanged. With a tight
  `--requests` budget this means inaktiv targets are only attempted with
  leftover budget, exactly as requested.
- `collect()` needs no change beyond the wider target set: inaktiv targets
  are now in `known`, so their cards store; incidental non-target cards stay
  discarded (unchanged policy).
- `sold_coverage`/`--status` output gains a second line for the inaktiv tier
  (pending count + how many have been priced) so sweep progress on the new
  tier is observable. The existing solgt coverage numbers keep their meaning.
- Attempt ledger (migration 009) applies to both tiers as-is.

## 3. Web API changes (`skannonser/web/api.py`, `publish/rows.py`)

- `_EIE_SELECT_TAIL` gains `e.updated_at AS "UPDATED_AT"` (shared fragments →
  available on every eie-shaped record; sheet export is header-driven and
  unaffected — the existing byte-identical guard test proves it).
- The closed-bucket query already joins `sold_prices`; the detail query too.
  A new pure helper `_derived_status(rec, grace_days) -> str | None` applies
  §1's table (None for open listings).
- Item shape:
  - `sold: bool` — now true only for derived **Solgt** (raw solgt, or
    promoted inaktiv).
  - `closed: bool` *(new)* — true for every closed listing (all three
    derived states). `sold=true` implies `closed=true`.
  - `tilgjengelighet` — for closed items this now carries the **derived
    label** ("Solgt"/"Inaktiv"/"Trukket"); open items keep the raw value.
    The raw value stays available on the detail endpoint via the existing
    raw-column spread (`Tilgjengelighet` key).
  - Sold-outcome keys (`sold_price`/`sold_date`/`price_suggestion`) ship for
    the whole closed bucket as today (needed for promotion + budpremie).
- `_sold_from_hidden` and the `bucket=sold` semantics are unchanged at the
  SQL level (the bucket still returns ALL closed listings — the split into
  Solgt vs Inaktiv/Trukket is per-item, client-side, so both layers share
  the one lazy fetch).

## 4. Frontend changes (`skannonser/web/static/`)

- **Layer toggles (Lag):** "Eie" → **"Finn.no"** (label only; internal
  `source: "eie"`, ui key `eie`, storage unchanged). New checkbox
  **"Inaktiv/Trukket"** (`id="toggle-inactive"`, ui key `inactive`, default
  false, persisted like `sold`). "Solgt" now shows only derived-Solgt items.
  Either closed toggle triggers the same `ensureSoldLoaded()` lazy fetch.
  `bucketOf(item)`: `sold` → sold layer; `closed && !sold` → inactive layer;
  else source layer. Deep links (`handleHash`) flip whichever closed toggle
  the target item needs.
- **Muted style:** closed-not-sold dots render grey (single neutral colour,
  not boligtype-coloured) via a feature property (`inactive: true`) and a
  case in the sold-variant layers' paint expressions (`map.js`). In
  budpremie mode they naturally fall into the existing "Ingen tinglyst pris"
  grey. "Solgt nedtoning" continues to control the whole closed set's
  opacity.
- **Badges:** popup source-tag and table badge show the derived label —
  "Solgt" (existing sold styling), "Inaktiv"/"Trukket" (new muted badge
  class), "Finn" (renamed from "Eie") / "DNB" for open listings. The popup's
  sold-price block (Solgt for / budpremie / "ingen tinglyst pris ennå")
  renders only for derived-Solgt items; Inaktiv/Trukket show no price block.
- **Tilgjengelighet filter + table column:** no code change needed beyond
  the API — `deriveVocabs` and the table column read `item.tilgjengelighet`,
  which now carries the derived label, so the filter options become
  Solgt/Inaktiv/Trukket (+ open statuses) automatically. Stored
  `tilgjengelighetHidden` keys keep matching (labels unchanged for open
  listings; "Solgt"/"Inaktiv" keys still exist with sharper meaning).
- **Table "Vis solgte" toggle** now shows the whole closed set (all three
  derived states — distinguishable via the Tilgjengelighet column/filter);
  label becomes "Vis solgte/inaktive".

## 5. Out of scope / unchanged

- Sheet export (Eie/Sold tabs): raw values, untouched.
- Notifications, ingest/refresh, DNB flow: untouched.
- No stored-status mutation, no new tables/migrations (reuses
  `sold_prices` + `sold_price_attempts`).
- The sweep's dormant/manual activation posture and throttle discipline:
  unchanged.

## 6. Testing

- Pure-helper tests for `_derived_status` (all four table rows + boundary at
  exactly G days + open listings → None).
- Sweep tests: widened target selection (inaktiv in-grace included,
  aged-out excluded), two-tier ordering under a capped budget (solgt
  exhausts budget first), inaktiv card storage end-to-end.
- API tests: closed items carry `closed`, derived `tilgjengelighet`, and
  correct `sold`; promoted-inaktiv (price present) reports Solgt; detail
  endpoint still exposes raw `Tilgjengelighet`.
- Existing suite (616) green; sheet byte-identical guard proves export
  untouched.
- Browser verification: four layer toggles incl. rename, muted styling,
  badges, filter vocabulary, deep links to both closed kinds.
