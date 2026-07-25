# Neighbour sold prices — design

**Date:** 2026-07-25
**Status:** Approved (design), not yet implemented

## Goal

Stop throwing away sold-price cards for listings we don't track. Every
`soldpropertiescard` response carries up to 15 cards; today `collect()` keeps
only cards whose finnkode is a sweep target and discards the rest — data we
already paid a request for. Keep all of it: neighbouring sales are the best
available signal for how a neighbourhood is actually priced, and sold-price
data is the one dataset that is *expensive* to re-acquire (throttle risk,
15-card caps, cards eventually falling off the endpoint).

**Hard invariant: zero additional FINN requests.** This feature only changes
what we do with responses we already fetch. Request counts, tier ordering,
budgets, the attempts ledger, and throttle behavior are untouched.

## Decisions (from the 2026-07-25 discussion)

- **Same table, not a separate one.** A neighbour card and a tracked
  listing's card are the same entity (a tinglyst sale record keyed by
  finnkode, same endpoint, same fields); only its relationship to `eiendom`
  differs. "Tracked or not" is derivable (`EXISTS` against `eiendom`), so no
  stored flag — a flag could drift, a join can't. Every existing consumer of
  `sold_prices` reaches it by joining *from* `eiendom`, so untracked rows are
  invisible to the sold bucket, Solgt-promotion, coverage, and budpremie by
  construction. Precedent: `enrich-sold --bbox --all` already stores
  untracked cards into `sold_prices`.
- **Asking prices: store what the card gives, fetch nothing.** The card's
  `priceSuggestion` is the asking price *at sale time* — if the seller cut
  the price, we see the cut number. True original asking would cost ~15
  ad-page fetches per tile against possibly-deleted pages; rejected outright
  on politeness grounds. Consequence to label honestly: tracked listings
  store *first* asking (our crawl), neighbours store *final* asking (the
  card), so neighbour budpremie reads slightly conservative.
- **No coordinates for neighbours (v1).** Cards carry an address but no
  lat/lng. We know which tracked listing's ~120 m box surfaced each card, so
  `discovered_near_finnkode` gives the "sales around listing X" lookup with
  zero geocoding spend. Exact positions via the companion `soldproperties`
  dots endpoint would cost requests — explicitly out of scope (v2 at most,
  own decision).

## 1. Parser — `parse_sold_card` widens

Capture five more fields the endpoint already returns (verified shape, see
the finn-sold-price-endpoint reference):

| card key | record key | type |
|---|---|---|
| `size` | `size` | int (m²) |
| `propertyType` | `property_type` | str (e.g. DETACHED) |
| `bedrooms` | `bedrooms` | int |
| `collectiveDebt` | `collective_debt` | int (kr) |
| `ownershipType` | `ownership_type` | str |

All optional/nullable, absent keys → None (existing convention). Skipped as
marginal: `realtorOfficeName`, `salesCostSum`.

## 2. Migration `011_neighbour_sold.sql`

`ALTER TABLE sold_prices ADD COLUMN` × 6, all nullable:
`size INTEGER`, `property_type TEXT`, `bedrooms INTEGER`,
`collective_debt INTEGER`, `ownership_type TEXT`,
`discovered_near_finnkode TEXT` (the tracked listing whose query box
surfaced this card; NULL for rows from `--bbox` probes and for a target's
own card).

Migration comment records the same-table rationale (entity identity +
join-guarded consumers + EXISTS-not-flag).

## 3. Repository — `SoldPricesRepo.upsert`

- The five card-fact columns join `_SET` (set-as-given; they're stable facts
  of the sale, later cards may fill gaps).
- `discovered_near_finnkode` joins `_FILL_ONLY` (first discovery anchor
  wins; a card re-seen near a different target must not flip-flop).
- Fill-only semantics on `sold_price`/`cadastral_sold_date` unchanged.

## 4. Sweep — `collect()` keeps everything

In `run_sold_sweep`, `collect()` currently drops cards not in `known`.
Change: parse and store EVERY card.

- Cards whose finnkode is a known target: exactly as today (drive `matched`,
  attempts, stats).
- All other cards: stored with `discovered_near_finnkode` = the target the
  current box is centered on — except a card that IS that target (its own
  enrichment, no self-anchor).
- Stats: `matched`/`stored` keep their current meaning (targets). New
  counter `neighbours_stored` reports the extra rows, surfaced in the run
  summary line.
- `--bbox --all` single-tile mode: unchanged behavior, now also capturing
  the new fields; `discovered_near_finnkode` stays NULL (no target).
- Dedup within a run: same card seen in two boxes upserts twice (fill-only
  anchor makes the second a near-no-op) — acceptable, no extra machinery.

Explicitly unchanged: target selection, tiers, reserve slice, attempts
ceiling, budget caps, adaptive shrink, throttle/suspend.

## 5. API + UI — "Solgt i nabolaget"

- New endpoint `GET /api/listings/{finnkode}/nabolag` →
  `{"sales": [{finnkode, address, sold_price, sold_date, price_suggestion,
  size, property_type, bedrooms, price_per_m2, tracked}]}` where
  `price_per_m2` = round(sold_price / size) (NULL-safe) and `tracked` =
  EXISTS in `eiendom`. Query: `sold_prices WHERE discovered_near_finnkode =
  ?`, newest `sold_date` first, cap 15. 404 for unknown finnkode is NOT
  required (empty list is fine for any id).
- Map popup gains a lazy "Solgt i nabolaget (N)" section: fetched when the
  popup opens, shows up to 5 rows (address · sold price · price/m² · date),
  a muted "ingen registrerte nabolagssalg ennå" when empty. Tracked
  neighbours link to their own popup via the existing `#finnkode=` hash.
- Table/detail view: nothing in v1.

Expectation-setting: data accumulates from deploy day (~2–4 cards per
request at the current cadence); the section will be sparse for the first
weeks. No backfill exists — past responses were never persisted.

## 6. Testing

- Parser: new fields captured, absent → None (extend the existing `_CARD`
  fixture with the real shape).
- Repo: new columns round-trip; `discovered_near_finnkode` fill-only;
  existing fill-only behavior unregressed.
- Sweep: a box response with target + neighbour cards stores both, anchors
  the neighbour to the target, does NOT anchor the target to itself,
  `matched`/`stored` counts unchanged by neighbours, `neighbours_stored`
  correct; a neighbour card must never mark a target matched or affect
  attempts. **Request-count invariance:** identical fetch call count before/
  after on the same scenario (the zero-extra-requests invariant as a test).
- Migration + API endpoint tests per existing conventions.

## Out of scope

- Fetching original asking prices for neighbours (politeness).
- Coordinates/map dots for neighbours (requests; v2 discussion).
- Pruning/retention policy (sold data is deliberately durable).
- Sheet export changes (join-guarded; neighbours never appear).
