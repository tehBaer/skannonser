# Salgsoppgave extraction — design

**Date:** 2026-07-27
**Status:** approved, pending implementation plan

## Problem

A FINN listing's *salgsoppgave* holds the facts that decide whether a property
is worth visiting: the surveyor's condition findings (tilstandsgrader), the
seller's own disclosure, whether a ferdigattest exists, what the eiendomsskatt
costs. None of it is currently in the scanner. Today you learn a flat has a
TG3 bathroom by opening the ad and reading 38,000 characters of prose.

The goal is a set of **typed, filterable columns** in the web app — sort by
TG3 count, filter out listings with no ferdigattest — not a searchable text
dump. That constraint drives most of what follows.

## Key finding: the text is already on disk

The salgsoppgave body is embedded in the ad HTML the scanner **already
caches**. Not in the visible DOM — in the page's serialised app state:

- current pages: `window.__reactRouterContext.streamController.enqueue("…")`,
  a React Router turbo-stream payload (a flat array where objects are
  `{"_<keyIdx>": <valueIdx>}` index references)
- older cached pages: `window.__remixContext = {…}`, plain JSON

Both decode to `objectData.ad.generalText` — a list of
`{heading, textUnsafe}` sections.

Measured across a 400-ad random sample of `data/eiendom/html_extracted/`:

| Metric | Result |
| --- | --- |
| Ads with salgsoppgave text | 387/400 (97%), median ~22k chars |
| Mentions tilstandsgrad (TG2/TG3) | 81% |
| TG3 specifically | 64% |
| Egenerklæring content | 83% |
| Ferdigattest / brukstillatelse | 68% |
| Radon | 32% |

The 13 misses are almost entirely `realestate-development-*` (new-build
projects, which genuinely have no salgsoppgave in this form) — not parser
failures.

**This needs zero new crawling.** All 7,731 cached files already contain it,
so the backfill runs offline, exactly like `backfill-details`.

The PDF behind "Se komplett salgsoppgave" is deliberately **out of scope**:
`prospectusView` points at 14+ different broker hosts (eiendomsmegler1,
nordvikbolig, eie, krogsveen, privatmegleren, dnbeiendom, …), each needing its
own scraper. FINN's own `documents` array carries direct finncdn PDFs but is
rare — 12 entries across 250 ads, mostly reguleringsplan attachments. What the
PDF adds over the embedded text is floor plans, the full tilstandsrapport with
photos and cost estimates, nabolagsprofil, vedtekter, and energiattest.

## Why the condition data needs a classifier

The text is broker prose, not structured data. Across 118 ads: **401 distinct
section headings**; the tilstandsrapport alone uses **160 distinct intro
phrasings** across 17 holder headings.

Counting literal `TG2` strings measures *broker writing style*, not defects.
One broker writes a single `Boligen har fått følgende TG2:` header followed by
six building parts; another writes `TG2 - Taktekking:`, `TG2 - Vinduer:` — one
literal per finding. Same defect count, 6× difference in the naive count. Only
~52% of TG-carrying ads have a cleanly parseable shape; ~46% are ambiguous.

The *labels* are a long tail — 238 distinct bullet labels across 457
occurrences, top-40 covering only 47% — but the *concepts* are bounded:
`Etasjeskille/gulv mot grunn` / `Etasjeskiller` / `Innvendig > Etasjeskille/gulv
mot grunn` are one thing; `Varmtvannsbereder`/`Varmtvannstank` one;
`Vannrør`/`Vannledning` one. Tilstandsrapporter follow **NS 3600**, which has a
standard building-part taxonomy.

So: **we define the enum; the model classifies into it.** A 17-value candidate
enum already covers **85%** of real building-part labels under crude keyword
matching alone, and a classifier handles the tail semantically where regex
cannot (`Stakeluke`→VVS, `Himling`→overflater). The regex probe also
mis-grabbed `"Kort vei til bl.a."` as a building part — the kind of error a
classifier constrained to a fixed vocabulary does not make.

## Approach: hybrid

- **Rules** for what is structurally regular: kr amounts, dates, presence
  booleans. Free, deterministic, fixture-tested.
- **A classifier** only for the tilstandsrapport and egenerklæring sections,
  where prose variance is the whole problem.

Money fields already covered by `parse_details.py` from the pricing `<dl>`
(`fellesgjeld`, `formuesverdi`, `kommunale_avg_aar`, `felleskost_mnd`) are
**not** re-extracted here — that would duplicate existing columns.
`eiendomsskatt` is the one money field genuinely missing.

## Architecture

```
data/eiendom/html_extracted/*.html      (already on disk — the durable raw store)
        │
        ├─ parse_salgsoppgave.py    decode both payload formats → typed sections
        │       │
        │       ├─ rules  ─────────────────────►  listing_salgsoppgave  (typed scalars)
        │       │
        │       └─ tilstand.py (classify) ─────►  listing_tg_findings   (row per finding)
        │                                          listing_egenerklaering (row per disclosure)
        │                    ▲
        │                    └── salgsoppgave_llm_cache  (sha256(text) → response)
```

New modules:

- `skannonser/ingest/finn/parse_salgsoppgave.py` — payload decoding (both
  formats) and the rules-extracted fields.
- `skannonser/enrich/tilstand.py` — the classification step and its cache.
- `skannonser/store/repositories/salgsoppgave.py` — persistence.
- `skannonser tools backfill-salgsoppgave` — mirrors `backfill-details`,
  including `--wipe`.

`parse_salgsoppgave.py` stays **separate from `parse_details.py`**, for the
same reason `parse_details` was kept separate from `parse.py`: different source
(embedded JSON vs DOM), different failure modes. Null-tolerant throughout —
any field that will not parse is `NULL`, and the parser never raises on
arbitrary input.

**The HTML cache is the audit trail.** Raw section text is deliberately *not*
stored in the DB: it is already durable on disk, and a prose column is exactly
the free-text field this design is meant to avoid. Re-extraction reads the
cache; no re-crawl, ever.

## Schema (migration 015)

Migration numbers 015+ are free as of 2026-07-27 (`ls migrations/` +
`git log origin/master`). Every column is `INTEGER`, `BOOLEAN`, `DATE`, or
enum-constrained `TEXT`. No prose anywhere.

```sql
CREATE TABLE listing_salgsoppgave (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    -- structured in the payload already; no parsing (98% coverage)
    byggeaar INTEGER,
    boligselgerforsikring BOOLEAN,
    -- rules-extracted
    eiendomsskatt_kr INTEGER,
    ferdigattest TEXT,          -- 'ferdigattest' | 'midlertidig' | 'ingen'
    radon_omtalt BOOLEAN,
    utleie TEXT,                -- 'tillatt' | 'ikke_tillatt' | 'egen_enhet'
    husdyr TEXT,                -- 'tillatt' | 'krever_godkjenning' | 'ikke_tillatt'
    heftelser BOOLEAN,
    -- classifier-derived rollups (denormalised so the web app can sort)
    tg2_count INTEGER,
    tg3_count INTEGER,
    tilstandsrapport_dato DATE,
    tilstandsrapport_utsteder TEXT,   -- enum + 'annet' (see below)
    egenerklaering_antall INTEGER,    -- rollup: rows in listing_egenerklaering
    parsed_at TEXT
);

CREATE TABLE listing_tg_findings (          -- mirrors listing_facilities
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg INTEGER NOT NULL,                    -- 2 | 3
    bygningsdel TEXT NOT NULL,              -- the enum below, or 'annet'
    tiltak TEXT,                            -- 'lokal_utbedring' | 'utskiftning'
                                            -- | 'videre_undersokelse'
                                            -- | 'overvaking' | 'estetisk'
    UNIQUE (finnkode, tg, bygningsdel)
);

CREATE TABLE listing_egenerklaering (       -- same shape as listing_tg_findings
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    forhold TEXT NOT NULL,                  -- enum below, or 'annet'
    UNIQUE (finnkode, forhold)
);

CREATE TABLE salgsoppgave_llm_cache (
    content_sha256 TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL
);
```

`salgsoppgave_llm_cache` is what preserves the *"derived, disposable,
rebuildable via `--wipe`"* property migration 010 established. A rebuild
replays cached responses at zero cost; only genuinely new or changed
salgsoppgave text reaches the API. The cache is keyed by content hash, not by
finnkode, so an unchanged listing re-crawled later is free.

## The bygningsdel vocabulary

18 values — the 17 below plus `annet`:

`vatrom`, `kjokken`, `tak`, `vinduer_dorer`, `yttervegg`, `etasjeskille`,
`grunn_drenering`, `vvs`, `elektrisk`, `ventilasjon`, `overflater`,
`balkong_terrasse`, `trapp`, `radon`, `vaskerom`, `utvendig_annet`, `helhet`

Measured distribution over 250 ads (404 mapped labels): vatrom 75, vvs 50,
vinduer_dorer 38, overflater 35, grunn_drenering 33, tak 33, ventilasjon 25,
helhet 23, kjokken 21, etasjeskille 17, yttervegg 14, balkong_terrasse 12,
radon 7, vaskerom 6, trapp 6, elektrisk 6, utvendig_annet 3.

**Unmappable labels bucket to `annet` and still count** toward `tg2_count` /
`tg3_count`. Never dropping a finding keeps the counts honest — the alternative
would silently understate a property's defect load by ~14%, which is the
opposite of what a red-flag feature is for. `annet` volume is also the signal
for when the enum needs a new value.

Structural boilerplate (`Vurdering av avvik`, `Tiltak`, `Konsekvens`,
`Bygningsdeler som har fått TG2`, …) is **not** a building part and must be
discarded, not bucketed — 66 of 536 observed labels were of this kind.

## The other two vocabularies

Same principle: fixed enums, declared in the JSON schema, `annet` as the
escape hatch.

`listing_egenerklaering.forhold` — conditions the seller discloses in their own
egenerklæring (83% of ads carry this section):

`vannskade`, `fuktskade`, `soppskade`, `brannskade`, `skadedyr`,
`ufaglaert_arbeid`, `manglende_dokumentasjon`, `tvist`, `palegg_offentlig`,
`annet`

One row per distinct disclosed condition, so `egenerklaering_antall` is
literally `COUNT(*)` for that finnkode — no judgement call about what counts as
an "issue". A seller disclosing nothing yields zero rows and a count of 0,
which is distinct from `NULL` (no egenerklæring section present at all).

`listing_salgsoppgave.tilstandsrapport_utsteder` — the surveyor firm:

`anticimex`, `norsk_takst`, `takstinstituttet`, `nito_takst`, `annet`

This list is drawn from what appears in the corpus and is expected to need
extension; `annet` volume is the signal, same as for `bygningsdel`.

## The classification step

- **Input:** only the TG-carrying and egenerklæring sections — measured at
  ~11.3k chars/ad, 48% of the full salgsoppgave text. Not the whole document.
- **Output:** strict JSON schema via `output_config.format`, with
  `bygningsdel` and `tiltak` declared as schema `enum`s. The API rejects
  off-vocabulary values, so typed output is enforced at the wire rather than
  trusted to prompt discipline. This is what makes "no free text" structural.
- **Model:** decided at stage 1 of the backfill (below), not up front.
- **Transport:** Batch API for backfill work (50% cheaper, results keyed by
  `custom_id` = finnkode — unordered, so never keyed by position). Single
  calls for day-to-day new listings.

### Cost

Estimated at ~26.6M input tokens for the full 7,731-ad corpus (~11.3k chars/ad
measured; chars→tokens assumed at 3.3, **not yet verified** — no `anthropic`
SDK, `ant` CLI, or API key was available in the design environment, so
`count_tokens` could not be run). Output ~3M tokens.

| Model | One-time backfill | Via Batch API (−50%) |
| --- | --- | --- |
| Opus 5 ($5/$25 per MTok) | ~$210 | ~$105 |
| Haiku 4.5 ($1/$5 per MTok) | ~$42 | ~$21 |

Ongoing cost is negligible — a few dozen new listings/day.

**Verify the token estimate with `count_tokens` before stage 2.** Nothing
downstream depends on the estimate being right, but the spend does.

## Backfill: staged

Run in stages, validating between them — the point is catching a bad prompt or
enum after ~$1 rather than after the full spend.

1. **~200 ads, both candidate models.** Compare findings against the listings
   by hand. Decide Opus 5 vs Haiku 4.5 on this evidence. Verify the token
   estimate here too.
2. **~1,000 ads.** Confirm the enum holds and watch `annet` volume.
3. **Remainder**, in batches.

7,731 requests at ~87MB fits within the Batch API's 100,000-request / 256MB
caps, so stages are a deliberate choice rather than a technical necessity.

**The backfill runs locally; the server needs no API key.** The server only
ever reads the resulting tables. New listings are classified on the next local
pipeline run — this keeps a new secret out of the deploy at the cost of daily
listings not being enriched until the local run happens.

Note for the operator: API usage bills per-token to an API account and does
**not** draw on the Claude Code subscription's 5-hour window. They are separate
pools.

## Testing

Golden fixtures, as with the existing 12 for `parse_details`:

- **Payload decoder** — fixtures for both the turbo-stream and Remix formats,
  plus a `realestate-development-*` ad (empty `generalText`) and a malformed
  payload, asserting no raise.
- **Rules fields** — fixtures covering each enum value and the NULL case.
- **Classifier** — the API call sits behind an injected seam, matching the
  existing convention (`_transport` in `skannonser/http.py`, `_sleep`/`_rand`
  in `jittered_delay`). Tests use recorded responses; `pytest` never touches
  the network.
- **Cache** — a second run with unchanged input issues no API call.

Baseline before this work: **662 passed** (CLAUDE.md documents 659; three were
added on `origin/master` since).

`ALL_MIGRATIONS` in `tests/rebuild/test_migrations.py` gains a line for 015;
expect a merge conflict there if another branch adds a migration, resolved by
keeping both lines in numeric order.

## Web app

`listing_salgsoppgave` columns join into the existing listing-details response
in `skannonser/web/api.py` alongside the migration-010 fields, and follow the
same None/`[]`-when-unparsed convention. Filter chips for `tg3_count` and
`ferdigattest` follow the existing `energimerke`/`eieform` distinct-value
pattern.

## Deploy

Committing a migration does not deploy it. The sequence is: merge → pull on the
server → `skannonser db migrate` → container restart. Because the backfill runs
locally, the enriched rows reach the server via the same path as any other
local pipeline write.

## Decisions

| Decision | Choice |
| --- | --- |
| Source | Embedded payload in cached HTML; broker PDFs out of scope |
| Approach | Hybrid — rules for regular fields, classifier for condition data |
| Output shape | Strictly typed; no free-text columns |
| Field scope | All four groups (condition, egenerklæring, legal booleans, structured extras + eiendomsskatt) |
| Unmappable labels | Bucket to `annet`, still counted |
| Model | Deferred to backfill stage 1 |
| API key | Backfill runs locally; server needs no key |
| Backfill | Staged, validating between stages |

## Risks

- **Token estimate unverified.** Could move the cost figures. Verify with
  `count_tokens` at stage 1; nothing but the spend depends on it.
- **FINN changes the payload format again.** There are already two formats in
  the cache; a third would need another decoder branch. The decoder should fail
  to `NULL` rather than raise, so this degrades rather than breaks.
- **Classifier miscounts findings.** Mitigated by staged validation. The
  `tg2_count`/`tg3_count` columns are the ones to spot-check by hand, since
  they are what the UI sorts on.
- **`annet` bucket grows.** Expected at ~14% initially. If it climbs, the enum
  needs new values and the affected rows re-classified — cheap, since
  re-classification is scoped by content hash.
