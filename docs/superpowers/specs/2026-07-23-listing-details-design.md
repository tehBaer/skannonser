# Listing details enrichment — design

**Date:** 2026-07-23
**Status:** Approved (design), not yet implemented

## Goal

Extract ~21 additional fields from FINN ad HTML we already have on disk and make
them filterable in the web app: rooms/eieform/fasiliteter/nabolag (group A),
the full money picture — totalpris, fellesgjeld, felleskost etc. (group B), and
energimerking + matrikkel/borettslag (group C). Group D (visning, megler,
beskrivelse) was considered and explicitly deferred — visning data is
time-sensitive and would need a freshness story the others don't.

The headline motivation is group B: today a 2 990 000 kr andel with
1 945 000 kr fellesgjeld and 13 813 kr/mnd felleskost sorts *above* a
5 190 000 kr selveier in the price filter. Totalpris and felleskost fix that
and enable derived columns (pris/kvm på totalpris, månedskost).

**No new crawling.** Every field parses from ad HTML already cached under
`data/eiendom/html_extracted/` (7,731 files; ~5,863 with a matching `eiendom`
row). Backfill is a local re-parse; ongoing capture rides the existing
ingest/refresh fetches. Zero extra FINN traffic, zero API spend.

**Surface:** web table + API only. Sheet columns are deferred (the Eie header
is a legacy-fidelity contract; widening it is a separate, later decision).

## Approach (decision record)

Three options considered:

1. **Widen `eiendom`** with ~17 more columns — rejected: forces changes
   through the frozen `NormalizedListing` legacy contract
   (`skannonser/ingest/base.py`, `extra="forbid"`, AST-pinned by test), and
   mixes crawl facts with lifecycle state in an already-wide table.
2. **Side tables `listing_details` (1:1) + `listing_facilities` (1:N)** —
   **chosen.** New parse path sits beside `parse_ad` without touching the
   frozen contract; the tables are 100 % re-derivable from cached HTML, so a
   FINN markup change or a new field is a wipe-and-rebuild, not a migration
   problem; facilities gets a real relational filter instead of
   `LIKE '%Heis%'`.
3. **EAV `listing_attributes`** — rejected: untyped, needs pivoting to
   filter/sort, makes the row query unpleasant.

Two standing principles folded in:

- **Derived fields are computed at query time, never stored**
  (`pris_kvm_totalpris`, `maanedskost`) — stored copies go stale silently.
- **The details tables are a disposable cache.** The recovery path for any
  parser change is `backfill-details --wipe`, never a data migration.

## 1. Parser — `skannonser/ingest/finn/parse_details.py`

New module beside `parse.py`. Signature: `parse_details(html: str,
finnkode: str) -> ListingDetails`. `ListingDetails` is a new, ordinary
pydantic model (snake_case fields) — deliberately NOT part of
`NormalizedListing`, whose field list is a frozen legacy contract.

Four extraction sources, each independent and null-tolerant (verified against
the 12 golden fixtures in `tests/rebuild/fixtures/finn/`):

| Source | Fields | Notes |
|---|---|---|
| GAM JSON: `<script id="advertising-initial-state">` → `config.adServer.gam.targeting` (list of `{key, value:[...]}`) | `bedrooms`, `rooms`, `floor` | Typed ints; coverage in fixtures 12/12, 10/12, 9/12 |
| Key-info `<dl>`: `data-testid="info-ownership-type"` `<dd>` | `eieform` | Norwegian display value ("Andel", "Eier (selveier)"). Fallback: map GAM `ownership_type` enum — FREEHOLD→"Eier (selveier)", PART_OWNERSHIP→"Andel", STOCK→"Aksje"; unknown enum → store raw enum string |
| `pricing-details` `<dl>` dt/dd pairs | `totalpris`, `omkostninger`, `fellesgjeld`, `felleskost_mnd`, `fellesformue`, `formuesverdi`, `kommunale_avg_aar` | dt labels: Totalpris, Omkostninger, Fellesgjeld, Felleskost/mnd., Fellesformue, Formuesverdi, Kommunale avg. Value parse: strip `\xa0`/spaces from "1 945 000 kr" → int; "15 088 kr per år" handled for kommunale avg |
| Dedicated testids | `nabolag` (`local-area-name` text); `energimerke` + `energifarge` (`energy-label` text "A - Mørkegrønn" split on " - "; a bare "Energimerking" heading with no value → both NULL); `facilities` (list of strings from `object-facilities` grid `<div>`s); matrikkel from `cadastre-info` label:value divs → `kommunenr`, `gardsnr`, `bruksnr`, `seksjonsnr`, `borettslag_navn`, `borettslag_orgnr`, `borettslag_andelsnr` | Facilities is a bounded controlled vocabulary (26 distinct values across 12 fixtures); matrikkel numbers stored as TEXT (identity keys, not quantities) |

Error handling: every field optional; a per-field parse failure yields NULL,
never an exception. Old cached HTML predating the current markup produces
sparse rows — acceptable by design. `parse_details` itself never raises on
arbitrary HTML input (worst case: an all-NULL row).

## 2. Storage — migration `010_listing_details.sql`

```sql
CREATE TABLE listing_details (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    bedrooms INTEGER, rooms INTEGER, floor INTEGER,
    eieform TEXT, nabolag TEXT,
    totalpris INTEGER, omkostninger INTEGER, fellesgjeld INTEGER,
    felleskost_mnd INTEGER, fellesformue INTEGER, formuesverdi INTEGER,
    kommunale_avg_aar INTEGER,
    energimerke TEXT, energifarge TEXT,
    kommunenr TEXT, gardsnr TEXT, bruksnr TEXT, seksjonsnr TEXT,
    borettslag_navn TEXT, borettslag_orgnr TEXT, borettslag_andelsnr TEXT,
    parsed_at TEXT
);
CREATE TABLE listing_facilities (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    facility TEXT NOT NULL,
    UNIQUE (finnkode, facility)
);
```

New repository `skannonser/store/repositories/details.py`:

- `upsert_details(rows)` — full-row `INSERT OR REPLACE`, batched. No
  partial-update ceremony: the table is a derived cache, the parser's output
  IS the row.
- `replace_facilities(finnkode, facilities)` — delete + insert.
- `coverage()` — counts for `--status` / logging (rows, non-NULL per key
  field).

## 3. Ingest integration

- **`pipeline.py` FINN parse step:** after a successful `parse_ad`, call
  `parse_details` on the same HTML and upsert details + facilities. A details
  failure is logged and never fails the listing upsert.
- **`refresh.py`:** it force-re-downloads ad HTML already; re-run
  `parse_details` on the fresh HTML there too — felleskost/totalpris changes
  get picked up for free.
- **DNB:** out of scope. DNB JSON-LD doesn't carry these fields; DNB rows get
  NULLs via LEFT JOIN, consistent with other eiendom-only columns.
- `nightly.py` sequence unchanged — details capture rides inside existing
  steps, no new step.

## 4. Backfill — `skannonser tools backfill-details`

- Iterates finnkodes in `eiendom`, reads
  `data/eiendom/html_extracted/{finnkode}.html` where present, parses,
  upserts. Purely local, zero network, idempotent, re-runnable.
- `--wipe`: delete both tables' contents first, rebuild from scratch (the
  recovery path for parser changes).
- `--status`: print coverage without parsing.
- Missing cache file → skip silently, count reported at the end.

## 5. Web API (`skannonser/web/api.py`, `publish/rows.py`)

- `_EIE_JOINS` gains `LEFT JOIN listing_details ld ON ld.finnkode =
  e.finnkode`; the SELECT gains the detail columns. Both the Eie and sold
  bucket queries share the fragments already, so sold rows get details too
  (kept from when the listing was active).
- `/api/listings` items gain flat keys: `soverom`, `rom`, `etasje`,
  `eieform`, `nabolag`, `energimerke`, `energifarge`, `totalpris`,
  `omkostninger`, `fellesgjeld`, `felleskost_mnd`, `fellesformue`,
  `formuesverdi`, `kommunale_avg_aar`, `facilities` (list of strings).
- Facilities: one `SELECT finnkode, facility FROM listing_facilities`,
  grouped in Python — no GROUP_CONCAT delimiter games.
- **Derived, computed in the API layer:** `pris_kvm_totalpris` =
  round(totalpris ÷ BRA-i), `maanedskost` = felleskost_mnd +
  round(kommunale_avg_aar ÷ 12) (kommunale-avg term contributes 0 when NULL;
  whole value NULL when felleskost_mnd is NULL; pris_kvm_totalpris NULL when
  either input NULL/zero).
- Matrikkel/borettslag fields appear only on `/api/listings/{finnkode}`
  (detail endpoint) — identity data, not filter data; keeps the list payload
  lean.
- `/api/meta` gains the observed vocabularies: distinct facilities (with
  counts), energimerke letters, eieform values — the filter UI builds itself
  from data, no hardcoded lists.

## 6. Web UI (`skannonser/web/static/`)

- **Filter panel (`filters.js`):** min soverom, max totalpris, max
  felleskost/mnd, eieform select, energimerke multi-select, facilities
  checkbox group (populated from `/api/meta`, sorted by frequency).
- **Table (`table.js`):** new sortable columns — soverom, etasje, eieform,
  energimerke, totalpris, felleskost/mnd, pris/kvm (totalpris), månedskost —
  using the table's existing column mechanics.
- **Popup (`popup.js`):** totalpris, felleskost/mnd, energimerke added to the
  existing rows; the detail view shows matrikkel/borettslag.
- Filters treat NULL as "unknown": a listing with NULL felleskost passes a
  max-felleskost filter only if the user opts in (an "inkluder ukjent"
  toggle, default on) — silently hiding sparse older rows would be worse than
  showing them.

## 7. Testing

- **Golden fixtures:** `*.details.expected.json` beside the 12 existing
  `*.expected.json` pairs in `tests/rebuild/fixtures/finn/`; one test
  parametrized over all 12, same pattern as the `parse_ad` golden test.
- **Unit tests (parser):** money parsing (`\xa0` separators, "per år"
  suffix), energy split incl. the empty-"Energimerking" case, eieform
  fallback mapping, missing sections → all-NULL, arbitrary/garbage HTML →
  all-NULL without raising.
- **Repo tests:** upsert/replace/coverage round-trips; migration applies
  cleanly on a copy of the live schema.
- **API tests:** new fields present, derived values correct incl. NULL
  propagation, meta vocabularies, sold-bucket rows carry details.
- **Backfill test:** tmp dir with a fixture HTML file + tmp DB; `--wipe` and
  missing-file skip behavior.
- **Contract guard:** the existing `NormalizedListing` AST-pinning test stays
  green untouched — proof the legacy contract wasn't disturbed.

## Out of scope

- Sheet (Eie/Sold tab) columns — deferred, revisit after the web table lands.
- Group D: visning, megler, tittel/beskrivelse.
- DNB details extraction.
- Any new FINN fetching; backfill is cache-only.
