# Radon in the tilstand classifier — design

**Date:** 2026-08-06
**Status:** approved, pending implementation plan
**Builds on:** `2026-08-05-tilstand-classifier-design.md` (shipped, migration 016).
Read that first — this reuses its cache, driver, CLI and provenance conventions.

## Problem

`listing_salgsoppgave.radon_omtalt` is a bare word-search for `\bradon\b`. It
renders as *Omtalt / Ikke omtalt* and it fires on **51% of ads**, because
almost every modern salgsoppgave carries a paragraph of statutory radon
advice whether or not the property has ever been measured. A field that is
true for half the market tells you nothing about any individual listing.

The document usually says something more specific. This extracts that.

## Measurements

500 random cached ads, 2026-08-06. Percentages are of the **248 ads that
mention radon at all** (51% of 491 ads with text).

| Statement in the prose | Share of radon-mentioning ads |
| --- | --- |
| No measurement has been taken | 39% |
| A measurement *was* taken | 4% |
| Any `Bq/m³` figure present | 4% |
| A value above the action limit | 2% |
| Generic "we recommend measuring" advice only | 10% |

So the realistic yield is: a solid signal on the ~39% that state "not
measured" (≈20% of all ads), and a rare but high-value red flag on the ~2%
measured above the limit. Everything else stays NULL.

## Two traps, both measured

These are why this belongs in the classifier and not in another regex.

**1. The numbers are mostly the law, not the house.** Of the nine ads
carrying a `Bq/m³` figure, seven were `200 Bq/m³` or `100 Bq/m³` — the
statutory thresholds, quoted verbatim in advisory boilerplate:

> …øvre anbefalte grenseverdi på **200 Bq/m3** i første etasje…

Only two (`28`, `7`) were plausibly the property's own measurement. A regex
that extracts "the Bq number" reports the legal limit as the listing's radon
level — the same class of error as `changeOfOwnershipInsurance` in the
Phase-1 spec: a field that looks exactly right and is wrong.

**2. Norwegian negation, again.** A probe pattern for "has a radon barrier"
and one for "has no radon barrier" each matched **76 ads — the same 76**,
both firing on:

> bygget er heller **ikke** utført med radonsperre

This is the documented `har ikke tegnet` / `har tegnet` trap, and it caught a
purpose-written probe in five minutes. Any positive pattern must be tested
after its negative counterpart, which is exactly the discipline a
schema-constrained model applies for free.

## Section selection must widen first

The classifier only sees sections `classify_input` selects. Measured over 400
ads, of the 75 with a **substantive** radon statement (not generic advice):

| Heading regex | Substantive radon visible | Input size |
| --- | --- | --- |
| current | 59/75 (**79%**) | 3,578,531 chars |
| `+ radon\|helse.{0,3}milj` | 70/75 (**93%**) | 3,602,426 chars (**+0.7%**) |

The lost text sits under headings literally named `Radonmåling` (9 ads),
`Radon` (2), and a scattering of others. Adding two alternatives to
`_KEEP_HEADING` recovers most of it for essentially no extra input cost.

**Do this in the same change as the schema.** Widening selection alters
`classify_input` output for ads whose sections change, which changes their
content hash — so those ads re-classify naturally rather than serving a
response produced from narrower text.

## Cache invalidation — the load-bearing decision

`salgsoppgave_llm_cache` is keyed by `sha256(classify_input(html))`. **The
prompt and output schema are not in the key.** Adding radon fields therefore
means every already-cached response lacks them, and:

- if the new fields are **required**, cached responses fail validation and
  every previously-classified ad errors;
- if they are **optional**, cached responses silently yield `radon_status =
  NULL` forever — indistinguishable from "classified, document said nothing".

The second is the worse outcome: it is silent, permanent, and looks like data.

**Decision: add a `schema_version` column to `salgsoppgave_llm_cache`, and
treat a version mismatch as a cache miss.**

```sql
ALTER TABLE salgsoppgave_llm_cache ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
```

`cache_get` takes the current `_SCHEMA_VERSION` and matches on both columns;
`cache_put` stamps it. Rows on an older version stay on disk (they cost
nothing and document what was paid for) but never satisfy a lookup.

Rejected alternatives: folding the version into the hash (works, but leaves
unattributable orphan rows and gives no way to ask "how many rows are stale");
making the fields optional (the silent-staleness trap above).

**Cost of this decision, stated plainly:** bumping `_SCHEMA_VERSION`
re-classifies the whole corpus at full price. Today that is 20 ads, all
classified in-session at no cost, so the bump is free. It will not be free
later — every future schema change should be batched rather than trickled.

## Output schema

Three new fields on `TilstandResponse`, all nullable, all enum-constrained at
the wire:

```python
radon_status: str | None       # ikke_malt | malt_under_grense
                               # | malt_over_grense | malt_ukjent_verdi
radonsperre: str | None        # finnes | mangler
radon_bq: int | None           # the property's OWN measured value
```

- `radon_status` is `NULL` unless the document says something substantive.
  Generic "we recommend measuring" advice is **not** a statement about this
  property and must leave it NULL — that boilerplate is what makes
  `radon_omtalt` useless, and repeating the mistake in a new column would
  waste the whole exercise.
- `malt_ukjent_verdi` exists because the 4% that confirm a measurement often
  do not state its result.
- `radon_bq` is the trap-one field. The prompt must say: emit it **only**
  when the figure is clearly this property's measured value, never when it is
  a quoted threshold (`grenseverdi`, `tiltaksgrense`, `anbefalt`), and never
  from the `aktsomhetskart` risk-area paragraphs. Expected coverage ~2%; a
  wrong value here is worse than no value.
- `radonsperre` must be read with negation in context, per trap two.

`radon_bq` is deliberately a free integer rather than a grid value: unlike
repair costs it is a measured quantity with a real unit, and rounding it
would destroy the only thing that makes it useful (its position relative to
the 100 and 200 thresholds).

## Storage (migration 017)

Verify 017 is still free at implementation time (`git fetch && ls
migrations/`); another session works in this repo.

```sql
ALTER TABLE listing_tilstand ADD COLUMN radon_status TEXT;
ALTER TABLE listing_tilstand ADD COLUMN radonsperre TEXT;
ALTER TABLE listing_tilstand ADD COLUMN radon_bq INTEGER;
ALTER TABLE salgsoppgave_llm_cache ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
```

These are per-listing facts, so they belong on the rollup table, and
`_ROLLUP_COLS` in `TilstandRepo` grows by three.

**Do not merge these with the existing `radon` value in
`listing_tg_findings.bygningsdel`.** That row means "the surveyor assigned a
tilstandsgrad to radon"; these columns mean "this is the measurement
situation". Both can be true, neither implies the other, and two ads in the
first batch already carry the TG-findings form.

## Relationship to Phase-1 `radon_omtalt`

Keep it. It is free, already computed, and available on 100% of parsed ads —
including every ad classification has not reached. It answers a different and
much weaker question, so:

- the API keeps serving both;
- the UI **prefers** `radon_status` and falls back to `radon_omtalt` only
  when the listing is unclassified;
- the table's existing Radon column (regex, green `(s)` marker) is relabelled
  **"Radon nevnt"**, and the new LLM column takes the name **"Radon"** with
  the violet provenance colour from the 2026-08-06 colourisation work.

Once classification coverage is high, the old column becomes a candidate for
removal — not in this change.

## Testing

- **Section selection**: a fixture whose radon text sits under a `Radonmåling`
  heading must appear in `classify_input` output. Assert the current fixtures'
  selected size grows by no more than a few percent.
- **The two traps, explicitly.** These are the tests that matter:
  - an ad quoting `grenseverdi på 200 Bq/m3` in advisory boilerplate →
    `radon_bq is None`, `radon_status is None`;
  - an ad reading `ikke foretatt radonmålinger, og bygget er heller ikke
    utført med radonsperre` → `ikke_malt` **and** `mangler` (never `finnes`).
- **Cache versioning**: a cached row at version *N-1* is not returned when
  the current version is *N*; a fresh write stamps the current version; and a
  version bump does not delete the old row.
- **Enum enforcement**: an off-vocabulary `radon_status` is rejected by
  `TilstandResponse`, matching the existing vocabulary tests.
- Recorded-response fixtures as usual; `pytest` never touches the network.

## Cost

No extra API calls — radon rides along in the existing per-ad request. Input
grows **+0.7%** from the wider selection. The only real cost is the
`_SCHEMA_VERSION` bump forcing re-classification, which is zero today
(20 cached ads, none paid for) and should be batched with any other schema
change that lands before the first large paid run.

## Risks

- **`radon_bq` picks up a threshold anyway.** Mitigated by the explicit
  prompt rule and a dedicated test; the field being NULL is an acceptable
  outcome, a wrong number is not. If stage-1 validation shows any
  threshold-valued extraction, drop the field rather than tune it.
- **`radon_status` inherits `radon_omtalt`'s boilerplate problem** by
  classifying generic advice as a statement. Watch the ratio: if
  `radon_status` is non-NULL on materially more than ~40% of classified ads,
  the prompt is treating advice as fact.
- **Another session takes 017.** Re-check before writing the file.
