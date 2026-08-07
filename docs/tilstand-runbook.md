# Tilstand classifier — backfill runbook

**Read this first: in practice, classification here does NOT use the API.**
Every response in the cache to date was produced *interactively* — Claude in a
normal chat session reads the `classify_input()` text and writes the JSON
directly. That is why cache rows are labelled
`claude-opus-5 (interactive session)` (migration 017 relabelled the ones that
had wrongly inherited `cache_put`'s API-path default).

Everything below is therefore split in two: the **interactive path**, which is
the one used and costs nothing, and the **API path**, kept because the code
supports it and the numbers are useful for sizing — not because it gets run.

## Interactive classification (the path actually used)

No API key, no `.[llm]` extra, no spend. The loop is:

1. Pick the ads to classify and dump each one's `classify_input(html)` text
   (from `data/eiendom/html_extracted/<finnkode>.html`).
2. Claude reads that text and emits one JSON object per ad conforming to
   `TILSTAND_SCHEMA` — the same schema the API path constrains against, so
   `TilstandResponse.model_validate` is the gate either way. **Validate before
   writing**; a response that fails validation must be fixed, not stored.
   Hand the classifier [`classifier-brief.md`](classifier-brief.md): it carries
   the corpus calibration (bare TG lists, cost snapping, the HMS/TGIU
   exclusions, the radon edge cases) that `_SYSTEM_PROMPT` alone does not.
   Batches fan out well — ten agents at ten ads each is the shape used so far.
3. Build a cache file: a JSON list of rows with `content_sha256`,
   `response_json`, `model` (`"claude-opus-5 (interactive session)"` — label it
   honestly), `effort` (`null`), `created_at`, `schema_version`.
4. Load it — this imports AND derives findings/rollups in one step, touching
   no network:

```
skannonser tools import-tilstand-cache --in <cachefile>.json
```

`content_sha256` must be the hash of the ad's **current** `classify_input()`
output. Any change to `_KEEP_HEADING` moves that hash for the ads whose
section selection changes (the 2026-08-06 radon/HMS widening moved 50 of them),
and a row filed under a stale hash is simply never found.

### A schema bump that only ADDS fields does not mean redoing the work

When `_SCHEMA_VERSION` goes up because new keys were added, the existing
responses are not wrong — they are incomplete. The non-new content came from
the same condition text and still stands. So instead of re-classifying:

- read only the passages relevant to the NEW fields,
- merge those keys into the existing `response_json`,
- rewrite at the current hash and the current `schema_version`.

That is exactly how radon was backfilled on 2026-08-07: of 270 cached
responses, 171 had no radon text at all in their selected sections and were
mechanically `null / null / null`; only 99 needed a human-or-model read of a
short excerpt. Nothing was re-classified from scratch.

## Before a run: check the cache schema version

`_SCHEMA_VERSION` in `skannonser/enrich/tilstand.py` is part of the cache key.
Bumping it makes every cached response a MISS. It was bumped to 2 on
2026-08-06 to add radon.

```
skannonser tools classify-tilstand --db main/database/properties.db --status
```

`stale_cache_rows` is the count produced under an older schema. On the
interactive path that is a work estimate, not a bill — and per the section
above, usually a much smaller one than the count suggests. On the API path it
is money. Either way, batch schema changes rather than trickling them out.

## Radon quality gates (both paths)

Run these after any batch that produced radon values, ~10 ads is enough.

Of the ads where `radon_bq` is non-NULL, confirm none is 100 or 200 — those are
the statutory thresholds quoted in advisory boilerplate, not measurements. Per
the design spec, if any threshold-valued extraction appears, DROP the field
rather than tune it:

```
SELECT finnkode, radon_status, radon_bq FROM listing_tilstand
WHERE radon_bq IS NOT NULL;
```

Also confirm `radon_status` is non-NULL on materially less than ~40% of
classified ads. Higher than that means generic advice is being read as a
statement about the property — the exact failure that makes the old
`radon_omtalt` field useless.

Three more radon cases — a bare `Radonmåling` under TGIU, "det antas …
ferdigattest", and an HMS block contradicting the seller's egenerklæring — were
settled while classifying the 2026-08-07 batch and are written up for
classifiers in [`classifier-brief.md`](classifier-brief.md).

Three judgement calls worth knowing about, all settled on 2026-08-07:

- **`aktsomhetskart` text is not a measurement.** "Området er vurdert til
  moderat til lav radonaktsomhetsgrad" describes the area, says nothing about
  the building, and leaves all three fields NULL.
- **"Ikke aktuelt, leiligheten ligger i 3. etasje"** is recorded as
  `ikke_malt`. It is literally true and the vocabulary has no "not
  applicable" value — but it does read as a mild negative on a flat where
  radon risk is negligible. Revisit if it ever becomes noisy.
- **A measurement still running** ("måling pågår, resultat vår 2026") is
  recorded as `malt_ukjent_verdi`, not `ikke_malt`: a false "not measured" is
  the worse of the two errors.

---

## API path (supported, not used here)

Prereqs, if this is ever run:
```
./.venv/bin/pip install -e ".[llm]"
export ANTHROPIC_API_KEY=...        # or `ant auth login`
# optional but recommended: set a spend limit in the Anthropic Console
```

The dollar figures in the stages below apply to this path only.

## Stage 0 — verify the token estimate (~free)

Run `count_tokens` over ~50 classify_input() texts and compare against
the spec's ~25M-token corpus estimate. If it's >2x off, recompute the
cost table before proceeding.

## Stage 1 — ~200 ads + validation (~$3)

```
skannonser tools classify-tilstand --limit 200
skannonser tools classify-tilstand --validate --limit 50
```

Gates (design spec): >=70% of blind estimates within one band of the
surveyor's figure, no systematic direction bias (model_higher vs
model_lower roughly balanced). Also hand-check ~30 ads for alvorlighet
and finding counts against the live FINN pages.

If the estimate gate FAILS: stop. The estimat path drops to NULL
(surveyor figures only) -- that is a prompt change + re-run, not a
schema change.

## Stage 2 — ~1,000 ads (~$17)

```
skannonser tools classify-tilstand --limit 1000
```

Watch the `annet` share of bygningsdel (SELECT bygningsdel, COUNT(*) ...):
>20% means the enum needs new values before the full run.

## Stage 3 — remainder, at your own pace and batch size

```
skannonser tools classify-tilstand --batch --limit 2000   # repeat as desired
# or, to finish everything in one go (~$110 via Batch):
skannonser tools classify-tilstand --batch --all
```

The CLI requires either `--limit` or `--all`; a classification run without
one of these is refused.

Polls until the batch ends (typically <1h), then derives rows from the
cache. Safe to interrupt and re-run: paid responses are cached by
content hash.

**Operational note:** The batch poll loop waits until the batch ends
(typically <1h) and has no client-side timeout. Results are cached only
AFTER the batch ends — a Ctrl-C mid-poll abandons that batch's results
client-side even though the batch keeps running (and billing) server-side,
and a re-run submits a NEW batch for the same ads, billing them twice.
If a batch seems stuck: check it in the Anthropic Console before killing
the command; prefer waiting. Ctrl-C in SYNC mode (no --batch) is always
safe — each ad is cached as it completes.

## What `--limit` buys you

The walk is ordered, so a bounded run is a *prefix of the priority list*, not a
sample. Order is status tier first — active, then inactive, then sold — and
inside each tier: ads with at least 80 m² BRA and both rush commutes (BRJ and
MVV) at 70 minutes or under, then ads we cannot rate because travel or area is
missing, then ads we know miss.

Against the corpus as of 2026-08-06 that means the first 381 classifications go
to active listings matching both criteria, and the 3434 sold ads sort last.

Nothing is excluded — `--all` still covers every ad, and cached responses replay
free in any order. Re-running with a larger `--limit` simply extends the prefix.

## Deploy

Merge -> push -> server pull -> `skannonser db backup` ->
`skannonser db migrate` (016 DROPs columns -- the backup matters) ->
sync/replay locally-built rows via the normal pipeline-write path ->
`docker compose up -d --build` (the --build is load-bearing).

## Ongoing

A few new listings a day. On the interactive path: dump the `classify_input()`
text for ads with no cached response at the current schema version, classify
them in a chat session, and load the result with `import-tilstand-cache` as
above. On the API path it is `skannonser tools classify-tilstand --limit 100`
— incremental, since cache hits replay free and only genuinely new
salgsoppgave text is billed.
