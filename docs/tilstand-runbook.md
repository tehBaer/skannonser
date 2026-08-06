# Tilstand classifier — backfill runbook

Prereqs (local machine only; the server never needs any of this):
```
./.venv/bin/pip install -e ".[llm]"
export ANTHROPIC_API_KEY=...        # or `ant auth login`
# optional but recommended: set a spend limit in the Anthropic Console
```

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

## Deploy

Merge -> push -> server pull -> `skannonser db backup` ->
`skannonser db migrate` (016 DROPs columns -- the backup matters) ->
sync/replay locally-built rows via the normal pipeline-write path ->
`docker compose up -d --build` (the --build is load-bearing).

## Ongoing

New listings are classified by re-running
`skannonser tools classify-tilstand --limit 100` locally (incremental:
cache hits are free, only genuinely new salgsoppgave text is billed --
a few ads/day, so any reasonable --limit covers it).
