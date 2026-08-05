# Salgsoppgave extraction — handoff

**Written 2026-07-29.** Phase 1 is built, merged and deployed. Phase 2 (the
condition-report classifier) is not started. This is what a new session needs
to build on it without rediscovering any of it.

Read this first, then the design spec
(`docs/superpowers/specs/2026-07-27-salgsoppgave-extraction-design.md`) and the
Phase 1 plan (`docs/superpowers/plans/2026-07-27-salgsoppgave-extraction.md`).

---

## 1. State right now

| | |
| --- | --- |
| master / server | `349e863`, in sync |
| Tests | 799 Python, 157 JS |
| Live rows | 6,217 listings, all with a `listing_salgsoppgave` row; 4,028 have a `ferdigattest` value |
| Phase 2 tables | `listing_tg_findings` 0, `listing_egenerklaering` 0, `salgsoppgave_llm_cache` 0 — empty by design |
| Migration numbers | **015 is taken. Use 016+.** |

Shipped and working end to end: extraction from cached HTML, storage, offline
backfill + CLI, ingest/refresh wiring, API exposure, popup and table display,
and filter chips for the three enums.

## 2. The one insight the whole feature rests on

**The salgsoppgave text is already in the ad HTML the scanner caches.** Not in
the visible DOM — in the page's serialised app state. No crawling is needed to
get it, and the broker PDFs behind "Se komplett salgsoppgave" are a trap
(`prospectusView` points at 14+ different broker hosts, each needing its own
scraper).

Two payload formats live side by side in `data/eiendom/html_extracted/`:

- current pages: `window.__reactRouterContext.streamController.enqueue("…")` —
  a React Router turbo-stream. A **flat array** where objects are
  `{"_<keyIdx>": <valueIdx>}` and *both sides are indices into that same
  array*, so it needs a resolver pass, not a plain `json.loads`. Root is
  index 0.
- older pages: `window.__remixContext = {…}` — ordinary nested JSON, payload
  one level deeper under `state`.

Both land at `objectData.ad`. `skannonser/ingest/finn/payload.py` handles both;
`decode_ad(html) -> dict | None` and `sections(ad) -> list[Section]` are the
entry points, and **both fail soft** — they return `None`/`[]` rather than
raising, on any input. Four later stages depend on that.

Measured: **~97% of cached ads carry the text**, median ~22k chars. The misses
are almost all `realestate-development-*` (new-build projects genuinely have no
salgsoppgave), not parser failures.

## 3. Phase 2 — what is actually left

The unbuilt half is the **tilstandsrapport classifier**: TG2/TG3 condition
findings and the seller's egenerklæring. That was the original motivation —
"which flats have a TG3 bathroom" — and it is still not answerable.

The tables and columns already exist (migration 015) and are empty:
`listing_tg_findings`, `listing_egenerklaering`, and five columns on
`listing_salgsoppgave` (`tg2_count`, `tg3_count`, `tilstandsrapport_dato`,
`tilstandsrapport_utsteder`, `egenerklaering_antall`).

### Why it needs a classifier, not regex

This was measured, not assumed. Across 118 ads: **401 distinct section
headings**, and the tilstandsrapport alone uses **160 distinct intro
phrasings** across 17 holder headings. Worse, counting literal `TG2` strings
measures *broker writing style*, not defects — one broker writes a single
`Boligen har fått følgende TG2:` header followed by six building parts, another
writes `TG2 - Taktekking:`, `TG2 - Vinduer:`. Same defect count, 6× difference.
Only ~52% of TG-carrying ads have a cleanly parseable shape.

The *labels* are a long tail (238 distinct bullet labels across 457
occurrences) but the *concepts* are bounded — tilstandsrapporter follow
**NS 3600**. So: **define the enum yourself and have the model classify into
it.** A 17-value candidate enum already covers **85%** of real building-part
labels under crude keyword matching alone; a classifier handles the tail
semantically (`Stakeluke`→VVS, `Himling`→overflater).

The vocabularies, the cost estimate (~26.6M input tokens; ~$21 Haiku / ~$105
Opus 5 via Batch API, **unverified — run `count_tokens` first**), and the
staged-backfill plan are all in the spec. Constrain the output with a strict
JSON schema so the enum is enforced at the wire, not by prompt discipline.

### Two traps waiting for you

Both are documented in the plan, both found by review, both will bite silently:

1. **`SalgsoppgaveRepo.upsert` uses `INSERT OR REPLACE`**, which deletes and
   reinserts the whole row. The five Phase-2 columns are not in `_SCALAR_COLS`,
   so a routine Phase-1 re-parse **nulls out your classifier results**. Write
   those columns in the same statement, or guarantee ordering.
2. **`SalgsoppgaveRepo.wipe()` deletes `listing_tg_findings` and
   `listing_egenerklaering`** — and `--wipe` is documented in the CLI as the
   thing to run "after a parser change", i.e. routine Phase-1 maintenance. The
   ordinary Phase-1 workflow destroys Phase-2 output. `salgsoppgave_llm_cache`
   survives, so it is replayable — but only by re-running Phase 2.

That cache exists precisely so a `--wipe` rebuild replays classifier results
for free instead of re-billing. Key it by content hash, not finnkode.

## 4. Hard-won lessons that cost real time

### Prose semantics

- **Norwegian negation is a trap.** `har ikke tegnet` *contains* `har tegnet`.
  Any positive pattern tested before its negative counterpart inverts the
  answer. This shipped three separate inversions before review caught them.
- **Section headings are not assertions.** `_flat_text` joins heading + body,
  and the standard heading "Midlertidig brukstillatelse og ferdigattest" names
  both documents regardless of which the property has. That misclassified
  **90 of 132** values — 30% of all ads on the highest-coverage field. Classify
  from section *bodies*; see `_ferdigattest_scope`.
- **Mentions are not facts.** `heftelser` and `radon_omtalt` are bare
  word-searches, so they mean "the document discusses this", not "the property
  has it". They render as `Omtalt`/`Ikke omtalt`, never Ja/Nei. Any new
  mention-detector must do the same.
- **`null` ≠ `False`.** `null` means no salgsoppgave text existed; `False`
  means it was read and the topic was absent. Collapsing them loses real
  information and makes filters lie.
- **Coverage %s in the spec are *mention* rates, not extraction rates.** A
  mention is often not a rule — `husdyr` mentions include a neighbouring farm's
  animals. Only a field at or near **0%** indicates a dead regex.

### A payload field that lies

**Do not use `ad.changeOfOwnershipInsurance` for boligselgerforsikring.** It
looks exactly right and is present on ~98% of ads — but that is field
*presence*, not correctness. It reads `False` on 103 ads whose prose says the
seller *has* taken it out, and `False` on the 22 saying they have not. Only 5
`True` in 300. Generalise: **a payload field's presence rate says nothing about
whether it means what its name suggests** — cross-check against the prose.

Conversely `ad.constructionYear` is sound but **redundant** —
`eiendom.info_construction_year` already covers 99% of rows and the API already
exposes `byggeaar`.

## 5. Environment traps in this repo

These are not theoretical; every one of them cost time in the last session.

- **`pytest` in a worktree tests the MAIN CLONE.** `tests/rebuild/` is a
  package but `tests/` is not, so pytest puts `tests/` on `sys.path` and never
  the repo root; the symlinked venv's editable finder then serves `skannonser`
  from `/Users/tehbaer/kode/skannonser`. **Always
  `PYTHONPATH=. ./.venv/bin/pytest`.** CLAUDE.md claims the opposite and is
  wrong. A green bare-`pytest` run in a worktree proves nothing.
- **Subagents start in the main clone**, not your worktree. One committed
  migration 015 straight to `master`. Every dispatch needs an explicit `cd` +
  `git branch --show-current` check, and **absolute paths for every file
  operation** — a relative path reads the main clone's files, including its
  own stale `.superpowers/sdd/task-N-brief.md`.
- **`preview_start` reads the main clone's `.claude/launch.json`** and ignores
  the worktree's, so it boots another session's database. Browser verification
  of worktree code is effectively unavailable; curl the served file instead.
- **Static files are served at `/`, not `/static/`.** Curling
  `/static/foo.js` returns nothing and looks exactly like a failed deploy.
- **Never `git stash`** — the stack is shared across worktrees and the main
  clone, and holds someone else's entry.
- **JS tests:** `node --test tests/web/*.test.mjs` (the bare directory form
  fails). No package.json, no runner config.

## 6. Deploying

Merging does not deploy. The sequence, from `README.md`:

```
git push
ssh mbp2016@100.77.139.22 'cd ~/kode/skannonser && git pull --ff-only'
# if a migration landed:
.venv/bin/skannonser db backup
.venv/bin/skannonser db migrate
.venv/bin/skannonser tools backfill-salgsoppgave
docker compose up -d --build
```

**The `--build` is load-bearing.** Application code and `web/static/` are baked
into the image, not bind-mounted — a plain `restart` deploys nothing. Verify by
curling a served file for a string you just added, at `/` not `/static/`.

Another session works in this repo concurrently. Check `git log origin/master`
before merging, and never assume master is where you left it.

## 7. Not done, deliberately

- **Filters for the three booleans and the eiendomsskatt range.** `Heftelser`,
  `Radon` and `Selgerforsikring` are genuinely three-state (Omtalt / Ikke
  omtalt / no prospectus) and this codebase has no tri-state control to copy —
  it would mean inventing UI rather than following a pattern.
- **Nobody has looked at the rendered UI.** Everything was verified by tests
  plus confirming the right bytes are served (see the `preview_start` trap
  above). If something looks wrong on screen, that is the gap — not a mystery.
- **Broker PDFs.** Out of scope on purpose; what they add over the embedded
  text is floor plans, the tilstandsrapport photos and cost estimates,
  nabolagsprofil, vedtekter and energiattest.
