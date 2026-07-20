# Rebuild status & carried obligations

**Last updated: 2026-07-20 (end of Phase 2).** This is the single pick-up point for continuing
the rebuild in a fresh session. Read this, the spec, and the current phase plan — everything
else is history.

- Spec: `docs/superpowers/specs/2026-07-20-skannonser-rebuild-design.md`
- Feature inventory: `docs/rebuild/2026-07-20-feature-inventory.md`
- Phase plans: `docs/superpowers/plans/2026-07-20-rebuild-phase-1-skeleton.md`, `…-phase-2-ingest.md`
- Session ledger (gitignored, laptop only, may be stale): `.superpowers/sdd/progress.md`

## Where we are

- **Phase 1 (skeleton) and Phase 2 (ingest port) are merged to master and deployed on the server**
  (`mbp2016@100.77.139.22`, repo `~/kode/skannonser`, tailnet). 119 rebuild tests green on both machines.
- **Legacy (`main/`, Makefile, cron wrapper) is still the production path.** Nothing cuts over
  until Phase 4. Legacy is frozen except for the sanctioned ops patch listed below.
- Proven equivalence: `skannonser verify parse` ran the full 7 731-ad HTML cache with **zero
  unexplained diffs** (harness diff-detection proven by negative control); a supervised
  parallel run on the server (real finn.no crawl vs legacy's own run) classified all 1 361
  diffs with **zero unexplained**.
- The new CLI (both machines): `skannonser config show | db backup/migrate/stats | run ingest/refresh | verify parse`.
- Migrations applied everywhere: 001 (adopt live schema), 002 (notify tables). Migration runner
  is atomic (per-file transaction + rollback). Nightly Docker backup on the server keeps 30.

## Sanctioned behavior changes vs legacy (the complete list — nothing else may diverge)

1. Finnkode parsed via `urllib.parse` (legacy: `split('finnkode=')[1]`).
2. Ad-link matching by explicit pattern (legacy: `len(href) <= 100` heuristic).
3. **Search-style matching keeps absolute-href listings legacy silently drops** (discovered on a
   real result page — the current production crawler loses real listings; regression-locked).
4. `max_pages=50` safety cap on crawl pagination (legacy unbounded; observed depth ~20).
5. One transaction per upsert batch; update-only-changed-columns (consequence: **`eiendom.updated_at`
   no longer bumps on unchanged rows — do not read it as "last seen"**, Phase 3/4).
6. DNB listing fetches send UA/timeout on every network fetch (legacy: only on its fallback path).
7. Legacy ops patch (only frozen-code edit): `SHEETS_AUTO_CONFIRM=1` bypass in
   `main/sync/update_rows_in_sheet.py` — API-derived changes auto-accept, non-API auto-skip.

Preserved legacy quirks (deliberate, test-pinned — change only with an explicit ruling):
- **Activate-on-second-appearance**: new listings stay `active=0` until their 2nd crawl (delays
  export/notify by one cycle). Twice attempted as a "fix", twice reverted. → post-cutover candidate.
- DNB matcher ignores `active`; `deactivate_missing([])` deactivates everything (pipeline guards it).

## Phase 3 named deliverables (enrichment port + Google API gateway)

The port scope is `main/post_process.py` + geocoding + donor system behind the new gateway, PLUS
these obligations discovered in Phase 2 — **forgetting any of these silently freezes data**:

1. `pris_kvm` computation and write path (legacy computes in post_process; new repo never writes it).
2. `image_hosted_url` write path (same class).
3. **`eiendom_processed` writes** (adresse_cleaned, google_maps_url, travel columns — new pipeline
   never touches the table; legacy upserts it on every ingest).
4. **`.str.title()` on Adresse** (post_process.py:242) — live `eiendom.adresse` is title-cased and
   feeds the sheets-facing `adresse_cleaned`; the new ingest writes raw case (~211 rows differ).
   Phase 3 must reproduce the transform or the user must consciously drop it.
5. Migration-runner note: add a trigger-block regression test for `_statements()` before any
   trigger-bearing migration.

## Before Phase 4 cutover (publish port)

- **Supervised DNB parallel run** — the DNB network path has never contacted dnbeiendom.no
  (Phase 2's checkpoint was finn-only).
- Sheet-export consumers must handle: `''`-vs-NULL text columns and postnummer leading zeros
  (legacy's CSV round-trip stripped `0581`→`581`; new pipeline preserves `0581`).
- `run ingest`'s archive dir is `data/eiendom/html_crawled_rebuild` (separated from legacy's);
  merge the two at cutover.
- Daily/weekly notify "added" metrics inherit the activate-on-2nd-appearance timing.

## Post-cutover candidates (user decisions, not scheduled)

- Fix the activate-on-second-appearance delay (new listings would export/notify same-day).
- Untrack `main/database/properties.db` from git (currently tracked+perpetually dirty; the
  stash-dance in every server pull exists because of this) — vs keeping git as a sync channel.
- Deferred minors: `deactivate_missing` empty-list guard in repos; AliasChoices for
  `SKANNONSER_DB_PATH`; anchor `DEFAULT_DOMAIN_PATH`; `require_db()` helper on a 4th db command;
  supercronic checksum in Dockerfile; backup `PRAGMA journal_mode=DELETE`; USER/HEALTHCHECK in
  Docker (with Phase 5 web service); crawl archives `response.text` vs legacy's `content`
  round-trip; progress logging in long crawls.

## Operational state (server)

- **Legacy nightly was failing every night for 8+ days** (diagnosed 2026-07-20): alternating
  nights died on a midnight DNS outage; other nights stored to DB but died at the interactive
  sheet-update prompt (EOF under cron), so travel/coords enrichment + part of sheet sync were
  stale since ~Jul 12. Remedies applied 2026-07-20: cron moved 00:00→**01:00** (DNS dodge) and
  `SHEETS_AUTO_CONFIRM=1` exported in `~/run_skannonser_daily.sh`.
  **VERIFY: the first ~01:00 log after Jul 20 in `~/skannonser-logs/` should end `full=0`.**
- Google Maps API key lives in `.env` on both machines (0600, gitignored); `main/config/config.py`
  is env-first with a `.env` fallback; the wrapper sources `.env`. Key was NOT rotated (never in
  git history — verified). Optional: confirm API restrictions in Cloud Console.
- Docker scheduler container runs nightly `skannonser db backup --keep 30` at 03:00 UTC.
- The server's `properties.db` is the authoritative live DB (laptop copy goes stale). It is
  git-tracked: every server pull needs the stash-dance (`git stash push main/database/properties.db`,
  pull, `git checkout stash@{0} -- …`, drop).

## Next step

Write the Phase 3 plan (enrichment + gateway) per spec §5.4/§5.5, folding in the five Phase 3
deliverables above. Process: superpowers brainstorming→writing-plans→subagent-driven execution
with per-task spec+quality reviews and a final whole-branch review — the loop caught eight real
defects across Phases 1-2; keep it.
