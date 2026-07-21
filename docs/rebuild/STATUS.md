# Rebuild status & carried obligations

**Last updated: 2026-07-20 (end of Phase 3).** This is the single pick-up point for continuing
the rebuild in a fresh session. Read this, the spec, and the current phase plan — everything
else is history.

- Spec: `docs/superpowers/specs/2026-07-20-skannonser-rebuild-design.md`
- Feature inventory: `docs/rebuild/2026-07-20-feature-inventory.md`
- Phase plans: `docs/superpowers/plans/2026-07-20-rebuild-phase-1-skeleton.md`, `…-phase-2-ingest.md`
- Session ledger (gitignored, laptop only, may be stale): `.superpowers/sdd/progress.md`

## Where we are

- **Phase 1 (skeleton) and Phase 2 (ingest port) are merged to master and deployed on the server**
  (`mbp2016@100.77.139.22`, repo `~/kode/skannonser`, tailnet). **Phase 3 (enrichment port +
  Google API gateway) is complete on branch `rebuild-phase-3`; merge to master is still pending.**
  293 rebuild tests green (laptop; server-side count is post-merge).
- **Legacy (`main/`, Makefile, cron wrapper) is still the production path.** Nothing cuts over
  until Phase 4. Legacy is frozen except for the sanctioned ops patch listed below.
- Proven equivalence: `skannonser verify parse` ran the full 7 731-ad HTML cache with **zero
  unexplained diffs** (harness diff-detection proven by negative control); a supervised
  parallel run on the server (real finn.no crawl vs legacy's own run) classified all 1 361
  diffs with **zero unexplained**. `skannonser verify enrich` (estimate/donor-prepass/sheet-value
  golden master) reports **zero diffs** against the live DB.
- The new CLI (both machines): `skannonser config show | db backup/migrate/stats |
  run ingest/refresh/geocode/enrich/validate-travel | estimate | verify parse/enrich`.
- Migrations: 001 (adopt live schema), 002 (notify tables), 003 (api_usage). 001-002 applied
  everywhere; **003 applied on the laptop only — SERVER APPLY is a named post-merge step**, same
  pattern as Phase 2's merge-then-migrate. Migration runner is atomic (per-file transaction +
  rollback). Nightly Docker backup on the server keeps 30.
- **Golden-master caveat:** enrich parity is proven for a common (finnkode-ordered) iteration
  order; donor pre-pass outcomes are order-dependent, so this is parity-for-a-fixed-order, not
  order-insensitive equivalence.

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
  export/notify by one cycle). Twice attempted as a "fix", twice reverted to keep equivalence.
  → **User has ruled it a bug: scheduled for removal right after Phase 4 cutover (see Backlog).**
- DNB matcher ignores `active`; `deactivate_missing([])` deactivates everything (pipeline guards it).

## Phase 3 named deliverables (enrichment port + Google API gateway)

The port scope is `main/post_process.py` + geocoding + donor system behind the new gateway, PLUS
these obligations discovered in Phase 2 — **forgetting any of these silently freezes data**:

1. `pris_kvm` computation and write path (legacy computes in post_process; new repo never writes it). — DONE (2026-07-20, phase 3)
2. `image_hosted_url` write path (same class). → RE-SCOPED to Phase 5 (2026-07-20): only legacy writer is manual Drive tooling (`predownload_thumbnails_to_drive.py`), not the nightly; the Phase 5 web app owns image serving.
3. **`eiendom_processed` writes** (adresse_cleaned, google_maps_url, travel columns — new pipeline
   never touches the table; legacy upserts it on every ingest). — DONE (2026-07-20, phase 3)
4. **`.str.title()` on Adresse** (post_process.py:242) — live `eiendom.adresse` is title-cased and
   feeds the sheets-facing `adresse_cleaned`; the new ingest writes raw case (~211 rows differ).
   Phase 3 must reproduce the transform or the user must consciously drop it. — DONE (2026-07-20, phase 3)
5. Migration-runner note: add a trigger-block regression test for `_statements()` before any
   trigger-bearing migration. — DONE (2026-07-20)

## Before Phase 4 cutover (publish port)

- **Supervised DNB parallel run** — the DNB network path has never contacted dnbeiendom.no
  (Phase 2's checkpoint was finn-only). — PROVEN (2026-07-21): real dnbeiendom.no crawl on
  the server, 350 rows crawled, all diffs classified time-drift, zero unexplained.
- Sheet-export consumers must handle: `''`-vs-NULL text columns and postnummer leading zeros
  (legacy's CSV round-trip stripped `0581`→`581`; new pipeline preserves `0581`).
- `run ingest`'s archive dir is `data/eiendom/html_crawled_rebuild` (separated from legacy's);
  merge the two at cutover.
- Daily/weekly notify "added" metrics inherit the activate-on-2nd-appearance timing.
- **Real Routes API call never exercised** — every Phase 3 test/checkpoint used fakes or hit 0
  candidates; the Phase 4 supervised run must include at least one real `run enrich` Routes call
  with ledger verification (mirrors the DNB-network gap above). — PROVEN (2026-07-21): 6 real
  Routes calls via `run enrich-dnb` on a server DB copy; ledger exact; values plausible vs sheet
  baseline.
- **DNB travel backfill** (legacy `scripts/backfill_dnbeiendom_travel_to_sheet.py`, subprocess
  step in run_eiendom_db.py:244-257) is not yet ported — Phase 4 must port or consciously retire
  it. The targeted re-request tool (`rerequest_suspicious_travel.py`) likewise stays
  legacy-manual until Phase 4.

## Backlog: approved fixes for after cutover

**1. KILL the activate-on-second-appearance quirk — USER DECISION 2026-07-20 (re-confirmed 2026-07-21): fix it.**
**BINDING: the Phase 4 implementation plan MUST include this as its FINAL task, executed immediately after the cron cutover is verified — Phase 4 is not complete until new listings activate (and therefore export/notify) on FIRST appearance. Do not write the Phase 4 plan without this task.**
Today a newly discovered listing stays `active=0` (invisible to sheet export and the daily
"added" notification) until the SECOND crawl that sees it — a full day's delay on exactly the
listings the user most wants to hear about. Preserved during the rebuild only because phases 2-4
require byte-equivalence with legacy for the golden-master/parallel-run comparisons. **Scheduled:
first change after Phase 4 cutover** — make listings active on first appearance (insert with
`active=1` in `ListingsRepo.upsert`), update the pinning tests (`test_insert_inactive_until_
second_appearance_legacy_semantics` and the pipeline e2e), and expect the daily notify "added"
count to shift by design. The reverted implementations from Tasks 6/13 show exactly where the
one-line change goes.

Other decided-later items (surface each for a go/no-go when its phase arrives):
- Sheet Postnummer display: legacy (and phase-4 bug-compatible export) lets USER_ENTERED coerce "0581"→581 in the sheet (verified live 2026-07-21). One-line fix (apostrophe-prefix at row construction) once the Apps Script map is retired or verified tolerant — or moot at Phase 5 (web UI reads the DB's correct values).
- **Targeted travel-value re-request tool** (a separate legacy script from
  `main/tools/validate_travel_values.py`, whose scoring core Task 10 ported to
  `skannonser/enrich/validate.py`) stays legacy-manual until Phase 4 — port scoped to the
  read-only validator only; the tool that re-requests flagged rows' travel times was
  intentionally left out of Task 10's scope.
- Untrack `main/database/properties.db` from git (currently tracked+perpetually dirty; the
  stash-dance in every server pull exists because of this) — vs keeping git as a sync channel.
- Deferred minors: `deactivate_missing` empty-list guard in repos; AliasChoices for
  `SKANNONSER_DB_PATH`; anchor `DEFAULT_DOMAIN_PATH`; `require_db()` helper on a 4th db command;
  supercronic checksum in Dockerfile; backup `PRAGMA journal_mode=DELETE`; USER/HEALTHCHECK in
  Docker (with Phase 5 web service); crawl archives `response.text` vs legacy's `content`
  round-trip; progress logging in long crawls.

**Standing rule: any NEW issue found in later phases that can't be fixed immediately gets added
to this backlog section in the same commit that discovers it — never only in chat or the
gitignored ledger.**

## Operational state (server)

- **Legacy nightly was failing every night for 8+ days** (diagnosed 2026-07-20): alternating
  nights died on a midnight DNS outage; other nights stored to DB but died at the interactive
  sheet-update prompt (EOF under cron), so travel/coords enrichment + part of sheet sync were
  stale since ~Jul 12. Remedies applied 2026-07-20: cron moved 00:00→**01:00** (DNS dodge) and
  `SHEETS_AUTO_CONFIRM=1` exported in `~/run_skannonser_daily.sh`.
  **PROVEN 2026-07-21: `full_2026-07-21_010001.log` ends `full=0 refresh=0 sold=0` — first fully clean nightly since ~Jul 12; both fixes work.**
- Google Maps API key lives in `.env` on both machines (0600, gitignored); `main/config/config.py`
  is env-first with a `.env` fallback; the wrapper sources `.env`. Key was NOT rotated (never in
  git history — verified). Optional: confirm API restrictions in Cloud Console.
- Docker scheduler container runs nightly `skannonser db backup --keep 30` at 03:00 UTC.
- The server's `properties.db` is the authoritative live DB (laptop copy goes stale). It is
  git-tracked: every server pull needs the stash-dance (`git stash push main/database/properties.db`,
  pull, `git checkout stash@{0} -- …`, drop).

## Review findings log (RESOLVED — do not re-litigate, do not undo)

Every defect the review loop caught in Phases 1-2, with its resolution. Each is fixed on master;
the delay/timing ones are regression-locked by tests that monkeypatch `time.sleep`/`random.uniform`
and fail if a default is removed — do not "simplify" those parameters away.

**Phase 1:**
1. Migration-001 generation doubled `IF NOT EXISTS` (plan's sed was not idempotent) → sed fixed in plan, file regenerated clean.
2. `db backup`/`db migrate` silently created an empty DB on a wrong path (exit 0, plausible empty backup) → exists-check + read-only URI open + loud exit 1, tested.
3. supercronic as PID 1 crash-looped ("Failed to fork exec") → `init: true` + `-no-reap`, verified locally and on the server.
4. Migration SQL was dropped from wheels → in Docker, `db migrate` silently no-opped → `package-data` fix + `pending()` raises on missing migrations dir, verified against a non-editable install.
5. No `.dockerignore` (build context shipped the DB, caches, `.env`) → added.

**Phase 2:**
6. Unsanctioned `active=1`-on-insert in ListingsRepo (changes when listings reach export/notify) → reverted to legacy activate-on-2nd-appearance, test-pinned.
7. Finn crawl dropped legacy's 200-500 ms inter-page delay → restored (`page_delay`, injectable).
8. Ad fetch dropped legacy's 0.1 s pre-fetch delay → restored (`fetch_delay`, cache hits exempt).
9. Pipeline double-upserted to force first-run activation (the plan's own test demanded it — plan defect acknowledged and amended) → double-upsert removed, legacy timing preserved.
10. DNB listing fetches lost cache/User-Agent/timeout/pacing entirely (hid in the task seam; DNB path was never network-tested) → routed through the HTML cache with legacy uid derivation, UA/timeout, 200-800 ms post-fetch delay.
11. Refresh dropped legacy's 0.2 s inter-listing delay → restored (`listing_delay`).
12. Crawl stop-condition silently changed (no-new-ads vs legacy's zero-matches; could truncate crawls on repeat-heavy pages) → aligned back to legacy, regression-tested.
13. A completely failed (zero-URL) crawl exited 0 silently → CLI exits 1 with an error.
14. `run` commands auto-applied migrations (contradicting explicit-migrate) → now fail loud asking for `db migrate`.
15. A crawl test was pinned to `data/eiendom/html_crawled/page1.html` — live, nightly-overwritten, machine-varying → frozen as a committed fixture.

**Phase 3:**
16. Gateway clock read local time instead of UTC for month-boundary budget accounting (could misalign with SQL's UTC `datetime('now')`, corrupting budget resets/counts near a month boundary) → clock unified on UTC.
17. `TransitCommute.minutes()`'s exception handling was scoped too broadly → narrowed to legacy's exception scope, so only the failures legacy itself treated as a `TRAVEL_API_ERROR` sentinel are caught that way.
18. `BudgetExceeded` could be caught by the broad handler and converted into a sentinel value instead of propagating → explicit `except BudgetExceeded: raise` added before the generic handler; contract locked (halt the loop, leave remaining rows untouched, CLI exits 3).
19. `ProcessedRepo.upsert` writes the four CNTR columns unconditionally; without a fix, every enrich per-row upsert would have silently nulled existing CNTR data on its next travel write (critical cutover data-destruction) → read-and-passthrough added so every enrich upsert carries the existing CNTR values forward.
20. Plan defect: candidacy required lat/lng coordinates before an API attempt, but legacy's transit API is address-based (no coords check) → candidacy narrowed to "missing value only"; coords matter only for donor assignment, not run-loop candidacy.
21. `force_api=True` persisted an in-loop-discovered mvv_uni donor link unconditionally ("link overshoot"), diverging from legacy's `post_process.py:1139` gate (only assign+persist when the donor chain value actually resolves) → gated on resolvable donor value, matching legacy's force_api semantics.
22. `estimate()`/`run_enrich()` never applied legacy's `SHEETS_MAX_PRICE` eligible-mask filter to candidacy/run scanning → caught live via `skannonser verify enrich` against a copy of the production DB (task-9); fixed (donor-cache construction/pre-pass deliberately stay unfiltered, per legacy); golden master re-ran all-zero.
23. STATUS.md's Phase 3 named-deliverables list wasn't being marked done as work landed (bookkeeping gap — risk of re-doing or silently dropping finished obligations) → deliverables 1/3/4 marked DONE, pre-cutover gaps tracked explicitly.
24. Final-review fix: the ported enrich loop only wrote `eiendom_processed` for rows it actually touched (price-eligible + API/donor-assigned), diverging from legacy's unconditional nightly bulk write over every post-processed row (`run_eiendom_db.py:196-229` → `db.py:508`) — pre-pass donor links on price-ineligible rows, `adresse_cleaned`/`google_maps_url` refresh on unrelated address edits, and processed rows for brand-new price-ineligible listings could all be silently lost → added an end-of-run metadata refresh pass over every active row, diff-gated against the run-start snapshot so an untouched row costs no write, restoring legacy's bulk-write equivalence.

Legacy bugs found while porting (in production today): the absolute-href listing drop (sanctioned
fix #3 fixes it in the rebuild), and the nightly cron failures (DNS at midnight + interactive
prompt EOF — remedied 2026-07-20, see Operational state).

## Next step

Write the Phase 3 plan (enrichment + gateway) per spec §5.4/§5.5, folding in the five Phase 3
deliverables above. Process: superpowers brainstorming→writing-plans→subagent-driven execution
with per-task spec+quality reviews and a final whole-branch review — the loop caught eight real
defects across Phases 1-2; keep it.
