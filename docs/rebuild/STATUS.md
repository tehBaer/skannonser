# Rebuild status & carried obligations

**REBUILD COMPLETE (2026-07-22).** The from-scratch rebuild (`skannonser/`) fully
replaces the legacy script collection (`main/`, deleted in Phase 6): nightly ingest
(FINN + DNB) → geocode/enrich (budget-gated Google Geocoding + Routes, donor-reuse
cost control) → publish (a Google Sheet read-only view + a tailnet-only FastAPI/
MapLibre web app) → Pushover notifications, all driven by the `skannonser` CLI.
Migrations 001-005 are applied everywhere (laptop + server); the server's crons run
the new CLI exclusively (`run nightly` at 01:00, `notify daily`/`weekly` at 07:00/Sun
08:00); the web app is live and user-accepted at `http://100.77.139.22:8377`
(tailnet-only); the Sheet continues exporting nightly as the read-only view. 499
tests pass, zero warnings. This file is now primarily a historical record plus the
"Follow-ups"/"Standing checks" pick-up point — see `README.md` at the repo root for
how the system works day to day.

- Spec: `docs/superpowers/specs/2026-07-20-skannonser-rebuild-design.md`
- Feature inventory: `docs/rebuild/2026-07-20-feature-inventory.md`
- Phase plans: `docs/superpowers/plans/` — `2026-07-20-rebuild-phase-1-skeleton.md`
  through `2026-07-22-rebuild-phase-6-teardown.md` (six phases, all complete)
- Session ledger (gitignored, laptop only, may be stale): `.superpowers/sdd/progress.md`

## Where we are

*(Collapsed to the end-state on 2026-07-22, Phase 6 Task 7. The phase-by-phase
narrative this section used to carry — Phase 1/2 merge, Phase 3 pending-merge,
Phase 4 cutover side-by-side diffs, Phase 5 web-app trial — is preserved in the
per-phase commit history, `.superpowers/sdd/task-*-report*.md`, and the findings
log below; nothing there was deleted, it's just no longer the "current" state.)*

- **Legacy is gone.** `main/` (scripts, Makefile, cron wrapper) was deleted in
  Phase 6 Task 5; `skannonser/` is the only codebase. The old legacy-comparison
  `skannonser verify parse/enrich/sheets/metrics` harnesses are also deleted (their
  job — proving the port byte-for-byte against `main.*` — is complete); see
  "Standing checks" below for what replaces them.
- **CLI** (`skannonser --help`, both machines): `config show` · `db backup/migrate/
  stats` · `run ingest/refresh/geocode/enrich/enrich-dnb/validate-travel/sheets/
  nightly` · `estimate` · `notify daily/weekly` · `web` · `tools
  import-sheet-annotations`.
- **Migrations 001-005 applied everywhere** (laptop + server): 001 (adopt live
  schema), 002 (notify tables), 003 (api_usage), 004 (dnb_travel), 005
  (annotations). Migration runner is atomic (per-file transaction + rollback).
- **Web app LIVE and USER-ACCEPTED** at `http://100.77.139.22:8377` — tailnet-only
  (the `web` docker-compose service; port bound to `100.77.139.22:8377`, confirmed
  refused on the server's LAN interface). `/healthz` → `{"status":"ok","db":true}`;
  `/` serves the MapLibre map, `/table` a sortable table, `/api/listings` the
  merged Eie/Sold/DNB feed; thumbnails serve from a local disk cache. It is the
  sole map consumer now — the Apps Script map was retired 2026-07-22 (all 20
  deployments `clasp undeploy`'d; the old `/exec` URL 404s).
- **Sheet still exports nightly** (`run sheets`, the last nightly step) as the
  read-only spreadsheet view — Eie/Sold/DNB/Stations tabs, full clear-and-rewrite.
- **Cutover is live**: the server's crontab runs the new CLI exclusively —
  `~/run_skannonser_daily.sh` calls `skannonser run nightly` at 01:00; separate
  07:00/Sun-08:00 crons call `skannonser notify daily`/`weekly`. No legacy fallback
  remains (legacy artifacts removed in Phase 6 Task 6).
- **Same-day listing activation** (former backlog item 1) is landed and proven in
  production end-to-end: a newly discovered listing is `active=1` from its first
  crawl, visible in the sheet/web app/notification the same day — no more
  one-cycle delay.
- **Test suite: 499 passed, zero warnings** (`pytest tests/rebuild -q`).
- **Golden-master caveat (retained for context):** the Phase 3/4 parity proofs
  covered a common (finnkode-ordered) iteration order; donor pre-pass outcomes
  are order-dependent, so equivalence was parity-for-a-fixed-order, not
  order-insensitive equivalence. This no longer matters operationally (there is
  nothing left to compare against), but it's worth knowing if the donor cache is
  ever revisited.

## Standing checks (2026-07-22, Phase 6 Task 2)

The legacy-comparison verify harnesses (`skannonser verify parse/enrich/sheets/metrics`,
`skannonser/verify/*.py`, `config/verify-allowlist.toml`) are deleted — their job (proving the
port byte-for-byte against `main.*`) is complete, and they cannot survive Phase 6 Task 5's
deletion of `main/` anyway. Going forward, the standing checks proving the system stays correct
are:

- **The fixture corpora** (`tests/rebuild/fixtures/` — 12 sampled real FINN ads with frozen
  legacy-parser expected output, a real DNB listing page, a real FINN result page) — these pin
  parsing/extraction behavior against real, previously-legacy-verified HTML without needing
  `main.*` at test time.
- **The full pytest suite** (`.venv/bin/python -m pytest tests/rebuild -q`) — **499 tests, zero
  warnings** (as of Phase 6 Task 6/7, post-teardown; was 496 right after Task 2 deleted the four
  `test_verify_*.py` files — 20 tests exclusively testing the now-gone harness modules themselves,
  not product behavior — then 499 once Phase 6 Task 5 added `test_no_legacy.py`'s three structural
  proofs).
- **The packaging structural test** (`tests/rebuild/test_packaging.py`) — proves the wheel builds
  with static completeness (migrations etc. included).
- **The no-legacy structural proofs** (`tests/rebuild/test_no_legacy.py`, added Phase 6 Task 5) —
  lock in that `main/`/`scripts/`/`apps_script/`/`Makefile`/`requirements.txt` stay untracked by
  git, that no `.py` file under `skannonser/` or `tests/rebuild/` imports the legacy `main`
  package, and that `pyproject.toml` carries no legacy references — the legacy system cannot
  silently creep back.
- **`/healthz`** — the live liveness/readiness check for the deployed web app.

(The Phase 6 Task 2-era caveat about a handful of test files still importing `main.*` for inline
pin/comparison purposes no longer applies — `main/` is deleted and `test_no_legacy.py` proves
zero such imports remain.)

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
- **Activate-on-second-appearance (`eiendom`/`ListingsRepo`)**: new listings stayed `active=0`
  until their 2nd crawl (delayed export/notify by one cycle). Twice attempted as a "fix", twice
  reverted to keep equivalence during phases 2-4. → **removed 2026-07-21 per user mandate
  (Task 11): `ListingsRepo.upsert`'s INSERT now writes `active=1` — listings activate on FIRST
  appearance.** The equivalent quirk on `dnbeiendom`/`DnbRepo` (driven by the live schema's own
  missing `active` default, not application code) is a SEPARATE, still-preserved quirk — out of
  this task's scope.
- DNB matcher ignores `active`; `deactivate_missing([])` deactivates everything (pipeline guards it).

## Phase 3 named deliverables (enrichment port + Google API gateway)

The port scope is `main/post_process.py` + geocoding + donor system behind the new gateway, PLUS
these obligations discovered in Phase 2 — **forgetting any of these silently freezes data**:

1. `pris_kvm` computation and write path (legacy computes in post_process; new repo never writes it). — DONE (2026-07-20, phase 3)
2. `image_hosted_url` write path (same class). → RE-SCOPED to Phase 5 (2026-07-20): only legacy writer is manual Drive tooling (`predownload_thumbnails_to_drive.py`), not the nightly; the Phase 5 web app owns image serving. → RETIRED (2026-07-21, Phase 5 Task 5): the `image_hosted_url` machinery (Drive pre-upload + hosted-URL serving) is superseded — the web app now serves listing thumbnails straight from a local disk cache (`GET /thumbs/{identifier}.jpg`, `skannonser/web/app.py`) populated by a new nightly step (`thumbs`, `skannonser/enrich/thumbs.py`'s `cache_thumbnails`, wired into `skannonser/nightly.py` between `refresh` and `sheets`). `predownload_thumbnails_to_drive.py` stays legacy-only (unused by the new pipeline) until its Phase-6 deletion; `image_hosted_url` itself is neither read nor written by any Phase-5 code path.
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

## Follow-ups

*(Formerly "Backlog: approved fixes for after cutover" — renamed 2026-07-22, Phase 6
Task 7, since cutover is done. Item 1 is closed/landed history, kept for the record;
the rest are the open/deferred items still worth knowing about.)*

**1. KILL the activate-on-second-appearance quirk — USER DECISION 2026-07-20 (re-confirmed 2026-07-21): fix it.**
**— LANDED (2026-07-21) and CONFIRMED (forced first-insert proof on a DB copy + live nightly running the activation code).**
`ListingsRepo.upsert`'s INSERT now writes `active=1` (Task 11): a newly discovered listing is
active — and therefore visible to sheet export, the web app, and the daily "added" notification —
from the FIRST crawl that sees it, no more one-day delay. Pinning tests updated (`test_insert_
active_on_first_appearance` in `test_listings_repo.py`, plus the `test_pipeline.py` e2e and every
other seeding site across `tests/rebuild` that relied on the old two-upsert-to-activate behavior —
see the Task 11 commit/report for the full list, including the handful of tests whose POINT was an
inactive row and now force it via a direct SQL `UPDATE eiendom SET active = 0` after upsert).
Confirmed end-to-end: a real finn.no crawl re-discovered a previously-removed listing (Inges gate
6A, 470374293) as a genuine first insert; it came out `active=1` immediately and appeared in the
Eie sheet payload the same day. Closed — nothing further to watch here.

Other decided-later items:
- **Sheet Postnummer display — RESOLVED-BY-WEB-APP.** Legacy (and the phase-4 bug-compatible
  export) let Sheets' USER_ENTERED input coerce `"0581"` → `581` in the Sheet (verified live
  2026-07-21). The web app reads the DB's correct, zero-padded values directly (`skannonser/web/
  api.py`), so this no longer blocks anything — it was only ever a Sheet/Apps-Script-map display
  quirk, and the Apps Script map is retired. The Sheet stays bug-compatible by design (matches its
  historical display; nothing consumes it that cares). A one-line fix remains available in
  `skannonser/publish/export.py`'s `norm_postnummer` (apostrophe-prefix the value at row
  construction) if the Sheet's own postnummer display is ever needed to match the DB.
- **Targeted travel-value re-request tool** (a separate legacy script from
  `main/tools/validate_travel_values.py`, whose scoring core Task 10 ported to
  `skannonser/enrich/validate.py`) was never ported — `skannonser run validate-travel` is
  read-only (flags suspicious rows; doesn't re-request them). Still open if that workflow is
  ever needed again; low priority (manual travel-time cleanup is rare).
- Deferred minors (carried verbatim, still open, none blocking): `deactivate_missing` empty-list
  guard in repos; AliasChoices for `SKANNONSER_DB_PATH`; anchor `DEFAULT_DOMAIN_PATH`;
  `require_db()` helper on a 4th db command; supercronic checksum in Dockerfile; backup `PRAGMA
  journal_mode=DELETE`; USER/HEALTHCHECK in Docker (with Phase 5 web service); crawl archives
  `response.text` vs legacy's `content` round-trip; progress logging in long crawls.

**Standing rule: any NEW issue found that can't be fixed immediately gets added to this
Follow-ups section in the same commit that discovers it — never only in chat or the gitignored
ledger.**

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
- **Phase 6 Task 3 (2026-07-22): `main/database/properties.db` is UNTRACKED from git** (`git rm
  --cached` + `.gitignore` entries for `main/database/properties.db`, `main/database/*.db-wal`,
  `main/database/*.db-shm`; commit `91f09b1`). Server transition done outside the nightly window:
  safety copy to `~/skannonser-preuntrack-20260722-094457.db`, DB moved to `/tmp/db-hold.db`,
  `git pull --ff-only` (fast-forwarded `be10f4f..91f09b1`, deleting the tracked blob), DB moved
  back into place — now untracked+ignored. `git status --short` shows nothing for the db;
  `db stats` and `/healthz` confirmed the DB intact; `docker compose ps` showed `scheduler` and
  `web` both `Up`/`Up (healthy)` throughout (volume mount unaffected by the git-side change).
  **The stash-dance is dead** — plain `git pull --ff-only` is now sufficient for every future
  server pull; the server's `properties.db` remains the authoritative live DB (laptop copy goes
  stale), it's just no longer synced through git.
- **Phase 6 Task 6 (2026-07-22): server is on the slimmed tree (post-teardown)** since 2026-07-22.
  Safety copy taken (`~/skannonser-preteardown-20260722-101909.db`), then the FIRST clean
  `git pull --ff-only` (no stash-dance) fast-forwarded `91f09b1..e2b6dc6` ("delete legacy system —
  one codebase remains"), deleting ~90 tracked legacy files. `main/database/properties.db` and the
  gitignored `main/config/thumbnail-service-key.json` credential (referenced by `.env`'s
  `GOOGLE_SERVICE_ACCOUNT_FILE`) both survived untouched, as expected (untracked/ignored). Residue
  left under `main/` as expected: `main/config/{config.py,credentials.json,drive_token.json,
  token.json,thumbnail-service-key.json}`, `main/temp/*.py`, and `__pycache__` dirs under
  `config/database/extractors/notify/runners/sync/tools/main` — all untracked leftovers, harmless.
  `pip install -e '.[dev]'` + `pytest tests/rebuild` → **499 passed, zero warnings**. Rebuilt
  `docker compose build web scheduler` from the slimmed tree and `docker compose up -d`; both
  containers came up `Up`/`Up (healthy)`; `/healthz` → `{"status":"ok","db":true}`; `db stats`
  sane. Legacy fallback artifacts removed (rollback target no longer exists in the repo):
  `~/run_skannonser_daily.legacy.sh`, `~/crontab.precutover.bak`. `crontab -l` unchanged — wrapper
  (`run_skannonser_daily.sh` @ 01:00) + `notify daily`/`weekly`/`battery`/`heartbeat`, nothing
  legacy. DB safety copies **kept** (cheap insurance, for eventual manual cleanup) at:
  `~/skannonser-predeploy-20260720-133616.db` (4,886,528 B),
  `~/skannonser-prephase2-20260720-184918.db` (4,898,816 B),
  `~/skannonser-prephase3-20260721-071949.db` (4,911,104 B),
  `~/skannonser-precutover-20260721-141109.db` (4,919,296 B),
  `~/skannonser-prep5deploy-1784701969.db` (4,956,160 B),
  `~/skannonser-prephase4merge-20260722-081522.db` (4,956,160 B),
  `~/skannonser-preuntrack-20260722-094457.db` (4,956,160 B),
  `~/skannonser-preteardown-20260722-101909.db` (4,956,160 B).
  **Next nightly (01:00) is the first to run on the slimmed tree** — watch-item: check
  `~/skannonser-logs/` / the scheduler container log after it runs.

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

None — the rebuild is complete (all six phases landed; see the header at the top of this
file). There is no standing "next phase." Future work starts from `README.md` and the
"Follow-ups" section above; if a new multi-task effort is ever needed, the process that
worked well across all six phases (superpowers brainstorming→writing-plans→subagent-driven
execution with per-task spec+quality reviews and a final whole-branch review — it caught
24 real defects, see the findings log above) is worth repeating.
