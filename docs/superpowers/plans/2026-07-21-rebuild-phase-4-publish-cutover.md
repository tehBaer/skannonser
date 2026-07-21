# Rebuild Phase 4 — Publish Port + Cron Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the publish layer (one-way Sheets export incl. the first working DNB tab, notify summaries, DNB travel), pass the two hard network gates, cut the server cron over from legacy `make full` to the rebuilt CLI — then land the user-mandated first-appearance activation fix as the binding final task.

**Architecture:** `skannonser/publish/` renders sheet payloads from the DB (donor-resolved, sheet-normalized strings) and writes them via a thin Sheets client; ALL tabs become full clear-and-rewrite (the interactive diff machinery dies). Manual sheet columns (Kommentar/Tag) are rescued once into a new `annotations` table and re-exported read-only. Notify summaries port onto the existing snapshot/metrics tables. DNB rows gain DB-backed travel columns (migration 004) enriched through the gateway. Two golden masters (`verify sheets`, `verify metrics`) gate parity; a supervised side-by-side night gates the cutover.

**Tech Stack:** Python 3.12 (`.venv`), google-api-python-client (already in the legacy venv — add to pyproject), stdlib sqlite3, pytest.

## Global Constraints

- `.venv/bin/python` only; tests via `.venv/bin/python -m pytest tests/rebuild -q`. Legacy frozen (read-only imports in verify/pin tests). Live DB never written by tests/verify — tmp copies.
- **The Apps Script map is an UNCHANGED consumer until Phase 5.** Tab names (`Eie`, `Sold`, `DNB`, `Stations`), header rows, value formats, and any HYPERLINK formulas must match what legacy writes — `verify sheets` enforces byte-parity on payloads. Read `main/sync/helper_sync_to_sheets.py` + `main/sync/sync_stations_to_sheet.py` for the authoritative formats before writing any export code.
- **Sheet normalization policy (resolves the tracked ''-vs-NULL + postnummer notes):** at EXPORT, text NULLs render as `""`; postnummer PAYLOAD carries the DB's 4-digit zero-padded string, sent via USER_ENTERED exactly like legacy — Sheets coerces it to a number ("0581"→581), reproducing today's sheet display byte-for-byte (EMPIRICALLY VERIFIED 2026-07-21: the live Eie tab shows truncated values; legacy has always done this). Do NOT apostrophe-escape in Phase 4 — the display fix is a post-cutover backlog item.
- NO REAL API CALLS in tests/fixtures/verify. Real network happens ONLY in the two named gate tasks (DNB crawl; Routes call) and the supervised cutover night — each capped and ledger-verified.
- No `input()` anywhere. Any behavior divergence from legacy needs a controller ruling + STATUS.md entry in the same commit.
- Sanctioned Phase 4 changes (the only ones): (1) all tabs full clear-and-rewrite (replaces append/diff/prune machinery); (2) sheet-normalization policy above; (3) DNB travel values stored in the DB instead of sheet-only (migration 004) — survives tab rewrites; (4) manual sheet columns move to the `annotations` table (one-time import, then read-only export); (5) THE FINAL TASK: first-appearance activation (user-mandated, post-cutover).
- Commits per green cycle; messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Sheets client port

**Files:**
- Create: `skannonser/publish/__init__.py` (empty), `skannonser/publish/sheets_client.py`
- Modify: `pyproject.toml` (add `google-api-python-client>=2.100`, `google-auth>=2.20`)
- Test: `tests/rebuild/test_sheets_client.py`

**Interfaces:**
- `SheetsClient(spreadsheet_id: str, service=None)` — when `service is None`, build from `get_secrets().google_service_account_file` (port the credential-resolution order from `main/googleUtils.py` — read it; service-account path first, matching what the server cron uses). Methods: `read_tab(tab: str) -> list[list]`, `rewrite_tab(tab: str, rows: list[list]) -> int` (clear + update in one batch; returns cells written), `tab_exists(tab: str) -> bool`.
- ALL tests inject a fake `service` (record calls; no google import needed at test time — import lazily inside `_build_service`).

- [ ] Steps: failing tests (rewrite_tab clears then updates with the given rows; read_tab returns values; no network/credentials touched when service injected) → implement → suite green → commit `rebuild(phase4): sheets client - service-account resolution, clear-and-rewrite tabs`.

---

### Task 2: Migration 004 (dnb travel) + 005 (annotations) + one-time manual-column rescue

**Files:**
- Create: `skannonser/store/migrations/004_dnb_travel.sql`, `skannonser/store/migrations/005_annotations.sql`
- Create: `skannonser/commands/tools_cmd.py` (`skannonser tools import-sheet-annotations`)
- Modify: `skannonser/cli.py` (register `tools`)
- Test: `tests/rebuild/test_migrations.py`, `tests/rebuild/test_annotations_import.py`

**Interfaces:**
- 004: `ALTER TABLE dnbeiendom ADD COLUMN pendl_rush_brj INTEGER; ALTER TABLE dnbeiendom ADD COLUMN pendl_rush_mvv INTEGER;` (guard: migrations are atomic but not IF-NOT-EXISTS for ALTER — the runner records application, that's the guard; keep the statements plain).
- 005: `CREATE TABLE IF NOT EXISTS annotations (finnkode TEXT PRIMARY KEY, kommentar TEXT, tag TEXT, imported_at TEXT, updated_at TEXT)` — the Phase 5 web UI will extend this; Phase 4 only needs kommentar/tag keyed by finnkode.
- `import_sheet_annotations(conn, client: SheetsClient, tab="Eie") -> dict` — reads the Eie tab ONCE, extracts Finnkode + Kommentar + Tag columns (header-alias tolerant — port the canonicalization from `helper_sync_to_sheets.py`'s header handling; read it), upserts non-empty ones into `annotations` (never overwrites a newer `updated_at`). CLI wraps it; idempotent.
- [ ] Steps: TDD (fresh-DB migrate list gains 004+005; import test with fake client fixture rows incl. an aliased header and an empty kommentar skipped) → implement → laptop `db backup && db migrate` → commit `rebuild(phase4): dnb travel + annotations migrations; sheet manual-column rescue tool`.

---

### Task 3: Export payload builders + sheet normalization

**Files:**
- Create: `skannonser/publish/export.py`
- Test: `tests/rebuild/test_export.py`

**Interfaces:**
- `norm_cell(v) -> str|int|float` (policy: None→"", postnummer handled separately), `norm_postnummer(v) -> str` (4-digit zero-pad; ""→"").
- `eie_rows(conn) -> tuple[list[str], list[list]]` — header + rows porting `db.py:788-880 get_eiendom_for_sheets` (the donor CASE/COALESCE SQL — reuse/extend the pattern already in `ProcessedRepo.sheet_travel_values`; the FULL column list and order comes from the legacy sheet: READ `helper_sync_to_sheets.py` to get the exact header row + per-column formatting incl. any HYPERLINK("url";"text") formulas and visibility filter [active + not solgt/inaktiv + SHEETS_MAX_PRICE/MIN_BRA_I — read `_sheet_filters` db.py:680-688]), plus `Kommentar`/`Tag` from `annotations` (read-only re-export).
- `sold_rows(conn)` — port `get_stale_eiendom_for_sheets` db.py:1008-1086 + the Sold-tab format from `sync_stale_eiendom_to_sheets` (helper 535+631).
- `dnb_rows(conn)` — the FIRST WORKING DNB export: columns per the legacy DNB tab (read `scripts/sync_dnbeiendom_sheet.py` + `scripts/export_dnbeiendom_to_sheet.py` for the header/format), travel values from the new dnbeiendom columns, matched rows (`duplicate_of_finnkode`) inherit the finn row's donor-resolved values.
- `stations_rows(conn)` — port `sync_stations_to_sheet.py` + `stations.py`'s flattener.
- [ ] Steps: TDD on seeded tmp DBs (visibility filter matrix; donor-resolved value appears; normalization: NULL text→"", postnummer 581→"0581"; annotations re-exported; HYPERLINK formula format if legacy uses one — pin with a literal expected string) → implement → commit `rebuild(phase4): export payload builders with sheet normalization and donor resolution`.

---

### Task 4: verify sheets — golden master

**Files:**
- Create: `skannonser/verify/sheets.py`; extend `skannonser/commands/verify_cmd.py`
- Test: `tests/rebuild/test_verify_sheets.py`

**Interfaces:** `verify_sheets(db_path) -> VerifySheetsResult{eie_diffs, sold_diffs, stations_diffs}` — legacy payloads (import `main.database.db.PropertyDatabase` + the legacy row-building from `helper_sync_to_sheets`/`sync_stations_to_sheet` — read how rows are built from the DataFrames and reproduce the legacy side faithfully; document approach) vs new builders, on a DB COPY, cell-by-cell with the normalization policy applied to BOTH sides' comparison (legacy None/NaN renders "" in sheets too). DNB tab has no legacy baseline (legacy's sync is unreachable code) — excluded from parity, covered by Task 3 tests only. CLI `verify sheets`, exit 1 on diffs, first-20 printing.
- [ ] Steps: unit test on seeded DB (agreement + a monkeypatched-desync detection case) → implement → **CHECKPOINT: run against a laptop DB copy — bar: zero diffs on Eie/Sold/Stations** (classify any diff with per-row evidence; port bug until proven otherwise) → commit `rebuild(phase4): verify sheets golden master`.

---

### Task 5: Notify port + verify metrics

**Files:**
- Create: `skannonser/notifications.py` (single module: metrics + summaries + send), `skannonser/commands/notify_cmd.py`
- Modify: `skannonser/cli.py`; `skannonser/store/repositories/listings.py` (snapshot/metrics accessors)
- Test: `tests/rebuild/test_notifications.py`

**Interfaces:**
- Repo methods porting db.py:729-786: `previous_active_snapshot() -> set[str]`, `replace_active_snapshot(finnkodes)`, `record_daily_metrics(metric_date, added, removed_sold, removed_delisted, total_active)`, `sum_daily_metrics_between(start, end) -> dict`, `count_sold_between(start, end) -> int`.
- `compute_daily_metrics(previous: set, current: set, sold_finnkodes: set) -> dict` — port `main/notify/listing_metrics.py` EXACTLY (it has legacy tests `tests/test_listing_metrics.py` — mirror every case in the new tests).
- `daily_summary(conn, send) / weekly_summary(conn, send)` — port `main/notify/daily_summary.py` / `weekly_summary.py` (baseline handling, message formats — keep the exact message text; the user's phone is the consumer).
- `default_send(message)` — `subprocess.run([get_secrets().notify_bin, ...])` best-effort (read `main/notify/send.py` for the exact CLI arguments the notify binary expects — match them).
- CLI `skannonser notify daily|weekly [--db PATH]` (pending-migrations fail-loud; injected send in tests).
- `verify_metrics(db_path)` in `skannonser/verify/metrics.py` + CLI `verify metrics`: fixed snapshot scenario computed by legacy `listing_metrics` vs new — zero diffs.
- [ ] Steps: TDD mirroring all legacy notify test cases + message-format pins → implement → verify metrics checkpoint on a DB copy → commit `rebuild(phase4): notify summaries port with metrics golden master`.

---

### Task 6: DNB travel enrichment (port-or-retire → PORT, DB-backed)

**Files:**
- Create: `skannonser/enrich/dnb_travel.py`
- Modify: `skannonser/commands/run_cmd.py` (`run enrich-dnb`)
- Test: `tests/rebuild/test_dnb_travel.py`

**Interfaces:** `run_dnb_travel(conn, domain, gateway, api_key, post=...) -> dict` — READ `scripts/backfill_dnbeiendom_travel_to_sheet.py` (387 lines) first: port its semantics (which DNB rows get travel: active, unmatched-to-finn, missing values; matched rows inherit finn values — at export time, Task 3, not stored), writing to the new dnbeiendom travel columns via a small `DnbRepo.set_travel(url, brj, mvv)` you add. BRJ+MVV only (legacy DNB backfill scope — verify against the file; no mvv_uni for DNB). Sentinels stored, not retried. BudgetExceeded → halt/exit 3 like `run enrich`.
- [ ] Steps: TDD (fake post; matched row NOT API-called; unmatched missing row called + stored; sentinel not retried) → implement → commit `rebuild(phase4): dnb travel enrichment - db-backed via gateway`.

---

### Task 7: HARD GATE A — supervised DNB network run (server)

No repo changes (evidence-only; report + STATUS update commit). Procedure (server, throwaway dir, rsync pattern from prior phases): copy live DB; `run ingest --source dnb` against the copy — REAL dnbeiendom.no crawl (first ever for the rebuilt path); classify every diff vs the live dnbeiendom table (time-drift vs UNEXPLAINED, per-row evidence; the phase-2 classification protocol). Bar: zero UNEXPLAINED. Then update STATUS: DNB-network gap bullet → PROVEN with date. Commit the STATUS edit: `rebuild(phase4): gate A passed - dnb network run classified clean`.

---

### Task 8: HARD GATE B — first real Routes call (server, capped)

Evidence-only + STATUS commit. On the server copy: pick the cheapest real candidate — if `run enrich-dnb` has ≥1 unmatched missing row, run it with a hard cap (stop after ≤5 calls — add `--limit` to `run enrich-dnb` in Task 6 for this); else force one finn candidate via `run enrich --targets brj --force-api` on a single-row scoped copy (document how). Verify: ledger rows `routes/ok` match call count exactly; stored minutes plausible (1..360) or sentinel; value sanity vs a coordinate-near neighbor. Update STATUS Routes-gap bullet → PROVEN. Commit: `rebuild(phase4): gate B passed - real routes call ledger-verified`.

---

### Task 9: Publish orchestrator + nightly command

**Files:**
- Create: `skannonser/nightly.py`; `skannonser/commands/` additions (`run sheets`, `run nightly`)
- Test: `tests/rebuild/test_nightly.py`

**Interfaces:**
- `run_sheets(conn, client) -> dict` — rewrites Eie, Sold, DNB, Stations tabs from Task 3 builders; returns per-tab row/cell counts.
- `run_nightly(conn, domain, gateway, api_key, client, fetch=..., post=...) -> dict` — the legacy `make full` replacement, sequential: ingest finn → ingest dnb → geocode → enrich (brj+mvv) → enrich mvv_uni → enrich-dnb → refresh stale-open → sheets → notify-daily is NOT here (separate cron, port of legacy's separate 07:00 cron). Per-step stats dict; a step failure records the error, SKIPS dependent steps conservatively (sheets still runs if only enrich failed — mirror legacy's wrapper section independence; read ~/run_skannonser_daily.sh's [A]/[B]/[C] structure from the phase-2 log knowledge: full, refresh-stale-open, sold-sync were independent sections), exit non-zero if any step failed. BudgetExceeded in any enrich step → recorded, continue to sheets (budget stop is not a failure).
- CLI `skannonser run sheets [--db]`, `skannonser run nightly [--db]` (fail-loud migrations; nightly refuses if GOOGLE key or service account missing).
- [ ] Steps: TDD with all-fakes end-to-end nightly on seeded tmp DB (step ordering, failure isolation, budget-stop-continues, stats) → implement → commit `rebuild(phase4): nightly orchestrator and sheets publisher`.

---

### Task 10: SUPERVISED SIDE-BY-SIDE NIGHT + CUTOVER (server; the phase gate)

Evidence + ops changes + STATUS commit.
1. **Side-by-side**: after legacy's 01:00 run completes (or trigger `make full` supervised), run `skannonser run nightly --db /tmp/p4/copy.db` against a fresh copy WITH a fake sheets client (add `--dry-run-sheets` flag writing payloads to JSON files instead — small Task 9 addition) — then diff: DB effects (active sets, statuses, enrichment writes) vs live post-legacy DB using the phase-2 classification protocol, AND sheet payloads vs `verify sheets` legacy baselines. Bar: zero UNEXPLAINED.
2. **Annotations rescue**: run `skannonser tools import-sheet-annotations` against the REAL sheet (read-only on the sheet), verify row count vs visible Kommentar/Tag cells.
3. **CUTOVER**: rewrite `~/run_skannonser_daily.sh` to call (venv activated, .env sourced — keep the existing env plumbing): `skannonser run nightly` (replacing sections [A]/[B]/[C]); switch the 07:00/08:00 notify crons to `skannonser notify daily`/`weekly`. Keep the legacy wrapper as `~/run_skannonser_daily.legacy.sh` (documented fallback, NOT scheduled). First live night: verify the log, the sheet (spot-check the map renders — ask the user to confirm visually), notify delivery, ledger.
4. STATUS updates: cutover date/state; legacy = fallback only; map-consumer note stands until Phase 5. Commit: `rebuild(phase4): cron cutover complete - rebuilt nightly is production`.
**A failed side-by-side or first-night check = STOP, revert cron to legacy wrapper, report.**

---

### Task 11 (BINDING FINAL — user-mandated): first-appearance activation

**Files:**
- Modify: `skannonser/store/repositories/listings.py` (INSERT writes `active=1`)
- Test: `tests/rebuild/test_listings_repo.py`, `tests/rebuild/test_pipeline.py` (update the pinning tests)
- Modify: `docs/rebuild/STATUS.md` (backlog item 1 → DONE; note the notify "added" shift)

Only after Task 10's cutover is verified. Change `ListingsRepo.upsert`'s INSERT to include `active=1`; rename/rewrite `test_insert_inactive_until_second_appearance_legacy_semantics` → `test_insert_active_on_first_appearance` (assert active==1 + membership in active_finnkodes() after ONE upsert); update the pipeline e2e (first run → 2 active) and any two-upsert seeding that existed only to activate (single upsert now suffices — simplify where intent allows); expect and document the daily-notify "added" count shift. The comment marking the quirk ("do not fix without a controller ruling") is REPLACED by one noting the user mandate + date. Commit: `rebuild(phase4): listings activate on first appearance (user mandate 2026-07-20) - export/notify same-day`.
Post-landing: push, server pull + suite, and watch the NEXT nightly: new listings should appear in the sheet + daily notification same-day. Record in STATUS.

---

## Phase 4 acceptance gate

1. Suite green (expect ~320+); no real API/network in tests.
2. `verify sheets` zero diffs (Eie/Sold/Stations) on a DB copy; `verify metrics` zero diffs.
3. Gates A (DNB network) and B (real Routes call) passed with ledger evidence, STATUS updated.
4. Side-by-side night zero UNEXPLAINED; cutover live; first production night verified (log + sheet + map renders + notify received); legacy wrapper preserved as unscheduled fallback.
5. Task 11 landed and observed: a genuinely new listing appears in the sheet/notification the SAME day (verify on the first post-Task-11 nightly).
6. STATUS.md current (cutover state, gates PROVEN, backlog item 1 closed).
