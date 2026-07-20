# Rebuild Phase 3 — Enrichment Port + Google API Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the enrichment layer (geocoding, transit travel times, the donor/reuse system, post-process derivations) onto the new skeleton with every Google API call flowing through a new budget-gateway, plus the Phase 3 deliverables from `docs/rebuild/STATUS.md`.

**Architecture:** A `gateway` module is the single choke point for Geocoding+Routes calls (persistent ledger in a new `api_usage` table, rate limiting, budget policy from domain config, notify warnings, hard stop). `enrich/` ports the legacy logic: one parameterized destination loop replaces post_process's three copy-pasted blocks; donor logic ports as pure functions; the `ProcessedRepo` reproduces `insert_or_update_eiendom_processed` semantics exactly (COALESCE fill-only travel columns). `skannonser estimate` replaces interactive confirms. Golden-master: `verify enrich` compares donor resolution + estimate counts + read-time donor CASE resolution against legacy on a DB copy with zero API calls.

**Tech Stack:** Python 3.12 (`.venv`), stdlib sqlite3, requests (injectable), pytest. No new dependencies.

**Port inventory (authoritative line refs for all tasks):** the Phase 3 planning session produced a verified inventory; its key facts are inlined per task below. Legacy sources: `main/post_process.py`, `main/location_features.py`, `main/tools/fill_missing_coordinates.py`, `main/database/db.py`.

## Global Constraints

- `.venv/bin/python` only; run tests as `.venv/bin/python -m pytest tests/rebuild -q`. Legacy (`main/`, `scripts/`, Makefile) frozen — imported read-only by verify/pin tests only.
- **The live DB (`main/database/properties.db`) is never written by tests or verify.** tmp copies only. The server's DB is authoritative.
- **NO REAL GOOGLE API CALLS anywhere in tests, fixtures, or `verify enrich`.** All HTTP is injected fakes. The ONLY real-API step in this phase is Task 10's capped live checkpoint (≤ 25 calls total, estimate first).
- No `input()` anywhere. No interactive confirms — budget policy + `skannonser estimate` replace them.
- Domain values come from `config/domain.toml`; travel/donor semantics must match legacy EXACTLY (sentinels -1/-2/-3; donor one-way no-chains cascade-collapse; COALESCE fill-only travel writes; MVV-UNI chain resolution + exclusivity + pre-sorting). Any divergence needs a controller ruling and a STATUS.md backlog entry in the same commit.
- Sanctioned Phase 3 behavior changes (the only ones): (1) three destination loops → one parameterized loop, identical per-destination outputs; (2) interactive confirms → policy + estimate command; (3) all API calls routed through the gateway (adds ledger writes; identical request payloads).
- STATUS.md deliverables in scope: pris_kvm, eiendom_processed writes, `.str.title()` Adresse, trigger-block migration test. **image_hosted_url is re-scoped to Phase 5** (its only legacy writer is the manual Drive tool, not the nightly; the web app owns image serving) — Task 1 records this in STATUS.md.
- Commits after every green cycle; messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Trigger-block migration test + STATUS bookkeeping

**Files:**
- Test: `tests/rebuild/test_migrations.py` (extend)
- Modify: `docs/rebuild/STATUS.md`

**Interfaces:** consumes `migrations._statements`; produces nothing new — closes STATUS deliverable 5.

- [ ] **Step 1: Write the failing-if-broken regression test**

```python
def test_statements_keeps_trigger_block_intact():
    sql = (
        "CREATE TABLE t (x INTEGER);\n"
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN\n"
        "  UPDATE t SET x = 1; UPDATE t SET x = 2;\n"
        "END;\n"
        "CREATE TABLE u (y INTEGER);\n"
    )
    stmts = migrations._statements(sql)
    assert len(stmts) == 3
    assert stmts[1].startswith("CREATE TRIGGER") and stmts[1].rstrip().endswith("END;")
```

Run: `.venv/bin/python -m pytest tests/rebuild/test_migrations.py -v` — should PASS immediately (locks existing behavior; if it fails, `_statements` is broken — fix per Task 1 of the phase-2 plan's semantics).

- [ ] **Step 2: STATUS.md edits** — in "Phase 3 named deliverables": mark item 5 done; move item 2 (image_hosted_url) to a new line under "Before Phase 4 cutover"→ no — move it to the Phase 5 scope with the rationale: "only legacy writer is manual Drive tooling (`predownload_thumbnails_to_drive.py`), not the nightly; Phase 5 web app owns image serving." Keep items 1/3/4 (they close at the end of this phase).

- [ ] **Step 3: Commit**

```bash
git add tests/rebuild/test_migrations.py docs/rebuild/STATUS.md
git commit -m "rebuild(phase3): trigger-block splitter regression test; image_hosted_url re-scoped to phase 5

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Migration 003 — api_usage ledger + budget/destination config

**Files:**
- Create: `skannonser/store/migrations/003_api_usage.sql`
- Modify: `config/domain.toml`, `skannonser/config/domain.py`
- Test: `tests/rebuild/test_migrations.py`, `tests/rebuild/test_domain.py` (extend)

**Interfaces:**
- Produces: table `api_usage(id INTEGER PK AUTOINCREMENT, called_at TEXT NOT NULL DEFAULT (datetime('now')), api TEXT NOT NULL, outcome TEXT NOT NULL, finnkode TEXT)` + index on `called_at`; domain config gains `[budget]` (`routes_monthly_cap: int`, `geocode_monthly_cap: int`, `warn_pcts: list[int]`, `routes_rpm: int = 60`, `geocode_rpm: int = 60`) and each `[[destinations]]` entry gains `df_column`, `db_column`, `exclusive: bool = false`.

- [ ] **Step 1: Migration SQL**

```sql
CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at TEXT NOT NULL DEFAULT (datetime('now')),
    api TEXT NOT NULL,
    outcome TEXT NOT NULL,
    finnkode TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_usage_called_at ON api_usage(called_at);
```

- [ ] **Step 2: domain.toml additions** (values: free-tier ~10 000 calls/month each for Geocoding and Routes Essentials — set caps conservatively BELOW free tier):

```toml
[budget]
routes_monthly_cap = 9000
geocode_monthly_cap = 9000
warn_pcts = [50, 80]
routes_rpm = 60
geocode_rpm = 60
```

and per destination (values from the port inventory §1c — column names verbatim):

```toml
[[destinations]]
key = "brj"
label = "BRJ (work, Sandvika)"
address = "Rådmann Halmrasts Vei 5"
df_column = "PENDL RUSH BRJ"
db_column = "pendl_rush_brj"

[[destinations]]
key = "mvv"
label = "MVV (Lambertseter)"
address = "Langbølgen 24, 1155 Oslo"
df_column = "PENDL RUSH MVV"
db_column = "pendl_rush_mvv"

[[destinations]]
key = "mvv_uni"
label = "MVV UNI (Gaustadalléen)"
address = "Gaustadalléen 30, 0373 Oslo"
df_column = "MVV UNI RUSH"
db_column = "pendl_rush_mvv_uni_rush"
exclusive = true
```

- [ ] **Step 3: pydantic models** — `Budget` model; extend `Destination` with `df_column: str`, `db_column: str`, `exclusive: bool = False`. Tests: migration creates the table on fresh DB (`ran == [..., "003_api_usage"]`); `load_domain().budget.routes_monthly_cap == 9000`; destinations carry the exact column names above. TDD as usual; apply `db backup` + `db migrate` on the LAPTOP only (server gets it at merge, per phase-2 precedent).

- [ ] **Step 4: Commit** (`rebuild(phase3): api_usage ledger migration; budget + destination column config`)

---

### Task 3: Gateway — the single API choke point

**Files:**
- Create: `skannonser/gateway.py`
- Test: `tests/rebuild/test_gateway.py`

**Interfaces:**
- Produces: `class BudgetExceeded(RuntimeError)`; `class Gateway`:
  - `__init__(self, conn, budget: Budget, notify=None, sleeper=time.sleep, clock=None)` — `notify` is a callable `(message: str) -> None` (default shells out to `NOTIFY_BIN` per `get_secrets().notify_bin`, best-effort, never raises); `clock` returns "YYYY-MM" (default from `datetime.now`).
  - `call(self, api: str, fn: Callable[[], T], finnkode: str | None = None) -> T` — rate-limits per `budget.{api}_rpm` (sleep `60/rpm` between calls per api), checks month-to-date count from `api_usage` BEFORE calling; at cap → record outcome `"blocked"` and raise `BudgetExceeded`; crossing a `warn_pcts` threshold (first call at/after pct) → `notify(...)` once per threshold per month (dedupe by querying whether a `"warn:{pct}"` outcome row exists this month, and record one); on success record `"ok"`, on exception record `"error"` and re-raise.
  - `month_usage(self, api: str) -> int` (counts ok+error rows this month, excludes `warn:*`/`blocked` bookkeeping rows).

- [ ] **Step 1: Failing tests** (tmp migrated DB; fake sleeper collecting calls; fake notify; fixed clock):

```python
def test_gateway_records_and_counts(...):        # ok + error outcomes counted; blocked/warn rows excluded
def test_gateway_rate_limits_per_api(...):       # two calls -> sleeper called with 60/rpm between them
def test_gateway_hard_stop_at_cap(...):          # seed cap-1 ok rows -> next ok; then BudgetExceeded, outcome 'blocked', fn NOT called
def test_gateway_warns_once_per_threshold(...):  # seed to 50% -> one notify containing '50'; again -> no second notify
def test_gateway_notify_failure_never_raises(...):  # notify raising -> call still succeeds
```

Write all five for real; then implement; suite green. Ledger writes commit immediately (own connection usage is fine — single-threaded).

- [ ] **Step 2: Commit** (`rebuild(phase3): google api gateway - ledger, rate limit, budget policy, notify warnings`)

---

### Task 4: Travel sentinels + Routes API client port

**Files:**
- Create: `skannonser/enrich/__init__.py` (empty), `skannonser/enrich/sentinels.py`, `skannonser/enrich/travel_api.py`
- Test: `tests/rebuild/test_travel_api.py`

**Interfaces:**
- `sentinels.py`: `TRAVEL_NO_ROUTES = -1`, `TRAVEL_UNREALISTIC = -2`, `TRAVEL_API_ERROR = -3`, `is_travel_sentinel(value) -> bool`, `sentinel_label(value) -> str` — port from `main/post_process.py:10-49` (single home; legacy duplicated them).
- `travel_api.py`: `next_monday_iso(hour, minute=0) -> str` (port `location_features.py:90-98`); `TransitCommute`:
  - `__init__(self, destination_address: str, gateway: Gateway, api_key: str, post=requests.post, max_minutes: int = 360)`
  - `build_request(self, address, postnummer) -> tuple[str, dict, dict]` (url, headers, body) — exactly legacy `PublicTransitCommuteTime.calculate` construction (`location_features.py:362-413`): origin `f"{address}, {postnummer}, Norway"` (postnummer optional), destination + ", Norway" unless already there, url `https://routes.googleapis.com/directions/v2:computeRoutes`, headers `X-Goog-Api-Key` + `X-Goog-FieldMask: routes.duration,routes.distanceMeters`, body `{origin.address, destination.address, travelMode: "TRANSIT", departureTime: next_monday_iso(8)}`.
  - `minutes(self, address, postnummer=None) -> int | None` — via `gateway.call("routes", ...)`; parse `routes[0].duration` (strip "s", `int(seconds/60)`, ≤0→None per `location_features.py:66-80`); unreasonable (`not 1 <= m <= max_minutes`) → `TRAVEL_UNREALISTIC`; no duration/routes → `TRAVEL_NO_ROUTES`; exception → `TRAVEL_API_ERROR`; HTTP status != 200 → `None`; missing key → `None`.

- [ ] **Step 1: Pin test — request construction equals legacy.** Instantiate legacy `PublicTransitCommuteTime("Rådmann Halmrasts Vei 5", config={"api_key": "K"})` and monkeypatch `requests.post` inside `main.location_features` to CAPTURE (url, headers, json) without network, call `.calculate("Storgata 1", "0155")`; build the same via `TransitCommute.build_request`; assert url, both headers, and the full body dict are equal (departureTime computed with the same `next_monday_iso(8)` on both sides — freeze it by monkeypatching both modules' datetime or comparing format+hour).
- [ ] **Step 2: Behavior tests** — parse/sentinel matrix with a fake `post`: 200+duration "1800s"→30; 200+duration "90000s" (>360 min)→-2; 200 no routes→-1; post raises→-3; status 500→None. Gateway ledger gets one row per call.
- [ ] **Step 3: Implement, green, commit** (`rebuild(phase3): sentinels module + transit routes client, request byte-pinned to legacy`)

---

### Task 5: ProcessedRepo — eiendom_processed semantics

**Files:**
- Create: `skannonser/store/repositories/processed.py`
- Test: `tests/rebuild/test_processed_repo.py`

**Interfaces:**
- `ProcessedRepo(conn)`:
  - `upsert(self, finnkode, adresse, postnummer, lat=None, lng=None, travel: dict[str, int | None] | None = None, cntr: dict[str, int | None] | None = None, travel_copy_from_finnkode=None)` — port `db.py:1285-1343` EXACTLY: `adresse_cleaned = clean_address(adresse)` (port `_clean_address` db.py:1463-1490 — split on `' - '`, `' ('`, `' ['`, `' /'`, keep first part); `google_maps_url` per `_generate_google_maps_url` db.py:1273-1283 (`""` if adresse or postnummer missing, else `https://www.google.com/maps/place/{adresse}+{postnummer}` spaces→+); coords normalized via lat/lng swap-correction (port `_normalize_coordinates` db.py:58-77); **UPDATE: lat/lng and the three travel columns (`pendl_rush_brj/pendl_rush_mvv/pendl_rush_mvv_uni_rush`) are COALESCE fill-only (never overwrite non-null with NULL); cntr columns, `travel_copy_from_finnkode`, `google_maps_url`, `adresse_cleaned` set unconditionally**; INSERT writes all columns.
  - `donor_seed(self) -> list[dict]` — port `get_travel_donor_seed` db.py:1361-1383 (keys `Finnkode, LAT, LNG, PENDL RUSH BRJ, PENDL RUSH MVV, MVV UNI RUSH, TRAVEL_COPY_FROM_FINNKODE`, ordered by updated_at DESC, non-empty finnkode).
  - `missing_coordinates(self, include_inactive=False) -> list[dict]` — port `get_eiendom_missing_coordinates` db.py:1172-1197 + the visibility filter from `fill_missing_coordinates.py:230-245` (drop solgt/inaktiv unless include_inactive).
  - `set_coordinates(finnkode, lat, lng) -> bool`, `mark_geocode_failed(finnkode)`, `clear_geocode_failed(finnkode)` — port db.py:1199-1272.
  - `sheet_travel_values(self, finnkode) -> dict` — the READ-TIME donor resolution: port the CASE/COALESCE pattern from `get_eiendom_for_sheets` db.py:829-852 (donor's value when link set + donor value non-null, else own; single-hop via join on `travel_copy_from_finnkode`). This is what `verify enrich` compares.
- [ ] **Steps:** failing tests covering: COALESCE fill-only (write 30 then None → stays 30) vs unconditional columns (donor pointer NULLs out); clean_address cases (`'Brynsveien 146 - Prosjekt'→'Brynsveien 146'`); maps-url empty cases; swap-corrected coords; donor_seed key set; missing_coordinates excludes geocode_failed + solgt; sheet_travel_values resolves donor over own value. Then implement (no pandas), green, commit (`rebuild(phase3): processed repository - legacy upsert/read semantics incl. read-time donor resolution`).

---

### Task 6: Geocoder port

**Files:**
- Create: `skannonser/enrich/geocode.py`
- Modify: `skannonser/commands/run_cmd.py` (add `run geocode`)
- Test: `tests/rebuild/test_geocode.py`

**Interfaces:**
- `geocode_address(address, postal_code, api_key, gateway, get=requests.get) -> tuple[float, float] | None` — port `fill_missing_coordinates.py:78-164` exactly: three-pass (strict postal component+exact-postal-check → relaxed with street-level signal + region-first-2-digits + no APPROXIMATE → address+country fallback), params (`language: no`, `region: no`, `components country:NO[|postal_code:X]`), country must be NO, postal zfill(4). Every HTTP call via `gateway.call("geocode", ...)`.
- `run_geocode(conn, domain, gateway, limit=0, get=..., sleeper=...) -> dict` — candidates from `ProcessedRepo.missing_coordinates`, success → `set_coordinates`, definitive failure → `mark_geocode_failed` (mirroring `--allow-failures` nightly behavior), stats dict. CLI `skannonser run geocode [--limit N] [--include-inactive]` (fails loud on pending migrations, per phase-2 pattern).
- [ ] **Steps:** failing tests with canned Geocoding JSON fixtures (strict hit; postal mismatch → falls to relaxed; APPROXIMATE rejected; wrong country rejected; total miss → marked failed; gateway ledger rows recorded). Implement, green, commit (`rebuild(phase3): geocoder port - three-pass Norway strategy through gateway`).

---

### Task 7: Donor system port (pure logic)

**Files:**
- Create: `skannonser/enrich/donor.py`
- Test: `tests/rebuild/test_donor.py`

**Interfaces:** operating on plain dicts (`{"finnkode", "lat", "lng", values: {df_column: int|None}, "donor_link": str|None}`):
- `build_donor_cache(rows, required_columns) -> list[tuple[lat, lng, finnkode]]` — port `_build_travel_donor_cache` post_process.py:116-141 (complete values, no donor link, valid coords, non-empty finnkode; sentinels count as valid values — verify against legacy's validity check and mirror exactly).
- `find_nearby_donor(lat, lng, cache, max_distance_m, exclude_finnkode) -> str | None` — port 144-162 (nearest within radius, never self). Distance function: port whatever legacy uses (read it — haversine or approximation; byte-match the formula).
- `assign_donors_prepass(rows, cache_by_target, reuse_within_meters) -> None` — port the pre-pass 534-587: assign nearest root donor to rows without links; cascade-collapse A→B to A→root when B becomes an acceptor; acceptors removed from caches (one-way, no chains).
- `resolve_mvv_uni_donor_value(finnkode, links, values) -> int | None` — port 473-504 (chain walk, cycle-guarded).
- [ ] **Steps:** failing tests with synthetic coordinate clusters (donor within/outside 300 m; self-exclusion; cascade-collapse produces root links only — assert no A→B→C chains survive; cycle in mvv_uni links terminates). Implement, green, commit (`rebuild(phase3): donor/reuse system port - cache, prepass cascade, chain resolution`).

---

### Task 8: Enrich orchestrator — one loop, three destinations + post-process derivations

**Files:**
- Create: `skannonser/enrich/travel.py`
- Modify: `skannonser/store/repositories/listings.py` (add `update_derived(finnkode, adresse_titled, pris_kvm)`), `skannonser/commands/run_cmd.py` (add `run enrich`), `skannonser/commands/` estimate command (new `estimate_cmd.py` + cli.py registration)
- Test: `tests/rebuild/test_enrich.py`

**Interfaces:**
- `compute_pris_kvm(pris, primary_area, usable_i_area, usable_area) -> int | None` — port post_process.py:397-420: area fallback chain primary→usable_i→usable, `round(price/area)` when both present and area>0, else None.
- `title_address(adresse) -> str` — Python equivalent of pandas `.str.title()` (post_process.py:423). NOTE: pandas `.str.title()` == `str.title()` per-string; test parity against pandas on samples incl. `"bjørnsons gate 2a"` (title-cases the letter after the digit — mirror exactly).
- `run_enrich(conn, domain, gateway, api_key, targets="all", post=..., force_api=False) -> dict` — the parameterized port of post_process passes:
  1. Load active listings + processed rows; apply `update_derived` per listing (titled Adresse written back to `eiendom.adresse`, pris_kvm to `eiendom.pris_kvm`) — closes STATUS deliverables 1+4; net DB state equals legacy's post-title-cased upsert.
  2. Donor seed via `ProcessedRepo.donor_seed`; build caches per destination (Task 7).
  3. Pre-pass donor assignment (reuse_within_meters from domain).
  4. Per destination in `domain.destinations` filtered by `targets` (mvv_uni ONLY when targets=="mvv_uni", port the exclusivity post_process.py:293-299; mvv_uni rows pre-sorted donors-first per 1099-1104; mvv_uni consults `resolve_mvv_uni_donor_value` before calling the API per 1146-1154): rows with NaN value and coords → donor value if link resolves, else `TransitCommute(dest.address, ...).minutes(...)`; write per-row immediately via `ProcessedRepo.upsert` (checkpoint semantics — a crash loses nothing).
  5. Sentinels stored as values, never retried (skip non-NaN including negatives); `force_api` ignores donor reuse (port TRAVEL_FORCE_API_FOR_MISSING).
- `estimate(conn, domain, targets="all") -> dict` — port BOTH previews (post_process.py:637-721): `max_attempts` (seed-donors only) and `simulated_attempts` (in-run donor growth), per destination + totals. CLI `skannonser estimate [--targets ...]` prints them and NEVER calls any API.
- CLI `skannonser run enrich [--targets all|brj|mvv|mvv_uni] [--force-api]` — non-interactive; BudgetExceeded → clean exit 3 with a "resumes next window" message (remaining rows simply stay NaN and are picked up next run — that IS the queue persistence).
- [ ] **Steps:** failing tests (fake post): pris_kvm matrix incl. fallback chain + zero-area; title parity vs pandas (import pandas in test only); enrich writes donor value without API call for linked rows; API result checkpointed via ProcessedRepo (COALESCE proven by pre-seeding); sentinel rows not retried; mvv_uni exclusivity + donors-first ordering + chain resolution consulted; estimate returns both counts and equals a hand-computed fixture scenario; BudgetExceeded mid-run leaves earlier rows written, exit code 3 at CLI. Implement, green, commit (`rebuild(phase3): enrich orchestrator - parameterized destinations, derivations, estimate command`).

---

### Task 9: verify enrich — golden master (no API)

**Files:**
- Create: `skannonser/verify/enrich.py`, extend `skannonser/commands/verify_cmd.py`
- Test: `tests/rebuild/test_verify_enrich.py`

**Interfaces:** `verify_enrich(db_path: Path) -> VerifyEnrichResult` with `.estimate_diffs`, `.donor_diffs`, `.sheet_value_diffs` (each a list; empty = pass); CLI `skannonser verify parse|enrich` — enrich mode:
1. **Estimate parity:** run legacy preview (`post_process_eiendom` with `calculate_google_directions=False`? No — the previews run inside the gated section; instead import and drive `_preview_api_calls`/`_simulate_in_run_api_calls` directly with a donor cache built by legacy `_build_travel_donor_cache` from the SAME DB copy) vs new `estimate(...)` — per-destination attempts must be equal.
2. **Donor prepass parity:** run legacy pre-pass logic vs `assign_donors_prepass` on identical synthetic frames built from the DB copy → identical `travel_copy_from_finnkode` assignments.
3. **Sheet-value parity:** for every finnkode, legacy `get_eiendom_for_sheets()` travel columns vs `ProcessedRepo.sheet_travel_values` → identical resolved values.
Read-only against a COPY of the DB (`cp` to tmp; never the live file). Zero API calls (assert no network by never providing a key).
- [ ] **Steps:** unit test on a small seeded tmp DB (build a scenario with one donor link, one sentinel, one NULL); then the CHECKPOINT: run `skannonser verify enrich` against a copy of the laptop DB — bar: all three diff lists empty. Any diff: investigate (port bug until proven otherwise; legacy-artifact classification requires per-row evidence like phase 2). Record the summary in the report. Commit (`rebuild(phase3): verify enrich - estimate/donor/sheet-value golden master`).

---

### Task 10: Supervised live checkpoint (capped real API) + travel validation port

**Files:**
- Create: `skannonser/enrich/validate.py`
- Modify: `skannonser/commands/run_cmd.py` (`run validate-travel`, read-only)
- Test: `tests/rebuild/test_validate.py`

Part A — **validation port**: `validate_travel(conn, domain) -> list[Finding]` porting `main/tools/validate_travel_values.py` heuristics (neighbor/postcode-group comparison, MAD outliers, thresholds SCORE_THRESHOLD=2/MIN_ABS_DIFF=15/MIN_REL_DIFF=0.25/MAD_MULT=2.0 from its CLI defaults — read the file, port the scoring exactly); CLI read-only listing findings. Tests on synthetic clusters (an outlier among neighbors is flagged; a consistent cluster is not). The targeted re-request tool stays legacy-manual until Phase 4 (note in STATUS backlog).

Part B — **live checkpoint (the phase gate), on the SERVER against a DB COPY**:
```
ssh mbp2016@100.77.139.22   # rsync branch code to /tmp/p3-live/repo as in phase 2, fresh venv
cp ~/kode/skannonser/main/database/properties.db /tmp/p3-live/copy.db
SKANNONSER_DB_PATH=/tmp/p3-live/copy.db ../venv/bin/skannonser estimate            # record counts
SKANNONSER_DB_PATH=... ../venv/bin/skannonser run geocode --limit 5                # ≤5 real geocode calls
SKANNONSER_DB_PATH=... ../venv/bin/skannonser run enrich --targets brj             # ONLY if estimate for brj ≤ 20; else stop and report
# then: sqlite3 copy.db 'SELECT api, outcome, COUNT(*) FROM api_usage GROUP BY 1,2'  # ledger inspection
# spot-check 3 enriched values against legacy's stored values for coordinate-identical donors/neighbors
rm -rf /tmp/p3-live
```
Bar: ledger rows match call counts exactly; geocoded coords land inside the polygon; enriched minutes plausible (1..360) or sentinel; **total real calls ≤ 25** — if estimate says more, do the geocode part only and report the enrich half as deferred pending controller/user go-ahead. `.env` on the server provides the key.
- [ ] Commit (`rebuild(phase3): travel validation port; live checkpoint evidence in report`)

---

## Phase 3 acceptance gate

1. `tests/rebuild` green (expect ~150+); no test or verify path performs a real API call.
2. `skannonser verify enrich` on a DB copy: estimate parity, donor-prepass parity, sheet-value parity — all empty diff lists.
3. Live checkpoint done within the ≤25-call cap with ledger rows matching, or explicitly deferred with counts recorded.
4. STATUS.md updated: deliverables 1/3/4/5 closed, image_hosted_url re-scoped to Phase 5, re-request tool + DNB travel backfill noted for Phase 4.
5. Legacy nightly still runs (now with working enrichment after the 2026-07-20 ops fixes — check the latest `~/skannonser-logs/full_*.log` ends `full=0` as part of the checkpoint ssh session and record it).
