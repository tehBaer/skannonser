# skannonser rebuild — design spec

Date: 2026-07-20
Status: approved design, pending implementation plan
Companion document: `docs/rebuild/2026-07-20-feature-inventory.md` (full feature inventory with
working/fragile/broken/dead flags — the authoritative list of what exists today and what must be preserved).

## 1. Context and goal

skannonser scrapes Finn.no and DNB Eiendom real-estate listings for a polygon-defined area,
enriches them with Google geocoding and transit travel times (with an elaborate donor/reuse
system to avoid paying for duplicate API calls), stores everything in SQLite, syncs to Google
Sheets, and renders an interactive map via Google Apps Script. It recently moved from a personal
machine to a server, and the owner wants to add features next: sold-price ingestion, good-listing
alerts, Google API usage warnings, and API quota queueing.

The current structure blocks that roadmap: interactive `input()` prompts in server paths, a
467-line Makefile as orchestrator, a half-finished CSV→SQLite migration, god modules
(`main/database/db.py` 1731 lines; `post_process_eiendom` ~970 lines with three copy-pasted
destination loops), two never-worked features (DNB→sheet sync, comment sync-back), ~40% dead
files, secrets in plaintext, and a map UI that is slow by architecture (no clustering, full
marker teardown per toggle, Sheets-as-API).

**Goal: "new skeleton, same organs."** Build a clean package with proper config, storage,
orchestration, an API-budget gateway, and a server-hosted web UI — and port the proven domain
logic into it module by module, each port verified against the current system's output before
the old path retires. Not a greenfield rewrite: parsers, donor logic, cost gating, and metrics
logic port as-is.

## 2. Decisions locked in with the owner

- **UI moves to a server web app** with a real map. Google Sheets remains only as a read-only
  export so listings can be browsed in a spreadsheet. All interactive sheet-sync machinery is dropped.
- **Annotations (comments/tags/corrections) move to the web UI**, stored in the DB. No sheet→DB
  sync-back of any kind.
- **Deployment is Docker Compose** on the server; deploy = `git pull && docker compose up -d --build`.
- **Fixed points** (build around, do not abstract away):
  - Google Geocoding + Routes APIs remain the source for coordinates and transit times.
  - The Finn search polygon and the three commute destinations — BRJ (Rådmann Halmrasts vei 5,
    Sandvika), MVV (Langbølgen 24, Lambertseter), MVV-UNI (Gaustadalléen 30) — are stable.
    They become config values but are not expected to churn.
  - Notification delivery stays with the external `notify` CLI → Pushover (separate repo).
- **SQLite stays** (not marked fixed by the owner, so it sits behind a thin repository layer to
  keep a later Postgres swap contained; no swap now).
- **Rebuild scope**: port of existing features + the Google API gateway/queue (core plumbing)
  + the new web map UI. Sold-price ingestion and good-listing alert rules come after, as
  separate designs on the new base.

## 3. Non-negotiable data preservation

1. **`main/database/properties.db` migrates, never regenerates.** Travel columns represent paid
   Google API calls; status history and sold listings cannot be re-scraped.
   (`data/eiendom.db` is a 0-byte decoy — ignore it.)
2. **The cached ad HTML archive** (`data/eiendom/html_extracted/`, gzipped snapshots) is kept —
   it powers reparsing without re-fetching and provides parser regression fixtures.
3. **Donor/reuse system semantics** port exactly: 300 m reuse radius, `travel_copy_from_finnkode`
   pointers, chain collapse, cycle detection, negative sentinels (-1 no-route / -2 unrealistic /
   -3 error) that prevent re-billing failures.
4. Before phase 1: back up the DB, and **revoke + rotate the Google Maps API key** currently in
   plaintext in `main/config/config.py`.

## 4. Rollout strategy

New `skannonser/` package in the same repo, alongside the frozen legacy code in `main/`,
`scripts/`, `apps_script/`. Same repo because golden-master verification needs both
implementations reading the same DB and HTML cache. Legacy paths retire one by one as ports are
verified; phase 6 deletes them entirely. No behavior changes are made to legacy code during the
rebuild (bug fixes land in the new implementation only, and get recorded as intentional diffs in
verification).

## 5. Architecture

Six components with hard boundaries, one CLI, everything non-interactive by default.

### 5.1 `skannonser/config`

- One typed settings module (pydantic-settings).
- **Secrets** — Google Maps API key, spreadsheet ID, Sheets service-account path, notify config —
  from `.env` / Docker secrets. Never in git, never in code.
- **Domain config** in a single TOML file, editable without touching code: Finn polygon points,
  commute destinations (name → address → DB column), price/area filters (today
  `SHEETS_MAX_PRICE=7 500 000`, `MIN_BRA_I=70`), donor reuse radius, rate/budget policy,
  schedule times.
- Config is validated at startup; the CLI has `skannonser config show` to print the effective
  merged configuration.

### 5.2 `skannonser/store`

- SQLite via small per-domain repositories: listings, enrichment, stations, annotations,
  metrics/history, api-ledger. No god class; SQL lives with its repository.
- **Versioned, numbered migrations** run only via `skannonser db migrate` — never on connect.
  Migration 001 adopts the existing live schema exactly as it is on disk (including drifted
  columns like `search_hit`), so the current DB file becomes version 1 without modification.
- New tables:
  - `annotations` — per-listing comments/tags/manual corrections from the web UI (supersedes the
    dead `listing_comments` and the sheet's Kommentar/Tag columns).
  - `polygon_points` — the Finn search polygon (today embedded in `finn_polygon_editor.py`
    source and re-parsed from source text at runtime).
  - `api_usage` — ledger for the API gateway (see 5.5).
- Cleanups performed as migrations, with the data verified first: reconcile the
  `eiendom_processed` orphan rows, drop the dead `listing_comments`, collapse `dnbeiendom`'s
  redundant `stale`/`active` pair.
- WAL mode on; one connection manager instead of per-method connect/close; batch upserts in
  transactions (today `insert_or_update_eiendom` commits per row).

### 5.3 `skannonser/ingest`

- Sources are plugins with a shared contract: `crawl() → raw refs`, `fetch() → cached HTML/JSON`,
  `parse() → NormalizedListing` records. The pipeline core handles caching, polygon filtering,
  upsert, and active/inactive lifecycle uniformly.
- **finn** source: ports crawl (polygon URL, pagination, page archival), the ad-HTML loader
  (atomic writes + gzipped dated snapshots — port as-is), and `parsing_helpers_common`
  field parsing verbatim. Fixes carried in: robust finnkode parsing (proper query-string parse,
  not `split('finnkode=')[1]`), drop the `len(href) <= 100` URL heuristic in favor of an
  explicit pattern.
- **dnbeiendom** source: ports the JSON-LD extraction (`ItemList` + `RealEstateListing`),
  point-in-polygon filter (one shared geometry util), and address+postcode matching to Finn rows.
  The ~10 superseded buffer/no-buffer variants collapse into this one path.
- **Status refresh** is a pipeline mode (`refresh`, `refresh-inactive`, `refresh-stale-open`)
  reusing the same fetch/parse path, writing status + append-only history.
- CSV intermediates (`0_URLs.csv`, `A_live.csv`) disappear: stages pass records in memory;
  the DB is the only sink. Raw-page archival remains for debuggability.
- The sold-price API source (future) implements the same contract; the design requires nothing
  further from it now.

### 5.4 `skannonser/enrich`

- Geocoding: ports Norway-restricted geocode, postcode validation, lat/lng swap correction,
  `geocode_failed` flagging.
- Travel times: **one parameterized function over configured destinations** replaces the three
  copy-pasted ~120-line loops. Semantics unchanged: TRANSIT mode, departure next Monday 08:00,
  per-destination columns as today.
- Donor/reuse, sentinels, and travel validation (neighbor/postcode/MAD heuristics + targeted
  re-request) port logically unchanged, as does donor-chain checking/repair tooling.
- Sentinel constants and helpers exist once (today duplicated verbatim in two files).
- Every outbound Google call goes through the gateway (5.5) — `enrich` contains no HTTP or
  rate-limit code of its own.

### 5.5 `skannonser/gateway` — Google API budget gateway (new)

The single choke point for all Geocoding and Routes calls, and the landing zone for two roadmap
items (usage warnings, quota queueing):

- **Persistent queue**: enrichment enqueues work items (finnkode, call type, params); the gateway
  drains them at a configured rate. Unprocessed items survive restarts (backed by `api_usage`
  ledger state, not memory).
- **Ledger**: every call recorded with type, timestamp, and outcome; daily and monthly usage
  derivable by query.
- **Budget policy** (from domain config): soft thresholds (e.g. 50%, 80% of the free-tier
  allowance) trigger a notify warning; the hard ceiling stops calls and defers the remaining
  queue to the next window. Defaults chosen so unattended runs can never surprise-bill.
- **Estimation kept**: `skannonser estimate` reproduces today's dry-run call-count predictions
  (max + optimistic-with-donor-reuse) without calling any API. The interactive confirm prompts
  are replaced by this command + the standing budget policy.

### 5.6 `skannonser/publish`

- **Sheets export, one-way only**: full rewrite of Eie, Sold, DNB, and Stations tabs on each
  sync. No cell diffing, no prompts, no reading manual columns back. This finally delivers the
  DNB tab (the current implementation is unreachable code).
- **Notify summaries**: daily (added/sold/delisted vs yesterday's snapshot) and weekly rollup
  port with their existing unit tests — the best-tested code in the repo. Delivery continues to
  shell out to the external `notify` CLI.

### 5.7 `skannonser/web`

FastAPI app serving a JSON API and the UI (replaces Apps Script + clasp entirely):

- **Map** (MapLibre GL): clustered markers, diff-based updates (no full teardown on toggle).
  Ports the current map's feature set: Eie/DNB/Sold layers with distinct marker styles and
  finnkode dedup, property-type colors with per-type visibility, metric filters that dim rather
  than hide (adjustable intensity), station overlays (radius circles, labels, per-line colors),
  the commute-to-Sandvika/Oslo-S station filter incl. transfer legs, polygon + bounds overlay,
  outside-boundary highlighting, listing popups with Finn/Maps links and thumbnails, and a
  missing-coordinates report. UI state persists in localStorage as today.
- **Table view**: sortable/filterable active-listings table — the "jump in and see everything in
  a spreadsheet" need, served without waiting on a Sheets export.
- **Annotations**: per-listing comments/tags/corrections, edited in the listing popup/table,
  stored in `annotations`, exported to the sheet as read-only columns.
- **Thumbnails**: served/cached by the web app (ports the Drive pre-hosting approach or serves
  from local cache — implementation plan decides; requirement is: no hotlinking Finn, no
  per-request scraping like Apps Script does today).
- Listing data endpoints return slim typed payloads (no raw-row dumps).
- Single-user, on a private server; auth is out of scope for this design (revisit if exposed).

### 5.8 CLI + scheduling

- One CLI (Typer): `run daily`, `refresh [--inactive|--stale-open]`, `estimate`, `sync-sheets`,
  `notify daily|weekly`, `db migrate|backup|stats`, `verify …`, `stations …`, `polygon …`.
  Replaces all 40+ Makefile targets; the Makefile is deleted in phase 6.
- Docker Compose, two services: `web` (FastAPI) and `scheduler` (supercronic running the daily
  pipeline, refreshes, and summaries on today's cadence: daily pipeline, notify 07:00 daily,
  weekly Sunday 08:00). Volumes: the DB file, HTML cache, config, secrets.
- Everything runs unattended; nothing calls `input()`.

### 5.9 Station & polygon tooling

- Station travel tables (stations/lines/travel, PDF timetable parsing, Oslo S transfer backfill,
  station geocoding) port as `skannonser stations …` subcommands, lowest priority — the data is
  already populated and stable.
- Polygon editing: the Leaflet editor is kept short-term; the polygon becomes DB-backed
  (`polygon_points`) with `skannonser polygon show|set`. A web-UI editor is a possible later
  addition, not in scope.

## 6. Verification along the way (core requirement)

The owner needs to check outputs at every step, now and when extending the system later.

- **Golden-master harness**: `skannonser verify <stage>` runs legacy and new implementations on
  the same inputs and diffs the outputs. Zero unexplained differences is the bar to retire a
  legacy path; intentional fixes (e.g. finnkode parsing) are recorded in an explicit allowlist
  with justification.
  - `verify parse` — cached ad HTML → parsed fields, old parser vs new.
  - `verify enrich` — donor resolution + estimate counts on the current DB, old vs new (no API calls).
  - `verify sheets` — sheet payload rows, old export query vs new.
  - `verify metrics` — daily metrics from a fixed snapshot, old vs new.
- **Regression fixtures**: a curated set of real cached ad pages (covering the known edge cases)
  becomes a pytest fixture suite for the parsers, kept after the rebuild.
- **Unit tests**: ported pure logic keeps/extends its tests (notify metrics tests carry over);
  new code (gateway policy, repositories, geometry) gets tests as built. pytest replaces the
  stdlib-unittest scattering.
- The harness outlives the rebuild: phase 6 keeps `verify` and the fixtures as the standing way
  to check outputs when adding sources/features.

## 7. Explicitly dropped (approved)

- Sheet→DB comment sync (never worked) and the sheet-edit workflow for Kommentar/Tag.
- Interactive cell-diff sheet updater and full-overwrite confirm prompts.
- Apps Script map + clasp deployment (after phase 5 parity).
- Archived rental/jobs pipelines (`main/update.py`, `main/export.py`, `extractors/archived/`).
- OSM Nominatim/Overpass walk-distance dead code; legacy travel columns no longer fetched.
- Legacy CSV flow (`B_aligned`, `C_filtered`, `AB_processed`, `_tmp_sheet_eie`, dated dumps)
  and the redundant `AB_processed.csv` write in post-process.
- `main/temp/`, `tmp/` one-offs; superseded DNB extractor/filter variants; misspelled Make aliases;
  the `Eie(unlisted)` no-op sync; the 0-byte `data/eiendom.db`.

## 8. Phases

Each phase ends with a working system and a concrete verification the owner can run.

1. **Skeleton** — package layout, config (secrets moved to `.env`, **old API key revoked/rotated**),
   store + migration 001 adopting the live DB, CLI scaffold, Docker Compose running on the server.
   Checkpoint: `skannonser db stats` matches known row counts; legacy pipeline still runs untouched.
2. **Ingest port** — Finn source, then DNB source, then refresh modes. Checkpoint: `verify parse`
   clean on the fixture set; a supervised parallel run produces identical DB effects.
3. **Enrich port** — geocoding + travel + donor system behind the gateway; budget policy live.
   Checkpoint: `verify enrich` clean; `estimate` parity with the legacy preview; first real
   enrichment run done at low rate with ledger inspection.
4. **Publish port** — one-way Sheets export (incl. the first working DNB tab) + notify summaries.
   Checkpoint: `verify sheets`/`verify metrics` clean; cron cutover of the daily pipeline to the
   new CLI.
5. **Web app** — API, map, table, annotations; thumbnail serving. Checkpoint: side-by-side with
   the Apps Script map for feature parity + performance; then Apps Script retired.
6. **Teardown** — delete `main/`, `scripts/`, `apps_script/`, Makefile, dead files and CSVs;
   final README describing the new system; `verify` + fixtures remain.

## 9. Out of scope (next designs, on this base)

- Sold-price API source (which API, matching sold records to listings, price-history UI).
- Good-listing alert rules (what "really good" means, scoring, alert delivery).
- Postgres swap; multi-user auth; web-based polygon editor.
