# skannonser

A personal real-estate scanner for the Oslo area. It crawls [Finn.no](https://www.finn.no)
and DNB Eiendom every night, stores everything in a local SQLite database, enriches
active listings with geocoding and public-transit commute times to three fixed
destinations (via Google's Geocoding and Routes APIs, behind a monthly budget), and
publishes the result two ways: a read-only Google Sheet and a tailnet-only web app
(a MapLibre map + sortable table + free-text annotations). A daily/weekly digest goes
out via Pushover (through the `notify` CLI).

This is a from-scratch rebuild of an earlier ad-hoc script collection (`main/`, now
deleted). The rebuild is complete and running in production. See
[History](#history) below for how it got here.

## Architecture

Everything lives under `skannonser/`, laid out by pipeline stage:

- **`config/`** — `domain.toml` loader (`domain.py`, filters/budget/destinations/polygon/
  DNB region GUIDs/crawl-pacing — the tuning surface) and `settings.py` (env/`.env`-backed secrets:
  API keys, spreadsheet id, DB path).
- **`store/`** — the SQLite layer: `connection.py` (WAL-safe connect helper),
  `migrations.py` (numbered, versioned SQL migrations applied explicitly, never on
  connect), and `repositories/` (`listings.py` for `eiendom`/FINN, `dnb.py` for
  `dnbeiendom`, `processed.py` for `eiendom_processed` travel/address data) — batched
  upsert + inactive-lifecycle logic per source.
- **`ingest/`** — crawling and parsing. `finn/` (`crawl.py` result-page crawler,
  `parse.py` ad-HTML parser, `refresh.py` status re-checks, `html_cache.py` the on-disk
  ad-HTML cache, `parse_details.py` the listing-details parser [soverom/eieform/
  fasiliteter/energimerke/totalpris/felleskost/matrikkel from the same cached ad HTML],
  `backfill.py` the offline details re-parse used by `tools backfill-details`) and
  `dnb/` (`crawl.py`, `parse.py` JSON-LD parser, `load.py` polygon filter + FINN address
  matching). `base.py` holds the shared normalized-listing model.
- **`enrich/`** — post-ingest enrichment, all API calls routed through `gateway.py`
  (see below). `geocode.py` (Google Geocoding, 3-pass Norway strategy), `travel.py`
  (the orchestrator for BRJ/MVV/MVV-UNI commute times via `travel_api.py`, Google
  Routes), `donor.py` (nearby-listing travel-time reuse to cut API spend), `dnb_travel.py`
  (BRJ/MVV backfill for DNB-only rows), `thumbs.py` (nightly local thumbnail cache),
  `validate.py` (read-only outlier scoring for stored travel values), `sentinels.py`
  (negative-int failure codes stored in place of a real value).
- **`gateway.py`** — the single choke point for paid Google APIs: per-minute rate
  limiting, monthly budget enforcement (with warn-threshold Pushover pings), and a
  call ledger in the `api_usage` table. Nothing calls Geocoding/Routes directly.
- **`pipeline.py`** — the FINN/DNB ingest orchestration (crawl → fetch/parse → upsert
  → mark-inactive) with guards against wiping the active set on a failed/empty crawl.
- **`nightly.py`** — the full nightly run: ingest(finn) → ingest(dnb) → geocode →
  enrich(all) → enrich(mvv_uni) → enrich-dnb → refresh(stale-open) → thumbs → sheets,
  each step isolated so one failure doesn't skip the rest.
- **`publish/`** — `rows.py`/`export.py` build the Eie/Sold/DNB/Stations sheet payloads
  (and back the web API's listing query), `sheets_client.py` wraps the Google Sheets
  service-account client (tab read/clear/rewrite), `annotations.py` does the one-time
  rescue of hand-typed Kommentar/Tag sheet columns into the `annotations` table.
- **`notifications.py`** — daily/weekly added/removed summaries, sent through the
  `notify` CLI (Pushover).
- **`web/`** — `app.py` (FastAPI app: `/healthz`, `/thumbs/{id}.jpg`, `/table`, static
  file serving, gzip), `api.py` (`/api/listings` [`?bucket=sold` for the sold-only
  payload], `/api/listings/{finnkode}`, `/api/meta`, `/api/missing-coords`,
  `/api/annotations/{finnkode}` GET/PUT; sold items carry the tinglyst
  `sold_price`/`sold_date`/`price_suggestion`, every item carries `scraped_at`),
  `static/` (MapLibre map, table view, filters, popups — plain JS, no build step).
  Map niceties: mobile drawer layout, collapsible sidebar panels, per-tag
  visibility + tag rings, "Ny"/"nye siden sist" freshness, sold-price rows in
  popups + a "budpremie" colour mode for sold dots, polygon-fit start view.
  Listing details (soverom/eieform/fasiliteter/energimerke/totalpris/felleskost)
  ride along in `/api/listings` and `/api/meta`, with derived totalpris-per-kvm
  and månedskost (felleskost + kommunale avg/12) computed in the API at query
  time (never stored); the table gains Totalpris/Total-per-kvm/Felleskost/
  Mnd-kost/Soverom/Eieform/Energi columns, all sortable.
  **Unified filtering** (2026-07-24): map and table share ONE filter state
  (`static/filterstate.js`, localStorage-backed, live-synced across open tabs
  via `storage` events) and one predicate (`listingExcluded` in
  `static/filters.js`). Sliders for continuous values (pris/totalpris/
  felleskost/mnd-kost/total-per-kvm/BRA-i/soverom/byggeår/reisetid), checkbox
  sets for small vocabularies (boligtype/eieform/energimerke/tag/
  tilgjengelighet), searchable multi-selects for postnummer/nabolag, and a
  "må ha fasiliteter" set — rendered as sidebar panels on the map and as
  Notion-style column-header popovers on the table (`static/tablefilters.js`).
  Filtered-out listings DIM on the map ("Filtret nedtoning" at 100 % hides them,
  clusters included) and are hidden from the table; the "inkluder ukjent"
  toggle is the single null-value policy. Both pages carry an active-filter
  count and a "Nullstill filtre" reset.
- **`ids.py`** — shared path-safe identifier helpers (DNB synthetic ids, thumbnail
  filenames) used by both `web/api.py` and `enrich/thumbs.py` so they can't drift.
- **`geo.py`** — polygon point-in-region test used by the DNB filter.
- **`textnorm.py`** — address/postcode string normalization shared by ingest and match
  logic.
- **`commands/`** — the Typer CLI wiring, one module per subcommand group
  (`run_cmd.py`, `db_cmd.py`, `config_cmd.py`, `notify_cmd.py`, `estimate_cmd.py`,
  `tools_cmd.py`, `web_cmd.py`); `cli.py` assembles them into the `skannonser` entry
  point.

## Ops runbook

**Server:** `mbp2016@100.77.139.22` (tailnet), repo at `~/kode/skannonser`. No Docker
for the CLI itself in dev, but the deployed services run in Docker (`docker-compose.yml`):

- **`scheduler`** — builds from `docker/Dockerfile`, runs `supercronic` on
  `docker/crontab` (currently just the nightly DB backup, `skannonser db backup --keep 30`
  at 03:00 UTC, keeping the newest 30). Mounts `main/database` (the live DB),
  `data/`, `config/`, `backups/`.
  Actual pipeline runs (ingest/enrich/sheets) are NOT run from this container's
  crontab — they're driven by the server's own crontab calling a wrapper script
  (`~/run_skannonser_daily.sh`, deployable copy at `ops/run_skannonser_daily.sh`)
  fired at a fixed early time, which sleeps a random start-jitter
  (`SKANNONSER_START_JITTER_S`, default 6h) before invoking `skannonser run nightly`
  so runs don't hit FINN at the same wall-clock minute daily;
  `skannonser notify daily`/`weekly` run off separate 07:00 / Sunday-08:00 cron
  entries.

  **Polite-access crawling:** FINN/DNB fetches send a browser `User-Agent`
  (`skannonser/http.py`, not the default `python-requests/…`) and pace themselves
  with wide jittered delays between result pages, ad fetches, and refresh listings.
  The delay ranges are the `[crawl]` section of `config/domain.toml`
  (`page_delay_*`/`fetch_delay_*`/`listing_delay_*`, seconds); slow by design (a run
  fetches only tens of pages/ads, so a paced run still finishes well under an hour).

  **Sold-price enrichment (separate, throttle-guarded):** `skannonser/enrich/sold.py`
  + `store/repositories/sold.py` + migrations `006_sold_prices.sql`/`007_sold_sweep_state.sql`
  fetch tinglyst sold prices from FINN's sold map into `sold_prices`, keyed by
  finnkode. `skannonser run enrich-sold` runs one budgeted **backlog** pass:
  suspend-aware, coverage-aware (targets listings sold >100 days ago, stops at
  80% coverage), fewest-prior-attempts-first with densest-clusters-first as the
  tiebreak (migration `009_sold_attempts.sql` ledgers per-target attempts so
  never-tinglyst sales can't monopolise the budget), hard-capped at
  `--requests` (default 4),
  querying a tight ~120 m box centered on each target listing (with one adaptive
  shrink if the target is crowded out of the 15-card cap). On throttle
  (429/403/503 or a block page) it **suspends itself, persists that, and pings
  Pushover** — every later run is then a no-op until `--resume`. `--status`
  reports coverage without any request; `--bbox` probes a single tile.
  Run it a few times a day, spaced out, via `ops/run_sold_backlog.sh` and its
  cron snippet — deliberately **separate from and not part of** `nightly.py`
  (a test enforces the latter). It targets a `robots.txt`-disallowed FINN path
  (`/map/`); keep the cadence low and let it stop itself if FINN pushes back.

  **Listing-details enrichment (no extra FINN traffic):** `skannonser/ingest/finn/parse_details.py`
  + `store/repositories/details.py` + migration `010_listing_details.sql` derive a
  `listing_details`/`listing_facilities` cache (soverom, eieform, energimerke,
  totalpris/felleskost and other pricing-details fields, matrikkel, facility list)
  by re-parsing ad HTML that FINN ingest/refresh already caches to disk — nothing
  new is fetched. It's rebuildable offline any time via `skannonser tools
  backfill-details [--wipe|--status]`, which replays `html_extracted/*.html`
  through the same parser.
- **`web`** — same image, runs `skannonser web --host 0.0.0.0 --port 8000`, published
  as `100.77.139.22:8377:8000` (tailnet-only — not bound to `0.0.0.0` on the host, so
  it's unreachable from the LAN). Healthcheck hits `GET /healthz` every 60s.

**`.env` (gitignored, 0600) keys** — see `.env.example`:

- `GOOGLE_MAPS_API_KEY` — Geocoding + Routes.
- `SPREADSHEET_ID` — the target Google Sheet.
- `GOOGLE_SERVICE_ACCOUNT_FILE` — path to the service-account JSON (default
  `main/config/thumbnail-service-key.json`).
- `NOTIFY_BIN` — the `notify` CLI binary name/path used for Pushover sends (default
  `notify`).
- `SKANNONSER_DB_PATH` (optional override; commented out in `.env.example`) — defaults
  to `main/database/properties.db`.

**Logs:** cron/wrapper output on the server lands under `~/skannonser-logs/`; container
logs via `docker compose logs scheduler` / `docker compose logs web`.

**Common commands** (`skannonser --help` for the full tree):

```
skannonser config show                          # effective config, secrets masked
skannonser db stats                              # row counts per table (quick health check)
skannonser db backup [--keep N]                  # online SQLite backup
skannonser db migrate                            # apply pending numbered migrations
skannonser run ingest                            # crawl+parse+upsert FINN/DNB
skannonser run refresh                           # re-check status of existing listings
skannonser run geocode                           # fill missing lat/lng (Geocoding API)
skannonser run enrich [--targets all|brj|mvv|mvv_uni]  # fill missing commute times
skannonser run enrich-dnb                        # BRJ/MVV commute times for DNB-only rows
skannonser run validate-travel                   # read-only outlier scan, no API calls
skannonser run sheets                            # rewrite Eie/Sold/DNB/Stations tabs
skannonser run nightly                           # the full nightly sequence, in order
skannonser estimate [--targets ...]               # predict enrich API-call volume, no calls
skannonser notify daily | weekly                 # Pushover summary via NOTIFY_BIN
skannonser web [--host --port --db]               # serve the FastAPI app (default :8377)
skannonser tools import-sheet-annotations         # one-time Kommentar/Tag → annotations rescue
skannonser tools backfill-details [--wipe|--status]  # offline re-parse of cached ad HTML into listing_details/listing_facilities
```

**Backup/restore:** `skannonser db backup --keep N` copies the live DB via SQLite's
online backup API (safe under WAL) into `backups/`; the `scheduler` container runs
this nightly at 03:00 UTC keeping 30. To restore, stop anything writing to the DB and
copy a `backups/properties-*.db` file over the live path (`main/database/properties.db`
by default).

**Budget policy:** `config/domain.toml`'s `[budget]` section caps Geocoding and Routes
calls per month (`geocode_monthly_cap`, `routes_monthly_cap`, default 9000 each) with
warn thresholds (`warn_pcts`, default `[50, 80]`) that trigger a Pushover notification
via the gateway. Both APIs are also per-minute rate-limited (`*_rpm`). Every call goes
through `gateway.py`'s ledger (`api_usage` table); exceeding the cap raises
`BudgetExceeded`, which `run enrich`/`run enrich-dnb`/`run geocode`/`run nightly` treat
as a clean stop (exit 3 for the direct commands; `run nightly` records it as
`budget_exhausted`, not a step failure, and still exits 0 if nothing else broke).
Donor/reuse logic (`enrich/donor.py`) cuts real spend further by reusing a nearby
listing's already-fetched travel time within `reuse_within_meters` (default 300m).

## Development

```
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
pytest tests/rebuild -q      # 616 tests, zero warnings
```

The standing correctness checks (now that the legacy `main/`-comparison verify
harnesses are gone) are:

- **The full pytest suite** (`tests/rebuild/`, 616 tests).
- **The fixture corpora** (`tests/rebuild/fixtures/`) — real, previously-legacy-verified
  FINN/DNB HTML pinned as golden input/output, so parsing behavior stays correct
  without needing the old `main.*` code at test time.
- **The packaging structural test** (`tests/rebuild/test_packaging.py`) — proves the
  built wheel is complete (migrations etc. actually ship).
- **`GET /healthz`** on the deployed web app — live liveness/readiness check.

**Migrations:** numbered SQL files under `skannonser/store/migrations/` (currently
`001`–`010`), applied explicitly and atomically (one transaction per file, with
rollback) by `skannonser db migrate` — never implicitly on connect. To add one, drop
in the next-numbered `.sql` file and run `skannonser db migrate`.

**Domain configuration:** `config/domain.toml` holds the tunable knobs — price/size
filters, the FINN search polygon, DNB region GUIDs, budget caps, and the three travel
destinations (BRJ, MVV, MVV-UNI) with their addresses and DB/sheet column names. This
is the file to edit for anything domain-specific; no code change needed.

## Follow-ups & standing notes

*(Carried over from the retired `docs/rebuild/STATUS.md`, 2026-07-23. Standing
rule kept from there: any new issue that can't be fixed immediately gets added
HERE in the same commit that discovers it — never only in chat.)*

- **`eiendom.updated_at` is NOT "last seen"** — upserts only bump it when a
  column actually changes. Use `scraped_at` (first seen) or `active` instead.
- **DNB activate-on-2nd-appearance quirk still exists** (`dnbeiendom`'s missing
  `active` default; the eiendom-side quirk was killed 2026-07-21). Deliberate,
  unported.
- **Targeted travel re-request tool never ported** — `run validate-travel` only
  flags suspicious rows; re-requesting them is manual. Low priority.
- **Deferred minors** (none blocking): `deactivate_missing` empty-list guard;
  AliasChoices for `SKANNONSER_DB_PATH`; anchor `DEFAULT_DOMAIN_PATH`;
  `require_db()` on a 4th db command; supercronic checksum in Dockerfile;
  backup `PRAGMA journal_mode=DELETE`; crawl archive `response.text` vs
  legacy `content`; progress logging in long crawls.
- **Server DB safety copies** live at `~/skannonser-pre*.db` on mbp2016 (eight
  snapshots, 2026-07-20 → 07-22) — cheap insurance, delete manually someday.
- **Sheet postnummer stays bug-compatible** (Sheets coerces `"0581"` → `581` on
  its own); the DB/web values are zero-padded (migration 008 backfilled the
  legacy-stripped rows). A `norm_postnummer` apostrophe-prefix fix exists in
  `skannonser/publish/export.py` if the Sheet display ever needs to match.
- **Deploy note (listing-details enrichment):** after deploying this feature to
  the server, run `skannonser db migrate` then `skannonser tools backfill-details`
  once (~5,900 local parses from already-cached ad HTML, no network, ~1 min) to
  populate `listing_details`/`listing_facilities` for existing rows; new rows
  fill in automatically on the next ingest/refresh.

## History

This codebase is the result of a ground-up rebuild (2026-07-20 → 2026-07-22) of an
earlier script collection, followed by a web-app UX/feature pass (2026-07-23:
sold prices in the UI, freshness, tags, mobile layout, plus the fixes listed in
that day's commits). For the full rebuild story — design decisions,
phase-by-phase progress, sanctioned behavior changes vs. the legacy system, and
the production cutover — see:

- `docs/superpowers/specs/` — the design specs (rebuild + earlier features).
- `docs/superpowers/plans/` — the phase-by-phase implementation plans.
- Git history — `docs/rebuild/STATUS.md` (the rebuild's running end-state
  record, retired 2026-07-23) is fully preserved there.
