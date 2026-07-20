# skannonser — Feature inventory & rebuild assessment

Date: 2026-07-20. Produced by a full read-through of the codebase ahead of a possible re-architecture.
Status legend: ✅ working · ⚠️ working but fragile/awkward · ❌ broken · 🪦 dead/legacy (candidate for deletion, not porting)

## System overview

```
Finn.no ──crawl──> 0_URLs.csv ──extract──> A_live.csv ──post-process──┐
DNB Eiendom ──crawl/extract (JSON-LD)──> A_live.csv ──polygon filter──┤
                                                                      v
                                    SQLite (main/database/properties.db)
                                      eiendom + eiendom_processed + dnbeiendom
                                      + stations/station_lines/station_travel
                                                                      │
                 Google Geocoding + Routes APIs (enrichment) <────────┤
                                                                      v
                                    Google Sheets (Eie / Sold / DNB / Stations / Polygon)
                                                                      v
                                    Apps Script web app → interactive Google Map
Orchestration: 467-line Makefile + cron on a personal machine ("mbp") — now moving to a server.
Notifications: main/notify (summaries/metrics) → external `notify` CLI (separate repo) → Pushover.
```

## 1. Ingestion

- ✅ **Finn crawl** — builds search URL from polygon + filter config, paginates results, regex-extracts `finnkode` ad URLs, archives raw pages (`main/crawl.py`).
- ✅ **Finn ad extraction** — per-ad HTML fetch with caching (atomic writes, gzipped dated snapshots on change — `main/extractors/ad_html_loader.py`), BeautifulSoup parsing of address/price/areas/status/year/image (`parsing_helpers_common.py`).
- ✅ **DNB crawl + extraction** — paginated search, listings read from embedded JSON-LD (`ItemList` / `RealEstateListing`), no HTML scraping (`extract_dnbeiendom.py`, `extract_dnbeiendom_ads.py`).
- ✅ **Polygon filtering** — Finn filtered at source via `polylocation` URL param; DNB post-filtered with ray-cast point-in-polygon (`filter_and_load_dnbeiendom_no_buffer.py`).
- ✅ **Cross-source dedup** — DNB listings matched to Finn rows by normalized address+postcode (`MatchedFinn_Finnkode` / `duplicate_of_finnkode`).
- ✅ **Active/inactive lifecycle** — listings absent from the latest crawl are marked inactive, never deleted.
- ⚠️ **Status refresh** — re-downloads listing pages to update availability (`sold`/`inaktiv`), with variants for inactive-only and stale-open (`main/sync/refresh_listings.py`); works but drives interactive Make targets.
- ⚠️ **Fragile parsing details** — `finnkode` parsed with naive `split('finnkode=')[1]`; crawl drops URLs longer than 100 chars as a heuristic.
- 🪦 **Rental/jobs pipelines** — `main/update.py` + `main/export.py` + `extractors/archived/*` (rental, Finn jobs, NAV jobs). `update.py` imports a module that no longer exists; entire scope abandoned.

## 2. Enrichment (Google APIs)

- ✅ **Geocoding** — Google Geocoding API for missing LAT/LNG, Norway-restricted, postcode validation, lat/lng swap correction, `geocode_failed` flag (`tools/fill_missing_coordinates.py`).
- ✅ **Travel times** — Google Routes API (TRANSIT) to three hardcoded destinations: work "BRJ" (Sandvika), "MVV" (Lambertseter), "MVV UNI" (Gaustadalléen); departure = next Monday 08:00 (`location_features.py`, `post_process.py`).
- ✅ **Donor/reuse system (the crown jewel of cost control)** — listings within 300 m copy travel values from a nearby "donor" listing instead of calling the API; includes donor-chain collapse, cycle detection, repair tooling (`check_donor_chains.py`, `populate_travel_from_donors.py`, `backfill_donor_links.py`).
- ✅ **Cost gating** — pre-run candidate counting and API-call estimation (max + optimistic with in-run reuse), interactive confirm with adjustable requests-per-minute, `TRAVEL_AUTO_CONFIRM` / `TRAVEL_REQUESTS_PER_MINUTE` env overrides, skip-if-zero-candidates preflight.
- ✅ **Failure sentinels** — negative travel values (-1/-2/-3) encode no-route/unrealistic/error so failed rows aren't re-billed.
- ✅ **Travel validation** — heuristic detection of suspicious stored travel values (neighbor/postcode-group comparison, MAD outliers) + targeted re-request of flagged rows only (`validate_travel_values.py`, `rerequest_suspicious_travel.py`).
- ✅ **Manual overrides** — per-finnkode price/address corrections applied at upsert (`manual_overrides` table; currently 0 rows).
- 🪦 **OSM walk-distance features** — Nominatim/Overpass grocery + transit walking classes exist but are never invoked.
- 🪦 **Legacy travel columns** — `pendl_morn_*`, `bil_*` etc. migrated forward on every startup but no longer fetched.

## 3. Storage (SQLite)

Live DB: `main/database/properties.db` (~4.8 MB). **`data/eiendom.db` is a 0-byte decoy.**

- ✅ `eiendom` (5 863 rows) — canonical Finn listings, PK `finnkode`.
- ✅ `eiendom_processed` (6 141 rows) — 1:1 sidecar: coords, cleaned address, ~20 travel columns, donor pointer. (Has orphan rows vs `eiendom`.)
- ✅ `dnbeiendom` (1 173 rows) — parallel DNB store, PK `url`, with `duplicate_of_finnkode`.
- ✅ `stations` / `station_lines` / `station_travel` (136/213/387) — transit stations, per-line membership, per-destination minutes incl. transfer legs.
- ✅ `eiendom_status_history`, `daily_listing_snapshot`, `daily_metrics` — append-only status log + notify metrics (DDL exists; tables created lazily).
- ⚠️ **Schema managed by startup ALTER TABLEs** — migrations run inside `_init_db` on every connect; live schema has drifted from the DDL (`search_hit` exists only on disk). No versioning.
- ⚠️ **Polygon points live in Python source** (`finn_polygon_editor.py`), re-parsed from source text at runtime by the DNB filter.
- ❌ `listing_comments` — table exists, sync code calls DB methods that don't exist; feature never functioned. 0 rows.
- 🪦 Legacy CSVs (`B_aligned.csv`, `C_filtered.csv`, `AB_processed.csv`, `_tmp_sheet_eie.csv`, dated dumps) — residue of the pre-SQLite flow; `post_process.py` still redundantly writes `AB_processed.csv`.

## 4. Google Sheets sync

- ✅ **DB→Eie tab**: append-new + cell-level diff updates (normalized value comparison, API-derived vs manual column classes, confirmation before overwriting non-null cells).
- ✅ **DB→Sold tab**: full rewrite of sold/inactive listings.
- ✅ **DB→Stations tab** + **Finn Polygon Coords tab** full rewrites.
- ✅ **Sheet housekeeping**: dedupe duplicate finnkode rows, prune no-longer-visible rows, header alias canonicalization, HYPERLINK-formula parsing.
- ✅ **Sheet-only column preservation** (LAT/LNG and manual columns survive updates).
- ⚠️ **Interactive `input()` gates** in full-sync and cell-update paths — blocks unattended server runs.
- ❌ **DB→DNB tab sync** — the function is accidentally defined *nested inside* the Sold-sync function and is unreachable.
- ❌ **Sheet→DB comment sync** — calls nonexistent DB methods; crashes if run.

## 5. Interactive map (Apps Script)

- ✅ Layers: Eie (circles), DNB (squares), Sold (grey), toggleable, deduped by finnkode.
- ✅ Property-type color coding with per-type visibility and custom colors.
- ✅ Metric filters that dim (not remove) non-matching markers; adjustable dim intensity.
- ✅ Station overlays: radius circles, labels, per-line colors, commute-to-Sandvika/Oslo-S filter with transfer-leg math.
- ✅ Finn polygon + bounds overlay; outside-boundary highlighting; proximity dimming.
- ✅ Popups with Finn/Maps links, full-row detail expansion, lazy thumbnails (server-side og:image scrape, base64-inlined, 6 h cache).
- ✅ Missing-coordinates report; UI state persisted in localStorage.
- ⚠️ **Performance design is the bottleneck**: legacy `google.maps.Marker` (no clustering), full teardown + rebuild of *all* markers on nearly every toggle/slider, full raw-row payloads serialized through `google.script.run`, geometry computed in Apps Script per request.
- ⚠️ **Deployment**: manual `clasp push/deploy` with `sed`-parsing of clasp output for the deployment ID.

## 6. Notifications

- ✅ **Daily summary** (07:00 cron) — diff of active set vs yesterday's snapshot: added/sold/delisted, metrics persisted.
- ✅ **Weekly summary** (Sunday 08:00) — 7-day rollup.
- ✅ **Pure-logic metrics module** with real unit tests (the best-tested code in the repo).
- ✅ **Delivery extracted** to a separate `notify` CLI repo (Pushover, battery monitor, heartbeat) — this repo just shells out to it.
- ⚠️ Stale `notify-battery` Make target points at a module that moved out.

## 7. Stations & timetables

- ✅ PDF timetable parsing (`rutetabeller tog/*.pdf`) → per-line station→destination minutes (`fill_station_travel_from_pdf.py`, 814 lines).
- ✅ Station geocoding, Oslo S transfer backfill, sheet↔DB station sync.

## 8. Ops & tooling

- ✅ `manage.py` CLI umbrella (stats, scrape, sync, backup, …).
- ✅ Visual Leaflet polygon editor for the Finn search boundary.
- ✅ Thumbnail pre-hosting to Drive (dodges Finn hotlink blocking).
- ⚠️ **Orchestration = 467-line Makefile** — pipeline logic (candidate counting, conditional API gating, confirm prompts, env plumbing) lives in shell; `full`/`full-no-scrape` duplicate large interactive blocks; untestable.
- ⚠️ **Scheduling = cron on "mbp"**, wrapper script lives outside the repo; `.github/workflows/` is empty and `make gha` doesn't exist despite being documented.
- ⚠️ **Secrets**: live Google Maps API key in plaintext `main/config/config.py`; OAuth tokens/service-account keys sit in the working tree (gitignored but unmanaged); spreadsheet ID and all three commute destinations hardcoded in source.
- 🪦 `main/temp/` and `tmp/` — ~30 one-off scripts, dated CSVs and logs mixed into the repo; ~10 superseded DNB extractor/filter variants; misspelled back-compat Make targets.

## What must be preserved regardless of approach

1. **The SQLite data.** Travel times = paid API calls; status history and sold listings are unre-scrapeable. The DB migrates, never regenerates.
2. **The donor/reuse + sentinel + validation system** — hard-won cost control, port logic as-is.
3. **Finn/DNB parsing knowledge** — accumulated edge-case handling in the parsers.
4. **The cached ad HTML archive** — enables reparsing without re-fetching.
5. **The Google Sheet** as long as it's the UI hub (manual columns like Kommentar live only there).

## Assessment summary

The domain logic is sound, battle-tested, and worth keeping. The *structure* is what hurts:
half-migrated CSV→SQLite pipeline, a 1 731-line DB god-class, a ~970-line post-process function
with three copy-pasted travel loops, interactive prompts blocking server automation, orchestration
in Make/shell, two genuinely broken features (DNB sheet sync, comment sync), ~40% of files
dead or superseded, secrets in plaintext, and a map UI whose architecture (no clustering,
full re-render, Sheets-as-API) is the direct cause of the performance problem.

**Recommendation: not a from-scratch rewrite, but a "new skeleton, same organs" rebuild** —
a clean package/repo with proper config, storage, and orchestration layers, into which the proven
logic (parsers, donor system, cost gating, metrics) is ported module by module, each port verified
against the current system's outputs (golden-master comparisons on the same input data) before the
old path is retired. This gives the checkable-along-the-way property without betting everything on
a big-bang rewrite.
