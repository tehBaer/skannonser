# Rebuild Phase 5 — Web App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A tailnet-only FastAPI web app on the server: MapLibre map with clustering (full Apps Script feature parity), sortable table view, inline annotations, and locally-cached thumbnails — the performant replacement for the Sheets+Apps Script UI.

**Architecture:** `skannonser/web/` serves a JSON API (listings with donor-resolved travel + annotations, meta with polygon/stations/filters) and a vanilla-JS + MapLibre frontend (vendored assets, no CDN, no build step). The listing row query is extracted from the sheet exporter into a shared module so API and Sheets export can never drift (golden-master re-run proves the refactor). A nightly thumbnail-cache step downloads each listing's image once; the web app serves them locally — the Drive-hosting machinery (`image_hosted_url`) officially retires. Docker compose gains a `web` service bound to the host's Tailscale IP.

**Tech Stack:** FastAPI + uvicorn (new deps), httpx (dev/test), MapLibre GL JS (vendored), vanilla ES modules, OSM raster basemap tiles.

## Global Constraints

- `.venv/bin/python` only; tests via `.venv/bin/python -m pytest tests/rebuild -q` — count grows from 423, ZERO warnings stays the bar.
- **The production nightly path may only change in two sanctioned ways:** (1) the shared-row-query refactor (Task 2 — `verify sheets` MUST re-run zero-diff to prove the Sheets payload is untouched), (2) the thumbnail-cache step added to `run_nightly` (Task 5). Nothing else touches pipeline/enrich/publish behavior.
- **No auth, tailnet-only**: the compose `web` service publishes its port ONLY on the host's Tailscale IP (`100.77.139.22`). Never `0.0.0.0` on the host side.
- Frontend: vanilla ES modules, no framework, no build step, no CDN at runtime — MapLibre JS/CSS vendored into `skannonser/web/static/vendor/` (committed). Basemap: OSM raster tiles (fair-use, single user).
- Keep JS thin — anything computable server-side (donor resolution, filter bounds, station transfer math) comes from the API; JS does rendering + interaction only.
- Apps Script map + Sheets export keep running unchanged — retirement only after a user-approved trial period (Task 9 records the trial start; the retirement itself is OUT of this phase).
- No real network in tests (fake fetch for thumbnails; httpx TestClient in-process for API). No `input()`.
- Commits per green cycle; messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: FastAPI skeleton + CLI

**Files:** Create `skannonser/web/__init__.py` (empty), `skannonser/web/app.py`; Modify `pyproject.toml` (deps `fastapi>=0.110`, `uvicorn>=0.29`; dev `httpx>=0.27`), `skannonser/commands/` (new `web_cmd.py`, register in cli.py); Test `tests/rebuild/test_web_app.py`.
**Interfaces:** `create_app(db_path: Path, domain: DomainConfig | None = None, thumbs_dir: Path | None = None) -> FastAPI` — lazy per-request sqlite connections (read-only URI mode for GET endpoints); `/healthz` → `{"status":"ok","db":true}` (checks the DB opens + migrations current, 503 otherwise); serves `static/` at `/` (index.html later). CLI `skannonser web [--host 127.0.0.1] [--port 8377] [--db PATH]` running uvicorn (pending-migrations fail-loud first, like other commands).
- [ ] TDD: TestClient healthz 200 on a migrated tmp DB; 503 on a pending-migrations DB; CLI registration test. Implement; suite green; commit `rebuild(phase5): fastapi skeleton, healthz, web command`.

---

### Task 2: Shared listing rows (refactor with golden-master proof)

**Files:** Create `skannonser/publish/rows.py`; Modify `skannonser/publish/export.py`; Test `tests/rebuild/test_export.py` (must keep passing unchanged).
**Interfaces:** `listing_rows(conn, *, include_hidden_fields: bool = False) -> list[dict]` — the Eie visibility-filtered, donor-resolved row dicts currently built inside `export.eie_rows` (same SQL, same fields, keys = the sheet header names; `include_hidden_fields=True` additionally returns `finnkode`-keyed extras the web needs: lat/lng floats, boligtype raw, image_url, active). `export.eie_rows` becomes a thin consumer (ordering + normalization unchanged).
- [ ] Steps: extract with NO behavior change; `test_export.py` green UNCHANGED; **gate: `.venv/bin/skannonser verify sheets` on a laptop DB copy → zero diffs (record output)**; commit `rebuild(phase5): shared listing row query - exporter refactor, golden-master proven`.

---

### Task 3: Listings/meta API

**Files:** Create `skannonser/web/api.py` (router); Modify `skannonser/web/app.py`; Test `tests/rebuild/test_web_api.py`.
**Interfaces:**
- `GET /api/listings` → `{"listings": [ {finnkode, adresse, postnummer, pris, pris_kvm, boligtype, tilgjengelighet, lat, lng, travel: {brj, mvv, mvv_uni}, bra_i, byggeaar, url, image: bool, kommentar, tag, source: "eie"|"dnb", sold: bool} ]}` — eie visible rows via `listing_rows` + DNB-unique rows + (param `?sold=1`) sold rows; annotations joined; travel donor-resolved.
- `GET /api/listings/{finnkode}` → full detail (all row fields incl. hidden).
- `GET /api/meta` → `{polygon: [[lng,lat]...], filters: {...domain filters}, boligtyper: [distinct values], stations: [{name, lat, lng, radius_m, lines: [...], travel: {to_sandvika, to_sandvika_transfer, ...}}]}` (stations from the stations repos/queries used by `stations_rows`).
- `GET /api/missing-coords` → visible rows lacking lat/lng.
- [ ] TDD on seeded tmp DBs (shapes; donor-resolved value present; DNB row appears with source "dnb"; sold excluded by default/included with param; annotations join; meta polygon == domain config; stations carry lines+travel). Implement; commit `rebuild(phase5): listings/meta/missing-coords api`.

---

### Task 4: Annotations API

**Files:** Modify `skannonser/web/api.py`; Test `tests/rebuild/test_web_api.py` (extend).
**Interfaces:** `PUT /api/annotations/{finnkode}` body `{"kommentar": str|null, "tag": str|null}` → upsert into `annotations`, `updated_at` = now (NOT equal to imported_at — so the sheet-import protection semantics treat it as a web edit); both-null → row deleted; response = stored state. `GET /api/annotations/{finnkode}`.
- [ ] TDD: create/update/clear cycle; a web edit is NOT overwritten by a subsequent `import_sheet_annotations` run (drive the real import with a fake client — locks the protection interplay end-to-end). Implement; commit `rebuild(phase5): annotations crud with import-protection interplay locked`.

---

### Task 5: Thumbnail cache (nightly step + serving)

**Files:** Create `skannonser/enrich/thumbs.py`; Modify `skannonser/nightly.py` (step 8 becomes thumbs, sheets becomes 9 — thumbs AFTER refresh, BEFORE sheets; failure isolates like any section), `skannonser/web/app.py` (serve `/thumbs/{finnkode}.jpg`), `docs/rebuild/STATUS.md`; Test `tests/rebuild/test_thumbs.py`, `tests/rebuild/test_nightly.py` (step-order pin update).
**Interfaces:** `cache_thumbnails(conn, dest_dir: Path, fetch=requests.get, fetch_delay=None, limit: int = 0) -> dict` — candidates: active listings (eie + dnb-unique) with non-empty image_url and NO existing `{dest_dir}/{finnkode}.jpg`; downloads with UA/timeout (reuse the dnb fetch discipline pattern), 0.1s default delay, failures tolerated (recorded, retried next night), `limit` caps downloads. Web: `GET /thumbs/{finnkode}.jpg` → FileResponse or 404. Default dest: `data/thumbs/` (gitignored — verify/add).
- [ ] TDD (fake fetch): downloads only missing; skips existing; failure recorded not fatal; nightly step order test updated; STATUS: image_hosted_url machinery marked RETIRED (web serves local cache; Drive tool stays legacy-only until phase 6 deletion). Commit `rebuild(phase5): nightly thumbnail cache + local serving; image_hosted_url retired`.

---

### Task 6: Frontend — map core

**Files:** Create `skannonser/web/static/index.html`, `static/app.js`, `static/map.js`, `static/style.css`, `static/vendor/` (maplibre-gl.js + .css — download pinned version 4.x via curl, commit; record the version + sha256 in the commit message); Test `tests/rebuild/test_web_static.py` (index served at `/`, vendor assets served, no CDN URLs in any static file — grep assertion).
**Behavior (parity targets from the Apps Script map):** MapLibre with OSM raster style; clustered GeoJSON source from `/api/listings` (cluster radius ~40, expands on click); unclustered circles colored by boligtype (port the color mapping from `apps_script/map/map.html` — read it; keep the same palette), DNB rendered as squares (symbol layer), Sold grey (loaded on toggle via `?sold=1`); FINN boundary polygon from `/api/meta`; popups: adresse, pris (formatted), pris/kvm, travel minutes (BRJ/MVV/UNI), Finn link, Google Maps link, thumbnail (`/thumbs/`), inline kommentar/tag editor (PUT on save); localStorage persistence of layer toggles.
- [ ] Steps: static test first (served, no-CDN grep) → build → manual smoke via `skannonser web` locally against a DB copy (record a screenshot-described walkthrough in the report) → commit `rebuild(phase5): maplibre map core - clusters, layers, popups, annotations`.

---

### Task 7: Frontend — filters + stations

**Files:** Create `static/filters.js`, `static/stations.js`; Modify `static/app.js`, `static/index.html`; Test extend `test_web_static.py` (assets served; still no CDN).
**Behavior (parity):** metric filters that DIM (opacity drop, adjustable intensity slider) rather than hide: price max, BRA-i min, travel-minutes max per destination; boligtype per-type visibility checkboxes; station overlays (circles radius_m, per-line colors + visibility toggles, name labels on hover); commute-to-Sandvika filter: dim listings whose nearest in-radius station's `to_sandvika` (or `to_sandvika_transfer` when the direct line is filtered off — port the transfer-fallback intent from `apps_script/map/map.html`'s commute logic; read it and document what you ported vs simplified) exceeds the slider; "hide outside station radius" dimming; missing-coords panel listing `/api/missing-coords`; all UI state persisted to localStorage.
- [ ] Manual smoke walkthrough recorded; commit `rebuild(phase5): filters, station overlays, commute filter`.

---

### Task 8: Table view

**Files:** Create `static/table.html`, `static/table.js`; Modify `skannonser/web/app.py` (route `/table`), `static/index.html` (nav link); Test extend `test_web_static.py`.
**Behavior:** all `/api/listings` rows (incl. a Sold toggle); click-to-sort columns (pris, pris_kvm, bra_i, travel columns, adresse); text filter box (adresse/postnummer/boligtype); kommentar/tag inline edit (PUT); finnkode links to Finn; row click pans the map (link to `/#finnkode` — map.js focuses on hash).
- [ ] Commit `rebuild(phase5): sortable table view with inline annotations`.

---

### Task 9: Deploy + acceptance trial

**Files:** Modify `docker-compose.yml` (new `web` service: same image, `command: ["uvicorn", ...]` or `skannonser web --host 0.0.0.0`, `ports: ["100.77.139.22:8377:8000"]`, healthcheck curl `/healthz`, `user:` non-root, `restart: unless-stopped`, volumes incl. `./data:/app/data` for thumbs), `docker/Dockerfile` (only if needed for uvicorn entry), `docs/rebuild/STATUS.md`.
**Procedure:** local compose build + up + healthz check; server: pull (DB stash-dance), compose up web, healthz via tailnet from the laptop (`curl http://100.77.139.22:8377/healthz`), run one `cache_thumbnails` catch-up (`--limit 50` first, then unlimited) supervised; verify the map loads from a tailnet browser (USER does this — the acceptance walkthrough: clusters, popups, filters, stations, table, annotation edit persists). STATUS: "web app LIVE (trial) 2026-07-XX; Apps Script retirement pending user approval after trial."
- [ ] Commit `rebuild(phase5): web service deployed tailnet-only - trial started`.

---

## Phase 5 acceptance gate
1. Suite green, zero warnings; no CDN URLs in static/; no real network in tests.
2. Task 2's `verify sheets` zero-diff proof recorded (exporter untouched by the refactor).
3. Nightly still green end-to-end with the thumbs step (observe one production night).
4. Web app reachable ONLY via tailnet (verify: `curl` from the server's public interface fails / port bound to 100.77.139.22).
5. USER walkthrough approved: map parity items + table + annotations round-trip.
6. STATUS current (trial state; image_hosted_url retired; Apps Script retirement decision pending).
