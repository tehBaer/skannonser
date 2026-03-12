PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,$(shell command -v python3 || command -v python))
REPO ?=
COORDS_LIMIT ?= 100
COORDS_RPM ?= 120
COORDS_INCLUDE_INACTIVE ?= 0
COORDS_CONFIRM ?= 1
COORDS_LIMIT ?= 100
COORDS_RPM ?= 120
COORDS_INCLUDE_INACTIVE ?= 0
COORDS_CONFIRM ?= 1

.PHONY: help sheets travel brj mvv full refresh refresh-inactive refresh-stale-open map-guide map-push map-deploy map-live-url coords-missing coords-fill coords-import-sheet addr-overrides polygon-edit finn-url polygon-sync

help:
	@echo "Available targets:"
	@echo "  make gha      - Run CI-safe scrape (scrape + DB update, no Google Directions API)"
	@echo "  make coords-fill - Geocode missing LAT/LNG in DB"
	@echo "                     Optional: COORDS_LIMIT=0 COORDS_RPM=40 COORDS_INCLUDE_INACTIVE=1"
	@echo "  make full     - Full manual run (scrape + coords fill + sheet sync)"
	@echo "                     Prompts before geocoding API call by default; set COORDS_CONFIRM=0 to skip prompt"
	@echo "                     Geocodes all missing coordinates (no limit)"
	@echo "  make coords-import-sheet - Import existing LAT/LNG from sheet back into DB"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make brj      - Fill missing BRJ transit travel fields only"
	@echo "  make mvv      - Fill missing MVV transit travel fields only"
	@echo "  make sheets   - Manually sync database to Google Sheets"

	@echo "  make refresh  - Re-download listing pages and refresh statuses"
	@echo "  make refresh-inactive - Re-download only listings with active=0"
	@echo "  make refresh-stale-open - Re-download active=0 listings except Tilgjengelighet=Solgt/Inaktiv"
	@echo "  make coords-missing - Report listings missing LAT/LNG in DB"
	@echo "  make addr-overrides - Manage address overrides (set/list/remove)"
	@echo "  make map-guide - Open setup guide for interactive map"
	@echo "  make polygon-edit - Open visual editor for FINN polygon coordinates"
	@echo "  make finn-url - Print generated FINN search URL from current polygon points"
	@echo "  make polygon-sync - Sync finn_polygon_points to 'Finn Polygon Coords' sheet"
	@echo "  make map-push  - Push Apps Script map files via clasp"
	@echo "  make map-deploy - Deploy Apps Script web app via clasp"
	@echo "  make map-live-url - Print live Apps Script web app URL (/exec)"
  



sheets:
	$(PYTHON) main/tools/manual_sheet_update.py

travel:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py

brj:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target brj

mvv:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target mvv

full:
	# 1) FINN crawling
	$(PYTHON) main/runners/run_eiendom_db.py --step crawl
	# 2) DNB crawling
	$(PYTHON) main/extractors/extract_dnbeiendom.py
	# 3) FINN extraction
	$(PYTHON) main/runners/run_eiendom_db.py --step extract
	# 4) DNB extraction
	$(PYTHON) main/extractors/extract_dnbeiendom_ads.py --input data/dnbeiendom/0_URLs.csv --output-folder data/dnbeiendom
	# Continue existing DB/sheet pipeline steps after extraction
	$(PYTHON) main/extractors/filter_and_load_dnbeiendom_no_buffer.py
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py
	COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process
	$(PYTHON) main/tools/manage.py sync eiendom

refresh:
	$(PYTHON) main/sync/refresh_listings.py

refresh-inactive:
	$(PYTHON) main/sync/refresh_listings.py --only-inactive

refresh-stale-open:
	$(PYTHON) main/sync/refresh_listings.py --only-inactive --exclude-status Solgt --exclude-status Inaktiv

map-guide:
	@echo "See docs/INTERACTIVE_MAP_SETUP.md"

polygon-edit:
	$(PYTHON) main/tools/finn_polygon_editor.py

finn-url:
	$(PYTHON) main/tools/finn_polygon_editor.py --print-url-only

polygon-sync:
	$(PYTHON) main/tools/sync_finn_polygon_sheet.py

map-push:
	@cd apps_script/map && clasp push

map-deploy:
	@cd apps_script/map && clasp deploy --description "Interactive map update"

map-live-url:
	@cd apps_script/map && DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE '/@HEAD/! s/.*(AKfy[a-zA-Z0-9_-]+).*/\1/p' | head -n 1)"; \
	if [ -z "$$DEPLOY_ID" ]; then \
		DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*/\1/p' | head -n 1)"; \
	fi; \
	if [ -z "$$DEPLOY_ID" ]; then \
		echo "No deployment ID found. Run 'make map-deploy' first."; \
		exit 1; \
	fi; \
	echo "https://script.google.com/macros/s/$$DEPLOY_ID/exec"

coords-missing:
	$(PYTHON) main/tools/report_missing_coordinates.py

coords-fill:
	$(PYTHON) main/tools/fill_missing_coordinates.py --limit "$(COORDS_LIMIT)" --rpm "$(COORDS_RPM)" $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)

coords-import-sheet:
	$(PYTHON) main/tools/import_coordinates_from_sheet.py --sheet Eie

addr-overrides:
	$(PYTHON) main/tools/address_overrides.py --help


