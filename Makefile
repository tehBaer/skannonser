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

.PHONY: help sheets travel brj mvv dnb-url dnb-sync dnb-export-travel dnb-backfill-travel dnb-backfill-travel-dryrun full full-no-scrape refresh refresh-inactive refresh-stale-open map-guide map-push map-deploy map-live-url coords-count coords-missing coords-fill coords-import-sheet addr-overrides polygon-edit finn-url polygon-sync find-grouped-address-count find-grouped-adress-count api-calls-new-address validate-travel

help:
	@echo "Available targets:"
	@echo "  make gha      - Run CI-safe scrape (scrape + DB update, no Google Directions API)"
	@echo "  make coords-fill - Geocode missing LAT/LNG in DB"
	@echo "                     Optional: COORDS_LIMIT=0 COORDS_RPM=40 COORDS_INCLUDE_INACTIVE=1"
	@echo "  make find-grouped-address-count - Count grouped unique address clusters"
	@echo "                     Uses TRAVEL_REUSE_WITHIN_METERS as cluster radius"
	@echo "                     Optional: RADIUS=500 INCLUDE_INACTIVE=1 VERBOSE=1"
	@echo "  make full     - Full manual run (scrape + coords fill + sheet sync)"
	@echo "  make full-no-scrape - Full manual run without scrape/crawl steps"
	@echo "                     Prompts before geocoding API call by default; set COORDS_CONFIRM=0 to skip prompt"
	@echo "                     Geocodes all missing coordinates (no limit)"
	@echo "  make coords-import-sheet - Import existing LAT/LNG from sheet back into DB"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make brj      - Fill missing BRJ transit travel fields only"
	@echo "  make mvv      - Fill missing MVV transit travel fields only"
	@echo "  make dnb-url  - Print DNB search URL used for URL extraction"
	@echo "  make dnb-sync - Sync DNB sheet from DB (update fields, delete stale, append new)"
	@echo "  make dnb-export-travel - Export new DNB-only rows to sheet with travel API calls"
	@echo "  make dnb-backfill-travel - Backfill BRJ/MVV into existing DNB sheet rows by URL"
	@echo "  make dnb-backfill-travel-dryrun - Backfill dry run (no API calls, no sheet writes)"
	@echo "  make sheets   - Manually sync database to Google Sheets"

	@echo "  make refresh  - Re-download listing pages and refresh statuses"
	@echo "  make refresh-inactive - Re-download only listings with active=0"
	@echo "  make refresh-stale-open - Re-download active=0 listings except Tilgjengelighet=Solgt/Inaktiv"
	@echo "  make coords-count - Count coordinate geocode candidates (no API calls)"
	@echo "  make coords-missing - Report listings missing LAT/LNG in DB"
	@echo "  make validate-travel - Flag suspicious stored travel values without API calls"
	@echo "                     Optional: TARGET=all RADIUS=750 INCLUDE_INACTIVE=1 TOP=100 CSV=tmp/travel_validation.csv"
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
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target brj

mvv:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target mvv
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target mvv

dnb-url:
	$(PYTHON) -c "from main.extractors.extract_dnbeiendom import SEARCH_URL; print(SEARCH_URL)"

dnb-sync:
	$(PYTHON) scripts/sync_dnbeiendom_sheet.py

dnb-export-travel:
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target all

dnb-backfill-travel:
	$(PYTHON) scripts/backfill_dnbeiendom_travel_to_sheet.py --target all

dnb-backfill-travel-dryrun:
	$(PYTHON) scripts/backfill_dnbeiendom_travel_to_sheet.py --target all --dry-run

full:
	# 1) FINN crawling
	$(PYTHON) main/runners/run_eiendom_db.py --step crawl
	# 2) DNB crawling
	$(PYTHON) main/extractors/extract_dnbeiendom.py
	# 3) FINN extraction
	$(PYTHON) main/runners/run_eiendom_db.py --step extract
	# 4) DNB extraction
	$(PYTHON) main/extractors/extract_dnbeiendom_ads.py --input data/dnbeiendom/0_URLs.csv --output-folder data/dnbeiendom
	# Coords preflight count (no API calls)
	COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --count-only $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)
	# Optional coords fill (asks by default; set COORDS_CONFIRM=0 to skip prompt)
	@if [ "$(COORDS_CONFIRM)" = "1" ]; then \
		printf "Run coords fill now (geocode missing LAT/LNG)? [y/N]: "; \
		read ans; \
		case "$$ans" in \
			y|Y|yes|YES) \
				COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --allow-failures $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,);; \
			*) \
				echo "Skipping coords fill.";; \
		esac; \
	else \
		COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --allow-failures $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,); \
	fi
	@echo ""
	@echo "--- Next step: continue DB + sheet pipeline ---"
	@echo ""
	# Continue existing DB/sheet pipeline steps after extraction
	$(PYTHON) main/extractors/filter_and_load_dnbeiendom_no_buffer.py
	$(PYTHON) scripts/sync_dnbeiendom_sheet.py
	TRAVEL_AUTO_CONFIRM="0" TRAVEL_REQUESTS_PER_MINUTE="60" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process
	$(PYTHON) main/tools/manage.py sync eiendom
	@echo ""
	@echo "--- Next step: refresh stale inactive listings (temporarily disabled) ---"
	@echo ""
	@echo "Skipping inactive refresh in 'make full' for now."

full-no-scrape:
	# 1) FINN extraction
	$(PYTHON) main/runners/run_eiendom_db.py --step extract
	# 2) DNB extraction
	$(PYTHON) main/extractors/extract_dnbeiendom_ads.py --input data/dnbeiendom/0_URLs.csv --output-folder data/dnbeiendom
	# Coords preflight count (no API calls)
	COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --count-only $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)
	# Optional coords fill (asks by default; set COORDS_CONFIRM=0 to skip prompt)
	@if [ "$(COORDS_CONFIRM)" = "1" ]; then \
		printf "Run coords fill now (geocode missing LAT/LNG)? [y/N]: "; \
		read ans; \
		case "$$ans" in \
			y|Y|yes|YES) \
				COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --allow-failures $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,);; \
			*) \
				echo "Skipping coords fill.";; \
		esac; \
	else \
		COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" $(PYTHON) main/tools/fill_missing_coordinates.py --limit "0" --rpm "$(COORDS_RPM)" --allow-failures $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,); \
	fi
	@echo ""
	@echo "--- Next step: continue DB + sheet pipeline ---"
	@echo ""
	# Continue existing DB/sheet pipeline steps after extraction
	$(PYTHON) main/extractors/filter_and_load_dnbeiendom_no_buffer.py
	$(PYTHON) scripts/sync_dnbeiendom_sheet.py
	TRAVEL_AUTO_CONFIRM="0" TRAVEL_REQUESTS_PER_MINUTE="60" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process
	$(PYTHON) main/tools/manage.py sync eiendom
	@echo ""
	@echo "--- Next step: refresh stale inactive listings (temporarily disabled) ---"
	@echo ""
	@echo "Skipping inactive refresh in 'make full-no-scrape' for now."

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
	@cd apps_script/map && DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*@([0-9]+).*/\2 \1/p' | sort -nr | head -n 1 | awk '{print $$2}')"; \
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

validate-travel:
	$(PYTHON) main/tools/validate_travel_values.py \
		--target "$(if $(TARGET),$(TARGET),all)" \
		$(if $(RADIUS),--radius-meters "$(RADIUS)",) \
		$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,) \
		$(if $(TOP),--top "$(TOP)",) \
		$(if $(CSV),--csv "$(CSV)",)

coords-count:
	$(PYTHON) main/tools/fill_missing_coordinates.py --limit 0 --count-only $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)

coords-fill:
	$(PYTHON) main/tools/fill_missing_coordinates.py --limit "$(COORDS_LIMIT)" --rpm "$(COORDS_RPM)" $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)

coords-import-sheet:
	$(PYTHON) main/tools/import_coordinates_from_sheet.py --sheet Eie

addr-overrides:
	$(PYTHON) main/tools/address_overrides.py --help

find-grouped-address-count:
	$(PYTHON) main/tools/find_grouped_address_count.py \
		$(if $(RADIUS),--radius-meters "$(RADIUS)",) \
		$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,) \
		$(if $(filter 1 yes true,$(VERBOSE)),--verbose,)

# Alias with original requested spelling.
find-grouped-adress-count: find-grouped-address-count

# Backward-compatible alias for previous target name.
api-calls-new-address: find-grouped-address-count


