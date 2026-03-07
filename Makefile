PYTHON ?= python
REPO ?=
ARTIFACT_PREFIX ?= html-delta-
ARTIFACT_DEST ?= artifacts/github
ARTIFACT_MANIFEST ?= artifacts/github/download_manifest.json
ARTIFACT_OLDER_DAYS ?= 7
COORDS_LIMIT ?= 100
COORDS_RPM ?= 120
COORDS_INCLUDE_INACTIVE ?= 0

.PHONY: help gha sheet travel brj mvv full refresh refresh-inactive artifacts-pull artifacts-cleanup-manifest artifacts-cleanup-prefix map-guide map-push map-deploy coords-missing coords-fill coords-import-sheet addr-overrides

help:
	@echo "Available targets:"
	@echo "  make gha      - Run CI-safe scrape (scrape + DB update, no Google Directions API)"
	@echo "  make coords-fill - Geocode missing LAT/LNG in DB"
	@echo "                     Optional: COORDS_LIMIT=0 COORDS_RPM=40 COORDS_INCLUDE_INACTIVE=1"
	@echo "  make coords-import-sheet - Import existing LAT/LNG from sheet back into DB"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make brj      - Fill missing BRJ transit travel fields only"
	@echo "  make mvv      - Fill missing MVV transit travel fields only"
	@echo "  make sheet    - Manually sync database to Google Sheets"

	@echo "  make refresh  - Re-download listing pages and refresh statuses"
	@echo "  make refresh-inactive - Re-download only listings with search_hit=0"
	@echo "  make full     - Full manual run (includes optional travel API prompts)"
	@echo "  make coords-missing - Report listings missing LAT/LNG in DB"
	@echo "  make addr-overrides - Manage address overrides (set/list/remove)"
	@echo "  make map-guide - Open setup guide for interactive map"
	@echo "  make map-push  - Push Apps Script map files via clasp"
	@echo "  make map-deploy - Deploy Apps Script web app via clasp"
	@echo "  make artifacts-pull             - Download artifacts, then delete remote copies"
	@echo "  make artifacts-cleanup-manifest - Delete artifacts listed as downloaded in manifest"
	@echo "  make artifacts-cleanup-prefix   - Delete artifacts by prefix/age"

gha:
	$(PYTHON) main/tools/run_eiendom_github.py

sheet:
	$(PYTHON) main/tools/manual_sheet_update.py

travel:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py

brj:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target brj

mvv:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target mvv

full:
	$(PYTHON) main/tools/manage.py run eiendom

refresh:
	$(PYTHON) main/sync/refresh_listings.py

refresh-inactive:
	$(PYTHON) main/sync/refresh_listings.py --only-inactive

map-guide:
	@echo "See docs/INTERACTIVE_MAP_SETUP.md"

map-push:
	@cd apps_script/map && clasp push

map-deploy:
	@cd apps_script/map && clasp deploy --description "Interactive map update"

coords-missing:
	$(PYTHON) main/tools/report_missing_coordinates.py

coords-fill:
	$(PYTHON) main/tools/fill_missing_coordinates.py --limit "$(COORDS_LIMIT)" --rpm "$(COORDS_RPM)" $(if $(filter 1 yes true,$(COORDS_INCLUDE_INACTIVE)),--include-inactive,)

coords-import-sheet:
	$(PYTHON) main/tools/import_coordinates_from_sheet.py --sheet Eie

addr-overrides:
	$(PYTHON) main/tools/address_overrides.py --help

artifacts-pull:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" pull --prefix "$(ARTIFACT_PREFIX)" --dest "$(ARTIFACT_DEST)" --manifest "$(ARTIFACT_MANIFEST)" --delete-after-download

artifacts-cleanup-manifest:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" cleanup-manifest --manifest "$(ARTIFACT_MANIFEST)" --prefix "$(ARTIFACT_PREFIX)"

artifacts-cleanup-prefix:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" cleanup-prefix --prefix "$(ARTIFACT_PREFIX)" --older-than-days "$(ARTIFACT_OLDER_DAYS)"
