PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,$(shell command -v python3 || command -v python))
REPO ?=
COORDS_LIMIT ?= 100
COORDS_RPM ?= 120
COORDS_INCLUDE_INACTIVE ?= 0
COORDS_CONFIRM ?= 1
TRAVEL_RPM ?= 60
TRAVEL_CONFIRM ?= 1
STATION_DEST ?= Sandvika
STATION_COLUMN ?= TO_SANDVIKA
STATION_PDF_URLS ?=
STATION_PDF_LIST ?=
STATION_LINE_PDFS ?=
STATION_LINE_PDF_LIST ?=
STATION_SHEET ?= Stations
STATION_PER_LINE ?= 1
SUSPICIOUS_CSV ?= tmp/travel_suspicious_findings.csv
SCORE_THRESHOLD ?= 2
MIN_ABS_DIFF ?= 15
MIN_REL_DIFF ?= 0.25
COORDS_LIMIT ?= 100
COORDS_RPM ?= 120
COORDS_INCLUDE_INACTIVE ?= 0
COORDS_CONFIRM ?= 1

.PHONY: help sheets travel brj mvv mvv-uni-rush dnb-url dnb-sync dnb-export-travel dnb-backfill-travel dnb-backfill-travel-dryrun backfill-donor-links backfill-donor-links-dryrun populate-travel-from-donors populate-travel-from-donors-dryrun check-donor-chains check-donor-chains-strict repair-donor-chains repair-donor-chains-dryrun full full-no-scrape refresh refresh-inactive refresh-stale-open map-guide map-push map-deploy map-url map-live-url coords-count coords-missing coords-fill coords-import-sheet addr-overrides polygon-edit finn-url polygon-sync find-grouped-address-count find-grouped-adress-count api-calls-new-address validate-travel validate-travel-rerequest-suspicious station-travel station-travel-dryrun stations-pull

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
	@echo "                     Prompts before travel API calls by default; set TRAVEL_CONFIRM=0 to skip prompt"
	@echo "                     Geocodes all missing coordinates (no limit)"
	@echo "                     Travel API rate: TRAVEL_RPM=60 (shows candidate progress)"
	@echo "  make coords-import-sheet - Import existing LAT/LNG from sheet back into DB"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make brj      - Fill missing BRJ transit travel fields only"
	@echo "  make mvv      - Fill missing MVV transit travel fields only"
	@echo "  make mvv-uni-rush - Fill missing MVV UNI RUSH transit travel fields only"
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
	@echo "  make validate-travel-rerequest-suspicious - Validate and re-request only high-suspicion listings"
	@echo "                     Optional: TARGET=all SCORE_THRESHOLD=2 MIN_ABS_DIFF=15 MIN_REL_DIFF=0.25"
	@echo "                     Optional: TRAVEL_AUTO_CONFIRM=1 TRAVEL_REQUESTS_PER_MINUTE=60 SUSPICIOUS_CSV=tmp/travel_suspicious_findings.csv"
	@echo "                     Note: uses forced API mode for targeted rows (ignores donor reuse during re-request)"
	@echo "                     Optional: TARGET=all RADIUS=750 INCLUDE_INACTIVE=1 TOP=100 CSV=tmp/travel_validation.csv"
	@echo "                     Thresholds: SCORE_THRESHOLD=2 MIN_ABS_DIFF=15 MIN_REL_DIFF=0.25 MAD_MULT=2.0"
	@echo "                     Groups: MIN_NEIGHBORS=4 MIN_POSTCODE_GROUP=5 MAX_TRAVEL_MINUTES=360"
	@echo "                     Display: FULL_TABLE=1 (show full untruncated reasons)"
	@echo "  make populate-travel-from-donors - Copy travel values from donors into recipient rows (travel_copy_from_finnkode set)"
	@echo "  make populate-travel-from-donors-dryrun - Dry run of the above (no DB writes)"
	@echo "  make check-donor-chains - Report donor-of-donor chains/cycles/self-links/broken refs"
	@echo "                     Optional: TOP=50 CSV=tmp/donor_chain_findings.csv"
	@echo "  make check-donor-chains-strict - Same check, exits non-zero if findings exist"
	@echo "  make travel-count-process - Estimate process-step travel API candidates (no API calls)"
	@echo "  make addr-overrides - Manage address overrides (set/list/remove)"
	@echo "  make map-guide - Open setup guide for interactive map"
	@echo "  make polygon-edit - Open visual editor for FINN polygon coordinates"
	@echo "  make finn-url - Print generated FINN search URL from current polygon points"
	@echo "  make polygon-sync - Sync finn_polygon_points to 'Finn Polygon Coords' sheet"
	@echo "  make map-push  - Push Apps Script map files via clasp"
	@echo "  make map-deploy - Deploy Apps Script web app via clasp and print the /exec URL"
	@echo "  make map-url - Print live Apps Script web app URL (/exec)"
	@echo "  make map-live-url - Alias for map-url"
	@echo "  make station-travel - Fill station destination minutes from timetable PDFs and sync to Stations sheet"
	@echo "                     Default input: local PDFs in 'rutetabeller tog/'"
	@echo "                     Optional override: STATION_PDF_URLS='url1 url2 ...' or STATION_PDF_LIST=path/to/urls.txt"
	@echo "                     Optional explicit mapping: STATION_LINE_PDFS='L1=url1 L2=url2' or STATION_LINE_PDF_LIST=path/to/line_urls.txt"
	@echo "                     Optional: STATION_DEST=Sandvika STATION_COLUMN=TO_SANDVIKA STATION_SHEET=Stations"
	@echo "                     Optional: STATION_PER_LINE=1 writes line columns named L1/R21/RE10 etc (default enabled)"
	@echo "  make station-travel-dryrun - Same computation without CSV/sheet writes"
	@echo "  make stations-pull - Pull Google Sheets 'Stations' tab into DB (source of truth)"
	@echo "                     Optional: STATION_SHEET=Stations STATION_DEST=Sandvika"
  



sheets:
	$(PYTHON) main/tools/manual_sheet_update.py

travel:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py

station-travel:
	$(PYTHON) scripts/fill_station_travel_from_pdf.py \
		$(if $(filter 1 yes true,$(STATION_PER_LINE)),--per-line,) \
		--destination "$(STATION_DEST)" \
		--column "$(STATION_COLUMN)" \
		--stations-sheet "$(STATION_SHEET)" \
		$(if $(STATION_PDF_LIST),--pdf-list-file "$(STATION_PDF_LIST)",) \
		$(if $(STATION_LINE_PDF_LIST),--line-pdf-list-file "$(STATION_LINE_PDF_LIST)",) \
		$(foreach m,$(STATION_LINE_PDFS),--line-pdf "$(m)") \
		$(foreach u,$(STATION_PDF_URLS),--pdf-url "$(u)")

station-travel-dryrun:
	$(PYTHON) scripts/fill_station_travel_from_pdf.py \
		--dry-run \
		--no-sync \
		$(if $(filter 1 yes true,$(STATION_PER_LINE)),--per-line,) \
		--destination "$(STATION_DEST)" \
		--column "$(STATION_COLUMN)" \
		--stations-sheet "$(STATION_SHEET)" \
		$(if $(STATION_PDF_LIST),--pdf-list-file "$(STATION_PDF_LIST)",) \
		$(if $(STATION_LINE_PDF_LIST),--line-pdf-list-file "$(STATION_LINE_PDF_LIST)",) \
		$(foreach m,$(STATION_LINE_PDFS),--line-pdf "$(m)") \
		$(foreach u,$(STATION_PDF_URLS),--pdf-url "$(u)")

stations-pull:
	$(PYTHON) scripts/sync_stations_from_sheet.py \
		--sheet-name "$(STATION_SHEET)" \
		--destination "$(STATION_DEST)"

brj:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target brj
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target brj

mvv:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target mvv
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target mvv

mvv-uni-rush:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py --target mvv_uni
	$(PYTHON) scripts/export_dnbeiendom_to_sheet.py --target mvv_uni

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

backfill-donor-links:
	$(PYTHON) scripts/backfill_donor_links.py

backfill-donor-links-dryrun:
	$(PYTHON) scripts/backfill_donor_links.py --dry-run

populate-travel-from-donors:
	$(PYTHON) scripts/populate_travel_from_donors.py

populate-travel-from-donors-dryrun:
	$(PYTHON) scripts/populate_travel_from_donors.py --dry-run

check-donor-chains:
	$(PYTHON) main/tools/check_donor_chains.py \
		$(if $(TOP),--top "$(TOP)",) \
		$(if $(CSV),--csv "$(CSV)",)

check-donor-chains-strict:
	$(PYTHON) main/tools/check_donor_chains.py \
		$(if $(TOP),--top "$(TOP)",) \
		$(if $(CSV),--csv "$(CSV)",) \
		--fail-on-findings

repair-donor-chains:
	$(PYTHON) main/tools/check_donor_chains.py --repair

repair-donor-chains-dryrun:
	$(PYTHON) main/tools/check_donor_chains.py --repair --dry-run

full:
	# 1) FINN crawling
	@echo ""
	@echo "== [1/7] FINN crawl =="
	@$(PYTHON) main/runners/run_eiendom_db.py --step crawl
	# 2) DNB crawling
	@echo ""
	@echo "== [2/7] DNB crawl =="
	@$(PYTHON) main/extractors/extract_dnbeiendom.py
	# 3) FINN extraction
	@echo ""
	@echo "== [3/7] FINN extract =="
	@$(PYTHON) main/runners/run_eiendom_db.py --step extract
	# 4) DNB extraction
	@echo ""
	@echo "== [4/7] DNB extract =="
	@$(PYTHON) main/extractors/extract_dnbeiendom_ads.py --input data/dnbeiendom/0_URLs.csv --output-folder data/dnbeiendom
	@echo ""
	@echo "== [5/7] DNB filter/load + sheet sync =="
	# Continue existing DB/sheet pipeline steps after extraction
	@$(PYTHON) main/extractors/filter_and_load_dnbeiendom_no_buffer.py
	@$(PYTHON) scripts/sync_dnbeiendom_sheet.py
	# Travel preflight estimate aligned with process step input (A_live.csv).
	@TRAVEL_CAND="$$($(PYTHON) main/tools/estimate_process_travel_missing.py --target all --format count)"; \
	if [ -z "$$TRAVEL_CAND" ]; then TRAVEL_CAND="0"; fi; \
	echo "[TRAVEL] process candidates=$$TRAVEL_CAND"; \
	if [ "$$TRAVEL_CAND" = "0" ]; then \
		echo "[TRAVEL] skip API calls (no missing BRJ/MVV)"; \
		EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="0" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process; \
	elif [ "$(TRAVEL_CONFIRM)" = "1" ]; then \
		printf "Run travel API calls now (fill missing BRJ/MVV)? [y/N]: "; \
		read ans; \
		case "$$ans" in \
			y|Y|yes|YES) \
				EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="1" TRAVEL_AUTO_CONFIRM="1" TRAVEL_REQUESTS_PER_MINUTE="$(TRAVEL_RPM)" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process;; \
			*) \
				echo "[TRAVEL] skip API calls (user declined)"; \
				EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="0" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process;; \
		esac; \
	else \
		EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="1" TRAVEL_AUTO_CONFIRM="1" TRAVEL_REQUESTS_PER_MINUTE="$(TRAVEL_RPM)" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process; \
	fi
	@echo ""
	@echo "== [6/7] Sync eiendom -> sheet =="
	@$(PYTHON) main/tools/manage.py sync eiendom
	@$(PYTHON) main/sync/update_rows_in_sheet.py
	@echo ""
	@echo "== [7/7] Refresh stale open =="
	@printf "Refresh stale open listings? [y/N]: "; \
	read ans; \
	case "$$ans" in \
		y|Y|yes|YES) \
			$(MAKE) refresh-stale-open;; \
		*) \
			echo "Skipped refresh stale open";; \
	esac

full-no-scrape:
	# 1) FINN extraction
	@echo ""
	@echo "== [1/5] FINN extract =="
	@$(PYTHON) main/runners/run_eiendom_db.py --step extract
	# 2) DNB extraction
	@echo ""
	@echo "== [2/5] DNB extract =="
	@$(PYTHON) main/extractors/extract_dnbeiendom_ads.py --input data/dnbeiendom/0_URLs.csv --output-folder data/dnbeiendom
	@echo ""
	@echo "== [3/5] DNB filter/load + sheet sync =="
	# Continue existing DB/sheet pipeline steps after extraction
	@$(PYTHON) main/extractors/filter_and_load_dnbeiendom_no_buffer.py
	@$(PYTHON) scripts/sync_dnbeiendom_sheet.py
	# Travel preflight estimate aligned with process step input (A_live.csv).
	@TRAVEL_CAND="$$($(PYTHON) main/tools/estimate_process_travel_missing.py --target all --format count)"; \
	if [ -z "$$TRAVEL_CAND" ]; then TRAVEL_CAND="0"; fi; \
	echo "[TRAVEL] process candidates=$$TRAVEL_CAND"; \
	if [ "$$TRAVEL_CAND" = "0" ]; then \
		echo "[TRAVEL] skip API calls (no missing BRJ/MVV)"; \
		EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="0" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process; \
	elif [ "$(TRAVEL_CONFIRM)" = "1" ]; then \
		printf "Run travel API calls now (fill missing BRJ/MVV)? [y/N]: "; \
		read ans; \
		case "$$ans" in \
			y|Y|yes|YES) \
				EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="1" TRAVEL_AUTO_CONFIRM="1" TRAVEL_REQUESTS_PER_MINUTE="$(TRAVEL_RPM)" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process;; \
			*) \
				echo "[TRAVEL] skip API calls (user declined)"; \
				EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="0" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process;; \
		esac; \
	else \
		EIENDOM_CALCULATE_GOOGLE_DIRECTIONS="1" TRAVEL_AUTO_CONFIRM="1" TRAVEL_REQUESTS_PER_MINUTE="$(TRAVEL_RPM)" TRAVEL_LOG_UPDATES_ONLY="1" COORDS_LIMIT="0" COORDS_RPM="$(COORDS_RPM)" COORDS_INCLUDE_INACTIVE="$(COORDS_INCLUDE_INACTIVE)" COORDS_CONFIRM="$(COORDS_CONFIRM)" $(PYTHON) main/runners/run_eiendom_db.py --step process; \
	fi
	@echo ""
	@echo "== [4/5] Sync eiendom -> sheet =="
	@$(PYTHON) main/tools/manage.py sync eiendom
	@$(PYTHON) main/sync/update_rows_in_sheet.py
	@echo ""
	@echo "== [5/5] Refresh stale open =="
	@printf "Refresh stale open listings? [y/N]: "; \
	read ans; \
	case "$$ans" in \
		y|Y|yes|YES) \
			$(MAKE) refresh-stale-open;; \
		*) \
			echo "Skipped refresh stale open";; \
	esac

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
	@cd apps_script/map && clasp deploy --description "Interactive map update" && \
	DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*@([0-9]+).*/\2 \1/p' | sort -nr | head -n 1 | awk '{print $$2}')"; \
	if [ -z "$$DEPLOY_ID" ]; then \
		DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*/\1/p' | head -n 1)"; \
	fi; \
	if [ -z "$$DEPLOY_ID" ]; then \
		echo "No deployment ID found after deploy."; \
		exit 1; \
	fi; \
	echo "https://script.google.com/macros/s/$$DEPLOY_ID/exec"

map-url:
	@cd apps_script/map && DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*@([0-9]+).*/\2 \1/p' | sort -nr | head -n 1 | awk '{print $$2}')"; \
	if [ -z "$$DEPLOY_ID" ]; then \
		DEPLOY_ID="$$(clasp deployments 2>/dev/null | sed -nE 's/.*(AKfy[a-zA-Z0-9_-]+).*/\1/p' | head -n 1)"; \
	fi; \
	if [ -z "$$DEPLOY_ID" ]; then \
		echo "No deployment ID found. Run 'make map-deploy' first."; \
		exit 1; \
	fi; \
	echo "https://script.google.com/macros/s/$$DEPLOY_ID/exec"

map-live-url: map-url

coords-missing:
	$(PYTHON) main/tools/report_missing_coordinates.py

travel-count-process:
	$(PYTHON) main/tools/estimate_process_travel_missing.py --target "$(if $(TARGET),$(TARGET),all)"

validate-travel:
	$(PYTHON) main/tools/validate_travel_values.py \
		--target "$(if $(TARGET),$(TARGET),all)" \
		$(if $(RADIUS),--radius-meters "$(RADIUS)",) \
		$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,) \
		$(if $(SCORE_THRESHOLD),--score-threshold "$(SCORE_THRESHOLD)",) \
		$(if $(MIN_ABS_DIFF),--min-abs-diff "$(MIN_ABS_DIFF)",) \
		$(if $(MIN_REL_DIFF),--min-relative-diff "$(MIN_REL_DIFF)",) \
		$(if $(MAD_MULT),--mad-multiplier "$(MAD_MULT)",) \
		$(if $(MIN_NEIGHBORS),--min-neighbors "$(MIN_NEIGHBORS)",) \
		$(if $(MIN_POSTCODE_GROUP),--min-postcode-group "$(MIN_POSTCODE_GROUP)",) \
		$(if $(MAX_TRAVEL_MINUTES),--max-travel-minutes "$(MAX_TRAVEL_MINUTES)",) \
		$(if $(filter 1 yes true,$(FULL_TABLE)),--full-table,) \
		$(if $(TOP),--top "$(TOP)",) \
		$(if $(CSV),--csv "$(CSV)",)

validate-travel-rerequest-suspicious:
	@mkdir -p "$(dir $(SUSPICIOUS_CSV))"
	@echo "Finding suspicious travel rows (target=$(if $(TARGET),$(TARGET),all), score>=$(SCORE_THRESHOLD))..."
	$(PYTHON) main/tools/validate_travel_values.py \
		--target "$(if $(TARGET),$(TARGET),all)" \
		$(if $(RADIUS),--radius-meters "$(RADIUS)",) \
		$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,) \
		$(if $(SCORE_THRESHOLD),--score-threshold "$(SCORE_THRESHOLD)",) \
		$(if $(MIN_ABS_DIFF),--min-abs-diff "$(MIN_ABS_DIFF)",) \
		$(if $(MIN_REL_DIFF),--min-relative-diff "$(MIN_REL_DIFF)",) \
		$(if $(MAD_MULT),--mad-multiplier "$(MAD_MULT)",) \
		$(if $(MIN_NEIGHBORS),--min-neighbors "$(MIN_NEIGHBORS)",) \
		$(if $(MIN_POSTCODE_GROUP),--min-postcode-group "$(MIN_POSTCODE_GROUP)",) \
		$(if $(MAX_TRAVEL_MINUTES),--max-travel-minutes "$(MAX_TRAVEL_MINUTES)",) \
		--csv "$(SUSPICIOUS_CSV)"
	@PREVIEW_OUT="$$(TRAVEL_FORCE_API_FOR_MISSING=1 $(PYTHON) main/tools/rerequest_suspicious_travel.py \
		--findings-csv "$(SUSPICIOUS_CSV)" \
		--target "$(if $(TARGET),$(TARGET),all)" \
		--score-threshold "$(if $(SCORE_THRESHOLD),$(SCORE_THRESHOLD),2)" \
		$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,) \
		--dry-run)"; \
	echo "$$PREVIEW_OUT"; \
	TRAVEL_CAND="$$(printf '%s\n' "$$PREVIEW_OUT" | awk -F': ' '/^Rows eligible for re-request:/ {print $$2}' | tail -n 1)"; \
	if [ -z "$$TRAVEL_CAND" ]; then TRAVEL_CAND="0"; fi; \
	echo "[TRAVEL] suspicious re-request candidates=$$TRAVEL_CAND"; \
	if [ "$$TRAVEL_CAND" = "0" ]; then \
		echo "[TRAVEL] skip API calls (no suspicious candidates)"; \
	elif [ "$(TRAVEL_CONFIRM)" = "1" ]; then \
		printf "Run suspicious re-request now? [y/N]: "; \
		read ans; \
		case "$$ans" in \
			y|Y|yes|YES) \
				TRAVEL_FORCE_API_FOR_MISSING=1 TRAVEL_AUTO_CONFIRM="$(if $(TRAVEL_AUTO_CONFIRM),$(TRAVEL_AUTO_CONFIRM),1)" TRAVEL_REQUESTS_PER_MINUTE="$(if $(TRAVEL_REQUESTS_PER_MINUTE),$(TRAVEL_REQUESTS_PER_MINUTE),$(TRAVEL_RPM))" $(PYTHON) main/tools/rerequest_suspicious_travel.py \
					--findings-csv "$(SUSPICIOUS_CSV)" \
					--target "$(if $(TARGET),$(TARGET),all)" \
					--score-threshold "$(if $(SCORE_THRESHOLD),$(SCORE_THRESHOLD),2)" \
					$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,);; \
			*) \
				echo "[TRAVEL] skip API calls (user declined)";; \
		esac; \
	else \
		TRAVEL_FORCE_API_FOR_MISSING=1 TRAVEL_AUTO_CONFIRM="$(if $(TRAVEL_AUTO_CONFIRM),$(TRAVEL_AUTO_CONFIRM),1)" TRAVEL_REQUESTS_PER_MINUTE="$(if $(TRAVEL_REQUESTS_PER_MINUTE),$(TRAVEL_REQUESTS_PER_MINUTE),$(TRAVEL_RPM))" $(PYTHON) main/tools/rerequest_suspicious_travel.py \
			--findings-csv "$(SUSPICIOUS_CSV)" \
			--target "$(if $(TARGET),$(TARGET),all)" \
			--score-threshold "$(if $(SCORE_THRESHOLD),$(SCORE_THRESHOLD),2)" \
			$(if $(filter 1 yes true,$(INCLUDE_INACTIVE)),--include-inactive,); \
	fi
	$(MAKE) validate-travel \
		TARGET="$(if $(TARGET),$(TARGET),all)" \
		RADIUS="$(RADIUS)" \
		INCLUDE_INACTIVE="$(INCLUDE_INACTIVE)" \
		SCORE_THRESHOLD="$(SCORE_THRESHOLD)" \
		MIN_ABS_DIFF="$(MIN_ABS_DIFF)" \
		MIN_REL_DIFF="$(MIN_REL_DIFF)" \
		MAD_MULT="$(MAD_MULT)" \
		MIN_NEIGHBORS="$(MIN_NEIGHBORS)" \
		MIN_POSTCODE_GROUP="$(MIN_POSTCODE_GROUP)" \
		MAX_TRAVEL_MINUTES="$(MAX_TRAVEL_MINUTES)" \
		FULL_TABLE="$(FULL_TABLE)" \
		TOP="$(TOP)" \
		CSV="$(CSV)"

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


