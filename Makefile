PYTHON ?= python

.PHONY: help gha sheet travel full refresh

help:
	@echo "Available targets:"
	@echo "  make gha      - Run CI-safe scrape (db only, no Google APIs)"
	@echo "  make sheet    - Manually sync database to Google Sheets"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make full     - Full manual run (includes optional travel API prompts)"
	@echo "  make refresh  - Re-download listing pages and refresh statuses"

gha:
	$(PYTHON) main/tools/run_eiendom_github.py

sheet:
	$(PYTHON) main/tools/manual_sheet_update.py

travel:
	$(PYTHON) main/tools/manual_fill_missing_travel_times.py

full:
	$(PYTHON) main/tools/manage.py run eiendom

refresh:
	$(PYTHON) main/sync/refresh_listings.py
