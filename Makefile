PYTHON ?= python
REPO ?=
ARTIFACT_PREFIX ?= html-delta-
ARTIFACT_DEST ?= artifacts/github
ARTIFACT_MANIFEST ?= artifacts/github/download_manifest.json
ARTIFACT_OLDER_DAYS ?= 7

.PHONY: help gha sheet travel full refresh artifacts-pull artifacts-cleanup-manifest artifacts-cleanup-prefix

help:
	@echo "Available targets:"
	@echo "  make gha      - Run CI-safe scrape (scrape + DB update, no Google Directions API)"
	@echo "  make sheet    - Manually sync database to Google Sheets"
	@echo "  make travel   - Fill missing travel-time fields only (manual)"
	@echo "  make full     - Full manual run (includes optional travel API prompts)"
	@echo "  make refresh  - Re-download listing pages and refresh statuses"
	@echo "  make artifacts-pull             - Download artifacts, then delete remote copies"
	@echo "  make artifacts-cleanup-manifest - Delete artifacts listed as downloaded in manifest"
	@echo "  make artifacts-cleanup-prefix   - Delete artifacts by prefix/age"

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

artifacts-pull:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" pull --prefix "$(ARTIFACT_PREFIX)" --dest "$(ARTIFACT_DEST)" --manifest "$(ARTIFACT_MANIFEST)" --delete-after-download

artifacts-cleanup-manifest:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" cleanup-manifest --manifest "$(ARTIFACT_MANIFEST)" --prefix "$(ARTIFACT_PREFIX)"

artifacts-cleanup-prefix:
	@if [ -z "$(REPO)" ]; then echo "Set REPO=owner/repo"; exit 1; fi
	$(PYTHON) main/tools/github_artifacts.py --repo "$(REPO)" cleanup-prefix --prefix "$(ARTIFACT_PREFIX)" --older-than-days "$(ARTIFACT_OLDER_DAYS)"
