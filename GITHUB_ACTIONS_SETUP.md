# GitHub Actions setup for manual scrape

This repository uses a manual-only workflow in `.github/workflows/nightly-eiendom.yml` (`workflow_dispatch` only, no cron schedule).

## What it runs

The workflow runs:

```bash
python main/tools/run_eiendom_github.py
```

This runs scraping + database update only.

It intentionally does **not** run:
- Google Sheets sync
- Google Sheets refresh/update scripts
- Google travel-time API calculations

## Required GitHub repository secrets

No Google API secrets are required for this workflow.

## Database persistence between runs

The workflow now persists SQLite state across runs using a GitHub Actions artifact named `properties-db`.

- Before scraping: it tries to restore the latest `properties-db` artifact into `main/database/`.
- After scraping: it uploads the updated database files (`properties.db`, and SQLite sidecar files if present).

This means listing state in `main/database/properties.db` is reused between manual runs, even though the runner itself is ephemeral.

Note: HTML files are still ephemeral in GitHub-hosted runners unless you explicitly upload them as artifacts too.

## Artifact cost control (pull locally, then delete)

To minimize GitHub artifact storage, use the local helper script to download artifacts and delete only those you have pulled.

### 1) Set token and repo (local terminal)

Use a PAT with `repo` scope (private repos) or enough Actions permissions to list/download/delete artifacts.

```bash
export GITHUB_TOKEN=YOUR_TOKEN
export REPO=owner/repo
```

### 2) Pull + delete in one step (recommended)

```bash
make artifacts-pull REPO=$REPO ARTIFACT_PREFIX=html-delta-
```

This writes downloaded zips + manifest locally under `artifacts/github/` and deletes remote artifacts after successful download.

### 3) Cleanup leftovers from manifest (if needed)

```bash
make artifacts-cleanup-manifest REPO=$REPO ARTIFACT_PREFIX=html-delta-
```

This deletes only artifacts that the manifest records as downloaded.

### 4) Optional age-based cleanup

Manual workflow: `.github/workflows/artifact-cleanup.yml`

- Actions tab → **Cleanup Artifacts** → **Run workflow**
- Set prefix (default `html-delta-`), age in days, and dry-run toggle.

Local equivalent:

```bash
make artifacts-cleanup-prefix REPO=$REPO ARTIFACT_PREFIX=html-delta- ARTIFACT_OLDER_DAYS=7
```

This is a safety cleanup by age/prefix (not tied to local download manifest).

## Manual scripts (run locally when you want control)

You can use the root `Makefile` shortcuts instead of typing full Python commands.

Run `make help` to list available commands.

### Makefile quick commands

- `make gha` → CI-safe scrape (scrape + database update, no Google Directions API)
- `make sheet` → manual Google Sheets sync
- `make travel` → fill only missing travel-time API fields
- `make full` → full manual run
- `make refresh` → refresh existing listing statuses from FINN
- `make artifacts-pull` → download artifacts locally, then delete remote copies
- `make artifacts-cleanup-manifest` → delete artifacts recorded as downloaded
- `make artifacts-cleanup-prefix` → delete artifacts by prefix + age

When you want to sync sheets manually:

```bash
python main/tools/manual_sheet_update.py
```

When you want to run only missing travel-time API calls (no scrape, no sheets):

```bash
python main/tools/manual_fill_missing_travel_times.py
```

When you want a full manual run (including optional travel API prompts):

```bash
python main/tools/manage.py run eiendom
```

## Triggering

- Manual only: Actions tab → **Manual Eiendom Scrape (No Google APIs)** → **Run workflow**.

## Notes

- GitHub-hosted runners are ephemeral; local files generated during a run are not persisted between runs.
