# GitHub Actions setup for nightly scrape

This repository includes a scheduled workflow in `.github/workflows/nightly-eiendom.yml`.

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

## Manual scripts (run locally when you want control)

You can use the root `Makefile` shortcuts instead of typing full Python commands.

Run `make help` to list available commands.

### Makefile quick commands

- `make gha` → CI-safe scrape (database only, no Google APIs)
- `make sheet` → manual Google Sheets sync
- `make travel` → fill only missing travel-time API fields
- `make full` → full manual run
- `make refresh` → refresh existing listing statuses from FINN

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

- Automatic: daily at `01:00 UTC`.
- Manual: Actions tab → **Nightly Eiendom Scrape (No Google APIs)** → **Run workflow**.

## Notes

- GitHub-hosted runners are ephemeral; local files generated during a run are not persisted between runs.
