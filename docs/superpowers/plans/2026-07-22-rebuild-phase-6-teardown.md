# Rebuild Phase 6 — Teardown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the legacy system (code, Makefile, Apps Script map, fallback artifacts), untrack the live DB from git, and leave one clean, documented codebase whose standing checks are the 516-test suite and its fixture corpora.

**Architecture:** Ordered demolition: gate first (proof the rebuilt nightly + same-day activation work unattended), then sunset the legacy-comparison verify harnesses (they import `main.*` and must go BEFORE the code they compare against), then the DB untrack with a safe tracked→untracked server transition, then Apps Script undeploy, then the big deletion with a structural no-legacy-imports proof, then server rollout + fallback cleanup, then README + STATUS finalization.

**Tech Stack:** git surgery, clasp (undeploy), ssh ops. No new runtime code.

## Global Constraints

- **EXECUTION GATE (Task 1): do not start Tasks 2+ until the 2026-07-23 scheduled nightly verification passed** (log ends `nightly=0`; daily metrics real; ≥1 listing first-seen-today is active — the same-day observation). Legacy fallback artifacts stay restorable until Task 6 explicitly removes them.
- The live DB (`main/database/properties.db` — path KEEPS its location; only git tracking changes) is never put at risk: every server git operation happens OUTSIDE the 01:00-01:15 nightly window, with a fresh `cp` safety copy first.
- **The Sheets export STAYS** (user's read-only spreadsheet view). Only the Apps Script *map deployment* retires (user-approved 2026-07-22).
- KEEP: `data/eiendom/html_extracted/` (7 731-file ingest cache), `data/eiendom/html_snapshots/`, `data/thumbs/`, `rutetabeller tog/` (station source PDFs), `docs/`, `config/`, `skannonser/`, `tests/rebuild/`, the notify CLI (separate repo), Docker scheduler + web services.
- After every deletion task: full suite `.venv/bin/python -m pytest tests/rebuild -q` green, ZERO warnings; `skannonser --help`, `run nightly --help`, `web --help` still work.
- Commits per green cycle; messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Gate verification (evidence-only)

Confirm the 2026-07-23 08:00 scheduled check reported: nightly=0, real daily metrics, same-day-active listings > 0. Independently re-verify over ssh (read-only: log tail + the same-day SQL). Record in the ledger AND update STATUS.md: task-11 observation gate → PROVEN with numbers; backlog item 1 → fully closed. Commit STATUS: `rebuild(phase6): gate passed - same-day activation proven in production`. **If the gate FAILED: STOP the phase, investigate, report.**

---

### Task 2: Sunset the legacy-comparison verify harnesses

**Files:** Delete `skannonser/verify/parse.py`, `verify/enrich.py`, `verify/sheets.py`, `verify/metrics.py`; slim `skannonser/commands/verify_cmd.py` (remove the four modes; if nothing remains, delete the command file + its cli.py registration); Delete `tests/rebuild/test_verify_parse.py`, `test_verify_enrich.py`, `test_verify_sheets.py`, `test_verify_metrics.py`.
**Why now:** these import `main.*` to compare old-vs-new — their job (proving the port) is complete; they cannot survive Task 5's deletion. The STANDING checks going forward are the fixture corpora (`tests/rebuild/fixtures/`) + the full suite — say exactly this in a STATUS "Standing checks" note in the same commit.
- [ ] Grep first: `grep -rn "import main\|from main" skannonser/ tests/rebuild/` — the hits must ALL be in the files this task deletes (plus test monkeypatching of legacy in any straggler — handle each; nothing outside this task's list may import main after it). Delete, slim cli, suite green (count drops — record it), commit `rebuild(phase6): sunset legacy-comparison verify harnesses - fixture suite is the standing check`.

---

### Task 3: Untrack the live DB (user decision 2026-07-22)

**Laptop:** `git rm --cached main/database/properties.db`; add `main/database/properties.db` + `main/database/*.db-wal` + `*.db-shm` to .gitignore; commit `rebuild(phase6): untrack live DB from git (user decision) - server is sole writer, backups via scheduler`. Push.
**Server transition (OUTSIDE the nightly window; verify time first):**
```
cp main/database/properties.db ~/skannonser-preuntrack-$(date +%Y%m%d-%H%M%S).db
mv main/database/properties.db /tmp/db-hold.db      # worktree file out of git's way
git pull --ff-only                                   # the deletion commit applies cleanly
mv /tmp/db-hold.db main/database/properties.db       # back in place, now untracked+ignored
git status --short                                   # must show NOTHING for the db
.venv/bin/skannonser db stats | head -3              # db intact
```
- [ ] Also verify the docker web + scheduler containers still see the DB (volume mount unaffected — `curl healthz`). Record every step. The stash-dance dies today — note it in STATUS (server-access memory note comes at phase end).

---

### Task 4: Retire the Apps Script map deployment

- [ ] From `apps_script/map/`: `clasp deployments` (record all), then `clasp undeploy <id>` for each web-app deployment (the user-facing /exec URLs go 404). Verify: curl the old map URL → no longer serves the map (Google shows a disabled/removed page). The SHEET and its tabs remain untouched (export continues nightly).
- [ ] STATUS: map consumers section → "Apps Script map RETIRED the execution date (fill in); the web app (tailnet :8377) is the only map. Sheet remains as the read-only spreadsheet view." Commit (docs only; the apps_script/ code deletion is Task 5's).

---

### Task 5: The deletion

**Files (git rm -r):** `main/` (ALL — the DB file is untracked by Task 3 and physically stays; `git rm -r main` would try to delete the untracked file? No — git rm only touches tracked files; the untracked DB survives. VERIFY with `git rm -r -n main/` dry-run first and confirm properties.db is absent from the list), `scripts/`, `apps_script/`, `Makefile`, `requirements.txt`, legacy tests `tests/test_*.py` + `tests/__init__.py` if orphaned, tracked legacy data residue (`git ls-files data/ | grep -v html_extracted` — review the list: legacy CSVs/dumps get deleted; NOTHING under html_extracted/snapshots), root strays (`Systematiserte Finn-annonser - Stations (2).csv`).
- [ ] Structural proofs after deletion, all in one new test `tests/rebuild/test_no_legacy.py`: (a) `main/`, `scripts/`, `apps_script/` absent from git tracking; (b) no file under `skannonser/` or `tests/rebuild/` contains `import main` / `from main`; (c) `pyproject.toml` has no legacy references. Suite green (516 minus task-2 removals plus this one — record); `skannonser run nightly --help` + `web --help` work; `pip wheel` still builds (the packaging test from phase 5 re-proves static completeness).
- [ ] `.gitignore` sweep: remove now-meaningless legacy entries (keep data/ rules that still apply). Commit `rebuild(phase6): delete legacy system - one codebase remains`.

---

### Task 6: Server rollout + fallback cleanup

- [ ] Server (outside nightly window): pull (NO stash-dance needed — first clean pull!), `pip install -e '.[dev]'`, suite green on server, `docker compose build web scheduler && docker compose up -d` (rebuild images from the slimmed tree; healthz + scheduler Up), `skannonser db stats` sane.
- [ ] Remove fallback artifacts (gate passed; rollback target no longer exists in the repo anyway): `rm ~/run_skannonser_daily.legacy.sh ~/crontab.precutover.bak`. KEEP the timestamped DB safety copies in ~ (cheap insurance; note their paths in STATUS for eventual manual cleanup). Verify crontab entries unchanged and pointing at the wrapper + notify CLI.
- [ ] Watch-item: the NEXT nightly runs against the slimmed tree — schedule/note a log check.

---

### Task 7: README + STATUS finalization

**Files:** Create `README.md` (the repo has never had one): what the system does (Finn/DNB scanner → SQLite → enrichment via budget-gated Google APIs → sheet export + tailnet web app + Pushover notifications), architecture map (skannonser/ package layout, one paragraph per module group), ops runbook (server, crons, wrapper, docker services, .env keys, backup/restore, common commands incl. estimate/validate-travel, where logs live), development (venv, test suite, fixture corpora as standing checks, migration workflow), pointers to docs/rebuild/STATUS.md + specs/plans as history.
**STATUS.md final sweep:** header → "REBUILD COMPLETE (the execution date (fill in))"; Where-we-are collapses to the end-state; backlog: postnummer display → RESOLVED-BY-WEB-APP (web reads correct DB values; sheet stays bug-compatible by design — note the one-line apostrophe fix remains available if the sheet display ever matters); deferred-minors list → carried to a "Follow-ups" section verbatim; findings log stays as history; add "Standing checks" section (suite + fixtures + packaging test + healthz).
- [ ] Commit `rebuild(phase6): README + final status - rebuild complete`. Push.

---

### Task 8: Final review + close

- [ ] Final review = post-hoc audit. RULING: phase 6 works DIRECTLY on master (the server pulls master in Tasks 3/6, the gate already passed, and deletions are the point) — record the range-start commit in the ledger at Task 1, and after Task 7 dispatch a final reviewer over that master range (mostly deletions: verify the keep-list survived, the structural proofs, README accuracy against the real system, STATUS truthfulness). Fix wave if needed.
- [ ] Memory + ledger close-out: update the persistent memories (server-access: stash-dance obsolete; rebuild-pickup: complete; day-delay: delivered) and mark the phase closed.

## Acceptance gate
1. Gate task passed with recorded numbers; 2. suite green zero warnings at final count, `test_no_legacy` proofs in place; 3. server on the slimmed tree, both containers healthy, next nightly clean; 4. old map URL dead, sheet still updating nightly, web app serving; 5. DB untracked with clean pulls proven; 6. README exists and is accurate; STATUS says COMPLETE.
