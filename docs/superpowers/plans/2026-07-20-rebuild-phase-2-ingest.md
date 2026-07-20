# Rebuild Phase 2 — Ingest Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Finn and DNB ingestion onto the new skeleton (sources → normalized records → repository upserts, no CSV intermediates), verified against the legacy parser by a golden-master harness — plus the three debts carried from Phase 1 (migration atomicity, notify-tables migration, backup retention).

**Architecture:** Sources are plugins under `skannonser/ingest/` producing `NormalizedListing` records whose field names are IDENTICAL to the legacy `A_live.csv` columns, so the repository port maps 1:1. The golden-master harness (`skannonser verify parse`) runs the legacy parser (imported read-only from `main.*`) and the new parser over the same cached ad HTML (7 731 files in `data/eiendom/html_extracted/`) and diffs field-by-field; zero unexplained differences is the bar for retiring the legacy path.

**Tech Stack:** Python 3.12 (`.venv`), BeautifulSoup4 + requests (already in the venv, add to pyproject), pydantic v2, Typer, pytest, stdlib sqlite3.

**Spec:** `docs/superpowers/specs/2026-07-20-skannonser-rebuild-design.md` §5.3 (ingest), §6 (verification), §8 phase 2. Phase 1 is merged (625a459) and deployed on the server.

## Global Constraints

- Python ≥3.11 via `.venv/bin/python` only; run tests as `.venv/bin/python -m pytest tests/rebuild -v`.
- Legacy code (`main/`, `scripts/`, Makefile) stays frozen. The verify harness may IMPORT legacy modules read-only; nothing may modify them.
- **The live DB (`main/database/properties.db`) is never written by tests, fixtures, or verify runs.** Anything needing a DB uses a tmp copy or a fresh migrated DB. The SERVER's DB is the authoritative one; the laptop copy is stale — golden-master runs against DB content must note which copy they used.
- New code only under `skannonser/`; tests under `tests/rebuild/`; no `input()` anywhere; no CSV intermediates between new pipeline stages (raw-page archival to disk is allowed for debuggability).
- `NormalizedListing` field names must equal the legacy `A_live.csv` column names produced by `main/extractors/extraction_eiendom.py:extract_eiendom_data` — derive them from that function, do not invent names.
- Intentional behavior changes vs legacy (exactly three sanctioned: robust finnkode parsing via `urllib.parse`; dropping the `len(href) <= 100` URL heuristic; search-style ad-link matching so absolute-href listings are no longer silently dropped — discovered in Task 7, controller-sanctioned, regression-locked by test) must appear in `config/verify-allowlist.toml` with a justification, and nowhere else. The third fix means the new crawler can find listings legacy misses — the Task 13 parallel-run comparison must treat "present in new, absent in legacy, AND the ad link is absolute-href on the crawled page" as explained.
- Network access in tests: none. All parser/crawl tests run on cached HTML/fixtures. Live-crawl smoke tests are explicit manual checkpoints.
- Add to `pyproject.toml` dependencies (Task 5): `beautifulsoup4>=4.12`, `requests>=2.31`.
- Commit after every green test cycle; messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Migration runner atomicity

Phase-1 debt (MUST land before migration 002). `migrations.migrate()` currently uses `conn.executescript()`, which autocommits per statement — a mid-migration failure leaves applied-but-unrecorded state.

**Files:**
- Modify: `skannonser/store/migrations.py`
- Test: `tests/rebuild/test_migrations.py` (extend)

**Interfaces:**
- Consumes: existing `migrations.migrate(conn) -> list[str]`, `pending(conn) -> list[Path]`, `MIGRATIONS_DIR`.
- Produces: same public API, now atomic per migration file; new private `_statements(sql: str) -> list[str]` splitting on complete statements via `sqlite3.complete_statement`.

- [ ] **Step 1: Write the failing test** (append to `tests/rebuild/test_migrations.py`)

```python
def test_failed_migration_rolls_back_and_is_not_recorded(tmp_path, monkeypatch):
    mig_dir = tmp_path / "migs"
    mig_dir.mkdir()
    (mig_dir / "001_good.sql").write_text("CREATE TABLE a (x INTEGER);")
    (mig_dir / "002_bad.sql").write_text(
        "CREATE TABLE b (x INTEGER);\nINSERT INTO nope VALUES (1);"
    )
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", mig_dir)
    conn = connection.connect(tmp_path / "x.db")

    with pytest.raises(sqlite3.OperationalError):
        migrations.migrate(conn)

    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    assert "a" in tables          # 001 fully applied and recorded
    assert "b" not in tables      # 002 rolled back entirely, no partial DDL
    applied = {r["id"] for r in conn.execute("SELECT id FROM schema_migrations")}
    assert applied == {"001_good"}
```

Add `import sqlite3` to the test file's imports if missing.

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_migrations.py::test_failed_migration_rolls_back_and_is_not_recorded -v`
Expected: FAIL — with `executescript`, table `b` exists (partial DDL persisted).

- [ ] **Step 3: Implement atomic migrate**

Replace `migrate()` in `skannonser/store/migrations.py` and add `_statements`:

```python
def _statements(sql: str) -> list[str]:
    """Split a script into complete statements using sqlite3.complete_statement."""
    statements, buf = [], ""
    for line in sql.splitlines(keepends=True):
        stripped = line.strip()
        if not buf and (not stripped or stripped.startswith("--")):
            continue
        buf += line
        if sqlite3.complete_statement(buf):
            statements.append(buf.strip())
            buf = ""
    if buf.strip():
        statements.append(buf.strip())
    return statements


def migrate(conn: sqlite3.Connection) -> list[str]:
    ran: list[str] = []
    for path in pending(conn):
        stmts = _statements(path.read_text(encoding="utf-8"))
        try:
            conn.execute("BEGIN IMMEDIATE")
            for stmt in stmts:
                conn.execute(stmt)
            conn.execute("INSERT INTO schema_migrations (id) VALUES (?)", (path.stem,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        ran.append(path.stem)
    return ran
```

Note: SQLite DDL is transactional, so rollback undoes `CREATE TABLE` too. `conn.execute` (not `executescript`) keeps everything inside the explicit transaction.

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/python -m pytest tests/rebuild -v`
Expected: all pass, including the existing fresh-DB/idempotency/adoption tests (001 must still apply through the new path).

- [ ] **Step 5: Commit**

```bash
git add skannonser/store/migrations.py tests/rebuild/test_migrations.py
git commit -m "rebuild(phase2): atomic per-file migrations with rollback

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Migration 002 — notify tables

The server DB has three tables that migration 001 does not create (legacy creates them lazily); fresh DBs must get them.

**Files:**
- Create: `skannonser/store/migrations/002_notify_tables.sql`
- Test: `tests/rebuild/test_migrations.py` (extend)

**Interfaces:**
- Consumes: migration runner from Task 1.
- Produces: fresh DBs contain `eiendom_status_history`, `daily_listing_snapshot`, `daily_metrics`.

- [ ] **Step 1: Write the migration** — DDL ported verbatim from `main/database/db.py:301-330` (guards added):

```sql
CREATE TABLE IF NOT EXISTS eiendom_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    finnkode TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_status_history_finnkode ON eiendom_status_history(finnkode);
CREATE TABLE IF NOT EXISTS daily_listing_snapshot (
    finnkode TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date TEXT PRIMARY KEY,
    added INTEGER NOT NULL DEFAULT 0,
    removed_sold INTEGER NOT NULL DEFAULT 0,
    removed_delisted INTEGER NOT NULL DEFAULT 0,
    total_active INTEGER NOT NULL DEFAULT 0
);
```

- [ ] **Step 2: Write the failing test**

```python
def test_migration_002_creates_notify_tables(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert ran == ["001_adopt_live_schema", "002_notify_tables"]
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"eiendom_status_history", "daily_listing_snapshot", "daily_metrics"} <= tables
```

(Fails before Step 1's file exists; with the file present it passes — write test first, watch it fail on `ran == [...]`, then add the file.)

- [ ] **Step 3: Run failing → add file → run passing**

Run: `.venv/bin/python -m pytest tests/rebuild/test_migrations.py -v` — the new test fails first (only 001 in `ran`), passes after the .sql file exists. Full suite green.

- [ ] **Step 4: Apply on BOTH machines (adoption no-ops on the server, which already has these tables)**

```bash
.venv/bin/skannonser db backup && .venv/bin/skannonser db migrate   # laptop
ssh mbp2016@100.77.139.22 'cd ~/kode/skannonser && .venv/bin/skannonser db backup && .venv/bin/skannonser db migrate && .venv/bin/skannonser db stats | head -4'
```

Expected: `Applied: 002_notify_tables` on both; server row counts unchanged.

- [ ] **Step 5: Commit**

```bash
git add skannonser/store/migrations/002_notify_tables.sql tests/rebuild/test_migrations.py
git commit -m "rebuild(phase2): migration 002 - notify tables (status history, snapshot, metrics)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Backup retention + partial-file cleanup

**Files:**
- Modify: `skannonser/commands/db_cmd.py` (backup command)
- Modify: `docker/crontab`
- Test: `tests/rebuild/test_db_backup.py` (extend)

**Interfaces:**
- Consumes: existing `backup` command.
- Produces: `skannonser db backup [--dest-dir backups] [--keep 30]` — after a successful backup, delete the oldest `properties-*.db` files beyond the newest `keep` (0 = keep all); on a failed copy, the partial dest file is removed before the error propagates.

- [ ] **Step 1: Write the failing tests** (append to `tests/rebuild/test_db_backup.py`)

```python
def test_backup_prunes_old_backups_beyond_keep(tmp_path, monkeypatch):
    src = tmp_path / "live.db"
    sqlite3.connect(src).execute("CREATE TABLE t (x INTEGER)").connection.commit()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))
    dest_dir = tmp_path / "backups"
    dest_dir.mkdir()
    for stamp in ("20260101-000000", "20260102-000000", "20260103-000000"):
        (dest_dir / f"properties-{stamp}.db").write_bytes(b"old")

    result = CliRunner().invoke(
        app, ["db", "backup", "--dest-dir", str(dest_dir), "--keep", "2"])

    assert result.exit_code == 0, result.output
    remaining = sorted(p.name for p in dest_dir.glob("properties-*.db"))
    assert len(remaining) == 2                       # newest 2 kept (incl. the one just made)
    assert "properties-20260101-000000.db" not in remaining
    assert "properties-20260102-000000.db" not in remaining


def test_backup_removes_partial_file_on_failure(tmp_path, monkeypatch):
    src = tmp_path / "live.db"
    sqlite3.connect(src).execute("CREATE TABLE t (x INTEGER)").connection.commit()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))
    dest_dir = tmp_path / "backups"

    import sqlite3 as _sqlite3
    real_connect = _sqlite3.Connection.backup
    def boom(self, *a, **k):
        raise _sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(_sqlite3.Connection, "backup", boom)

    result = CliRunner().invoke(app, ["db", "backup", "--dest-dir", str(dest_dir)])

    assert result.exit_code != 0
    assert not list(dest_dir.glob("properties-*.db"))
```

Add `import sqlite3` at the top of the file if missing.

- [ ] **Step 2: Run to verify both fail**

Run: `.venv/bin/python -m pytest tests/rebuild/test_db_backup.py -v`
Expected: prune test fails (3+1 files remain); partial-file test fails (partial file left).

- [ ] **Step 3: Implement**

Update `backup` in `skannonser/commands/db_cmd.py`:

```python
@app.command()
def backup(
    dest_dir: Path = typer.Option(Path("backups"), help="Backup directory"),
    keep: int = typer.Option(30, help="How many newest backups to keep (0 = keep all)"),
) -> None:
    """Copy the live DB via SQLite's online backup API (safe under WAL)."""
    src = get_secrets().db_path
    if not src.exists():
        typer.echo(f"Error: database not found at {src}", err=True)
        raise typer.Exit(code=1)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"properties-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    src_conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    dest_conn = sqlite3.connect(dest)
    try:
        with dest_conn:
            src_conn.backup(dest_conn)
    except Exception:
        dest_conn.close()
        dest.unlink(missing_ok=True)
        raise
    finally:
        src_conn.close()
        if not dest_conn.in_transaction:
            dest_conn.close()
    typer.echo(f"Backed up {src} -> {dest}")
    if keep > 0:
        backups = sorted(dest_dir.glob("properties-*.db"))
        for old in backups[:-keep]:
            old.unlink()
            typer.echo(f"Pruned {old.name}")
```

(If the double-close bookkeeping fights you, simplest correct form: close `dest_conn` in the `except` before unlink and in a plain `finally` guard with a `closed` flag — the tests define the behavior that matters.)

Update `docker/crontab` to pass retention explicitly:

```
# Nightly DB backup at 03:00 UTC, keep newest 30.
0 3 * * * skannonser db backup --keep 30
```

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/python -m pytest tests/rebuild -v` — all green.

- [ ] **Step 5: Commit**

```bash
git add skannonser/commands/db_cmd.py docker/crontab tests/rebuild/test_db_backup.py
git commit -m "rebuild(phase2): backup retention (--keep) and partial-file cleanup on failure

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Shared geometry + normalization utils

Deduplicates logic currently copy-pasted across the DNB filter variants.

**Files:**
- Create: `skannonser/geo.py`
- Create: `skannonser/ingest/__init__.py` (empty)
- Create: `skannonser/textnorm.py`
- Test: `tests/rebuild/test_geo.py`

**Interfaces:**
- Produces: `geo.is_point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool` (polygon points are `(lng, lat)` — same convention as `config/domain.toml`); `textnorm.normalize_addr(s: str) -> str`; `textnorm.normalize_pc(pc) -> str`.

- [ ] **Step 1: Write the failing test**

```python
from skannonser.config.domain import load_domain
from skannonser.geo import is_point_in_polygon
from skannonser.textnorm import normalize_addr, normalize_pc


def test_oslo_center_inside_polygon_north_sea_outside():
    polygon = load_domain().polygon_points
    assert is_point_in_polygon(59.9139, 10.7522, polygon)       # Oslo center
    assert not is_point_in_polygon(58.0, 3.0, polygon)          # North Sea


def test_normalizers_match_legacy():
    import sys
    sys.path.insert(0, ".")
    from main.extractors.filter_and_load_dnbeiendom_no_buffer import (
        normalize_addr as legacy_addr, normalize_pc as legacy_pc)
    samples = ["  Storgata 1 B, 0155 OSLO ", "Ullevålsveien 3", "", "Bjørnsons gate 2A"]
    for s in samples:
        assert normalize_addr(s) == legacy_addr(s)
    for pc in ["0155", 155, "0155.0", None, ""]:
        assert normalize_pc(pc) == legacy_pc(pc)
```

- [ ] **Step 2: Run to verify it fails** (ModuleNotFoundError), then **port**:
`is_point_in_polygon` verbatim from `main/extractors/filter_and_load_dnbeiendom_no_buffer.py:17-38`; `normalize_addr`/`normalize_pc` verbatim from the same file lines 40-55, into the two new modules. The legacy-comparison test pins byte-equality of the port.

- [ ] **Step 3: Run passing, full suite, commit**

```bash
git add skannonser/geo.py skannonser/textnorm.py skannonser/ingest/__init__.py tests/rebuild/test_geo.py
git commit -m "rebuild(phase2): shared point-in-polygon and address/postcode normalizers (legacy-pinned)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: NormalizedListing model + source contract

**Files:**
- Create: `skannonser/ingest/base.py`
- Modify: `pyproject.toml` (add `beautifulsoup4>=4.12`, `requests>=2.31` to dependencies; re-run `.venv/bin/python -m pip install -e '.[dev]'`)
- Test: `tests/rebuild/test_ingest_base.py`

**Interfaces:**
- Produces:
  - `NormalizedListing` — a pydantic model whose fields are EXACTLY the dict keys returned by `main/extractors/extraction_eiendom.py:extract_eiendom_data` (read that function, lines 13-56, and transcribe the keys; all values typed `str | int | float | None` as appropriate, everything optional except `Finnkode` and `URL`). Extra method: `to_row() -> dict` returning the dict with the same keys (for DataFrame/repository consumption).
  - `class Source(Protocol)`: `name: str`; `crawl(self) -> list[str]` (listing URLs); `parse(self, url: str) -> NormalizedListing | None`.

- [ ] **Step 1: Read `main/extractors/extraction_eiendom.py:13-56`** and list the exact returned dict keys in your report (they become the model fields — the plan intentionally does not restate them; the source is authoritative).

- [ ] **Step 2: Write the failing test**

```python
from skannonser.ingest.base import NormalizedListing


def test_fields_match_legacy_extractor_keys():
    import ast
    from pathlib import Path
    src = Path("main/extractors/extraction_eiendom.py").read_text()
    # Collect every string key assigned into the result dict of extract_eiendom_data.
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "extract_eiendom_data")
    keys = {t.slice.value for n in ast.walk(fn)
            for t in [n] if isinstance(n, ast.Subscript)
            and isinstance(n.slice, ast.Constant) and isinstance(n.slice.value, str)}
    dict_keys = {k.value for n in ast.walk(fn) if isinstance(n, ast.Dict)
                 for k in n.keys if isinstance(k, ast.Constant)}
    legacy_keys = keys | dict_keys
    model_fields = set(NormalizedListing.model_fields)
    missing = legacy_keys - model_fields
    assert not missing, f"model missing legacy fields: {missing}"


def test_roundtrip_to_row():
    listing = NormalizedListing(Finnkode="123", URL="https://finn.no/x?finnkode=123")
    row = listing.to_row()
    assert row["Finnkode"] == "123"
```

(If the legacy function builds the record differently than assumed — e.g. via a dict literal only — adapt the AST harvest accordingly; the assertion "every legacy key is a model field" is the requirement.)

- [ ] **Step 3: Run failing → implement `base.py` → run passing.** Model config: `model_config = ConfigDict(extra="forbid")`. Full suite green.

- [ ] **Step 4: Commit**

```bash
git add skannonser/ingest/base.py pyproject.toml tests/rebuild/test_ingest_base.py
git commit -m "rebuild(phase2): NormalizedListing model pinned to legacy extractor fields; source protocol

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Listings repository (upsert + lifecycle)

The delicate one: port the DB-write semantics exactly, but batched in one transaction (legacy commits per row).

**Files:**
- Create: `skannonser/store/repositories/__init__.py` (empty)
- Create: `skannonser/store/repositories/listings.py`
- Test: `tests/rebuild/test_listings_repo.py`

**Interfaces:**
- Consumes: `connection.connect`, `NormalizedListing`.
- Produces: `class ListingsRepo` with `__init__(self, conn: sqlite3.Connection)`;
  `upsert(self, listings: list[NormalizedListing]) -> dict` returning `{"inserted": int, "updated": int, "excluded": int}`;
  `mark_inactive(self, active_finnkodes: list[str]) -> int` (rows deactivated);
  `active_finnkodes(self) -> set[str]`.

- [ ] **Step 1: Read the legacy source of truth**: `main/database/db.py:368-375` (`_is_excluded_eiendom_url`), `377-539` (`insert_or_update_eiendom` — note the column mapping from A_live keys to DB columns, the overrides application via `overrides.apply_overrides_to_data`, the update-only-changed-columns behavior, and `updated_at`/`active` handling), `541-565` (`mark_inactive` — sets `active=0` for finnkodes absent from the given list, never deletes). Your port must reproduce: the same column mapping, the same exclusion rule, the same override application, and the same active/inactive semantics — but with ONE transaction for the whole batch and `sqlite3` instead of pandas.

- [ ] **Step 2: Write the failing tests**

```python
import sqlite3

import pytest

from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo


@pytest.fixture()
def repo(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    return ListingsRepo(conn)


def _listing(finnkode: str, **kw) -> NormalizedListing:
    return NormalizedListing(
        Finnkode=finnkode, URL=f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}", **kw)


def test_upsert_inserts_then_updates(repo):
    r1 = repo.upsert([_listing("111")])
    assert r1 == {"inserted": 1, "updated": 0, "excluded": 0}
    r2 = repo.upsert([_listing("111")])
    assert r2["inserted"] == 0


def test_mark_inactive_deactivates_missing_never_deletes(repo):
    repo.upsert([_listing("111"), _listing("222")])
    n = repo.mark_inactive(["111"])
    assert n == 1
    assert repo.active_finnkodes() == {"111"}
    total = repo.conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0]
    assert total == 2                      # 222 deactivated, not deleted


def test_upsert_is_one_transaction(repo, monkeypatch):
    listings = [_listing("111"), _listing("BAD")]
    real_execute = repo.conn.execute
    calls = {"n": 0}
    def flaky(sql, *a):
        calls["n"] += 1
        if "BAD" in str(a):
            raise sqlite3.OperationalError("boom")
        return real_execute(sql, *a)
    monkeypatch.setattr(repo.conn, "execute", flaky)
    with pytest.raises(sqlite3.OperationalError):
        repo.upsert(listings)
    monkeypatch.undo()
    assert repo.conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0] == 0
```

(Adjust `_listing` kwargs once Task 5 fixed the real field names; the three behaviors asserted — idempotent upsert, deactivate-not-delete, all-or-nothing batch — are the requirements. If the transaction-injection test proves brittle against your implementation shape, replace it with one that makes the SECOND listing violate a real constraint and assert the first was rolled back.)

- [ ] **Step 3: Run failing → implement → run passing.** Full suite green.

- [ ] **Step 4: Golden-master check against legacy on a DB copy** (manual checkpoint, record output in your report):

```bash
cp main/database/properties.db /tmp/gm-legacy.db && cp main/database/properties.db /tmp/gm-new.db
.venv/bin/python - <<'EOF'
# Load data/eiendom/A_live.csv (last legacy run's output); apply legacy
# insert_or_update_eiendom to gm-legacy.db and ListingsRepo.upsert to gm-new.db;
# then diff: SELECT * FROM eiendom ORDER BY finnkode on both, column by column,
# ignoring updated_at timestamps. Print the first 10 differences or "IDENTICAL".
EOF
```

Write this comparison script for real (it is throwaway, keep it in your report, not the repo). Bar: IDENTICAL, or every difference explained by the sanctioned allowlist items. If A_live.csv is stale on the laptop, fetch the server's copy via scp first — never run against a live DB.

- [ ] **Step 5: Commit**

```bash
git add skannonser/store/repositories/ tests/rebuild/test_listings_repo.py
git commit -m "rebuild(phase2): listings repository - batched upsert + inactive lifecycle, legacy semantics

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Finn crawl port

**Files:**
- Create: `skannonser/ingest/finn/__init__.py` (empty)
- Create: `skannonser/ingest/finn/crawl.py`
- Test: `tests/rebuild/test_finn_crawl.py`

**Interfaces:**
- Consumes: `load_domain()` (polygon, `url_max_price`).
- Produces: `build_search_url(domain: DomainConfig) -> str` (ports `build_finn_polylocation` from `main/runners/run_eiendom_db.py:106-119` and the filter suffix from `get_finn_scrape_config` lines 121-155); `extract_ad_urls(html: str) -> list[tuple[str, str]]` returning `(finnkode, url)` pairs; `crawl(domain, fetch=requests.get, archive_dir: Path | None = None, max_pages: int = 50) -> list[tuple[str, str]]` paginating until a page yields no new ads, archiving each page's HTML to `archive_dir` when given.

**Sanctioned fixes (allowlisted):** finnkode extracted with `urllib.parse.urlparse`/`parse_qs` (legacy: `url.split('finnkode=')[1]`, breaks with trailing params); ad links matched by explicit pattern `re.compile(r'/realestate/homes/ad\.html\?[^"\']*finnkode=\d+')`-style logic derived from `main/crawl.py:15-48` instead of the `len(href) <= 100` heuristic. Read `main/crawl.py` first and keep the matching as close to legacy as those two fixes allow.

- [ ] **Step 1: Write the failing test** — use a real archived result page as fixture:

```python
import gzip
from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.finn.crawl import build_search_url, extract_ad_urls

FIXTURE = Path("data/eiendom/html_crawled/page1.html")


def test_build_search_url_contains_polygon_and_price():
    url = build_search_url(load_domain())
    assert "polylocation=" in url
    assert "price_to=7500000" in url or "7500000" in url


def test_extract_ad_urls_from_real_result_page():
    pairs = extract_ad_urls(FIXTURE.read_text(encoding="utf-8", errors="replace"))
    assert len(pairs) >= 10
    finnkodes = [fk for fk, _ in pairs]
    assert all(fk.isdigit() for fk in finnkodes)
    assert len(set(finnkodes)) == len(finnkodes)          # deduped


def test_finnkode_robust_to_trailing_params():
    html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=123456789&utm_source=x">a</a>'
    assert extract_ad_urls(html) == [("123456789",
        "https://www.finn.no/realestate/homes/ad.html?finnkode=123456789&utm_source=x")]
```

Compare `build_search_url` output against legacy in the same test file:

```python
def test_search_url_matches_legacy():
    import sys
    sys.path.insert(0, ".")
    from main.runners.run_eiendom_db import get_finn_scrape_config
    legacy_url = get_finn_scrape_config()[0] if isinstance(get_finn_scrape_config(), tuple) else None
    # Read get_finn_scrape_config's actual return shape first and adapt:
    # the requirement is that the new URL string equals the legacy url_base exactly.
```

Read `main/runners/run_eiendom_db.py:121-155` and finish this test so it asserts exact string equality with the legacy-generated URL.

- [ ] **Step 2: Run failing → port → run passing.** Full suite green.

- [ ] **Step 3: Commit**

```bash
git add skannonser/ingest/finn/ tests/rebuild/test_finn_crawl.py
git commit -m "rebuild(phase2): finn crawl port - legacy-equal search URL, robust ad extraction

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: HTML cache port

**Files:**
- Create: `skannonser/ingest/finn/html_cache.py`
- Test: `tests/rebuild/test_html_cache.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `save_ad_html(project_dir: Path, uid: str, html: str, snapshot_dir: Path | None = None, today: str | None = None) -> Path` and `load_or_fetch(url: str, project_dir: Path, uid: str, fetch=requests.get) -> str` — port of `main/extractors/ad_html_loader.py` (atomic write, gzipped dated snapshot only when content changed, canonical path `{project_dir}/html_extracted/{uid}.html`). Path layout MUST stay identical — the 7 731 existing cached files must be readable by the new code.

- [ ] **Step 1: Port the existing legacy tests**: read `tests/test_ad_html_loader.py` (82 lines) and rewrite its cases against the new module in `tests/rebuild/test_html_cache.py` (same behaviors: atomic canonical write; snapshot created on content change, not on identical content). Add one new test:

```python
def test_reads_existing_legacy_cache(tmp_path):
    proj = tmp_path / "proj"
    (proj / "html_extracted").mkdir(parents=True)
    (proj / "html_extracted" / "42.html").write_text("<html>cached</html>")
    html = load_or_fetch("https://x", proj, "42", fetch=_fail_if_called)
    assert html == "<html>cached</html>"
```

with `def _fail_if_called(*a, **k): raise AssertionError("network hit for cached ad")`.

- [ ] **Step 2: Run failing → port from `main/extractors/ad_html_loader.py` (lines 19-113, near-verbatim; pathlib instead of os.path is fine, layout identical) → run passing.** Full suite green.

- [ ] **Step 3: Commit**

```bash
git add skannonser/ingest/finn/html_cache.py tests/rebuild/test_html_cache.py
git commit -m "rebuild(phase2): ad HTML cache port - atomic writes, dated snapshots, legacy layout

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Finn parser port + fixture corpus

**Files:**
- Create: `skannonser/ingest/finn/parse.py`
- Create: `tests/rebuild/fixtures/finn/` (12 cached ads + expected JSON, generated below)
- Create: `tests/rebuild/fixtures/finn/generate_expected.py` (the generator, committed for reproducibility)
- Test: `tests/rebuild/test_finn_parse.py`

**Interfaces:**
- Consumes: `NormalizedListing`.
- Produces: `parse_ad(html: str, finnkode: str, url: str) -> NormalizedListing` — ports every field extractor from `main/extractors/parsing_helpers_common.py` (getAllSizes, getBuyPrice, getAddress, getStatus, getConstructionYear, getPlotOwnership, getPropertyType, getImageUrl, ...) and the assembly logic of `main/extractors/extraction_eiendom.py:extract_eiendom_data` (lines 13-56), byte-identical output.

- [ ] **Step 1: Build the fixture corpus.** Write `tests/rebuild/fixtures/finn/generate_expected.py`:

```python
"""Regenerate expected-output fixtures by running the LEGACY parser.
Usage: .venv/bin/python tests/rebuild/fixtures/finn/generate_expected.py
Picks a deterministic spread of cached ads and freezes the legacy parser's
field dict for each as <finnkode>.expected.json, copying the HTML alongside.
"""
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

CACHE = Path("data/eiendom/html_extracted")
DEST = Path(__file__).parent
# Deterministic sample: sort by name, take every len//12-th — covers old and new ads.
ads = sorted(CACHE.glob("*.html"))
sample = ads[:: max(1, len(ads) // 12)][:12]

from bs4 import BeautifulSoup
from main.extractors import parsing_helpers_common as legacy

for path in sample:
    finnkode = path.stem
    soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="replace"), "html.parser")
    expected = {
        "Adresse": legacy.getAddress(soup),
        "Pris": legacy.getBuyPrice(soup),
        "Status": legacy.getStatus(soup),
        "Byggeaar": legacy.getConstructionYear(soup),
        "Eieform": legacy.getPlotOwnership(soup),
        "Boligtype": legacy.getPropertyType(soup),
        "ImageUrl": legacy.getImageUrl(soup),
        "Sizes": legacy.getAllSizes(soup),
    }
    (DEST / f"{finnkode}.expected.json").write_text(
        json.dumps(expected, ensure_ascii=False, indent=1, default=str))
    shutil.copy(path, DEST / f"{finnkode}.html")
    print("fixture:", finnkode)
```

Adapt the `expected` dict keys to whatever `extract_eiendom_data` actually calls each field (read it first — the fixture keys must match the NormalizedListing/legacy field names from Task 5). Run it; commit the 12 HTML + 12 JSON files.

- [ ] **Step 2: Write the failing test**

```python
import json
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse import parse_ad

FIXTURES = Path(__file__).parent / "fixtures" / "finn"
CASES = sorted(FIXTURES.glob("*.expected.json"))


@pytest.mark.parametrize("expected_path", CASES, ids=lambda p: p.stem.split(".")[0])
def test_parse_matches_legacy_fixture(expected_path):
    finnkode = expected_path.stem.split(".")[0]
    html = (FIXTURES / f"{finnkode}.html").read_text(encoding="utf-8", errors="replace")
    expected = json.loads(expected_path.read_text())
    listing = parse_ad(html, finnkode, f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}")
    row = listing.to_row()
    for field, want in expected.items():
        assert row.get(field) == want, f"{finnkode}.{field}: {row.get(field)!r} != {want!r}"
```

(Field-name mapping between fixture keys and row keys must be exact — resolve it while implementing Step 1.)

- [ ] **Step 3: Run failing → port the parser → run passing** (all 12 parametrized cases). Full suite green.

- [ ] **Step 4: Commit**

```bash
git add skannonser/ingest/finn/parse.py tests/rebuild/fixtures/finn/ tests/rebuild/test_finn_parse.py
git commit -m "rebuild(phase2): finn parser port with legacy-frozen fixture corpus

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: verify parse — the golden-master harness

**Files:**
- Create: `skannonser/verify/__init__.py` (empty)
- Create: `skannonser/verify/parse.py`
- Create: `skannonser/commands/verify_cmd.py`
- Create: `config/verify-allowlist.toml`
- Modify: `skannonser/cli.py` (register `verify` sub-app)
- Test: `tests/rebuild/test_verify_parse.py`

**Interfaces:**
- Consumes: `parse_ad` (Task 9), legacy `main.extractors` (imported inside the harness only).
- Produces: `verify_parse(cache_dir: Path, limit: int | None, allowlist: dict) -> VerifyResult` where `VerifyResult` has `.total: int`, `.identical: int`, `.allowlisted: int`, `.diffs: list[FieldDiff]` (`FieldDiff`: `finnkode, field, legacy_value, new_value`); CLI `skannonser verify parse [--limit N] [--cache-dir data/eiendom]` printing a summary and the first 20 unexplained diffs, exit 1 if any unexplained diff.

- [ ] **Step 1: Write `config/verify-allowlist.toml`**

```toml
# Sanctioned intentional differences between legacy and rebuilt parsers.
# Every entry needs a reason. `skannonser verify parse` treats matching diffs
# as explained; anything else fails the run.

[[allow]]
field = "Finnkode"
reason = "urllib-based query parsing; legacy split('finnkode=')[1] keeps trailing params"

[[allow]]
field = "URL"
reason = "URL canonicalization side effect of robust finnkode parsing"
```

(If verification later shows these never fire, delete them — an allowlist entry that explains nothing is noise.)

- [ ] **Step 2: Write the failing test** (unit-level, small fake cache):

```python
from pathlib import Path

from skannonser.verify.parse import verify_parse


def test_verify_parse_reports_identical_on_fixture_corpus():
    result = verify_parse(Path("data/eiendom"), limit=12, allowlist={})
    assert result.total == 12
    assert result.identical + result.allowlisted + len(result.diffs) == result.total
```

- [ ] **Step 3: Implement.** `verify_parse` iterates `cache_dir/html_extracted/*.html` (sorted, up to limit), for each: run the legacy field extractors (same calls as the fixture generator) and `parse_ad`, compare field-by-field, classify each difference as allowlisted (field matches an `[[allow]]` entry) or a real diff. The CLI command wires it with Typer options and the TOML allowlist.

- [ ] **Step 4: THE CHECKPOINT — full-corpus run:**

Run: `.venv/bin/skannonser verify parse` (all 7 731 cached ads; takes a while, that's fine).
Expected: `unexplained diffs: 0`. Anything else: investigate each diff — either the port is wrong (fix it) or the difference is legitimately sanctioned (only the two known fixes qualify; anything new needs the controller's sign-off before an allowlist entry is added). Record the summary line in your report.

- [ ] **Step 5: Commit**

```bash
git add skannonser/verify/ skannonser/commands/verify_cmd.py config/verify-allowlist.toml skannonser/cli.py tests/rebuild/test_verify_parse.py
git commit -m "rebuild(phase2): golden-master verify parse harness with allowlist

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 11: DNB source port (crawl + parse)

**Files:**
- Create: `skannonser/ingest/dnb/__init__.py` (empty)
- Create: `skannonser/ingest/dnb/crawl.py`
- Create: `skannonser/ingest/dnb/parse.py`
- Modify: `config/domain.toml` + `skannonser/config/domain.py` (add `[dnb]` section: `region_guids` list ported verbatim from `main/extractors/extract_dnbeiendom.py:26-28`, `max_pages`)
- Test: `tests/rebuild/test_dnb.py`

**Interfaces:**
- Produces: `dnb.crawl.build_search_url(domain) -> str`; `dnb.crawl.extract_listing_urls(html: str) -> list[str]` (JSON-LD `ItemList` first, anchor fallback — port from `extract_dnbeiendom.py:64-128`); `dnb.parse.parse_listing(html: str, url: str) -> dict | None` (JSON-LD `RealEstateListing` → the same dict shape legacy `extract_fields_from_entry` produces, `extract_dnbeiendom_ads.py:47-106`).

- [ ] **Step 1: Create fixtures** from `data/dnbeiendom/html_crawled/page1.html` (already in repo) and one saved listing page: fetch ONE live DNB listing page with `curl -sL <url from data/dnbeiendom/0_URLs.csv line 2> -o tests/rebuild/fixtures/dnb/listing1.html` (single manual fetch, sanctioned; commit the file).

- [ ] **Step 2: Write the failing tests**

```python
from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.dnb import crawl, parse

FIXTURES = Path(__file__).parent / "fixtures" / "dnb"


def test_search_url_contains_all_region_guids():
    d = load_domain()
    url = crawl.build_search_url(d)
    for guid in d.dnb.region_guids:
        assert guid in url


def test_extract_urls_from_real_search_page():
    html = Path("data/dnbeiendom/html_crawled/page1.html").read_text(errors="replace")
    urls = crawl.extract_listing_urls(html)
    assert len(urls) >= 5
    assert all(u.startswith("https://") for u in urls)


def test_parse_listing_jsonld():
    html = (FIXTURES / "listing1.html").read_text(errors="replace")
    row = parse.parse_listing(html, "https://dnbeiendom.no/x")
    assert row is not None
    assert row.get("lat") and row.get("lng")
    assert row.get("address")
```

(Adapt the asserted keys to legacy `extract_fields_from_entry`'s actual keys — read it first; key names must match legacy exactly.)

- [ ] **Step 3: Run failing → port → run passing.** Also compare `extract_listing_urls` output against the legacy function on the same fixture page (add a pin-test importing `main.extractors.extract_dnbeiendom._extract_listing_urls_from_html` and asserting equal lists). Full suite green.

- [ ] **Step 4: Commit**

```bash
git add skannonser/ingest/dnb/ config/domain.toml skannonser/config/domain.py tests/rebuild/test_dnb.py tests/rebuild/fixtures/dnb/
git commit -m "rebuild(phase2): dnb source port - JSON-LD crawl and parse, legacy-pinned

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 12: DNB load — polygon filter, Finn matching, repository

**Files:**
- Create: `skannonser/store/repositories/dnb.py`
- Create: `skannonser/ingest/dnb/load.py`
- Test: `tests/rebuild/test_dnb_load.py`

**Interfaces:**
- Consumes: `geo.is_point_in_polygon`, `textnorm.normalize_addr/normalize_pc`, `ListingsRepo.conn` pattern.
- Produces: `DnbRepo(conn)` with `upsert(rows: list[dict]) -> dict`, `deactivate_missing(active_urls: list[str]) -> int` (ports the dnbeiendom-table semantics from legacy `filter_and_load_dnbeiendom_no_buffer.py:57-144`, incl. `duplicate_of_finnkode` matching against eiendom rows by normalized address+postcode); `load.filter_and_match(rows, domain, conn) -> list[dict]` (polygon filter + finn-match annotation).

- [ ] **Step 1: Write the failing tests** — migrated tmp DB, insert one eiendom row with a known address via `ListingsRepo`, then:

```python
def test_polygon_filter_drops_outside_rows(...):
    # row with lat/lng in Oslo passes; row in the North Sea is dropped


def test_finn_match_sets_duplicate_of_finnkode(...):
    # dnb row with same normalized address+postcode as the eiendom row
    # gets duplicate_of_finnkode == that finnkode


def test_deactivate_missing_never_deletes(...):
    # mirror of the eiendom lifecycle test, keyed on url
```

Write these three for real against the interfaces above (they are the requirements; shapes follow Task 6's test patterns — migrated tmp DB via `connection.connect` + `migrations.migrate`, no mocks).

- [ ] **Step 2: Run failing → implement → run passing.** Full suite green.

- [ ] **Step 3: Commit**

```bash
git add skannonser/store/repositories/dnb.py skannonser/ingest/dnb/load.py tests/rebuild/test_dnb_load.py
git commit -m "rebuild(phase2): dnb load - polygon filter, finn matching, dnb repository

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 13: Pipeline + `skannonser run ingest` + parallel-run checkpoint

**Files:**
- Create: `skannonser/pipeline.py`
- Create: `skannonser/commands/run_cmd.py`
- Modify: `skannonser/cli.py` (register `run` sub-app)
- Test: `tests/rebuild/test_pipeline.py`

**Interfaces:**
- Consumes: everything from Tasks 4-12.
- Produces: `run_finn_ingest(domain, conn, project_dir: Path, fetch=requests.get, skip_crawl_urls: list[tuple[str, str]] | None = None) -> dict` (counts: crawled, parsed, failed, upserted, deactivated) and `run_dnb_ingest(...)` equivalent; CLI `skannonser run ingest --source finn|dnb|all [--db PATH]` (db override for supervised runs), non-interactive, exit non-zero if parse-failure rate exceeds 20% (protects against Finn layout changes silently deactivating everything — mirrors the intent of legacy's failure CSV).

- [ ] **Step 1: Write the failing test** — full offline pipeline against fixtures:

```python
def test_finn_pipeline_offline_end_to_end(tmp_path):
    conn = connection.connect(tmp_path / "p.db")
    migrations.migrate(conn)
    proj = tmp_path / "proj"
    # Seed the cache with two fixture ads so fetch is never called:
    fixture_dir = Path("tests/rebuild/fixtures/finn")
    cases = sorted(fixture_dir.glob("*.html"))[:2]
    (proj / "html_extracted").mkdir(parents=True)
    for c in cases:
        shutil.copy(c, proj / "html_extracted" / c.name)
    urls = [(c.stem, f"https://www.finn.no/realestate/homes/ad.html?finnkode={c.stem}")
            for c in cases]

    stats = run_finn_ingest(load_domain(), conn, proj,
                            fetch=_fail_if_called, skip_crawl_urls=urls)

    assert stats["parsed"] == 2 and stats["failed"] == 0
    assert conn.execute("SELECT COUNT(*) FROM eiendom WHERE active=1").fetchone()[0] == 2
```

- [ ] **Step 2: Run failing → implement pipeline + CLI → run passing.** Full suite green.

- [ ] **Step 3: SUPERVISED PARALLEL-RUN CHECKPOINT (the phase gate).** On the SERVER (fresh data, real crawl — one supervised run, no Google API involvement since ingest never calls Google):

```bash
ssh mbp2016@100.77.139.22
cd ~/kode/skannonser && git pull --ff-only   # after the stash-dance if DB dirty
cp main/database/properties.db /tmp/parallel-new.db
.venv/bin/skannonser run ingest --source finn --db /tmp/parallel-new.db
# Then let the nightly legacy run happen (or trigger make full's crawl steps 1-2 manually),
# and diff /tmp/parallel-new.db's eiendom table against the live DB's:
# same active finnkode set, same field values for rows touched today
# (ignore updated_at; travel/coords columns are Phase 3's domain — expect them
# only in the live DB where legacy post-process wrote them).
```

Bar: identical active sets and identical parsed fields. Write the actual diff query, run it, put the result in your report. Any discrepancy is a port bug until proven otherwise.

- [ ] **Step 4: Commit**

```bash
git add skannonser/pipeline.py skannonser/commands/run_cmd.py skannonser/cli.py tests/rebuild/test_pipeline.py
git commit -m "rebuild(phase2): ingest pipeline and run command, offline-tested end to end

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 14: Refresh modes port

**Files:**
- Create: `skannonser/ingest/finn/refresh.py`
- Modify: `skannonser/commands/run_cmd.py` (add `refresh` command)
- Modify: `skannonser/store/repositories/listings.py` (add status methods)
- Test: `tests/rebuild/test_refresh.py`

**Interfaces:**
- Consumes: `html_cache.load_or_fetch` (with `force_save` semantics for refresh — re-download), `ListingsRepo`.
- Produces: `ListingsRepo.update_status(finnkode: str, new_status: str) -> None` and `ListingsRepo.record_status_change_if_changed(finnkode, old, new) -> bool` (port from `db.py:616-660` — append to `eiendom_status_history` only on actual change); `refresh_listings(conn, domain, project_dir, mode: str, fetch=...) -> dict` porting `main/sync/refresh_listings.py:24-213` including the three row-selection modes (`all`, `inactive`, `stale-open` — selection SQL ported from `db.py:926-1006`); CLI `skannonser run refresh [--mode all|inactive|stale-open]`.

- [ ] **Step 1: Write the failing tests** (offline, fixture HTML with a known status badge; migrated tmp DB seeded via `ListingsRepo`):

```python
def test_status_history_appends_only_on_change(...):
    # update_status twice with same value -> one history row


def test_refresh_updates_status_from_html(...):
    # seed listing; fake fetch returns fixture ad HTML whose status differs;
    # refresh_listings(...) updates eiendom.tilgjengelighet and appends history


def test_stale_open_mode_selects_correct_rows(...):
    # port the selection semantics: active=0, excluding Tilgjengelighet in
    # ('Solgt','Inaktiv'), within SHEETS_MAX_PRICE/MIN_BRA_I (read db.py:958-1006
    # and assert the same WHERE behavior on seeded rows)
```

Write all three for real against seeded data — no mocks beyond the injected `fetch`.

- [ ] **Step 2: Run failing → port → run passing.** Full suite green. Existing legacy tests `tests/test_status_history.py` / `test_refresh_records_history.py` describe the exact history semantics — read them and mirror their cases.

- [ ] **Step 3: Commit**

```bash
git add skannonser/ingest/finn/refresh.py skannonser/commands/run_cmd.py skannonser/store/repositories/listings.py tests/rebuild/test_refresh.py
git commit -m "rebuild(phase2): refresh modes port with append-only status history

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Phase 2 acceptance gate

1. Full suite green: `.venv/bin/python -m pytest tests/rebuild -v`.
2. `skannonser verify parse` over the full 7 731-ad cache: **0 unexplained diffs** (allowlist contains only the two sanctioned fixes, each demonstrably firing or deleted).
3. Supervised parallel run on the server (Task 13 Step 3): identical active sets and parsed fields vs the legacy nightly run.
4. Migrations 001+002 applied on laptop AND server; backup retention active in the crontab.
5. Legacy pipeline still runs untouched (it remains the production path until Phase 4's cron cutover).
