# Rebuild Phase 1 — Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the new `skannonser/` package — typed config, versioned migrations adopting the live DB, a non-interactive CLI, and Docker Compose on the server — without changing any legacy behavior.

**Architecture:** New package `skannonser/` lives beside the frozen legacy code (`main/`, `scripts/`). Migration 001 adopts the live SQLite schema verbatim (dumped from the real DB, `IF NOT EXISTS`-guarded), so the existing DB file becomes schema version 1 untouched. Secrets move to `.env`; domain values move to `config/domain.toml`. Docker Compose runs a supercronic scheduler whose only Phase-1 job is a nightly DB backup.

**Tech Stack:** Python ≥3.11 (repo venv is 3.12.12), Typer, pydantic v2 + pydantic-settings, stdlib `sqlite3` + `tomllib`, pytest, Docker Compose + supercronic.

**Spec:** `docs/superpowers/specs/2026-07-20-skannonser-rebuild-design.md` (sections 5.1, 5.2, 5.8, phase 1). Later phases get their own plans at each phase boundary.

## Global Constraints

- Python ≥ 3.11. Use `.venv/bin/python` (3.12.12) — the system python is 3.9 and must not be used.
- Legacy code (`main/`, `scripts/`, `apps_script/`, Makefile) is frozen: no behavior changes, with ONE approved exception — `main/config/config.py` switches to env-only API-key reading (Task 7).
- The live DB is `main/database/properties.db` and is shared with the still-running legacy pipeline. Run `skannonser db backup` before any schema operation against it. `data/eiendom.db` is a 0-byte decoy — never touch it.
- No `input()` or interactive prompts anywhere in new code.
- Secrets only via `.env`/env vars — never committed, never hardcoded. The plaintext key in `main/config/config.py` gets revoked in Task 7.
- New code lives only under `skannonser/`; new tests under `tests/rebuild/`.
- Domain values must exactly equal today's legacy values (they are restated verbatim inside Task 3).
- Run tests as: `.venv/bin/python -m pytest tests/rebuild -v`.
- Commit after every green test cycle; commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Package scaffold + pyproject + CLI entry point

**Files:**
- Create: `pyproject.toml`
- Create: `skannonser/__init__.py`
- Create: `skannonser/cli.py`
- Create: `tests/rebuild/__init__.py`
- Test: `tests/rebuild/test_cli.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `skannonser.cli.app` (a `typer.Typer` instance later tasks attach sub-apps to) and `skannonser.cli.main() -> None` (console-script entry). Installed command name: `skannonser`.

- [ ] **Step 1: Write the failing test**

`tests/rebuild/__init__.py` — empty file. `tests/rebuild/test_cli.py`:

```python
from typer.testing import CliRunner

from skannonser.cli import app


def test_cli_help_exits_zero():
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "skannonser" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_cli.py -v`
Expected: FAIL/ERROR with `ModuleNotFoundError: No module named 'skannonser'` (or missing typer).

- [ ] **Step 3: Write the scaffold**

`pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "skannonser"
version = "0.1.0"
description = "skannonser rebuilt: Finn/DNB listing scanner"
requires-python = ">=3.11"
dependencies = [
    "typer>=0.12",
    "pydantic>=2.7",
    "pydantic-settings>=2.2",
]

[project.optional-dependencies]
dev = ["pytest>=8"]

[project.scripts]
skannonser = "skannonser.cli:main"

[tool.setuptools.packages.find]
include = ["skannonser*"]

[tool.pytest.ini_options]
testpaths = ["tests/rebuild"]
```

(`testpaths` points at the new suite only; legacy unittest files in `tests/` keep running via their own `python -m unittest` habits and get consolidated in a later phase.)

`skannonser/__init__.py`:

```python
__version__ = "0.1.0"
```

`skannonser/cli.py`:

```python
import typer

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")


def main() -> None:
    app()
```

Install editable: `.venv/bin/python -m pip install -e '.[dev]'`

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/rebuild/test_cli.py -v`
Expected: PASS (1 passed). Also verify the console script: `.venv/bin/skannonser --help` prints the help text.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml skannonser/__init__.py skannonser/cli.py tests/rebuild/
git commit -m "rebuild(phase1): package scaffold with typer CLI entry point

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Secrets settings from environment / .env

**Files:**
- Create: `skannonser/config/__init__.py` (empty)
- Create: `skannonser/config/settings.py`
- Create: `.env.example`
- Modify: `.gitignore` (ensure `.env` and `backups/` are ignored)
- Test: `tests/rebuild/test_settings.py`, `tests/rebuild/conftest.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `skannonser.config.settings.Secrets` (pydantic-settings model) and `get_secrets() -> Secrets` (lru_cached). Fields: `google_maps_api_key: str` (default `""`), `spreadsheet_id: str` (default `""`), `google_service_account_file: Path | None`, `notify_bin: str` (default `"notify"`), `db_path: Path` (default `main/database/properties.db`, env override `SKANNONSER_DB_PATH`).

- [ ] **Step 1: Write the failing test**

`tests/rebuild/conftest.py`:

```python
import pytest

from skannonser.config.settings import get_secrets


@pytest.fixture(autouse=True)
def clear_secrets_cache():
    get_secrets.cache_clear()
    yield
    get_secrets.cache_clear()
```

`tests/rebuild/test_settings.py`:

```python
from pathlib import Path

from skannonser.config.settings import Secrets, get_secrets


def test_defaults(monkeypatch):
    monkeypatch.delenv("SKANNONSER_DB_PATH", raising=False)
    s = Secrets(_env_file=None)
    assert s.db_path == Path("main/database/properties.db")
    assert s.notify_bin == "notify"
    assert s.google_maps_api_key == ""


def test_env_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "x.db"))
    s = get_secrets()
    assert s.google_maps_api_key == "test-key"
    assert s.db_path == tmp_path / "x.db"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_settings.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.config'`.

- [ ] **Step 3: Write the implementation**

`skannonser/config/settings.py`:

```python
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Secrets(BaseSettings):
    """Secrets and machine-specific paths. Values come from env vars / .env only."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    google_maps_api_key: str = ""
    spreadsheet_id: str = ""
    google_service_account_file: Path | None = None
    notify_bin: str = "notify"
    db_path: Path = Field(
        default=Path("main/database/properties.db"),
        validation_alias="SKANNONSER_DB_PATH",
    )


@lru_cache
def get_secrets() -> Secrets:
    return Secrets()
```

`.env.example`:

```
# Copy to .env and fill in. Never commit .env.
GOOGLE_MAPS_API_KEY=
SPREADSHEET_ID=
GOOGLE_SERVICE_ACCOUNT_FILE=main/config/service_account.json
NOTIFY_BIN=notify
# SKANNONSER_DB_PATH=main/database/properties.db
```

`.gitignore` — append (only the lines not already present):

```
.env
backups/
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/rebuild/test_settings.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add skannonser/config/ .env.example .gitignore tests/rebuild/conftest.py tests/rebuild/test_settings.py
git commit -m "rebuild(phase1): env-based secrets settings with .env support

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Domain config TOML + loader + `skannonser config show`

**Files:**
- Create: `config/domain.toml`
- Create: `skannonser/config/domain.py`
- Create: `skannonser/commands/__init__.py` (empty)
- Create: `skannonser/commands/config_cmd.py`
- Modify: `skannonser/cli.py`
- Test: `tests/rebuild/test_domain.py`

**Interfaces:**
- Consumes: `get_secrets()` from Task 2.
- Produces: `skannonser.config.domain.load_domain(path: Path | None = None) -> DomainConfig`, with `DomainConfig` exposing `.filters` (`sheets_max_price:int, url_max_price:int, min_bra_i:int, include_unlisted:bool`), `.coords` (`lat_min/lat_max/lng_min/lng_max: float`), `.travel` (`reuse_within_meters:int, max_travel_minutes:int`), `.destinations: list[Destination]` (`key,label,address: str`), `.polygon_points: list[tuple[float, float]]` in **(lng, lat)** order (matches legacy). CLI gains the `config` sub-app.

- [ ] **Step 1: Write the domain TOML (values verbatim from legacy)**

`config/domain.toml`:

```toml
# Domain configuration — the tuning surface of the system.
# Values ported 1:1 from main/config/filters.py and main/post_process.py on 2026-07-20.

[filters]
sheets_max_price = 7500000
url_max_price = 7500000
min_bra_i = 70
include_unlisted = true

[coords]
lat_min = 57.0
lat_max = 72.0
lng_min = 4.0
lng_max = 32.0

[travel]
reuse_within_meters = 300
max_travel_minutes = 360

[[destinations]]
key = "brj"
label = "BRJ (work, Sandvika)"
address = "Rådmann Halmrasts Vei 5"

[[destinations]]
key = "mvv"
label = "MVV (Lambertseter)"
address = "Langbølgen 24, 1155 Oslo"

[[destinations]]
key = "mvv_uni"
label = "MVV UNI (Gaustadalléen)"
address = "Gaustadalléen 30, 0373 Oslo"

[polygon]
# Finn search polygon, (lng, lat) pairs — ported from main/runners/run_eiendom_db.py:140
points = [
    [10.656738281250, 59.884802942124],
    [10.536789920973, 59.797487966246],
    [10.545723856072, 59.709734171804],
    [10.332641601563, 59.700380312509],
    [9.971542814941, 59.874465805403],
    [11.260986328125, 60.440962535310],
    [11.585234663086, 60.136034630691],
    [10.947750935529, 59.714239974969],
    [10.721282958984, 59.712097173323],
    [10.715468622953, 59.849132221282],
]
```

- [ ] **Step 2: Write the failing test**

`tests/rebuild/test_domain.py`:

```python
import pytest
from pydantic import ValidationError

from skannonser.config.domain import DomainConfig, load_domain


def test_load_domain_matches_legacy_values():
    d = load_domain()
    assert d.filters.sheets_max_price == 7_500_000
    assert d.filters.min_bra_i == 70
    assert d.travel.reuse_within_meters == 300
    assert [dest.key for dest in d.destinations] == ["brj", "mvv", "mvv_uni"]
    assert len(d.polygon_points) == 10
    lng, lat = d.polygon_points[0]
    assert d.coords.lng_min <= lng <= d.coords.lng_max
    assert d.coords.lat_min <= lat <= d.coords.lat_max


def test_polygon_must_have_three_points():
    with pytest.raises(ValidationError):
        DomainConfig(
            filters=dict(sheets_max_price=1, url_max_price=1, min_bra_i=1, include_unlisted=True),
            coords=dict(lat_min=57.0, lat_max=72.0, lng_min=4.0, lng_max=32.0),
            travel=dict(reuse_within_meters=300, max_travel_minutes=360),
            destinations=[dict(key="a", label="A", address="x")],
            polygon_points=[(10.0, 59.0), (10.1, 59.1)],
        )


def test_polygon_points_outside_coord_bounds_rejected():
    with pytest.raises(ValidationError):
        DomainConfig(
            filters=dict(sheets_max_price=1, url_max_price=1, min_bra_i=1, include_unlisted=True),
            coords=dict(lat_min=57.0, lat_max=72.0, lng_min=4.0, lng_max=32.0),
            travel=dict(reuse_within_meters=300, max_travel_minutes=360),
            destinations=[dict(key="a", label="A", address="x")],
            polygon_points=[(100.0, 59.0), (10.1, 59.1), (10.2, 59.2)],
        )
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_domain.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.config.domain'`.

- [ ] **Step 4: Write the implementation**

`skannonser/config/domain.py`:

```python
import tomllib
from pathlib import Path

from pydantic import BaseModel, field_validator, model_validator

DEFAULT_DOMAIN_PATH = Path("config/domain.toml")


class Filters(BaseModel):
    sheets_max_price: int
    url_max_price: int
    min_bra_i: int
    include_unlisted: bool


class CoordBounds(BaseModel):
    lat_min: float
    lat_max: float
    lng_min: float
    lng_max: float


class Travel(BaseModel):
    reuse_within_meters: int
    max_travel_minutes: int


class Destination(BaseModel):
    key: str
    label: str
    address: str


class DomainConfig(BaseModel):
    filters: Filters
    coords: CoordBounds
    travel: Travel
    destinations: list[Destination]
    polygon_points: list[tuple[float, float]]  # (lng, lat), legacy order

    @field_validator("polygon_points")
    @classmethod
    def _polygon_min_size(cls, v: list[tuple[float, float]]) -> list[tuple[float, float]]:
        if len(v) < 3:
            raise ValueError("polygon needs at least 3 points")
        return v

    @model_validator(mode="after")
    def _polygon_within_bounds(self) -> "DomainConfig":
        for lng, lat in self.polygon_points:
            if not (self.coords.lng_min <= lng <= self.coords.lng_max):
                raise ValueError(f"polygon lng {lng} outside coord bounds")
            if not (self.coords.lat_min <= lat <= self.coords.lat_max):
                raise ValueError(f"polygon lat {lat} outside coord bounds")
        return self


def load_domain(path: Path | None = None) -> DomainConfig:
    with open(path or DEFAULT_DOMAIN_PATH, "rb") as f:
        raw = tomllib.load(f)
    raw["polygon_points"] = raw.pop("polygon", {}).get("points", [])
    return DomainConfig(**raw)
```

`skannonser/commands/config_cmd.py`:

```python
import typer

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets

app = typer.Typer(no_args_is_help=True, help="Configuration inspection")


@app.command()
def show() -> None:
    """Print effective configuration (secrets masked)."""
    secrets = get_secrets()
    typer.echo(f"db_path: {secrets.db_path}")
    typer.echo(f"google_maps_api_key: {'set' if secrets.google_maps_api_key else 'MISSING'}")
    typer.echo(f"spreadsheet_id: {'set' if secrets.spreadsheet_id else 'MISSING'}")
    typer.echo(f"google_service_account_file: {secrets.google_service_account_file}")
    typer.echo(f"notify_bin: {secrets.notify_bin}")
    typer.echo(load_domain().model_dump_json(indent=2))
```

`skannonser/cli.py` — replace contents with:

```python
import typer

from skannonser.commands import config_cmd

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")
app.add_typer(config_cmd.app, name="config")


def main() -> None:
    app()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/rebuild -v`
Expected: all PASS. Also run `.venv/bin/skannonser config show` from the repo root — prints masked secrets and the full domain JSON.

- [ ] **Step 6: Commit**

```bash
git add config/domain.toml skannonser/config/domain.py skannonser/commands/ skannonser/cli.py tests/rebuild/test_domain.py
git commit -m "rebuild(phase1): domain config TOML with validated loader and config show

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `skannonser db backup` + take the first backup

**Files:**
- Create: `skannonser/commands/db_cmd.py`
- Modify: `skannonser/cli.py`
- Test: `tests/rebuild/test_db_backup.py`

**Interfaces:**
- Consumes: `get_secrets().db_path` from Task 2.
- Produces: CLI `skannonser db backup [--dest-dir backups]` → writes `backups/properties-YYYYMMDD-HHMMSS.db` via the SQLite online-backup API (WAL-safe). The `db` sub-app object `skannonser.commands.db_cmd.app`, which Tasks 5–6 extend.

- [ ] **Step 1: Write the failing test**

`tests/rebuild/test_db_backup.py`:

```python
import sqlite3

from typer.testing import CliRunner

from skannonser.cli import app


def test_backup_copies_database(tmp_path, monkeypatch):
    src = tmp_path / "live.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.execute("INSERT INTO t VALUES (42)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(src))

    dest_dir = tmp_path / "backups"
    result = CliRunner().invoke(app, ["db", "backup", "--dest-dir", str(dest_dir)])

    assert result.exit_code == 0, result.output
    copies = list(dest_dir.glob("properties-*.db"))
    assert len(copies) == 1
    check = sqlite3.connect(copies[0])
    assert check.execute("SELECT x FROM t").fetchone()[0] == 42
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_db_backup.py -v`
Expected: FAIL — exit code 2 / "No such command 'db'".

- [ ] **Step 3: Write the implementation**

`skannonser/commands/db_cmd.py`:

```python
import sqlite3
from datetime import datetime
from pathlib import Path

import typer

from skannonser.config.settings import get_secrets

app = typer.Typer(no_args_is_help=True, help="Database maintenance")


@app.command()
def backup(dest_dir: Path = typer.Option(Path("backups"), help="Backup directory")) -> None:
    """Copy the live DB via SQLite's online backup API (safe under WAL)."""
    src = get_secrets().db_path
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"properties-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    src_conn = sqlite3.connect(src)
    dest_conn = sqlite3.connect(dest)
    with dest_conn:
        src_conn.backup(dest_conn)
    src_conn.close()
    dest_conn.close()
    typer.echo(f"Backed up {src} -> {dest}")
```

`skannonser/cli.py` — add the db sub-app:

```python
import typer

from skannonser.commands import config_cmd, db_cmd

app = typer.Typer(no_args_is_help=True, help="skannonser — rebuilt pipeline CLI")
app.add_typer(config_cmd.app, name="config")
app.add_typer(db_cmd.app, name="db")


def main() -> None:
    app()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/rebuild -v`
Expected: all PASS.

- [ ] **Step 5: Take the real backup (phase-1 safety requirement)**

Run from repo root: `.venv/bin/skannonser db backup`
Expected output: `Backed up main/database/properties.db -> backups/properties-<stamp>.db`.
Verify: `sqlite3 backups/properties-*.db "SELECT COUNT(*) FROM eiendom;"` → `5863` (or current count if the legacy pipeline has run since 2026-07-20).

- [ ] **Step 6: Commit**

```bash
git add skannonser/commands/db_cmd.py skannonser/cli.py tests/rebuild/test_db_backup.py
git commit -m "rebuild(phase1): db backup command via SQLite online backup API

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Migration runner + migration 001 adopting the live schema

**Files:**
- Create: `skannonser/store/__init__.py` (empty)
- Create: `skannonser/store/connection.py`
- Create: `skannonser/store/migrations.py`
- Create: `skannonser/store/migrations/001_adopt_live_schema.sql` (generated — Step 1)
- Modify: `skannonser/commands/db_cmd.py`
- Test: `tests/rebuild/test_migrations.py`

**Interfaces:**
- Consumes: `get_secrets().db_path`; the `db` sub-app from Task 4.
- Produces:
  - `skannonser.store.connection.connect(db_path: Path) -> sqlite3.Connection` — WAL on, foreign_keys on, `sqlite3.Row` row factory.
  - `skannonser.store.migrations.migrate(conn) -> list[str]` (applied migration ids, in order) and `pending(conn) -> list[Path]`; state tracked in table `schema_migrations(id TEXT PRIMARY KEY, applied_at TEXT)`.
  - CLI `skannonser db migrate`.

- [ ] **Step 1: Generate migration 001 from the live DB**

The migration must be the live schema verbatim, guarded so it is a no-op on the live DB and a full create on a fresh DB:

```bash
mkdir -p skannonser/store/migrations
sqlite3 main/database/properties.db .schema \
  | grep -v 'sqlite_sequence' \
  | sed -E 's/^CREATE TABLE /CREATE TABLE IF NOT EXISTS /; s/^CREATE INDEX /CREATE INDEX IF NOT EXISTS /; s/^CREATE UNIQUE INDEX /CREATE UNIQUE INDEX IF NOT EXISTS /' \
  > skannonser/store/migrations/001_adopt_live_schema.sql
```

Then open the generated file and sanity-check: it must contain `CREATE TABLE IF NOT EXISTS` statements for exactly these 8 tables — `eiendom`, `eiendom_processed`, `dnbeiendom`, `manual_overrides`, `listing_comments`, `stations`, `station_lines`, `station_travel` — plus their indexes, and no `sqlite_sequence` line. Do not edit the DDL itself.

- [ ] **Step 2: Write the failing test**

`tests/rebuild/test_migrations.py`:

```python
from skannonser.store import connection, migrations

EXPECTED_TABLES = {
    "eiendom", "eiendom_processed", "dnbeiendom", "manual_overrides",
    "listing_comments", "stations", "station_lines", "station_travel",
}


def _tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {r["name"] for r in rows}


def test_migrate_fresh_db_creates_full_schema(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert ran == ["001_adopt_live_schema"]
    assert EXPECTED_TABLES <= _tables(conn)
    assert "schema_migrations" in _tables(conn)


def test_migrate_is_idempotent(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    assert migrations.migrate(conn) == []
    assert migrations.pending(conn) == []


def test_migrate_adopts_preexisting_schema(tmp_path):
    """Simulates the live DB: schema already exists, migration must no-op cleanly."""
    conn = connection.connect(tmp_path / "live.db")
    sql = (migrations.MIGRATIONS_DIR / "001_adopt_live_schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)  # pre-existing schema, no migration bookkeeping
    ran = migrations.migrate(conn)
    assert ran == ["001_adopt_live_schema"]
    assert EXPECTED_TABLES <= _tables(conn)


def test_connection_settings(tmp_path):
    conn = connection.connect(tmp_path / "x.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_migrations.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.store'`.

- [ ] **Step 4: Write the implementation**

`skannonser/store/connection.py`:

```python
import sqlite3
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
```

`skannonser/store/migrations.py`:

```python
import sqlite3
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _applied(conn: sqlite3.Connection) -> set[str]:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " id TEXT PRIMARY KEY,"
        " applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
    )
    return {row["id"] for row in conn.execute("SELECT id FROM schema_migrations")}


def pending(conn: sqlite3.Connection) -> list[Path]:
    applied = _applied(conn)
    return [p for p in sorted(MIGRATIONS_DIR.glob("*.sql")) if p.stem not in applied]


def migrate(conn: sqlite3.Connection) -> list[str]:
    ran: list[str] = []
    for path in pending(conn):
        conn.executescript(path.read_text(encoding="utf-8"))
        conn.execute("INSERT INTO schema_migrations (id) VALUES (?)", (path.stem,))
        conn.commit()
        ran.append(path.stem)
    return ran
```

`skannonser/commands/db_cmd.py` — add below `backup`:

```python
from skannonser.store import connection, migrations


@app.command()
def migrate() -> None:
    """Apply pending schema migrations (versioned, explicit — never on connect)."""
    conn = connection.connect(get_secrets().db_path)
    ran = migrations.migrate(conn)
    typer.echo(f"Applied: {', '.join(ran) if ran else 'nothing (up to date)'}")
```

(Imports go at the top of the file with the existing ones.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/rebuild -v`
Expected: all PASS.

- [ ] **Step 6: Adopt the live DB (checkpoint — backup from Task 4 must exist)**

```bash
.venv/bin/skannonser db migrate
```

Expected: `Applied: 001_adopt_live_schema` (pure no-op on data — every statement is IF NOT EXISTS).
Then verify nothing changed:

```bash
sqlite3 main/database/properties.db "PRAGMA integrity_check; SELECT COUNT(*) FROM eiendom; SELECT id FROM schema_migrations;"
```

Expected: `ok`, the same eiendom count as the Task 4 backup, and `001_adopt_live_schema`.
Running `.venv/bin/skannonser db migrate` again prints `Applied: nothing (up to date)`.

- [ ] **Step 7: Commit**

```bash
git add skannonser/store/ skannonser/commands/db_cmd.py tests/rebuild/test_migrations.py
git commit -m "rebuild(phase1): versioned migration runner; 001 adopts live schema

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: `skannonser db stats`

**Files:**
- Modify: `skannonser/commands/db_cmd.py`
- Test: `tests/rebuild/test_db_stats.py`

**Interfaces:**
- Consumes: `connection.connect`, `get_secrets().db_path`.
- Produces: CLI `skannonser db stats` printing `<table>: <rowcount>` for every non-sqlite table, sorted by name. This is the Phase-1 acceptance command.

- [ ] **Step 1: Write the failing test**

`tests/rebuild/test_db_stats.py`:

```python
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.store import connection, migrations


def test_stats_lists_tables_with_counts(tmp_path, monkeypatch):
    db = tmp_path / "s.db"
    conn = connection.connect(db)
    migrations.migrate(conn)
    conn.execute("INSERT INTO stations (name, lat, lng) VALUES ('Test st', 59.9, 10.7)")
    conn.commit()
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(db))

    result = CliRunner().invoke(app, ["db", "stats"])

    assert result.exit_code == 0, result.output
    assert "stations: 1" in result.output
    assert "eiendom: 0" in result.output
```

Note: if the INSERT fails because the live `stations` schema has other NOT NULL columns, adapt the INSERT to the generated 001 DDL (read the .sql file) — the test's point is a nonzero count in one table, zero in another.

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/rebuild/test_db_stats.py -v`
Expected: FAIL — "No such command 'stats'".

- [ ] **Step 3: Write the implementation**

Add to `skannonser/commands/db_cmd.py`:

```python
@app.command()
def stats() -> None:
    """Row counts per table — the quick health/acceptance check."""
    conn = connection.connect(get_secrets().db_path)
    tables = [
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for t in tables:
        n = conn.execute(f'SELECT COUNT(*) AS n FROM "{t}"').fetchone()["n"]
        typer.echo(f"{t}: {n}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/rebuild -v`
Expected: all PASS.

- [ ] **Step 5: Live checkpoint**

Run: `.venv/bin/skannonser db stats`
Expected (counts as of 2026-07-20; legacy runs may have shifted them slightly):

```
dnbeiendom: 1173
eiendom: 5863
eiendom_processed: 6141
listing_comments: 0
manual_overrides: 0
schema_migrations: 1
station_lines: 213
station_travel: 387
stations: 136
```

- [ ] **Step 6: Commit**

```bash
git add skannonser/commands/db_cmd.py tests/rebuild/test_db_stats.py
git commit -m "rebuild(phase1): db stats command

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Secrets cutover — rotate the Google Maps API key

The live key currently sits in plaintext in `main/config/config.py` (gitignored but on disk, and previously the fallback when the env var is unset).

**Files:**
- Modify: `main/config/config.py` (local, untracked — the ONE approved legacy change)
- Create: `.env` (local, untracked)

**Interfaces:**
- Consumes: `.env.example` from Task 2.
- Produces: both legacy and new code read `GOOGLE_MAPS_API_KEY` from the environment only; no plaintext key on disk outside `.env`.

- [ ] **Step 1: MANUAL (owner) — create a replacement key**

In Google Cloud Console → APIs & Credentials: create a new API key, restrict it to the Geocoding API and Routes API. Do **not** delete the old key yet.

- [ ] **Step 2: Create `.env` from the example**

```bash
cp .env.example .env
# then edit .env: paste the NEW key into GOOGLE_MAPS_API_KEY, and set
# SPREADSHEET_ID to the value currently hardcoded in main/googleUtils.py:13
```

- [ ] **Step 3: Make legacy config env-only**

In `main/config/config.py`, replace the line `GOOGLE_MAPS_API_KEY = "AIza…"` with:

```python
import os

GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
```

(Keep everything else in the file untouched.)

- [ ] **Step 4: MANUAL (owner) — feed the env to the legacy cron wrapper**

The daily wrapper (`~/run_skannonser_daily.sh`, outside this repo) must now export the env before invoking make/python. Add near the top:

```bash
set -a
source /path/to/skannonser/.env
set +a
```

- [ ] **Step 5: Verify legacy still works with the new key, then revoke the old**

Run a cheap legacy call that uses the key, e.g. `make coords-count` (no API calls) followed by one geocode: `make coords-fill COORDS_LIMIT=1` — confirm it succeeds with the new key.
**MANUAL (owner):** delete the old key in Google Cloud Console.

- [ ] **Step 6: Commit**

Nothing to commit for untracked files; commit only if `.gitignore` needed additions:

```bash
git status --short   # confirm .env and main/config/config.py are NOT staged/tracked
```

---

### Task 8: Docker Compose + scheduler on the server

**Files:**
- Create: `docker/Dockerfile`
- Create: `docker/crontab`
- Create: `docker-compose.yml`
- Modify: `.gitignore` (ensure nothing new leaks; no change expected)

**Interfaces:**
- Consumes: the installed `skannonser` CLI; `.env`; volumes for DB/config.
- Produces: `docker compose up -d` runs a `scheduler` service (supercronic) whose Phase-1 job is a nightly `skannonser db backup`; `docker compose run --rm scheduler skannonser <cmd>` is the ad-hoc execution path. Later phases add services (web) and crontab lines.

- [ ] **Step 1: Write the Dockerfile**

`docker/Dockerfile`:

```dockerfile
FROM python:3.12-slim

# supercronic: container-friendly cron runner.
# On an ARM server use build-arg SUPERCRONIC_URL=...supercronic-linux-arm64
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl sqlite3 \
    && curl -fsSL -o /usr/local/bin/supercronic "$SUPERCRONIC_URL" \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY skannonser ./skannonser
RUN pip install --no-cache-dir .

COPY docker/crontab /app/docker/crontab
CMD ["supercronic", "/app/docker/crontab"]
```

`docker/crontab`:

```
# Phase 1: nightly DB backup at 03:00. Later phases add pipeline/notify jobs here.
0 3 * * * skannonser db backup
```

- [ ] **Step 2: Write docker-compose.yml**

```yaml
services:
  scheduler:
    build:
      context: .
      dockerfile: docker/Dockerfile
    env_file: .env
    environment:
      SKANNONSER_DB_PATH: /app/main/database/properties.db
    volumes:
      - ./main/database:/app/main/database
      - ./data:/app/data
      - ./config:/app/config
      - ./backups:/app/backups
    restart: unless-stopped
```

- [ ] **Step 3: Build and verify locally**

```bash
docker compose build scheduler
docker compose run --rm scheduler skannonser db stats
```

Expected: the same table counts as Task 6 Step 5 (same DB file via the volume).

- [ ] **Step 4: MANUAL (owner) — deploy on the server**

On the server: `git pull`, copy/create `.env` (never transfer it through chat/mail — scp it or retype), pick the right `SUPERCRONIC_URL` for the server's architecture, then:

```bash
docker compose up -d --build
docker compose logs scheduler   # supercronic starts, job registered
docker compose run --rm scheduler skannonser db stats
```

Expected: stats match the server's DB. The legacy pipeline keeps running exactly as before, outside Docker.

- [ ] **Step 5: Commit**

```bash
git add docker/ docker-compose.yml
git commit -m "rebuild(phase1): docker compose scheduler with nightly db backup

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Phase 1 acceptance gate

All must hold before writing the Phase 2 (ingest port) plan:

1. `.venv/bin/python -m pytest tests/rebuild -v` — all green.
2. `.venv/bin/skannonser db stats` matches known row counts on the live DB; `schema_migrations` contains `001_adopt_live_schema`; `PRAGMA integrity_check` says `ok`.
3. `skannonser config show` prints the domain config with values identical to `main/config/filters.py` and the polygon in `main/runners/run_eiendom_db.py:140`.
4. The old Google Maps API key is revoked; legacy pipeline runs green with the new key from `.env`.
5. `docker compose run --rm scheduler skannonser db stats` works on the server.
6. A dated backup exists in `backups/` and `backups/` is gitignored.
