# Parser Drift Canary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Alert on the same night when a FINN markup change stops a parser field from extracting, by comparing each run's parsed batch against the fill rates already stored in the database.

**Architecture:** A new pure module `skannonser/ingest/drift.py` computes field-presence rates over the parsed batch and over stored rows, and reports fields that collapsed. `run_finn_ingest` calls it immediately **before** `upsert` (so tonight's rows cannot pollute the baseline) and folds the findings into its stats dict. `run_cmd.py` echoes them to stderr, sends a notification, and exits non-zero — exactly how guard 2's operational alert is layered on top of the pipeline today.

**Tech Stack:** Python 3.12, pydantic v2, sqlite3 (stdlib), typer, pytest. No new dependencies.

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-07-27-parser-drift-canary-design.md`. Read it first.
- **Alert only — no behaviour change.** The canary must never skip, abort, or alter ingest. Unlike guard 2 there is nothing to protect by halting.
- **Never break ingest.** Every drift call site is wrapped so an exception in this code cannot fail a run. Diagnostics must not become an outage.
- Constants, fixed by the spec: `MIN_BATCH = 100`, `MIN_BASELINE = 0.05`, `DROP_FACTOR = 0.4`, `BASELINE_ROWS = 200`.
- The watch lists are **derived** from the existing column maps, never hand-copied, so a field added to either parser is watched automatically.
- `Tilgjengelighet` is excluded: its presence tracks listing lifecycle, not markup.
- Tests run with `./.venv/bin/python -m pytest` from the worktree root. Baseline before this work: **665 passed**.
- Commit after every task.

---

### Task 1: Pure comparison core

**Files:**
- Create: `skannonser/ingest/drift.py`
- Test: `tests/rebuild/test_drift.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `DriftFinding` (frozen dataclass: `field: str`, `baseline_rate: float`, `batch_rate: float`, `sample_size: int`); `is_present(value) -> bool`; `batch_rates(rows: list[dict], field_columns: dict[str, str]) -> dict[str, float]`; `compare(batch: dict[str, float], baseline: dict[str, float], sample_size: int, baseline_rows: int) -> list[DriftFinding]`; constants `MIN_BATCH`, `MIN_BASELINE`, `DROP_FACTOR`, `BASELINE_ROWS`.

Both `batch_rates` and `baseline_rates` (Task 2) key their output by **database column name**, not parser field name, so `compare` never has to translate.

- [ ] **Step 1: Write the failing tests**

Create `tests/rebuild/test_drift.py`:

```python
"""Parser-drift canary: field-presence rates for a parsed batch vs stored rows."""
import pytest

from skannonser.ingest.drift import (
    BASELINE_ROWS,
    DriftFinding,
    batch_rates,
    compare,
    is_present,
)


# --- presence ---------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", []])
def test_absent_values(value):
    assert is_present(value) is False


@pytest.mark.parametrize("value", [0, "0", "100", "Selveier", ["Balkong"], 0.0])
def test_present_values(value):
    """0 is a parsed value, not a missing one -- a naive truthiness check
    would drop `bedrooms=0` and understate the rate."""
    assert is_present(value) is True


# --- batch rates ------------------------------------------------------------


def test_batch_rates_keys_by_column_name():
    rows = [{"Primærrom": "100"}, {"Primærrom": ""}, {"Primærrom": "73"}]
    rates = batch_rates(rows, {"Primærrom": "info_primary_area"})
    assert rates == {"info_primary_area": pytest.approx(2 / 3)}


def test_batch_rates_empty_batch_is_zero_not_error():
    assert batch_rates([], {"Primærrom": "info_primary_area"}) == {
        "info_primary_area": 0.0
    }


# --- compare ----------------------------------------------------------------


def _cmp(batch, baseline, n=1000, rows=5000):
    return compare(batch, baseline, n, rows)


def test_collapse_is_reported():
    """The P-ROM shape: a healthy 19% field reading 0% tonight."""
    found = _cmp({"info_primary_area": 0.0}, {"info_primary_area": 0.19})
    assert found == [
        DriftFinding(
            field="info_primary_area",
            baseline_rate=0.19,
            batch_rate=0.0,
            sample_size=1000,
        )
    ]


def test_small_batch_is_skipped():
    """A quiet night must not alert -- 0/40 against a 19% baseline is noise."""
    assert _cmp({"info_primary_area": 0.0}, {"info_primary_area": 0.19}, n=40) == []


def test_small_baseline_table_is_skipped():
    """A freshly wiped listing_details reads every field as 0%."""
    assert (
        _cmp(
            {"energimerke": 0.0},
            {"energimerke": 0.0},
            rows=BASELINE_ROWS - 1,
        )
        == []
    )


def test_rare_field_fluctuating_stays_quiet():
    """BRA-b is legitimately rare (7%); 7% -> 5% is not drift."""
    assert _cmp({"info_usable_b_area": 0.05}, {"info_usable_b_area": 0.07}) == []


def test_field_below_min_baseline_is_not_watched():
    assert _cmp({"info_gross_area": 0.0}, {"info_gross_area": 0.001}) == []


def test_rising_field_never_alerts():
    assert _cmp({"info_primary_area": 0.19}, {"info_primary_area": 0.0}) == []


def test_multiple_collapses_all_reported():
    found = _cmp(
        {"info_primary_area": 0.0, "energimerke": 0.011, "rooms": 0.82},
        {"info_primary_area": 0.19, "energimerke": 0.854, "rooms": 0.81},
    )
    assert sorted(f.field for f in found) == ["energimerke", "info_primary_area"]
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_drift.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'skannonser.ingest.drift'`.

- [ ] **Step 3: Write the module**

Create `skannonser/ingest/drift.py`:

```python
"""Parser-drift canary (2026-07-27 design spec).

`pipeline.py`'s guard 2 catches a FINN layout change that makes parsing
THROW. Nothing catches one that makes parsing SUCCEED, EMPTILY -- which is
how `info_primary_area` sat at 0% for months while every test stayed green.
This module is guard 2's sibling for the quiet failure mode.

Every function here is pure and side-effect free except `baseline_rates`,
which only reads. Nothing in this module may alter ingest behaviour: it
reports, it never protects.

It detects REGRESSIONS FROM A WORKING STATE. A field that has read 0% since
before the baseline existed looks perfectly stable -- catching those needs a
parser-vs-source audit, which is out of scope.
"""
from dataclasses import dataclass
from typing import Any, Sequence

# Below this many parsed ads, the batch cannot distinguish drift from a quiet
# night, so the check stands down rather than guessing. Verified against real
# runs: every nightly since the 2026-07-22 cutover parsed 947-1009 ads.
MIN_BATCH = 100
# Fields rarer than this are too noisy to watch (BRA-b is legitimately ~7%).
MIN_BASELINE = 0.05
# Report when the batch rate falls below baseline * this.
DROP_FACTOR = 0.4
# Below this many stored rows the baseline isn't trusted -- `listing_details`
# is a disposable cache and `backfill-details --wipe` empties it by design.
BASELINE_ROWS = 200


@dataclass(frozen=True)
class DriftFinding:
    field: str
    baseline_rate: float
    batch_rate: float
    sample_size: int


def is_present(value: Any) -> bool:
    """`NormalizedListing` signals absence with "", `ListingDetails` with
    None, and `facilities` with []. One predicate covers all three.

    Explicitly NOT a truthiness check: 0 is a parsed value (`bedrooms=0`),
    not a missing one.
    """
    if value is None:
        return False
    if isinstance(value, (str, list, tuple, dict)):
        return len(value) > 0
    return True


def batch_rates(
    rows: list[dict], field_columns: dict[str, str]
) -> dict[str, float]:
    """Fraction of `rows` carrying each watched field, keyed by DB COLUMN
    name so it lines up with `baseline_rates` without translation."""
    total = len(rows)
    out: dict[str, float] = {}
    for field, column in field_columns.items():
        if total == 0:
            out[column] = 0.0
            continue
        present = sum(1 for row in rows if is_present(row.get(field)))
        out[column] = present / total
    return out


def compare(
    batch: dict[str, float],
    baseline: dict[str, float],
    sample_size: int,
    baseline_rows: int,
) -> list[DriftFinding]:
    """Fields whose batch rate collapsed against the stored baseline.

    Returns [] -- not an error -- when either sample is too small to judge.
    The caller distinguishes "checked, clean" from "skipped" by the guards,
    not by the return value.
    """
    if sample_size < MIN_BATCH or baseline_rows < BASELINE_ROWS:
        return []
    findings = []
    for column, baseline_rate in baseline.items():
        if baseline_rate < MIN_BASELINE:
            continue
        batch_rate = batch.get(column)
        if batch_rate is None:
            continue
        if batch_rate < baseline_rate * DROP_FACTOR:
            findings.append(
                DriftFinding(
                    field=column,
                    baseline_rate=baseline_rate,
                    batch_rate=batch_rate,
                    sample_size=sample_size,
                )
            )
    return findings
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_drift.py -v
```

Expected: 18 passed (`test_absent_values` and `test_present_values` are parametrized: 3 + 6 cases).

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/drift.py tests/rebuild/test_drift.py
git commit -m "feat(drift): pure field-presence comparison core"
```

---

### Task 2: Baseline read and watch lists

**Files:**
- Modify: `skannonser/ingest/drift.py`
- Test: `tests/rebuild/test_drift.py`

**Interfaces:**
- Consumes: `is_present`, `batch_rates`, `compare`, `DriftFinding` from Task 1.
- Produces: `LISTING_FIELD_COLUMNS: dict[str, str]`; `DETAIL_FIELD_COLUMNS: dict[str, str]`; `baseline_rates(conn, table: str, columns: Sequence[str]) -> tuple[dict[str, float], int]`; `check(conn, listing_rows: list[dict], detail_rows: list[dict]) -> list[DriftFinding]`.

`check` is the single entry point the pipeline calls in Task 3.

- [ ] **Step 1: Write the failing tests**

Append to `tests/rebuild/test_drift.py`:

```python
# --- baseline + watch lists -------------------------------------------------

from skannonser.ingest.drift import (  # noqa: E402
    DETAIL_FIELD_COLUMNS,
    LISTING_FIELD_COLUMNS,
    baseline_rates,
    check,
)
from skannonser.store import connection, migrations  # noqa: E402


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def test_listing_watch_list_excludes_lifecycle_field():
    """Tilgjengelighet tracks a listing's lifecycle, not FINN's markup --
    watching it would false-alarm on every crawl of live ads."""
    assert "Tilgjengelighet" not in LISTING_FIELD_COLUMNS
    assert LISTING_FIELD_COLUMNS["Primærrom"] == "info_primary_area"
    assert LISTING_FIELD_COLUMNS["Boligtype"] == "info_property_type"


def test_detail_watch_list_is_identity_over_scalar_columns():
    assert DETAIL_FIELD_COLUMNS["energimerke"] == "energimerke"
    assert "facilities" not in DETAIL_FIELD_COLUMNS
    assert "finnkode" not in DETAIL_FIELD_COLUMNS


def test_baseline_rates_counts_all_rows(conn):
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES ('1','u',100)"
    )
    conn.execute(
        "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES ('2','u2',NULL)"
    )
    conn.commit()
    rates, rows = baseline_rates(conn, "eiendom", ["info_primary_area"])
    assert rows == 2
    assert rates["info_primary_area"] == pytest.approx(0.5)


def test_baseline_rates_empty_table_no_zero_division(conn):
    rates, rows = baseline_rates(conn, "eiendom", ["info_primary_area"])
    assert rows == 0
    assert rates["info_primary_area"] == 0.0


def test_baseline_rates_missing_table_is_survivable(conn):
    """A pre-migration-010 database has no listing_details. The canary must
    stand down, not crash the ingest it is only observing."""
    conn.execute("DROP TABLE listing_details")
    conn.commit()
    rates, rows = baseline_rates(conn, "listing_details", ["energimerke"])
    assert rates == {}
    assert rows == 0


def test_check_reports_collapse_across_both_tables(conn):
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
    conn.commit()
    listing_rows = [{"Primærrom": ""} for _ in range(MIN_BATCH)]
    found = check(conn, listing_rows, [])
    assert [f.field for f in found] == ["info_primary_area"]
    assert found[0].baseline_rate == pytest.approx(1.0)
    assert found[0].batch_rate == 0.0


def test_check_clean_run_reports_nothing(conn):
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
    conn.commit()
    listing_rows = [{"Primærrom": "100"} for _ in range(MIN_BATCH)]
    assert check(conn, listing_rows, []) == []


def test_each_table_judged_on_its_own_sample_size(conn):
    """Detail parsing is best-effort -- pipeline.py swallows detail failures
    by design -- so the details batch can be smaller than the listings batch.
    Sharing one sample size would misreport both: here the listings batch
    clears MIN_BATCH and must be judged, while the details batch does not and
    must be skipped."""
    for i in range(BASELINE_ROWS):
        conn.execute(
            "INSERT INTO eiendom (finnkode, url, info_primary_area) VALUES (?,?,100)",
            (str(i), f"u{i}"),
        )
        conn.execute(
            "INSERT INTO listing_details (finnkode, energimerke) VALUES (?, 'C')",
            (str(i),),
        )
    conn.commit()

    found = check(
        conn,
        [{"Primærrom": ""} for _ in range(MIN_BATCH)],       # judged
        [{"energimerke": None} for _ in range(MIN_BATCH - 1)],  # too small
    )

    assert [f.field for f in found] == ["info_primary_area"]
    assert found[0].sample_size == MIN_BATCH
```

Add `MIN_BATCH` to the existing import block at the top of the file.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_drift.py -v
```

Expected: `ImportError: cannot import name 'LISTING_FIELD_COLUMNS'`.

- [ ] **Step 3: Extend the module**

Add to `skannonser/ingest/drift.py` — imports at the top, the rest at the end:

```python
import sqlite3

from skannonser.store.repositories.details import _SCALAR_COLS
from skannonser.store.repositories.listings import _INT_COLUMNS, _TEXT_COLUMNS

# Derived, never hand-copied: a field added to either parser is watched
# automatically. These are module-private in their home modules, but they ARE
# the single source of truth for field -> column, and a second hand-maintained
# copy here is exactly the rot this canary exists to catch.
#
# Tilgjengelighet is excluded deliberately -- see `is_present`'s module
# docstring and the design spec's decision 7.
LISTING_FIELD_COLUMNS: dict[str, str] = {
    field: column
    for field, column in {**_TEXT_COLUMNS, **_INT_COLUMNS}.items()
    if field != "Tilgjengelighet"
}

# `ListingDetails` field names already match their columns 1:1. `facilities`
# lives in the other table and `finnkode` is the key, so neither is watched.
DETAIL_FIELD_COLUMNS: dict[str, str] = {col: col for col in _SCALAR_COLS}


def baseline_rates(
    conn: sqlite3.Connection, table: str, columns: Sequence[str]
) -> tuple[dict[str, float], int]:
    """Fill rate of each column across EVERY row of `table`, plus the row
    count so the caller can tell an empty table from a genuinely 0% field.

    All rows, not just active ones: markup presence is not lifecycle-dependent
    for the watched fields, and all rows is the larger sample.

    A missing table (a database predating migration 010) yields ({}, 0) --
    the canary observes ingest, it must never be able to break it.
    """
    columns = list(columns)
    if not columns:
        return {}, 0
    selects = ", ".join(f'COUNT("{c}")' for c in columns)
    try:
        row = conn.execute(f"SELECT COUNT(*), {selects} FROM {table}").fetchone()
    except sqlite3.Error:
        return {}, 0
    total = row[0]
    if total == 0:
        return {c: 0.0 for c in columns}, 0
    return {c: row[i + 1] / total for i, c in enumerate(columns)}, total


def check(
    conn: sqlite3.Connection,
    listing_rows: list[dict],
    detail_rows: list[dict],
) -> list[DriftFinding]:
    """Every drifted field across both tables.

    MUST be called BEFORE the batch is upserted, or tonight's rows are
    already folded into the baseline it is compared against.

    `eiendom` and `listing_details` are judged independently, each on its own
    sample size: detail parsing is best-effort (`pipeline.py` swallows detail
    failures by design) so the two batches can differ in size.
    """
    findings: list[DriftFinding] = []
    for table, field_columns, rows in (
        ("eiendom", LISTING_FIELD_COLUMNS, listing_rows),
        ("listing_details", DETAIL_FIELD_COLUMNS, detail_rows),
    ):
        baseline, baseline_row_count = baseline_rates(
            conn, table, list(field_columns.values())
        )
        findings.extend(
            compare(
                batch_rates(rows, field_columns),
                baseline,
                len(rows),
                baseline_row_count,
            )
        )
    return findings
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_drift.py -v
```

Expected: 26 passed.

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/drift.py tests/rebuild/test_drift.py
git commit -m "feat(drift): baseline read and derived watch lists"
```

---

### Task 3: Wire into the ingest pipeline

**Files:**
- Modify: `skannonser/pipeline.py` (imports; `run_finn_ingest`, around the `repo.upsert(listings)` call and the return dict)
- Test: `tests/rebuild/test_pipeline.py`

**Interfaces:**
- Consumes: `check(conn, listing_rows, detail_rows) -> list[DriftFinding]` from Task 2.
- Produces: `run_finn_ingest(...)` returns an extra key `drift: list[DriftFinding]` in its stats dict. Every other key is unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `tests/rebuild/test_pipeline.py`. The helper mirrors the
`skip_crawl_urls` pattern that file's other offline tests already use
(`test_finn_pipeline_reports_crawled_upserted_deactivated`), so nothing here
touches the network.

```python
# --- parser-drift canary ----------------------------------------------------


def _ingest_over_fixtures(tmp_path, n=2):
    """Offline FINN ingest over `n` golden fixtures."""
    proj = tmp_path / "proj"
    cases = sorted(FINN_FIXTURES.glob("*.html"))[:n]
    (proj / "html_extracted").mkdir(parents=True)
    for c in cases:
        shutil.copy(c, proj / "html_extracted" / c.name)
    urls = [
        (c.stem, f"https://www.finn.no/realestate/homes/ad.html?finnkode={c.stem}")
        for c in cases
    ]
    conn = connection.connect(tmp_path / "p.db")
    migrations.migrate(conn)
    return run_finn_ingest(
        load_domain(), conn, proj, fetch=_fail_if_called, skip_crawl_urls=urls
    )


def test_finn_ingest_reports_drift_in_stats(monkeypatch, tmp_path):
    """The canary's verdict rides along in the stats dict."""
    from skannonser.ingest import drift

    sentinel = [
        drift.DriftFinding(
            field="info_primary_area",
            baseline_rate=0.19,
            batch_rate=0.0,
            sample_size=1000,
        )
    ]
    monkeypatch.setattr("skannonser.pipeline.drift_check", lambda *a, **k: sentinel)
    stats = _ingest_over_fixtures(tmp_path)
    assert stats["drift"] == sentinel


def test_finn_ingest_survives_drift_failure(monkeypatch, tmp_path):
    """A bug in the canary must never fail the ingest it only observes."""
    def boom(*a, **k):
        raise RuntimeError("canary exploded")

    monkeypatch.setattr("skannonser.pipeline.drift_check", boom)
    stats = _ingest_over_fixtures(tmp_path)
    assert stats["drift"] == []
    assert stats["parsed"] > 0
    assert stats["upserted"] > 0


def test_drift_baseline_is_read_before_upsert(monkeypatch, tmp_path):
    """If the baseline were read after upsert, tonight's rows would already
    be folded into it and a same-night collapse would be invisible."""
    seen = {}

    def spy(conn, listing_rows, detail_rows):
        seen["rows_at_call"] = conn.execute(
            "SELECT COUNT(*) FROM eiendom"
        ).fetchone()[0]
        return []

    monkeypatch.setattr("skannonser.pipeline.drift_check", spy)
    _ingest_over_fixtures(tmp_path)
    assert seen["rows_at_call"] == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_pipeline.py -k drift -v
```

Expected: `AttributeError: <module 'skannonser.pipeline'> has no attribute 'drift_check'`.

- [ ] **Step 3: Wire it in**

In `skannonser/pipeline.py`, add to the imports:

```python
from skannonser.ingest.drift import check as drift_check
```

Then, in `run_finn_ingest`, replace:

```python
    repo = ListingsRepo(conn)
    upsert_stats = repo.upsert(listings)
```

with:

```python
    repo = ListingsRepo(conn)

    # Parser-drift canary (2026-07-27 design spec). MUST run before upsert --
    # afterwards tonight's rows are part of the baseline it compares against,
    # and a same-night collapse becomes invisible. Diagnostic only: it never
    # alters ingest, and a failure here is swallowed exactly like the details
    # parse above, for the same reason.
    drift_findings = []
    try:
        drift_findings = drift_check(
            conn,
            [listing.to_row() for listing in listings],
            [detail.model_dump() for detail in details],
        )
    except Exception:
        pass

    upsert_stats = repo.upsert(listings)
```

And add to the returned dict, after `details_upserted`:

```python
        "drift": drift_findings,
```

Update `run_finn_ingest`'s docstring — the "Returns counts:" line should end `..., `details_upserted`, plus `drift` (a list of `DriftFinding`; see `skannonser/ingest/drift.py`).`

- [ ] **Step 4: Run the tests to verify they pass**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_pipeline.py -v
```

Expected: all pass, including the pre-existing guard-1 and guard-2 tests.

- [ ] **Step 5: Run the full suite**

```bash
./.venv/bin/python -m pytest
```

Expected: 694 passed (665 baseline + 26 drift + 3 pipeline). Investigate any pre-existing test that changed.

- [ ] **Step 6: Commit**

```bash
git add skannonser/pipeline.py tests/rebuild/test_pipeline.py
git commit -m "feat(drift): run the canary before upsert in run_finn_ingest"
```

---

### Task 4: Surface it — stderr, notification, exit code

**Files:**
- Modify: `skannonser/notifications.py` (add `format_drift_message`)
- Modify: `skannonser/commands/run_cmd.py` (add `_drift_ok`, call it beside `_failure_rate_ok`)
- Test: `tests/rebuild/test_notifications.py`, `tests/rebuild/test_cli.py`

**Interfaces:**
- Consumes: `DriftFinding` from Task 1; the `drift` stats key from Task 3.
- Produces: `format_drift_message(findings: list[DriftFinding]) -> str`; `_drift_ok(source: str, stats: dict, send=default_send) -> bool`.

`_drift_ok` takes `send` as a parameter so tests never shell out to the `notify` binary.

- [ ] **Step 1: Write the failing formatter test**

Append to `tests/rebuild/test_notifications.py`:

```python
def test_format_drift_message():
    from skannonser.ingest.drift import DriftFinding
    from skannonser.notifications import format_drift_message

    message = format_drift_message(
        [
            DriftFinding("info_primary_area", 0.192, 0.0, 1043),
            DriftFinding("energimerke", 0.854, 0.011, 1043),
        ]
    )
    assert message.splitlines() == [
        "Parser drift: 2 field(s)",
        "info_primary_area  19.2% -> 0.0%  (n=1043)",
        "energimerke  85.4% -> 1.1%  (n=1043)",
    ]
```

- [ ] **Step 2: Run it to verify it fails**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_notifications.py -k drift -v
```

Expected: `ImportError: cannot import name 'format_drift_message'`.

- [ ] **Step 3: Add the formatter**

In `skannonser/notifications.py`, beside the other `format_*` functions:

```python
def format_drift_message(findings: list) -> str:
    """One line per drifted field, for the parser-drift canary alert.
    `findings` are `skannonser.ingest.drift.DriftFinding`."""
    lines = [f"Parser drift: {len(findings)} field(s)"]
    for f in findings:
        lines.append(
            f"{f.field}  {f.baseline_rate:.1%} -> {f.batch_rate:.1%} "
            f"(n={f.sample_size})"
        )
    return "\n".join(lines)
```

- [ ] **Step 4: Run it to verify it passes**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_notifications.py -k drift -v
```

Expected: PASS.

- [ ] **Step 5: Write the failing CLI-guard tests**

Append to `tests/rebuild/test_cli.py`:

```python
def test_drift_ok_true_when_no_findings():
    from skannonser.commands.run_cmd import _drift_ok

    sent = []
    assert _drift_ok("FINN", {"drift": []}, send=lambda *a, **k: sent.append(a)) is True
    assert sent == []


def test_drift_ok_false_and_notifies_on_findings(capsys):
    from skannonser.commands.run_cmd import _drift_ok
    from skannonser.ingest.drift import DriftFinding

    sent = []
    ok = _drift_ok(
        "FINN",
        {"drift": [DriftFinding("info_primary_area", 0.19, 0.0, 1000)]},
        send=lambda title, message, priority=0: sent.append((title, message, priority)),
    )
    assert ok is False
    assert sent[0][0] == "Parser drift"
    assert "info_primary_area" in sent[0][1]
    assert sent[0][2] == 1
    assert "info_primary_area" in capsys.readouterr().err


def test_drift_ok_tolerates_missing_key():
    """Older stats dicts, and run_dnb_ingest, carry no `drift` key."""
    from skannonser.commands.run_cmd import _drift_ok

    assert _drift_ok("DNB", {}, send=lambda *a, **k: None) is True
```

- [ ] **Step 6: Run them to verify they fail**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_cli.py -k drift -v
```

Expected: `ImportError: cannot import name '_drift_ok'`.

- [ ] **Step 7: Add the guard and call it**

In `skannonser/commands/run_cmd.py`, import the formatter and sender:

```python
from skannonser.notifications import default_send, format_drift_message
```

Add beside `_failure_rate_ok`:

```python
def _drift_ok(source: str, stats: dict, send=default_send) -> bool:
    """Report parser drift for operator visibility and send an alert.

    Purely an alert -- unlike guard 2 there is nothing to protect by
    stopping, because silent field loss degrades data rather than destroying
    the active set. Returns False only so the CLI exits non-zero.
    """
    findings = stats.get("drift") or []
    if not findings:
        return True
    message = format_drift_message(findings)
    typer.echo(f"ERROR: {source} {message}", err=True)
    send("Parser drift", message, 1)
    return False
```

Then call it in the `ingest` command's FINN block. That block is
`run_cmd.py:119-122` and currently reads:

```python
        if not _crawled_ok("finn", stats):
            ok = False
        if not _failure_rate_ok("finn", stats):
            ok = False
```

Add a third clause immediately after, matching the existing shape exactly:

```python
        if not _drift_ok("finn", stats):
            ok = False
```

The existing `raise typer.Exit(code=1)` at `run_cmd.py:133` already fires when
`ok` is False, so no other change is needed.

**Do not add the call to the DNB block** (`run_cmd.py:127-130`).
`run_dnb_ingest` has no drift check — DNB is out of scope per the spec — and
`_drift_ok` would return True there anyway, but adding it would imply
coverage that does not exist.

- [ ] **Step 8: Run the tests to verify they pass**

```bash
./.venv/bin/python -m pytest tests/rebuild/test_cli.py -k drift -v
```

Expected: 3 passed.

- [ ] **Step 9: Run the full suite**

```bash
./.venv/bin/python -m pytest
```

Expected: 698 passed (694 + 1 notifications + 3 CLI). Any pre-existing CLI test that now exits non-zero means `_drift_ok` was wired into a path that also runs for DNB or for a stats dict without the key — fix the wiring, not the test.

- [ ] **Step 10: Commit**

```bash
git add skannonser/notifications.py skannonser/commands/run_cmd.py \
        tests/rebuild/test_notifications.py tests/rebuild/test_cli.py
git commit -m "feat(drift): alert on parser drift via stderr, notify and exit code"
```

---

## Verification

After Task 4, confirm the canary would actually have caught the P-ROM bug, using the real corpus in the **main clone** (`/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted/`, 3 GB, not in git — it is absent from a fresh worktree):

```bash
./.venv/bin/python -c "
import glob, random
from bs4 import BeautifulSoup
from skannonser.ingest.finn.parse import parse_ad
from skannonser.ingest.drift import LISTING_FIELD_COLUMNS, batch_rates, compare
files = glob.glob('/Users/tehbaer/kode/skannonser/data/eiendom/html_extracted/*.html')
random.seed(11)
rows = [parse_ad(open(p, encoding='utf-8', errors='replace').read(), 'x', 'u').to_row()
        for p in random.sample(files, 300)]
rates = batch_rates(rows, LISTING_FIELD_COLUMNS)
print('P-ROM rate now:', round(rates['info_primary_area'], 3))
broken = dict(rates, info_primary_area=0.0)
print('findings if it broke:', compare(broken, rates, len(rows), 5000))
"
```

Expected: a P-ROM rate near 0.19, and exactly one `DriftFinding` for `info_primary_area` in the simulated-breakage line. If the rate reads 0.0, the Task-1 `is_present` predicate is treating `""` as present.

## Notes for the implementer

- **Do not add a migration.** This design adds no schema. `015` is claimed by the salgsoppgave work on another branch.
- **`nightly.py` needs no change.** `_run_step` already records each step's whole stats dict, so the `drift` key rides into the step report for free once Task 3 lands. If you find yourself editing `nightly.py`, stop and re-read Task 3.
- `drift.py` imports `_SCALAR_COLS`, `_INT_COLUMNS` and `_TEXT_COLUMNS` — module-private names in their home modules. This is deliberate and spec'd (decision 6): they are the single source of truth for field-to-column mapping, and a second hand-maintained copy is exactly the rot this canary exists to catch. If a reviewer objects, the fix is to make those names public in their home modules, not to duplicate them.
- The 12 golden fixtures in `tests/rebuild/fixtures/finn/` cannot test drift — they are frozen and parse identically forever. All drift tests use synthetic batches.
- Expected suite counts assume a 665 baseline. Re-run `./ops/setup-worktree.sh` if yours differs before assuming you broke something.
