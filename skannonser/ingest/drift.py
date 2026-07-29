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
import sqlite3
from dataclasses import dataclass
from typing import Any, Sequence

from skannonser.store.repositories.details import _SCALAR_COLS
from skannonser.store.repositories.listings import _INT_COLUMNS, _TEXT_COLUMNS

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
