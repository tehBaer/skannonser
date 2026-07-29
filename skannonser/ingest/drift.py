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
