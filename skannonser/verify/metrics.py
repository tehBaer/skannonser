"""Golden-master harness: compare ``skannonser.notifications.compute_daily_metrics``
against LEGACY ``main.notify.listing_metrics.compute_daily_metrics``, driving
the REAL legacy function (never a reimplementation).

Three fixed synthetic scenarios cover legacy's own
``tests/test_listing_metrics.py`` -- its two data cases (no-change, and the
added/removed split-by-sold-vs-delisted case) -- plus an empty-set edge case
that isn't itself one of legacy's pinned cases, plus one ``live_db`` scenario
built from a caller-supplied DB COPY: the real ``previous_active_snapshot``
(``ListingsRepo``) vs the real active/price/area-filtered tracked set
(``skannonser.notifications._active_tracked_finnkodes``) vs the real
currently-'Solgt' subset of whatever's missing from the tracked set
(``skannonser.notifications._finnkodes_with_status``) -- i.e. exactly the
three sets ``daily_summary`` itself would compute for a real run today.
Read-only: the DB copy is never written to.

New's parameter order is ``(previous, current, sold_finnkodes)``; legacy's is
``(current, previous, sold_removed)`` -- callers on the legacy side must swap
accordingly (done once, in ``verify_metrics`` below).

Legacy (``main.notify.listing_metrics``) is imported lazily -- mirrors
``skannonser/verify/parse.py``'s ``_import_legacy``.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

from skannonser.notifications import (
    _active_tracked_finnkodes,
    _finnkodes_with_status,
    compute_daily_metrics,
)
from skannonser.store import connection as skconn
from skannonser.store.repositories.listings import ListingsRepo

_REPO_ROOT = Path(__file__).resolve().parents[2]

_SCALAR_FIELDS = ("added", "removed_sold", "removed_delisted", "total_active")
_SET_FIELDS = ("added_finnkodes", "removed_finnkodes")


@dataclass(frozen=True)
class MetricsDiff:
    scenario: str
    field: str
    legacy_value: object
    new_value: object


@dataclass
class VerifyMetricsResult:
    diffs: list = field(default_factory=list)


def _import_legacy():
    """Import legacy's pure metrics module, adding the repo root to
    `sys.path` first if needed (installed console script case) -- same
    pattern as `verify/parse.py`'s `_import_legacy`."""
    try:
        from main.notify.listing_metrics import compute_daily_metrics as legacy_compute
    except ModuleNotFoundError:
        root = str(_REPO_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)
        from main.notify.listing_metrics import compute_daily_metrics as legacy_compute
    return legacy_compute


def _fixed_scenarios() -> dict[str, tuple[set, set, set]]:
    """(previous, current, sold_finnkodes) triples covering every case in
    legacy's `tests/test_listing_metrics.py`."""
    return {
        "empty": (set(), set(), set()),
        "no_change": ({"a", "b"}, {"a", "b"}, set()),
        "added_and_removed_split": (
            {"a", "b", "c", "d"},
            {"c", "d", "e"},
            {"a"},
        ),
    }


def _live_db_scenario(db_path: Path) -> tuple[set, set, set]:
    """The real (previous, current, sold_finnkodes) triple `daily_summary`
    would compute against this DB right now."""
    conn = skconn.connect(db_path)
    try:
        previous = ListingsRepo(conn).previous_active_snapshot()
        current = _active_tracked_finnkodes(conn)
        removed = previous - current
        sold = _finnkodes_with_status(conn, removed, "Solgt")
        return previous, current, sold
    finally:
        conn.close()


def verify_metrics(db_path: Path) -> VerifyMetricsResult:
    legacy_compute = _import_legacy()
    result = VerifyMetricsResult()

    scenarios = _fixed_scenarios()
    scenarios["live_db"] = _live_db_scenario(db_path)

    for name, (previous, current, sold) in scenarios.items():
        new = compute_daily_metrics(previous, current, sold)
        legacy = legacy_compute(current, previous, sold)  # legacy order: current, previous, sold_removed

        for f in _SCALAR_FIELDS:
            lv, nv = getattr(legacy, f), new[f]
            if lv != nv:
                result.diffs.append(MetricsDiff(name, f, lv, nv))
        for f in _SET_FIELDS:
            lv, nv = getattr(legacy, f), new[f]
            if lv != nv:
                result.diffs.append(MetricsDiff(name, f, lv, nv))

    return result
