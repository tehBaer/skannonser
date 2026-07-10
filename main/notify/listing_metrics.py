"""Pure daily added/removed metrics from active-set diffs."""
from dataclasses import dataclass, field


@dataclass
class DailyMetrics:
    added: int
    removed_sold: int
    removed_delisted: int
    total_active: int
    added_finnkodes: set = field(default_factory=set)
    removed_finnkodes: set = field(default_factory=set)


def compute_daily_metrics(current: set, previous: set, sold_removed: set) -> DailyMetrics:
    added = set(current) - set(previous)
    removed = set(previous) - set(current)
    removed_sold = removed & set(sold_removed)
    removed_delisted = removed - set(sold_removed)
    return DailyMetrics(
        added=len(added),
        removed_sold=len(removed_sold),
        removed_delisted=len(removed_delisted),
        total_active=len(current),
        added_finnkodes=added,
        removed_finnkodes=removed,
    )


def format_daily_message(m: DailyMetrics) -> str:
    removed_total = m.removed_sold + m.removed_delisted
    return (
        f"\U0001F3E0 Today: +{m.added} added, -{removed_total} removed "
        f"({m.removed_sold} sold, {m.removed_delisted} delisted). "
        f"Active: {m.total_active}."
    )
