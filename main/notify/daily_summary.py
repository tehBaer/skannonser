"""Daily orchestrator: diff active set, notify, persist metrics + snapshot."""
from datetime import date

from main.database.db import PropertyDatabase
from main.notify.send import send as default_send
from main.notify.listing_metrics import compute_daily_metrics, format_daily_message


def run(db_path=None, today=None, send=default_send) -> bool:
    db = PropertyDatabase(db_path)
    today = today or date.today().isoformat()
    previous = db.get_previous_active_snapshot()
    current = db.get_active_tracked_finnkodes()

    if not previous:
        # First run: establish baseline; do not report a misleading full diff.
        db.replace_active_snapshot(current)
        db.record_daily_metrics(today, 0, 0, 0, len(current))
        return send("Listings baseline",
                    f"\U0001F4CA Baseline set: {len(current)} active listings tracked.", 0)

    removed = previous - current
    sold = db.get_finnkodes_with_status(removed, "Solgt")
    metrics = compute_daily_metrics(current, previous, sold)
    ok = send("Daily listings", format_daily_message(metrics), 0)
    db.record_daily_metrics(today, metrics.added, metrics.removed_sold,
                            metrics.removed_delisted, metrics.total_active)
    db.replace_active_snapshot(current)
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
