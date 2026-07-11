"""Weekly Sunday summary: added over the past 7 days + sold transitions."""
from datetime import date, timedelta

from main.database.db import PropertyDatabase
from main.notify.send import send as default_send


def format_weekly_message(added: int, sold: int) -> str:
    return f"\U0001F4C5 This week: +{added} added, {sold} sold."


def run(db_path=None, today=None, send=default_send) -> bool:
    db = PropertyDatabase(db_path)
    end = date.fromisoformat(today) if today else date.today()
    start = end - timedelta(days=6)  # inclusive 7-day window
    metrics = db.sum_daily_metrics_between(start.isoformat(), end.isoformat())
    sold = db.count_sold_between(start.isoformat(), end.isoformat())
    return send("Weekly summary", format_weekly_message(metrics["added"], sold), 0)


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
