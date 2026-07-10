import os, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase
from main.notify import weekly_summary


class WeeklySummaryTests(unittest.TestCase):
    def test_format_message(self):
        self.assertEqual(weekly_summary.format_weekly_message(48, 19),
                         "\U0001F4C5 This week: +48 added, 19 sold.")

    def test_run_aggregates_added_over_window_and_counts_sold(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "t.db")
        db = PropertyDatabase(db_path)
        # Window ending Sunday 2026-07-12 covers 2026-07-06..2026-07-12
        db.record_daily_metrics("2026-07-06", 5, 0, 0, 100)
        db.record_daily_metrics("2026-07-10", 7, 0, 0, 101)
        db.record_daily_metrics("2026-06-30", 99, 0, 0, 90)  # outside window

        sent = []
        weekly_summary.run(db_path=db_path, today="2026-07-12",
                           send=lambda t, m, p=0: sent.append((t, m, p)) or True)
        self.assertEqual(len(sent), 1)
        self.assertIn("+12 added", sent[0][1])


if __name__ == "__main__":
    unittest.main()
