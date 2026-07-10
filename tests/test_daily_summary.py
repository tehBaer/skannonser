import os, sqlite3, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase
from main.notify import daily_summary


class DailySummaryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "t.db")
        self.db = PropertyDatabase(self.db_path)
        self._conn = sqlite3.connect(self.db_path)
        self.sent = []

    def tearDown(self):
        self._conn.close()
        self._tmp.cleanup()

    def _fake_send(self, title, message, priority=0):
        self.sent.append((title, message, priority))
        return True

    def _insert(self, finnkode, active=1, status=None):
        self._conn.execute(
            "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
            "VALUES (?,?,?,?,?,?)",
            (finnkode, active, 3000000, 80, status, f"https://finn.no/{finnkode}"),
        )
        self._conn.commit()

    def test_first_run_sets_baseline_without_diff(self):
        self._insert("1"); self._insert("2")
        ok = daily_summary.run(db_path=self.db_path, today="2026-07-10", send=self._fake_send)
        self.assertTrue(ok)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("Baseline", self.sent[0][1])
        self.assertEqual(self.db.get_previous_active_snapshot(), {"1", "2"})

    def test_second_run_reports_added_and_removed(self):
        self.db.replace_active_snapshot({"1", "2"})
        self._insert("2"); self._insert("3")
        self._insert("1", active=0, status="Solgt")  # removed, sold
        ok = daily_summary.run(db_path=self.db_path, today="2026-07-11", send=self._fake_send)
        self.assertTrue(ok)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0][1],
                         "\U0001F3E0 Today: +1 added, -1 removed (1 sold, 0 delisted). Active: 2.")
        self.assertEqual(self.db.get_previous_active_snapshot(), {"2", "3"})
        self.assertEqual(self.db.sum_daily_metrics_between("2026-07-11", "2026-07-11"),
                         {"added": 1, "removed_sold": 1, "removed_delisted": 0})


if __name__ == "__main__":
    unittest.main()
