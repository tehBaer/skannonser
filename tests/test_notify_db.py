import os, sqlite3, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase


class NotifyDbTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = PropertyDatabase(os.path.join(self._tmp.name, "t.db"))
        self._conn = sqlite3.connect(self.db.db_path)

    def tearDown(self):
        self._conn.close()
        self._tmp.cleanup()

    def _insert(self, finnkode, active=1, pris=3000000, brai=80, status=None):
        self._conn.execute(
            "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
            "VALUES (?,?,?,?,?,?)",
            (finnkode, active, pris, brai, status, f"https://finn.no/{finnkode}"),
        )
        self._conn.commit()

    def test_active_tracked_respects_filters(self):
        self._insert("1", active=1, pris=3000000, brai=80)   # pass
        self._insert("2", active=0, pris=3000000, brai=80)   # inactive -> excluded
        self._insert("3", active=1, pris=9000000, brai=80)   # too pricey -> excluded
        self._insert("4", active=1, pris=3000000, brai=50)   # too small -> excluded
        self.assertEqual(self.db.get_active_tracked_finnkodes(), {"1"})

    def test_finnkodes_with_status(self):
        self._insert("10", status="Solgt")
        self._insert("11", status=None)
        self.assertEqual(self.db.get_finnkodes_with_status(["10", "11", "99"], "Solgt"), {"10"})

    def test_snapshot_round_trip_and_replace(self):
        self.db.replace_active_snapshot({"a", "b"})
        self.assertEqual(self.db.get_previous_active_snapshot(), {"a", "b"})
        self.db.replace_active_snapshot({"b", "c"})
        self.assertEqual(self.db.get_previous_active_snapshot(), {"b", "c"})

    def test_daily_metrics_record_and_sum(self):
        self.db.record_daily_metrics("2026-07-06", 5, 2, 1, 100)
        self.db.record_daily_metrics("2026-07-07", 3, 0, 4, 101)
        got = self.db.sum_daily_metrics_between("2026-07-06", "2026-07-07")
        self.assertEqual(got, {"added": 8, "removed_sold": 2, "removed_delisted": 5})

    def test_record_daily_metrics_is_idempotent_per_date(self):
        self.db.record_daily_metrics("2026-07-06", 5, 2, 1, 100)
        self.db.record_daily_metrics("2026-07-06", 9, 9, 9, 200)  # replace same date
        self.assertEqual(self.db.sum_daily_metrics_between("2026-07-06", "2026-07-06"),
                         {"added": 9, "removed_sold": 9, "removed_delisted": 9})

    def test_count_sold_between(self):
        self.db.record_status_change_if_changed("50", "", "Solgt")   # observed_at = now
        self.db.record_status_change_if_changed("51", "", "Reservert")
        from datetime import date
        today = date.today().isoformat()
        self.assertEqual(self.db.count_sold_between(today, today), 1)


if __name__ == "__main__":
    unittest.main()
