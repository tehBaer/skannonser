"""Integration test: refresh_all_listings records status changes to history.

Only the network boundary (refresh_listing) is mocked; the DB row and the
history recording are real.
"""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase
from main.sync import refresh_listings


class RefreshRecordsHistoryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "test.db")
        self.db = PropertyDatabase(self.db_path)
        # Insert a real listing currently "Til salgs".
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO eiendom (finnkode, tilgjengelighet, adresse, url, active) "
            "VALUES (?, ?, ?, ?, 1)",
            ("999", "Til salgs", "Somewhere 1", "https://finn.no/999"),
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self._tmp.cleanup()

    def test_status_change_is_written_to_history(self):
        fake_result = {"finnkode": "999", "tilgjengelighet": "Solgt", "success": True, "error": None}
        with mock.patch.object(refresh_listings, "refresh_listing", return_value=fake_result):
            refresh_listings.refresh_all_listings(db_path=self.db_path, delay=0)

        history = self.db.get_status_history("999")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["old_status"], "Til salgs")
        self.assertEqual(history[0]["new_status"], "Solgt")


if __name__ == "__main__":
    unittest.main()
