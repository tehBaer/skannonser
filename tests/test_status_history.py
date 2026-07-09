"""Tests for the append-only status-change history on PropertyDatabase."""
import os
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase


class StatusHistoryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = PropertyDatabase(os.path.join(self._tmp.name, "test.db"))

    def tearDown(self):
        self._tmp.cleanup()

    def test_change_is_recorded_and_returns_true(self):
        recorded = self.db.record_status_change_if_changed("123", "", "Solgt")
        self.assertTrue(recorded)
        history = self.db.get_status_history("123")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["old_status"], "")
        self.assertEqual(history[0]["new_status"], "Solgt")
        self.assertIsNotNone(history[0]["observed_at"])

    def test_unchanged_status_records_nothing_and_returns_false(self):
        recorded = self.db.record_status_change_if_changed("123", "Solgt", "Solgt")
        self.assertFalse(recorded)
        self.assertEqual(self.db.get_status_history("123"), [])

    def test_whitespace_only_difference_is_not_a_change(self):
        recorded = self.db.record_status_change_if_changed("123", "Solgt", "  Solgt  ")
        self.assertFalse(recorded)
        self.assertEqual(self.db.get_status_history("123"), [])

    def test_multiple_changes_accumulate_in_order(self):
        self.db.record_status_change_if_changed("123", "", "Reservert")
        self.db.record_status_change_if_changed("123", "Reservert", "Solgt")
        history = self.db.get_status_history("123")
        self.assertEqual([(h["old_status"], h["new_status"]) for h in history],
                         [("", "Reservert"), ("Reservert", "Solgt")])

    def test_history_is_scoped_per_finnkode(self):
        self.db.record_status_change_if_changed("111", "", "Solgt")
        self.db.record_status_change_if_changed("222", "", "Reservert")
        self.assertEqual(len(self.db.get_status_history("111")), 1)
        self.assertEqual(self.db.get_status_history("111")[0]["new_status"], "Solgt")


if __name__ == "__main__":
    unittest.main()
