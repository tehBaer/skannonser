"""Tests for atomic canonical writes and gzipped snapshot-on-change in ad_html_loader."""
import gzip
import os
import sys
import tempfile
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.extractors import ad_html_loader
from main.extractors.ad_html_loader import save_ad_html


class SaveAdHtmlTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.project = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def canonical_path(self, uid):
        return os.path.join(self.project, "html_extracted", f"{uid}.html")

    def snapshot_path(self, uid, day):
        return os.path.join(self.project, "html_snapshots", f"{uid}.{day}.html.gz")

    def test_writes_canonical_file_with_exact_content(self):
        save_ad_html(self.project, "111", "<html>hello</html>", today="20260709")
        with open(self.canonical_path("111"), encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "<html>hello</html>")

    def test_new_uid_creates_baseline_snapshot(self):
        save_ad_html(self.project, "222", "<html>A</html>", today="20260709")
        snap = self.snapshot_path("222", "20260709")
        self.assertTrue(os.path.exists(snap), "expected a baseline snapshot for a brand-new uid")
        with gzip.open(snap, "rt", encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "<html>A</html>")

    def test_unchanged_resave_creates_no_snapshot(self):
        # Simulate a canonical saved on a prior day.
        os.makedirs(os.path.join(self.project, "html_extracted"))
        with open(self.canonical_path("333"), "w", encoding="utf-8") as fh:
            fh.write("<html>same</html>")
        save_ad_html(self.project, "333", "<html>same</html>", today="20260709")
        self.assertFalse(
            os.path.exists(self.snapshot_path("333", "20260709")),
            "unchanged content must not produce a snapshot",
        )

    def test_changed_resave_creates_dated_snapshot_of_new_content(self):
        os.makedirs(os.path.join(self.project, "html_extracted"))
        with open(self.canonical_path("444"), "w", encoding="utf-8") as fh:
            fh.write("<html>old</html>")
        save_ad_html(self.project, "444", "<html>new</html>", today="20260709")
        # canonical updated
        with open(self.canonical_path("444"), encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "<html>new</html>")
        # snapshot holds the NEW content, dated today
        snap = self.snapshot_path("444", "20260709")
        self.assertTrue(os.path.exists(snap))
        with gzip.open(snap, "rt", encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "<html>new</html>")

    def test_failed_write_preserves_existing_canonical(self):
        """A crash during the write must never destroy a good prior canonical."""
        os.makedirs(os.path.join(self.project, "html_extracted"))
        with open(self.canonical_path("555"), "w", encoding="utf-8") as fh:
            fh.write("<html>GOOD</html>")
        with mock.patch.object(ad_html_loader.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                save_ad_html(self.project, "555", "<html>BROKEN</html>", today="20260709")
        # Original content survived intact.
        with open(self.canonical_path("555"), encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "<html>GOOD</html>")


if __name__ == "__main__":
    unittest.main()
