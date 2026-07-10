import os, sys, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify.listing_metrics import compute_daily_metrics, format_daily_message


class ListingMetricsTests(unittest.TestCase):
    def test_added_and_removed_split_sold_vs_delisted(self):
        previous = {"a", "b", "c", "d"}
        current = {"c", "d", "e"}           # added e; removed a, b
        sold_removed = {"a"}                # a was sold; b delisted
        m = compute_daily_metrics(current, previous, sold_removed)
        self.assertEqual(m.added, 1)
        self.assertEqual(m.removed_sold, 1)
        self.assertEqual(m.removed_delisted, 1)
        self.assertEqual(m.total_active, 3)
        self.assertEqual(m.added_finnkodes, {"e"})
        self.assertEqual(m.removed_finnkodes, {"a", "b"})

    def test_no_change(self):
        m = compute_daily_metrics({"a", "b"}, {"a", "b"}, set())
        self.assertEqual((m.added, m.removed_sold, m.removed_delisted, m.total_active), (0, 0, 0, 2))

    def test_format_message(self):
        m = compute_daily_metrics({"c", "d", "e"}, {"a", "b", "c", "d"}, {"a"})
        self.assertEqual(
            format_daily_message(m),
            "\U0001F3E0 Today: +1 added, -2 removed (1 sold, 1 delisted). Active: 3.",
        )


if __name__ == "__main__":
    unittest.main()
