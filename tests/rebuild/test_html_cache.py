"""Tests for the ad HTML cache: atomic canonical writes, gzipped
snapshot-on-change, and legacy cache-directory compatibility.

Rewrite of `tests/test_ad_html_loader.py` against the new
`skannonser.ingest.finn.html_cache` module, plus a test pinning that the
~7,731 existing legacy-written cache files remain readable.
"""

import gzip
from unittest import mock

import pytest

from skannonser.ingest.finn import html_cache
from skannonser.ingest.finn.html_cache import load_or_fetch, save_ad_html


def _fail_if_called(*a, **k):
    raise AssertionError("network hit for cached ad")


def canonical_path(project, uid):
    return project / "html_extracted" / f"{uid}.html"


def snapshot_path(project, uid, day):
    return project / "html_snapshots" / f"{uid}.{day}.html.gz"


def test_writes_canonical_file_with_exact_content(tmp_path):
    save_ad_html(tmp_path, "111", "<html>hello</html>", today="20260709")
    assert canonical_path(tmp_path, "111").read_text(encoding="utf-8") == "<html>hello</html>"


def test_new_uid_creates_baseline_snapshot(tmp_path):
    save_ad_html(tmp_path, "222", "<html>A</html>", today="20260709")
    snap = snapshot_path(tmp_path, "222", "20260709")
    assert snap.exists(), "expected a baseline snapshot for a brand-new uid"
    with gzip.open(snap, "rt", encoding="utf-8") as fh:
        assert fh.read() == "<html>A</html>"


def test_unchanged_resave_creates_no_snapshot(tmp_path):
    # Simulate a canonical saved on a prior day.
    canonical = canonical_path(tmp_path, "333")
    canonical.parent.mkdir(parents=True)
    canonical.write_text("<html>same</html>", encoding="utf-8")

    save_ad_html(tmp_path, "333", "<html>same</html>", today="20260709")

    assert not snapshot_path(tmp_path, "333", "20260709").exists(), (
        "unchanged content must not produce a snapshot"
    )


def test_changed_resave_creates_dated_snapshot_of_new_content(tmp_path):
    canonical = canonical_path(tmp_path, "444")
    canonical.parent.mkdir(parents=True)
    canonical.write_text("<html>old</html>", encoding="utf-8")

    save_ad_html(tmp_path, "444", "<html>new</html>", today="20260709")

    # canonical updated
    assert canonical.read_text(encoding="utf-8") == "<html>new</html>"
    # snapshot holds the NEW content, dated today
    snap = snapshot_path(tmp_path, "444", "20260709")
    assert snap.exists()
    with gzip.open(snap, "rt", encoding="utf-8") as fh:
        assert fh.read() == "<html>new</html>"


def test_failed_write_preserves_existing_canonical(tmp_path):
    """A crash during the write must never destroy a good prior canonical."""
    canonical = canonical_path(tmp_path, "555")
    canonical.parent.mkdir(parents=True)
    canonical.write_text("<html>GOOD</html>", encoding="utf-8")

    with mock.patch.object(html_cache.os, "replace", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            save_ad_html(tmp_path, "555", "<html>BROKEN</html>", today="20260709")

    # Original content survived intact.
    assert canonical.read_text(encoding="utf-8") == "<html>GOOD</html>"


def test_reads_existing_legacy_cache(tmp_path):
    proj = tmp_path / "proj"
    (proj / "html_extracted").mkdir(parents=True)
    (proj / "html_extracted" / "42.html").write_text("<html>cached</html>")
    html = load_or_fetch("https://x", proj, "42", fetch=_fail_if_called)
    assert html == "<html>cached</html>"


def test_load_or_fetch_fetches_and_caches_on_miss(tmp_path):
    class FakeResponse:
        content = b"<html><body>fresh</body></html>"

        def raise_for_status(self):
            pass

    calls = []

    def fake_fetch(url):
        calls.append(url)
        return FakeResponse()

    html = load_or_fetch("https://x/99", tmp_path, "99", fetch=fake_fetch, fetch_delay=lambda: None)

    assert calls == ["https://x/99"]
    assert "fresh" in html
    assert canonical_path(tmp_path, "99").exists()
    # Second call is served from cache -- fetch must not be called again.
    html2 = load_or_fetch("https://x/99", tmp_path, "99", fetch=_fail_if_called, fetch_delay=lambda: None)
    assert html2 == canonical_path(tmp_path, "99").read_text(encoding="utf-8")


def _fake_fetch_ok(url):
    """Minimal ok response for testing fetch_delay."""
    class FakeResponse:
        content = b"<html>ok</html>"

        def raise_for_status(self):
            pass

    return FakeResponse()


def test_fetch_delay_fires_only_on_network_path(tmp_path):
    """Verify that fetch_delay is called only on cache misses, never on hits."""
    calls = []

    proj = tmp_path / "proj"
    (proj / "html_extracted").mkdir(parents=True)
    (proj / "html_extracted" / "7.html").write_text("<html>cached</html>")

    # cache hit: no delay
    load_or_fetch("https://x", proj, "7", fetch=_fail_if_called, fetch_delay=lambda: calls.append(1))
    assert calls == [], "fetch_delay must not fire on cache hit"

    # network path: delay fires
    load_or_fetch("https://x", proj, "8", fetch=_fake_fetch_ok, fetch_delay=lambda: calls.append(1))
    assert calls == [1], "fetch_delay must fire on cache miss before fetch"
