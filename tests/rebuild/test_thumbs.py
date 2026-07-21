"""Tests for skannonser.enrich.thumbs.cache_thumbnails (Phase 5 Task 5).

No network: every fetch is a fake callable recording its calls and returning
a canned fake response object (``status_code``/``content``, mirroring
``requests.Response``'s attributes actually used). Every DB is a migrated tmp
sqlite file (same convention as tests/rebuild/test_web_api.py).
"""

from __future__ import annotations

import sqlite3

import pytest

from skannonser.enrich.thumbs import cache_thumbnails
from skannonser.ids import dnb_identifier
from skannonser.store import connection, migrations


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "thumbs.db")
    migrations.migrate(c)
    return c


def _ins_eiendom(conn: sqlite3.Connection, finnkode: str, *, active=1, image_url="https://img/x.jpg"):
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, image_url) VALUES (?, ?, ?)",
        (finnkode, active, image_url),
    )
    conn.commit()


def _ins_dnb(conn: sqlite3.Connection, url: str, *, active=1, image_url="https://img/dnb.jpg"):
    """dnbeiendom has no image_url column on the real schema (see
    skannonser.enrich.thumbs's "DNB image_url COLUMN" docstring) -- tests
    that need a DNB candidate ALTER the *test's own tmp db* to add one,
    exactly like a future migration would; this never touches the real
    migrations."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(dnbeiendom)").fetchall()]
    if "image_url" not in cols:
        conn.execute("ALTER TABLE dnbeiendom ADD COLUMN image_url TEXT")
    conn.execute(
        "INSERT INTO dnbeiendom (url, active, image_url) VALUES (?, ?, ?)",
        (url, active, image_url),
    )
    conn.commit()


class FakeResponse:
    def __init__(self, status_code=200, content=b"jpeg-bytes"):
        self.status_code = status_code
        self.content = content


def make_fetch(responses=None, default=None, calls=None):
    """``responses``: {url: FakeResponse | Exception}. Anything not listed
    falls back to ``default`` (or a plain 200 FakeResponse)."""
    responses = responses or {}
    calls = calls if calls is not None else []

    def fetch(url, headers=None, timeout=None):
        calls.append({"url": url, "headers": headers, "timeout": timeout})
        resp = responses.get(url, default if default is not None else FakeResponse())
        if isinstance(resp, Exception):
            raise resp
        return resp

    return fetch


def no_delay() -> None:
    pass


# ---------------------------------------------------------------------------
# Candidate selection / downloads-only-missing / skip-existing
# ---------------------------------------------------------------------------


def test_downloads_only_missing_skips_existing(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "111")
    _ins_eiendom(conn, "222")
    (dest).mkdir()
    (dest / "222.jpg").write_bytes(b"already-here")

    calls = []
    fetch = make_fetch(calls=calls)
    stats = cache_thumbnails(conn, dest, fetch=fetch, fetch_delay=no_delay)

    assert stats == {"candidates": 1, "downloaded": 1, "skipped_existing": 1, "failed": 0}
    assert (dest / "111.jpg").read_bytes() == b"jpeg-bytes"
    assert (dest / "222.jpg").read_bytes() == b"already-here"  # untouched
    assert len(calls) == 1  # no fetch issued for the already-cached row
    assert calls[0]["url"] == "https://img/x.jpg"


def test_inactive_and_empty_image_url_rows_are_not_candidates(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "inactive", active=0)
    _ins_eiendom(conn, "no_image", image_url=None)
    _ins_eiendom(conn, "blank_image", image_url="   ")

    calls = []
    stats = cache_thumbnails(conn, dest, fetch=make_fetch(calls=calls), fetch_delay=no_delay)

    assert stats == {"candidates": 0, "downloaded": 0, "skipped_existing": 0, "failed": 0}
    assert calls == []
    assert list(dest.glob("*")) == []


def test_dest_dir_created_if_missing(conn, tmp_path):
    dest = tmp_path / "does" / "not" / "exist" / "yet"
    assert not dest.exists()

    cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    assert dest.is_dir()


# ---------------------------------------------------------------------------
# Failure handling: non-fatal, recorded, retried next call, no partial file.
# ---------------------------------------------------------------------------


def test_non_200_response_recorded_failed_not_fatal_no_file_written(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "bad")

    stats = cache_thumbnails(
        conn, dest, fetch=make_fetch(default=FakeResponse(status_code=404)), fetch_delay=no_delay
    )

    assert stats == {"candidates": 1, "downloaded": 0, "skipped_existing": 0, "failed": 1}
    assert not (dest / "bad.jpg").exists()
    assert list(dest.glob("*.tmp")) == []  # no stray tmp file either


def test_exception_during_fetch_recorded_failed_no_partial_file(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "boom")

    def exploding_fetch(url, headers=None, timeout=None):
        raise ConnectionError("network exploded")

    stats = cache_thumbnails(conn, dest, fetch=exploding_fetch, fetch_delay=no_delay)

    assert stats["failed"] == 1
    assert stats["downloaded"] == 0
    assert not (dest / "boom.jpg").exists()
    assert list(dest.glob("*.tmp")) == []


def test_failed_download_is_retried_on_next_call_no_marker_file(conn, tmp_path):
    """A failure leaves no marker -- the very next call sees the same row as
    a candidate again and can retry it."""
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "flaky")

    # First call: fetch always fails.
    stats1 = cache_thumbnails(
        conn, dest, fetch=make_fetch(default=FakeResponse(status_code=500)), fetch_delay=no_delay
    )
    assert stats1["failed"] == 1
    assert not (dest / "flaky.jpg").exists()

    # Second call: fetch now succeeds -- same candidate is retried and wins.
    stats2 = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)
    assert stats2 == {"candidates": 1, "downloaded": 1, "skipped_existing": 0, "failed": 0}
    assert (dest / "flaky.jpg").read_bytes() == b"jpeg-bytes"


# ---------------------------------------------------------------------------
# limit caps DOWNLOADS, not the reported candidate count.
# ---------------------------------------------------------------------------


def test_limit_caps_download_attempts_not_candidate_count(conn, tmp_path):
    dest = tmp_path / "thumbs"
    for fk in ("a", "b", "c"):
        _ins_eiendom(conn, fk, image_url=f"https://img/{fk}.jpg")

    calls = []
    stats = cache_thumbnails(
        conn, dest, fetch=make_fetch(calls=calls), fetch_delay=no_delay, limit=1
    )

    assert stats["candidates"] == 3  # full missing-file set, regardless of limit
    assert stats["downloaded"] == 1
    assert stats["failed"] == 0
    assert len(calls) == 1  # only one network fetch actually attempted
    downloaded_files = list(dest.glob("*.jpg"))
    assert len(downloaded_files) == 1


def test_limit_zero_means_no_cap(conn, tmp_path):
    dest = tmp_path / "thumbs"
    for fk in ("a", "b", "c"):
        _ins_eiendom(conn, fk, image_url=f"https://img/{fk}.jpg")

    stats = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay, limit=0)

    assert stats["downloaded"] == 3
    assert stats["candidates"] == 3


# ---------------------------------------------------------------------------
# Fetch discipline: UA + timeout, delay called once per network attempt only.
# ---------------------------------------------------------------------------


def test_fetch_called_with_user_agent_and_timeout(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "111")

    calls = []
    cache_thumbnails(conn, dest, fetch=make_fetch(calls=calls), fetch_delay=no_delay)

    assert len(calls) == 1
    assert calls[0]["timeout"] == 15
    assert "User-Agent" in calls[0]["headers"]


def test_delay_called_once_per_network_attempt_not_per_skipped_row(conn, tmp_path):
    dest = tmp_path / "thumbs"
    dest.mkdir()
    _ins_eiendom(conn, "111")
    _ins_eiendom(conn, "already", image_url="https://img/already.jpg")
    (dest / "already.jpg").write_bytes(b"x")

    delay_calls = []
    cache_thumbnails(
        conn, dest, fetch=make_fetch(), fetch_delay=lambda: delay_calls.append(1)
    )

    assert delay_calls == [1]  # only the one real network attempt paced


def test_default_fetch_delay_sleeps_100ms(conn, tmp_path, monkeypatch):
    import time as time_module

    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "111")

    slept = []
    monkeypatch.setattr(time_module, "sleep", lambda s: slept.append(s))

    cache_thumbnails(conn, dest, fetch=make_fetch())  # fetch_delay left at default

    assert slept == [0.1]


# ---------------------------------------------------------------------------
# Atomic write: tmp+rename, no partial file ever visible under the final name.
# ---------------------------------------------------------------------------


def test_atomic_write_uses_tmp_then_rename(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "111")

    cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    assert (dest / "111.jpg").exists()
    assert list(dest.glob("*.tmp")) == []  # tmp file cleaned up (renamed away)


# ---------------------------------------------------------------------------
# DNB identifier filenames (shared skannonser.ids.dnb_identifier).
# ---------------------------------------------------------------------------


def test_dnb_candidate_uses_shared_synthetic_identifier_as_filename(conn, tmp_path):
    dest = tmp_path / "thumbs"
    url = "https://dnb.no/some-listing"
    _ins_dnb(conn, url)

    stats = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    expected_identifier = dnb_identifier(url)
    assert stats["downloaded"] == 1
    assert (dest / f"{expected_identifier}.jpg").exists()


def test_dnb_without_image_url_column_contributes_zero_candidates(conn, tmp_path):
    """On the REAL schema (never ALTERed in this test), dnbeiendom has no
    image_url column at all -- cache_thumbnails must not raise, and DNB rows
    simply never become candidates."""
    dest = tmp_path / "thumbs"
    conn.execute(
        "INSERT INTO dnbeiendom (url, active) VALUES (?, ?)", ("https://dnb.no/x", 1)
    )
    conn.commit()

    stats = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    assert stats == {"candidates": 0, "downloaded": 0, "skipped_existing": 0, "failed": 0}


def test_inactive_dnb_row_with_image_url_column_not_a_candidate(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_dnb(conn, "https://dnb.no/inactive", active=0)

    stats = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    assert stats == {"candidates": 0, "downloaded": 0, "skipped_existing": 0, "failed": 0}


# ---------------------------------------------------------------------------
# Mixed eie + dnb candidate set in one call.
# ---------------------------------------------------------------------------


def test_eie_and_dnb_candidates_both_downloaded_in_one_call(conn, tmp_path):
    dest = tmp_path / "thumbs"
    _ins_eiendom(conn, "999")
    url = "https://dnb.no/mixed"
    _ins_dnb(conn, url)

    stats = cache_thumbnails(conn, dest, fetch=make_fetch(), fetch_delay=no_delay)

    assert stats["candidates"] == 2
    assert stats["downloaded"] == 2
    assert (dest / "999.jpg").exists()
    assert (dest / f"{dnb_identifier(url)}.jpg").exists()
