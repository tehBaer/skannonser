"""Nightly thumbnail cache (Phase 5 Task 5).

Downloads a local ``{identifier}.jpg`` for every ACTIVE listing that has a
source image but no cached thumbnail yet, so the web app
(``skannonser/web/app.py``'s ``GET /thumbs/{identifier}.jpg``) can serve
images from local disk instead of hot-linking (or, legacy's approach,
pre-uploading to Google Drive -- ``main/tools/predownload_thumbnails_to_drive.py``,
now legacy-only, see ``docs/rebuild/STATUS.md``).

CANDIDATE SET: every ACTIVE ``eiendom`` row with a non-empty ``image_url``
(identifier: its raw ``finnkode``), plus every ACTIVE ``dnbeiendom`` row with
a non-empty ``image_url`` (identifier: the SAME synthetic ``dnb:<hash>`` id
``skannonser.web.api`` uses -- imported from the shared ``skannonser.ids``
module so the two call sites can never drift), MINUS whichever of those
already has a ``{dest_dir}/{identifier}.jpg`` file on disk. A row with an
existing file is not re-downloaded; it also isn't counted as a "candidate"
(see ``skipped_existing`` below) -- it's simply already done.

DNB image_url COLUMN: ``dnbeiendom`` has NO ``image_url`` column today (see
``skannonser.web.api``'s "IMAGE DECISION" docstring) -- the production DB
would raise ``sqlite3.OperationalError`` on an unconditional
``SELECT image_url FROM dnbeiendom``. This module checks for the column's
existence via ``PRAGMA table_info`` first (``_dnb_candidates``), so today it
silently contributes zero DNB candidates (matching reality) rather than
crashing the nightly step, while working automatically the day a migration
adds the column -- no further change needed here.

FETCH DISCIPLINE: mirrors ``skannonser.pipeline``'s DNB per-listing fetch
discipline (``_default_dnb_listing_fetch``) -- a fixed User-Agent and a 15s
timeout on every network fetch. ``fetch_delay`` (default: ``time.sleep(0.1)``,
injectable for tests) runs before every fetch, a light pacing measure since
this hits an external image host once per candidate.

FAILURE HANDLING: a non-200 response or any exception during fetch/write is
recorded in ``stats["failed"]`` and the candidate is simply skipped -- no
failure-marker file is ever written, so a failed download is retried on the
very next call (the missing-file candidate query picks it right back up).

ATOMIC WRITE: every download is written to a ``{identifier}.jpg.tmp`` sibling
first and only ``rename()``d into place after the full body has been
written successfully -- a request that fails partway through (network error,
truncated body) never leaves a partial ``.jpg`` file behind for the web app
to serve.

``limit`` caps the number of DOWNLOAD ATTEMPTS (network fetches) this call
makes -- not the reported ``candidates`` count, which always reflects the
full missing-file set regardless of ``limit`` (so a capped run's stats still
show how much work is left for the next call).
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import requests

from skannonser.ids import dnb_identifier

# Mirrors skannonser/pipeline.py's `_DNB_LISTING_USER_AGENT`/
# `_DNB_LISTING_TIMEOUT` discipline (see that module's docstring) -- applied
# here to every thumbnail image fetch, not just DNB listing pages.
_THUMBS_USER_AGENT = "Mozilla/5.0 (compatible; skannonser-thumbs/1.0)"
_THUMBS_TIMEOUT = 15


def _default_fetch_delay() -> None:
    time.sleep(0.1)


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def _eie_candidates(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """``(finnkode, image_url)`` for every ACTIVE ``eiendom`` row with a
    non-empty ``image_url``."""
    rows = conn.execute(
        "SELECT finnkode, image_url FROM eiendom "
        "WHERE active = 1 AND image_url IS NOT NULL AND TRIM(image_url) != ''"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _dnb_candidates(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """``(dnb:<hash>, image_url)`` for ACTIVE ``dnbeiendom`` rows with a
    non-empty ``image_url`` -- see module docstring "DNB image_url COLUMN":
    guarded so this is a no-op (empty list) on the current schema instead of
    raising."""
    if not _table_has_column(conn, "dnbeiendom", "image_url"):
        return []
    rows = conn.execute(
        "SELECT url, image_url FROM dnbeiendom "
        "WHERE active = 1 AND image_url IS NOT NULL AND TRIM(image_url) != ''"
    ).fetchall()
    return [(dnb_identifier(r[0]), r[1]) for r in rows]


def cache_thumbnails(
    conn: sqlite3.Connection,
    dest_dir: Path,
    fetch=requests.get,
    fetch_delay=None,
    limit: int = 0,
) -> dict:
    """Download a local ``{identifier}.jpg`` for every missing-thumbnail
    candidate (see module docstring for the full candidate/failure/atomic-
    write contract). Returns
    ``{"candidates", "downloaded", "skipped_existing", "failed"}``.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    delay = fetch_delay if fetch_delay is not None else _default_fetch_delay

    rows = _eie_candidates(conn) + _dnb_candidates(conn)

    stats = {"candidates": 0, "downloaded": 0, "skipped_existing": 0, "failed": 0}
    attempted = 0

    for identifier, image_url in rows:
        dest_path = dest_dir / f"{identifier}.jpg"
        if dest_path.exists():
            stats["skipped_existing"] += 1
            continue

        stats["candidates"] += 1
        if limit and attempted >= limit:
            continue
        attempted += 1

        delay()
        tmp_path = dest_dir / f"{identifier}.jpg.tmp"
        try:
            response = fetch(
                image_url,
                headers={"User-Agent": _THUMBS_USER_AGENT},
                timeout=_THUMBS_TIMEOUT,
            )
            if response.status_code != 200:
                stats["failed"] += 1
                continue
            tmp_path.write_bytes(response.content)
            tmp_path.rename(dest_path)
            stats["downloaded"] += 1
        except Exception:  # noqa: BLE001 - recorded, retried next call, never fatal
            stats["failed"] += 1
            tmp_path.unlink(missing_ok=True)

    return stats


__all__ = ["cache_thumbnails"]
