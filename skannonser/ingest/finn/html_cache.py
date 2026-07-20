"""Ad HTML cache: canonical per-uid file + gzipped dated snapshots.

Port of `main/extractors/ad_html_loader.py` (lines 19-113): `_atomic_write`,
`save_ad_html`, `download_and_save_ad_html`, `load_or_fetch_ad_html`.

Path layout is unchanged from legacy so the ~7,731 existing cached files
remain readable as-is:

- canonical: `{project_dir}/html_extracted/{uid}.html`
- snapshot:  `{project_dir}/html_snapshots/{uid}.{YYYYMMDD}.html.gz`
  (written only when the canonical content actually changes; an unchanged
  re-save produces no snapshot)

Two behavioral simplifications versus legacy, both driven by the brief's
signatures:

- `load_or_fetch(url, project_dir, uid, fetch=requests.get)` takes `uid`
  explicitly instead of parsing it out of `url` via regex -- callers (e.g.
  the new crawler) already have the finnkode from `extract_ad_urls`, so the
  legacy `isNAV`-branching regex parse is dead weight here. `isNAV` itself
  is dropped: it was only ever passed `True` by the archived NAV job
  extractor (`main/extractors/archived/extraction_jobs_NAV.py`), never by
  the eiendom flow (`main/extractors/extraction_eiendom.py`), which is what
  this port serves.
- `load_or_fetch` returns the HTML string directly (legacy's
  `load_or_fetch_ad_html` returned a parsed `BeautifulSoup`); callers parse
  as needed.

Sanctioned scope extension (Task 14, ledgered): the refresh flow
(`skannonser/ingest/finn/refresh.py`) needs to force a re-download of
already-cached ads to detect status changes -- legacy's
`load_or_fetch_ad_html(..., force_save=True)`. `load_or_fetch` grew a
`force: bool = False` parameter for this: `force=True` skips the cache-read
and always fetches + saves, mirroring legacy's `force_save` branch exactly.
"""

import gzip
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import requests
from bs4 import BeautifulSoup


def _atomic_write(path: Path, data, *, binary: bool = False) -> None:
    """Write ``data`` to ``path`` atomically.

    Writes to a temp file in the same directory then ``os.replace``s it into
    place, so a failed/partial write can never truncate an existing good file.
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    mode = "wb" if binary else "w"
    open_kwargs = {} if binary else {"encoding": "utf-8"}
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, mode, **open_kwargs) as handle:
            handle.write(data)
        os.replace(tmp_path, path)
    except BaseException:
        # Leave any existing file untouched; discard the temp.
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def save_ad_html(
    project_dir: Path,
    uid: str,
    html: str,
    snapshot_dir: Path | None = None,
    today: str | None = None,
) -> Path:
    """Persist ad HTML for ``uid``.

    The canonical copy ``{project_dir}/html_extracted/{uid}.html`` is written
    atomically. When the content differs from the previous canonical (or none
    existed yet), a gzipped, date-stamped snapshot is also archived under
    ``{project_dir}/html_snapshots/{uid}.{YYYYMMDD}.html.gz`` so prior
    versions are never overwritten. Unchanged re-downloads produce no
    snapshot.

    Returns the canonical file path.
    """
    project_dir = Path(project_dir)
    canonical_path = project_dir / "html_extracted" / f"{uid}.html"

    previous = None
    if canonical_path.exists():
        previous = canonical_path.read_text(encoding="utf-8")
    changed = previous is None or previous != html

    _atomic_write(canonical_path, html)

    if changed:
        if snapshot_dir is None:
            snapshot_dir = project_dir / "html_snapshots"
        day = today or datetime.now().strftime("%Y%m%d")
        snapshot_path = Path(snapshot_dir) / f"{uid}.{day}.html.gz"
        _atomic_write(snapshot_path, gzip.compress(html.encode("utf-8")), binary=True)

    return canonical_path


def load_or_fetch(
    url: str,
    project_dir: Path,
    uid: str,
    fetch=requests.get,
    fetch_delay: Callable[[], None] | None = None,
    force: bool = False,
) -> str:
    """Return cached HTML for ``uid`` if present, else fetch, cache, and
    return it.

    Direct port of `download_and_save_ad_html` + the cache-hit branch of
    `load_or_fetch_ad_html`: a fetched response is re-serialized through
    BeautifulSoup (`str(soup)`) before being saved/compared, matching legacy
    exactly (so change-detection against the ~7,731 existing canonical files,
    which were themselves saved this way, is apples-to-apples).

    When `fetch_delay` is None, sleeps 0.1s before the network fetch to
    rate-limit ad page fetches (legacy behavior). A cache hit does not sleep.

    `force=True` mirrors legacy's `force_save` path
    (`main/extractors/ad_html_loader.py:101-104`, used by the status-refresh
    flow): the cache-read is skipped entirely, so a fetch+save always
    happens even for a `uid` that already has a canonical file on disk.
    `save_ad_html`'s own change-detection still governs whether a new dated
    snapshot is written -- an unchanged re-fetch produces no snapshot even
    under `force=True`.
    """
    project_dir = Path(project_dir)
    canonical_path = project_dir / "html_extracted" / f"{uid}.html"
    if not force and canonical_path.exists():
        return canonical_path.read_text(encoding="utf-8")

    # Apply fetch delay before network request (legacy behavior: 0.1s per fetch)
    if fetch_delay is not None:
        fetch_delay()
    else:
        time.sleep(0.1)

    response = fetch(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    html = str(soup)
    save_ad_html(project_dir, uid, html)
    return html
