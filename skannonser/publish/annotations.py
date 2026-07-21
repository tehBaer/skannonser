"""One-time/idempotent rescue of the sheet's manually-typed Kommentar/Tag
columns into the `annotations` table (migration 005), keyed by Finnkode.

The legacy sheet has always let a human type free-text comments and a short
tag directly into the "Kommentar"/"Tag" columns of the Eie tab -- there is no
canonical source for that data other than the sheet itself. This module pulls
it into the DB once (and safely again on any re-run) so Phase 5's web UI has
something to build on, without the DB export ever having to depend on those
columns still existing in the sheet.

Header handling is a deliberately narrow port of
`main/sync/helper_sync_to_sheets.py`'s `canonicalize_header_name` /
`HEADER_ALIASES` pattern (lower-cased, stripped lookup into an alias map) --
trimmed down to just the three columns this tool cares about, since the full
alias table there (lat/lng casings, `MVV UNI RUSH` legacy header) is about
the export side, not this rescue tool. `main/sync/sync_comments_from_sheet.py`
is legacy's equivalent of this whole module (same three columns, same
Finnkode-keyed upsert idea) but with a much heavier metadata-column /
conflict-resolution scheme (`Kommentar__edited_at` etc. from an Apps Script)
that Phase 4 does not need -- Phase 5's web UI will own edit provenance
directly in `annotations.updated_at` instead.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from skannonser.publish.sheets_client import SheetsClient

# Case/whitespace-tolerant header lookup, e.g. " finnkode", "KOMMENTAR", "Tag ".
_HEADER_ALIASES: dict[str, str] = {
    "finnkode": "Finnkode",
    "kommentar": "Kommentar",
    "tag": "Tag",
}

_DIGITS = re.compile(r"\d+")


def _canonical_header(name: Any) -> str | None:
    raw = str(name or "").strip()
    if not raw:
        return None
    return _HEADER_ALIASES.get(raw.lower())


def _extract_finnkode(raw: Any) -> str:
    """Return the Finnkode digits for a cell.

    `SheetsClient.read_tab` uses the Sheets API's default valueRenderOption
    (FORMATTED_VALUE), so a `=HYPERLINK(url, "12345678")` cell already comes
    back as its displayed text -- the finnkode digits, not the formula (see
    `skannonser/publish/sheets_client.py::read_tab`). The HYPERLINK unwrap
    below is defensive only (e.g. a future render-option change, or a cell
    someone pasted as literal formula text), mirroring the same fallback in
    legacy's `main/sync/sync_comments_from_sheet.py::_normalize_finnkode` /
    `main/sync/update_rows_in_sheet.py::normalize_finnkode_for_compare`.
    """
    text = str(raw or "").strip()
    if not text:
        return ""
    if "HYPERLINK" in text.upper():
        parts = text.split('"')
        if len(parts) >= 4:
            text = parts[3].strip()
    match = _DIGITS.search(text)
    return match.group(0) if match else text


_UPSERT_SQL = """
INSERT INTO annotations (finnkode, kommentar, tag, imported_at, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(finnkode) DO UPDATE SET
    kommentar = excluded.kommentar,
    tag = excluded.tag,
    imported_at = excluded.imported_at,
    updated_at = excluded.updated_at
WHERE (annotations.updated_at IS NULL OR annotations.updated_at = annotations.imported_at)
  AND (annotations.kommentar IS NOT excluded.kommentar OR annotations.tag IS NOT excluded.tag)
"""


def import_sheet_annotations(
    conn: sqlite3.Connection, client: SheetsClient, tab: str = "Eie"
) -> dict:
    """Read `tab` ONCE (`client.read_tab` -- this never writes the sheet) and
    upsert non-empty Kommentar/Tag values into `annotations`, keyed by
    Finnkode.

    Never-clobber-the-web-UI contract: a row is only (re-)written when BOTH
      1. its incoming kommentar/tag actually differ from what's stored, and
      2. the stored row hasn't been touched by anything other than a prior
         run of this import -- i.e. its `updated_at` is NULL or equal to its
         own `imported_at`.
    Once Phase 5's web UI edits a row it must bump `updated_at` without
    touching `imported_at`; from that point this import will never touch
    that row again. This also makes a re-run over unchanged sheet data a
    true no-op -- no timestamp churn -- since condition 1 alone blocks it.

    Returns counts: rows_read, candidates (non-empty kommentar-or-tag rows
    with a Finnkode), inserted, updated, skipped (candidate rows left alone
    because unchanged or protected by a newer web-UI edit).
    """
    rows = client.read_tab(tab)
    result = {"rows_read": 0, "candidates": 0, "inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return result

    header = [_canonical_header(c) for c in rows[0]]
    if "Finnkode" not in header:
        return result
    finn_idx = header.index("Finnkode")
    kommentar_idx = header.index("Kommentar") if "Kommentar" in header else None
    tag_idx = header.index("Tag") if "Tag" in header else None
    if kommentar_idx is None and tag_idx is None:
        return result

    ts = datetime.now(timezone.utc).isoformat()

    for raw_row in rows[1:]:
        result["rows_read"] += 1
        finn_raw = raw_row[finn_idx] if len(raw_row) > finn_idx else ""
        finnkode = _extract_finnkode(finn_raw)
        if not finnkode:
            continue

        kommentar = ""
        if kommentar_idx is not None and len(raw_row) > kommentar_idx:
            kommentar = str(raw_row[kommentar_idx] or "").strip()
        tag = ""
        if tag_idx is not None and len(raw_row) > tag_idx:
            tag = str(raw_row[tag_idx] or "").strip()

        if not kommentar and not tag:
            continue
        result["candidates"] += 1

        existed = conn.execute(
            "SELECT 1 FROM annotations WHERE finnkode = ?", (finnkode,)
        ).fetchone()

        cur = conn.execute(_UPSERT_SQL, (finnkode, kommentar, tag, ts, ts))
        if cur.rowcount:
            if existed is None:
                result["inserted"] += 1
            else:
                result["updated"] += 1
        else:
            result["skipped"] += 1

    conn.commit()
    return result
