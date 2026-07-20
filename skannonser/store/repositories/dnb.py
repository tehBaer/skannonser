"""DNB Eiendom repository: batched upsert + inactive lifecycle for the
``dnbeiendom`` table.

Ported from ``main/database/db.py`` (``insert_or_update_dnbeiendom``,
db.py:1548-1625) and the stale-deactivation block of
``main/extractors/filter_and_load_dnbeiendom_no_buffer.main`` (lines
113-141). Semantics are preserved verbatim, with the same two sanctioned
modernizations as ``ListingsRepo`` (see
``skannonser/store/repositories/listings.py``):

  * the whole upsert batch runs in ONE transaction (legacy committed the
    whole DataFrame loop at once, with no rollback boundary of its own);
  * pure ``sqlite3`` + dicts, no pandas.

Legacy quirk preserved deliberately: the LIVE ``dnbeiendom.active`` column
(``skannonser/store/migrations/001_adopt_live_schema.sql``) has NO schema
default -- unlike ``main/database/db.py``'s own in-process
``CREATE TABLE ... active BOOLEAN DEFAULT 1`` (a `CREATE TABLE IF NOT EXISTS`
that never actually runs against the already-existing live table, so its
``DEFAULT 1`` is aspirational, not what's live). On the real schema this
port targets, a fresh INSERT that never mentions ``active`` leaves it NULL
(falsy), and only the UPDATE branch -- hit on a row's SECOND appearance --
hard-sets ``active = 1``. This mirrors ``eiendom``'s "activate on second
appearance" quirk, driven by the live schema rather than by application
code. Do not "fix" this without a controller ruling.

The ``stale`` column that still lingers on the live ``dnbeiendom`` table is
never read or written anywhere in legacy's DNB code paths (confirmed by
exhaustive grep) -- this port writes nothing to it either.

There is no ``image_url`` column on ``dnbeiendom`` at all, so the
``IMAGE_URL`` key on input rows (which can hold a raw JSON-LD ``ImageObject``
dict rather than a string -- see ``skannonser.ingest.dnb.parse``'s
docstring) is simply never read here, exactly as legacy never read it in
``insert_or_update_dnbeiendom``.

Input rows are dicts shaped like ``skannonser.ingest.dnb.parse.parse_listing``'s
output, normally pre-annotated with a ``duplicate_of_finnkode`` key by
``skannonser.ingest.dnb.load.filter_and_match``. Legacy read the duplicate
finnkode from ``MatchedFinn_Finnkode`` first, falling back to
``duplicate_of_finnkode``; both are accepted here for the same reason.
"""

import sqlite3


def _to_int(value) -> int | None:
    """Pandas-free port of ``db.py:_to_int``: None/NaN/non-numeric -> None."""
    if value is None:
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    if f != f:  # NaN
        return None
    return int(round(f))


def _to_float(value) -> float | None:
    """Port of the module-level ``db.py:_to_float_or_none``."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_postnummer(value) -> str:
    """Port of the inline postnummer block (db.py:1564-1572): zero-pad to 4
    digits, preserving leading zeros; fall back to a bare stripped string
    when the value isn't numeric-coercible."""
    if value is None:
        return ""
    try:
        f = float(value)
    except (ValueError, TypeError):
        return str(value).strip()
    if f != f:  # NaN
        return ""
    try:
        return str(int(f)).zfill(4)
    except (ValueError, TypeError):
        return str(value).strip()


class DnbRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        # Own the transaction boundaries explicitly, matching ListingsRepo.
        self.conn.isolation_level = None

    # -- helpers ---------------------------------------------------------

    def _build_data(self, row: dict) -> dict:
        url = str(row.get("URL") or row.get("url") or "").strip()
        dnb_id = str(row.get("Id") or row.get("dnb_id") or "").strip()
        adresse = row.get("Adresse") or row.get("adresse") or row.get("StreetAddress") or ""
        postnummer = _to_postnummer(
            row.get("Postnummer") or row.get("postnummer") or row.get("PostalCode")
        )
        pris = _to_int(row.get("Pris") or row.get("pris") or row.get("Price"))
        lat = _to_float(row.get("LAT") or row.get("lat") or row.get("Latitude"))
        lng = _to_float(row.get("LNG") or row.get("lng") or row.get("Longitude"))
        dup_raw = row.get("MatchedFinn_Finnkode")
        if dup_raw is None:
            dup_raw = row.get("duplicate_of_finnkode")
        duplicate = str(dup_raw).strip() if dup_raw else ""
        prop_raw = (
            row.get("PropertyType") or row.get("property_type") or row.get("Boligtype") or ""
        )
        property_type = str(prop_raw).strip() if prop_raw else ""
        return {
            "url": url,
            "dnb_id": dnb_id,
            "adresse": adresse,
            "postnummer": postnummer,
            "pris": pris,
            "lat": lat,
            "lng": lng,
            "duplicate_of_finnkode": duplicate,
            "property_type": property_type,
        }

    # -- public API ------------------------------------------------------

    def upsert(self, rows: list[dict]) -> dict:
        """Insert or update ``dnbeiendom`` rows, matched by ``url`` first and
        ``dnb_id`` as fallback. Direct port of
        ``insert_or_update_dnbeiendom`` (db.py:1548-1625): every existing-row
        match is counted as "updated" regardless of whether any field
        actually changed (legacy did no change-detection here, unlike
        ``ListingsRepo``)."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            inserted = updated = 0
            for row in rows:
                data = self._build_data(row)
                url = data["url"]
                dnb_id = data["dnb_id"]
                if not url and not dnb_id:
                    # Legacy skips rows without an identifier.
                    continue

                existing = None
                if url:
                    existing = conn.execute(
                        "SELECT id FROM dnbeiendom WHERE url = ?", (url,)
                    ).fetchone()
                if existing is None and dnb_id:
                    existing = conn.execute(
                        "SELECT id FROM dnbeiendom WHERE dnb_id = ?", (dnb_id,)
                    ).fetchone()

                if existing is not None:
                    conn.execute(
                        """
                        UPDATE dnbeiendom
                        SET dnb_id = COALESCE(?, dnb_id), adresse = ?, postnummer = ?, pris = ?,
                            lat = COALESCE(?, lat), lng = COALESCE(?, lng),
                            duplicate_of_finnkode = COALESCE(?, duplicate_of_finnkode),
                            property_type = COALESCE(?, property_type),
                            active = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            dnb_id or None,
                            data["adresse"],
                            data["postnummer"],
                            data["pris"],
                            data["lat"],
                            data["lng"],
                            data["duplicate_of_finnkode"] or None,
                            data["property_type"] or None,
                            existing["id"],
                        ),
                    )
                    updated += 1
                else:
                    conn.execute(
                        """
                        INSERT INTO dnbeiendom
                            (dnb_id, url, adresse, postnummer, pris, lat, lng,
                             duplicate_of_finnkode, property_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            dnb_id or None,
                            url or None,
                            data["adresse"],
                            data["postnummer"],
                            data["pris"],
                            data["lat"],
                            data["lng"],
                            data["duplicate_of_finnkode"] or None,
                            data["property_type"] or None,
                        ),
                    )
                    inserted += 1
        except Exception:
            conn.rollback()
            raise
        conn.commit()
        return {"inserted": inserted, "updated": updated}

    def deactivate_missing(self, active_urls: list[str]) -> int:
        """Deactivate ``dnbeiendom`` rows whose (normalized) url is absent
        from ``active_urls``. Never deletes; returns the number of rows
        deactivated.

        Ported from the stale-deactivation block in
        ``filter_and_load_dnbeiendom_no_buffer.main`` (lines 113-138): URLs
        are compared after ``.strip().lower().rstrip('/')`` normalization on
        both sides, only currently-active (``active = 1``) rows are
        candidates, and -- exactly as legacy's ``if r[1] and ...`` guard --
        rows with a NULL/empty url are never deactivated. An empty
        ``active_urls`` deactivates every active row that has a url (legacy
        never receives an empty live-url set in production -- it skips the
        whole block when the CSV is missing -- but an empty set fed to its
        listcomp would deactivate everything with a url, which is what this
        does).
        """
        conn = self.conn
        normalized_active = {u.strip().lower().rstrip("/") for u in active_urls if u}
        rows = conn.execute("SELECT id, url FROM dnbeiendom WHERE active = 1").fetchall()
        to_deactivate = [
            r["id"]
            for r in rows
            if r["url"] and r["url"].strip().lower().rstrip("/") not in normalized_active
        ]
        if not to_deactivate:
            return 0
        placeholders = ",".join("?" * len(to_deactivate))
        conn.execute(
            f"UPDATE dnbeiendom SET active = 0, updated_at = CURRENT_TIMESTAMP "
            f"WHERE id IN ({placeholders})",
            to_deactivate,
        )
        return len(to_deactivate)
