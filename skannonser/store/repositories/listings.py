"""Listings repository: batched upsert + inactive lifecycle for the ``eiendom`` table.

Ported from ``main/database/db.py`` (``_is_excluded_eiendom_url``,
``insert_or_update_eiendom``, ``mark_inactive``) and
``main/database/overrides.py`` (``apply_overrides_to_data``). Semantics are
preserved verbatim, with two sanctioned modernizations over the legacy code:

  * the whole upsert batch runs in ONE transaction (legacy committed per row);
  * pure ``sqlite3`` + dicts, no pandas.

The input contract is :class:`skannonser.ingest.base.NormalizedListing`, whose
field names are exactly the legacy A_live extractor keys. Two DB columns the
legacy write also touched — ``image_hosted_url`` and ``pris_kvm`` — are NOT part
of that extractor contract (they are populated by separate downstream pipeline
stages), so this repository does not write them.
"""

import sqlite3

from skannonser.ingest.base import NormalizedListing

# A_live extractor key -> eiendom column, reproduced from
# ``insert_or_update_eiendom``'s ``data`` dict (db.py:423-444). ``Byggeår`` maps
# to ``info_construction_year`` even though the legacy ``column_mapping`` dict
# omitted it — the ``data`` dict includes it, so we do too.
_TEXT_COLUMNS: dict[str, str] = {
    "Tilgjengelighet": "tilgjengelighet",
    "Adresse": "adresse",
    "Postnummer": "postnummer",
    "IMAGE_URL": "image_url",
    "Eierskap, tomt": "info_plot_ownership",
    "Boligtype": "info_property_type",
}
_INT_COLUMNS: dict[str, str] = {
    "Pris": "pris",
    "Bruksareal": "info_usable_area",
    "Internt bruksareal (BRA-i)": "info_usable_i_area",
    "Primærrom": "info_primary_area",
    "Bruttoareal": "info_gross_area",
    "Eksternt bruksareal (BRA-e)": "info_usable_e_area",
    "Innglasset balkong (BRA-b)": "info_usable_b_area",
    "Balkong/Terrasse (TBA)": "info_open_area",
    "Tomteareal": "info_plot_area",
    "Byggeår": "info_construction_year",
}

# Columns written on insert/update, excluding the ``finnkode`` key (handled
# explicitly). ``url`` IS included here. Order is stable for deterministic SQL.
_DATA_COLUMNS: list[str] = (
    ["tilgjengelighet", "adresse", "postnummer", "pris", "url", "image_url"]
    + [
        "info_usable_area",
        "info_usable_i_area",
        "info_primary_area",
        "info_gross_area",
        "info_usable_e_area",
        "info_usable_b_area",
        "info_open_area",
        "info_plot_area",
        "info_plot_ownership",
        "info_property_type",
        "info_construction_year",
    ]
)


def _is_excluded_eiendom_url(url: str) -> bool:
    """FINN URL patterns we do not persist in ``eiendom`` (db.py:368-375)."""
    u = (url or "").strip().lower()
    return (
        "/realestate/projectsingle/" in u
        or "/realestate/newbuildings/" in u
        or "/realestate/planned/" in u
    )


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


class ListingsRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        # Own the transaction boundaries explicitly (autocommit off between our
        # own BEGIN/COMMIT). This makes the single-batch guarantee robust.
        self.conn.isolation_level = None

    # -- helpers ---------------------------------------------------------

    def _build_data(self, finnkode: str, url: str, row: dict) -> dict:
        data: dict[str, object] = {"finnkode": finnkode, "url": url}
        for key, col in _TEXT_COLUMNS.items():
            # Legacy's ``row.get(key, "")`` only defaulted to "" when the key
            # was ABSENT; a present-but-empty cell (NaN) was written as NULL.
            # ``NormalizedListing.to_row()`` always carries all keys, so the
            # faithful port preserves None as NULL rather than coercing to "".
            data[col] = row.get(key)
        for key, col in _INT_COLUMNS.items():
            data[col] = _to_int(row.get(key))
        return data

    def _apply_overrides(self, finnkode: str, data: dict) -> dict:
        """Port of ``overrides.apply_overrides_to_data`` (overrides.py:163-183).

        The override tuple is ``(pris, adresse, postnummer, reason)``.
        """
        row = self.conn.execute(
            "SELECT pris, adresse, postnummer FROM manual_overrides WHERE finnkode = ?",
            (finnkode,),
        ).fetchone()
        if row is not None:
            if row["pris"] is not None:
                data["pris"] = row["pris"]
            if row["adresse"]:
                data["adresse"] = row["adresse"]
            if row["postnummer"]:
                data["postnummer"] = row["postnummer"]
        return data

    # -- public API ------------------------------------------------------

    def upsert(self, listings: list[NormalizedListing]) -> dict:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            inserted = updated = excluded = 0
            for listing in listings:
                r = listing.to_row()
                finnkode = str(r.get("Finnkode", "") or "").strip()
                if not finnkode:
                    continue
                url = str(r.get("URL", "") or "")
                if _is_excluded_eiendom_url(url):
                    excluded += 1
                    continue

                data = self._build_data(finnkode, url, r)
                data = self._apply_overrides(finnkode, data)

                existing = conn.execute(
                    "SELECT * FROM eiendom WHERE finnkode = ?", (finnkode,)
                ).fetchone()

                if existing is None:
                    # Legacy's INSERT never sets ``active`` — the schema default
                    # (``active BOOLEAN DEFAULT 0``) applies, so a new finnkode
                    # only becomes active on its SECOND appearance (see the
                    # UPDATE branch below, which does set active=1). This is a
                    # deliberate legacy quirk, not an oversight — do not "fix"
                    # it here without a controller ruling.
                    cols = ["finnkode"] + _DATA_COLUMNS
                    placeholders = ", ".join("?" * len(cols))
                    params = [data["finnkode"]] + [data[c] for c in _DATA_COLUMNS]
                    conn.execute(
                        f"INSERT INTO eiendom ({', '.join(cols)}) VALUES ({placeholders})",
                        params,
                    )
                    inserted += 1
                else:
                    # Update only columns whose value actually changed; always
                    # reactivate on re-appearance (legacy set active=1 on update).
                    changed = [c for c in _DATA_COLUMNS if existing[c] != data[c]]
                    reactivate = not existing["active"]
                    if changed or reactivate:
                        set_cols = changed + ["active"]
                        assignments = ", ".join(f"{c} = ?" for c in set_cols)
                        params = [data[c] for c in changed] + [1, finnkode]
                        conn.execute(
                            f"UPDATE eiendom SET {assignments}, "
                            f"updated_at = CURRENT_TIMESTAMP WHERE finnkode = ?",
                            params,
                        )
                        updated += 1
        except Exception:
            conn.rollback()
            raise
        conn.commit()
        return {"inserted": inserted, "updated": updated, "excluded": excluded}

    def mark_inactive(self, active_finnkodes: list[str]) -> int:
        """Deactivate rows whose finnkode is absent from ``active_finnkodes``.

        Never deletes; returns the number of rows deactivated. Ported from
        ``db.py:mark_inactive`` (541-565), scoped to the ``eiendom`` table.
        """
        conn = self.conn
        if active_finnkodes:
            placeholders = ",".join("?" * len(active_finnkodes))
            cur = conn.execute(
                f"UPDATE eiendom SET active = 0, updated_at = CURRENT_TIMESTAMP "
                f"WHERE finnkode NOT IN ({placeholders}) AND active = 1",
                active_finnkodes,
            )
        else:
            cur = conn.execute(
                "UPDATE eiendom SET active = 0, updated_at = CURRENT_TIMESTAMP "
                "WHERE active = 1"
            )
        return cur.rowcount

    def active_finnkodes(self) -> set[str]:
        rows = self.conn.execute(
            "SELECT finnkode FROM eiendom WHERE active = 1"
        ).fetchall()
        return {row["finnkode"] for row in rows}

    def update_status(self, finnkode: str, new_status: str) -> None:
        """Update ``tilgjengelighet`` (status) for a listing.

        Port of ``db.py:update_eiendom_status`` (616-638). Note: ``active``
        is managed by the upsert/mark_inactive lifecycle, not by status
        refresh -- this never touches it.
        """
        self.conn.execute(
            "UPDATE eiendom SET tilgjengelighet = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE finnkode = ?",
            (new_status, finnkode),
        )

    def record_status_change_if_changed(self, finnkode: str, old_status, new_status) -> bool:
        """Append a row to ``eiendom_status_history`` when the status
        actually changed.

        Port of ``db.py:record_status_change_if_changed`` (640-660).
        Statuses are compared after stripping whitespace. Returns True when
        a history row was written, False when the status was unchanged.
        """
        old_norm = str(old_status or "").strip()
        new_norm = str(new_status or "").strip()
        if old_norm == new_norm:
            return False
        self.conn.execute(
            "INSERT INTO eiendom_status_history (finnkode, old_status, new_status) "
            "VALUES (?, ?, ?)",
            (str(finnkode).strip(), old_norm, new_norm),
        )
        return True
