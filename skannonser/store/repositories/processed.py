"""Processed-location repository: ``eiendom_processed`` upsert/read semantics.

Ported from ``main/database/db.py``:

  * ``insert_or_update_eiendom_processed`` (1285-1343) -> ``ProcessedRepo.upsert``.
    THE critical semantics, preserved verbatim: on UPDATE, ``adresse_cleaned``
    is set unconditionally (recomputed from whatever ``adresse`` was passed
    this call -- a later call with ``adresse=None`` DOES clobber a previously
    cleaned value, matching legacy); ``lat``/``lng`` and the three travel
    columns (``pendl_rush_brj``, ``pendl_rush_mvv``, ``pendl_rush_mvv_uni_rush``)
    are ``COALESCE(?, existing)`` -- fill-only, a non-null existing value
    survives a ``None`` write; the four CNTR columns, ``travel_copy_from_finnkode``
    and ``google_maps_url`` are set unconditionally (a later ``None`` DOES
    overwrite a previous non-null value -- e.g. clearing a donor pointer). On
    INSERT every column is written as given.
  * ``_normalize_coordinates`` (58-73) -> ``normalize_coordinates``.
  * ``_clean_address`` (1463-1490) -> ``clean_address``.
  * ``_generate_google_maps_url`` (1273-1283) -> ``google_maps_url``.
  * ``get_travel_donor_seed`` (1361-1383) -> ``ProcessedRepo.donor_seed``.
  * ``get_eiendom_missing_coordinates`` (1172-1197), combined with the
    visibility filter from ``main/tools/fill_missing_coordinates.py:230-245``
    -> ``ProcessedRepo.missing_coordinates``. Unlike the legacy CLI tool
    (where ``--include-inactive`` was a dead flag -- the base query always
    hard-filtered ``active = 1`` and excluded solgt/inaktiv, so the wrapper's
    extra filter never had anything left to do), this port makes the
    active/solgt/inaktiv filter genuinely conditional on ``include_inactive``.
  * ``set_eiendom_coordinates`` / ``mark_eiendom_geocode_failed`` /
    ``clear_eiendom_geocode_failed`` (1199-1271) -> ``set_coordinates`` /
    ``mark_geocode_failed`` / ``clear_geocode_failed``.
  * The donor CASE/COALESCE pattern from ``get_eiendom_for_sheets`` (829-852)
    -> ``ProcessedRepo.sheet_travel_values``: the READ-TIME resolution used
    when a listing's ``travel_copy_from_finnkode`` points at a donor row --
    the donor's value wins when the link is set AND the donor's value is
    non-null, otherwise the listing's own value is used. Single hop only
    (no chained donors). This is what ``verify enrich`` compares.

No pandas -- pure ``sqlite3`` + dicts, matching ``ListingsRepo``/``DnbRepo``.
"""

import sqlite3
from typing import Any, Optional

# Coordinate bounds for Norway. Mirrors ``main/config/filters.py``
# (``COORD_LAT_MIN/MAX``, ``COORD_LNG_MIN/MAX``) -- also the exact fallback
# values ``db.py:_get_coord_bounds`` uses when that config import fails, so a
# single hardcoded constant here is faithful either way.
_LAT_MIN, _LAT_MAX = 57.0, 72.0
_LNG_MIN, _LNG_MAX = 4.0, 32.0


def _to_float_or_none(value: Any) -> Optional[float]:
    """Port of the module-level ``db.py:_to_float_or_none`` (13-19)."""
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _is_na(value: Any) -> bool:
    """Pandas-free stand-in for ``pd.isna`` on scalars: ``None`` or float NaN."""
    if value is None:
        return True
    try:
        return value != value  # True only for NaN
    except Exception:
        return False


def _is_in_bounds(lat: float, lng: float) -> bool:
    return _LAT_MIN <= lat <= _LAT_MAX and _LNG_MIN <= lng <= _LNG_MAX


def normalize_coordinates(lat: Any, lng: Any) -> tuple[Optional[float], Optional[float], bool]:
    """Return ``(lat, lng, swapped)`` when valid; otherwise ``(None, None, False)``.

    Port of ``db.py:_normalize_coordinates`` (58-73): out-of-bounds
    coordinates are checked again with lat/lng swapped (the common
    data-entry mistake) before being given up on entirely.
    """
    lat_v = _to_float_or_none(lat)
    lng_v = _to_float_or_none(lng)
    if lat_v is None or lng_v is None:
        return None, None, False

    if _is_in_bounds(lat_v, lng_v):
        return lat_v, lng_v, False

    if _is_in_bounds(lng_v, lat_v):
        return lng_v, lat_v, True

    return None, None, False


def clean_address(address: Any) -> Any:
    """Strip a trailing free-text suffix off a street address.

    Port of ``db.py:_clean_address`` (1463-1490). Splits on the first of
    ``' - '``, ``' ('``, ``' ['``, ``' /'`` (checked in that order) and keeps
    the part before it. Falsy/NaN input is returned unchanged (not coerced).

    Examples:
        'Brynsveien 146 - Prosjekt' -> 'Brynsveien 146'
        'Jarenlia 107 (Bolignr. J-02)' -> 'Jarenlia 107'
    """
    if not address or _is_na(address):
        return address

    address = str(address).strip()
    for delimiter in (" - ", " (", " [", " /"):
        if delimiter in address:
            address = address.split(delimiter)[0].strip()
    return address


def google_maps_url(adresse: Any, postnummer: Any) -> str:
    """Port of ``db.py:_generate_google_maps_url`` (1273-1283).

    ``""`` when either ``adresse`` or ``postnummer`` is missing/NaN;
    otherwise ``https://www.google.com/maps/place/{adresse}+{postnummer}``
    with spaces replaced by ``+``.
    """
    if _is_na(adresse) or _is_na(postnummer):
        return ""

    adresse_str = str(adresse).strip()
    postnummer_str = str(postnummer).strip()
    search_query = f"{adresse_str}+{postnummer_str}".replace(" ", "+")
    return f"https://www.google.com/maps/place/{search_query}"


class ProcessedRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- write side --------------------------------------------------------

    def upsert(
        self,
        finnkode: str,
        adresse: Any,
        postnummer: Any,
        lat: Any = None,
        lng: Any = None,
        travel: Optional[dict] = None,
        cntr: Optional[dict] = None,
        travel_copy_from_finnkode: Any = None,
    ) -> None:
        """Insert or update processed location data for a property.

        Port of ``db.py:insert_or_update_eiendom_processed`` (1285-1343).
        See the module docstring for the exact fill-only-vs-unconditional
        column semantics.
        """
        conn = self.conn
        travel = travel or {}
        cntr = cntr or {}

        lat_norm, lng_norm, _swapped = normalize_coordinates(lat, lng)

        adresse_cleaned = clean_address(adresse)
        maps_url = google_maps_url(adresse_cleaned, postnummer)

        pendl_rush_brj = travel.get("pendl_rush_brj")
        pendl_rush_mvv = travel.get("pendl_rush_mvv")
        pendl_rush_mvv_uni_rush = travel.get("pendl_rush_mvv_uni_rush")

        pendl_morn_cntr = cntr.get("pendl_morn_cntr")
        bil_morn_cntr = cntr.get("bil_morn_cntr")
        pendl_dag_cntr = cntr.get("pendl_dag_cntr")
        bil_dag_cntr = cntr.get("bil_dag_cntr")

        existing = conn.execute(
            "SELECT id FROM eiendom_processed WHERE finnkode = ?", (finnkode,)
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE eiendom_processed
                SET adresse_cleaned = ?,
                    lat = COALESCE(?, lat),
                    lng = COALESCE(?, lng),
                    pendl_rush_brj = COALESCE(?, pendl_rush_brj),
                    pendl_rush_mvv = COALESCE(?, pendl_rush_mvv),
                    pendl_rush_mvv_uni_rush = COALESCE(?, pendl_rush_mvv_uni_rush),
                    pendl_morn_cntr = ?, bil_morn_cntr = ?,
                    pendl_dag_cntr = ?, bil_dag_cntr = ?,
                    travel_copy_from_finnkode = ?,
                    google_maps_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
                """,
                (
                    adresse_cleaned,
                    lat_norm,
                    lng_norm,
                    pendl_rush_brj,
                    pendl_rush_mvv,
                    pendl_rush_mvv_uni_rush,
                    pendl_morn_cntr,
                    bil_morn_cntr,
                    pendl_dag_cntr,
                    bil_dag_cntr,
                    travel_copy_from_finnkode,
                    maps_url,
                    finnkode,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO eiendom_processed
                (finnkode, adresse_cleaned, lat, lng,
                 pendl_rush_brj, pendl_rush_mvv, pendl_rush_mvv_uni_rush,
                 pendl_morn_cntr, bil_morn_cntr, pendl_dag_cntr, bil_dag_cntr,
                 travel_copy_from_finnkode, google_maps_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    finnkode,
                    adresse_cleaned,
                    lat_norm,
                    lng_norm,
                    pendl_rush_brj,
                    pendl_rush_mvv,
                    pendl_rush_mvv_uni_rush,
                    pendl_morn_cntr,
                    bil_morn_cntr,
                    pendl_dag_cntr,
                    bil_dag_cntr,
                    travel_copy_from_finnkode,
                    maps_url,
                ),
            )
        conn.commit()

    def set_coordinates(self, finnkode: str, lat: Any, lng: Any) -> bool:
        """Set lat/lng for a listing, creating the row if needed.

        Port of ``db.py:set_eiendom_coordinates`` (1199-1238). Invalid
        coordinates (unparseable, or out of bounds even after the swap
        correction) are rejected -- no row is written, returns ``False``.
        A successful write also clears ``geocode_failed``.
        """
        if not finnkode:
            return False

        lat_norm, lng_norm, _swapped = normalize_coordinates(lat, lng)
        if lat_norm is None or lng_norm is None:
            return False

        conn = self.conn
        existing = conn.execute(
            "SELECT id FROM eiendom_processed WHERE finnkode = ?", (str(finnkode),)
        ).fetchone()

        if existing:
            cur = conn.execute(
                """
                UPDATE eiendom_processed
                SET lat = ?, lng = ?, geocode_failed = 0, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
                """,
                (lat_norm, lng_norm, str(finnkode)),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO eiendom_processed (finnkode, lat, lng, geocode_failed)
                VALUES (?, ?, ?, 0)
                """,
                (str(finnkode), lat_norm, lng_norm),
            )

        changed = cur.rowcount > 0
        conn.commit()
        return changed

    def mark_geocode_failed(self, finnkode: str) -> None:
        """Port of ``db.py:mark_eiendom_geocode_failed`` (1240-1258)."""
        if not finnkode:
            return
        conn = self.conn
        existing = conn.execute(
            "SELECT id FROM eiendom_processed WHERE finnkode = ?", (str(finnkode),)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE eiendom_processed SET geocode_failed = 1, "
                "updated_at = CURRENT_TIMESTAMP WHERE finnkode = ?",
                (str(finnkode),),
            )
        else:
            conn.execute(
                "INSERT INTO eiendom_processed (finnkode, geocode_failed) VALUES (?, 1)",
                (str(finnkode),),
            )
        conn.commit()

    def clear_geocode_failed(self, finnkode: str) -> None:
        """Port of ``db.py:clear_eiendom_geocode_failed`` (1260-1271)."""
        if not finnkode:
            return
        self.conn.execute(
            "UPDATE eiendom_processed SET geocode_failed = 0, "
            "updated_at = CURRENT_TIMESTAMP WHERE finnkode = ?",
            (str(finnkode),),
        )
        self.conn.commit()

    # -- read side -----------------------------------------------------

    def donor_seed(self) -> list[dict]:
        """Travel donor seed rows, for cross-source reuse.

        Port of ``db.py:get_travel_donor_seed`` (1361-1383). Intentionally
        sourced from ``eiendom_processed`` alone (no join to ``eiendom``) so
        synthetic finnkoder can also participate as donors.
        """
        rows = self.conn.execute(
            """
            SELECT
                finnkode as "Finnkode",
                lat as "LAT",
                lng as "LNG",
                pendl_rush_brj as "PENDL RUSH BRJ",
                pendl_rush_mvv as "PENDL RUSH MVV",
                pendl_rush_mvv_uni_rush as "MVV UNI RUSH",
                travel_copy_from_finnkode as "TRAVEL_COPY_FROM_FINNKODE"
            FROM eiendom_processed
            WHERE finnkode IS NOT NULL AND TRIM(finnkode) != ''
            ORDER BY updated_at DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def missing_coordinates(self, include_inactive: bool = False) -> list[dict]:
        """Listings missing lat/lng, excluding permanent geocode failures.

        Port of ``db.py:get_eiendom_missing_coordinates`` (1172-1197) merged
        with the visibility filter from
        ``main/tools/fill_missing_coordinates.py:230-245``: by default
        (``include_inactive=False``) only active, non-solgt/inaktiv listings
        are candidates; passing ``include_inactive=True`` also surfaces
        inactive and solgt/inaktiv listings.
        """
        query = """
            SELECT
                e.finnkode as "Finnkode",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.url as "URL",
                e.active as "active",
                e.tilgjengelighet as "Tilgjengelighet",
                ep.lat as "LAT",
                ep.lng as "LNG"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            WHERE (ep.lat IS NULL OR ep.lng IS NULL)
              AND (ep.geocode_failed IS NULL OR ep.geocode_failed = 0)
        """
        if not include_inactive:
            query += """
              AND e.active = 1
              AND (e.tilgjengelighet IS NULL OR LOWER(e.tilgjengelighet) NOT IN ('solgt', 'inaktiv'))
            """
        query += " ORDER BY e.scraped_at DESC"

        rows = self.conn.execute(query).fetchall()
        return [dict(r) for r in rows]

    def sheet_travel_values(self, finnkode: str) -> dict:
        """Read-time donor resolution for the three rush-hour travel columns.

        Ports the CASE/COALESCE donor pattern from
        ``db.py:get_eiendom_for_sheets`` (829-852), scoped to a single
        finnkode: when ``travel_copy_from_finnkode`` is set (non-empty) AND
        the donor row has a non-null value for that column, the donor's
        value wins; otherwise the listing's own value is used. Single hop
        only -- the donor is never itself resolved through its own pointer.

        Returns a dict with keys ``"PENDL RUSH BRJ"``, ``"PENDL RUSH MVV"``,
        ``"MVV UNI RUSH"`` (all ``None`` if the finnkode has no
        ``eiendom_processed`` row at all).
        """
        row = self.conn.execute(
            """
            SELECT
                CASE
                    WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                         AND ep_src.pendl_rush_brj IS NOT NULL
                    THEN ep_src.pendl_rush_brj
                    ELSE ep.pendl_rush_brj
                END as "PENDL RUSH BRJ",
                CASE
                    WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                         AND ep_src.pendl_rush_mvv IS NOT NULL
                    THEN ep_src.pendl_rush_mvv
                    ELSE ep.pendl_rush_mvv
                END as "PENDL RUSH MVV",
                CASE
                    WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
                         AND ep_src.pendl_rush_mvv_uni_rush IS NOT NULL
                    THEN ep_src.pendl_rush_mvv_uni_rush
                    ELSE ep.pendl_rush_mvv_uni_rush
                END as "MVV UNI RUSH"
            FROM eiendom_processed ep
            LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
            WHERE ep.finnkode = ?
            """,
            (str(finnkode),),
        ).fetchone()

        if row is None:
            return {"PENDL RUSH BRJ": None, "PENDL RUSH MVV": None, "MVV UNI RUSH": None}
        return dict(row)
