"""
Station database module.

Normalized schema for railway/transit station data:
  stations        — one row per physical station
  station_lines   — one row per (station, line) combination
  station_travel  — travel-time minutes per (station_line, destination)

DB is the source of truth; the Google Sheet is a derived export.
"""
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Any
import os
import re
import unicodedata


def _default_db_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "properties.db")


def _destination_column_name(destination: str) -> str:
    text = " ".join((destination or "").strip().split())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9]+", "_", text).strip("_")
    return f"TO_{text or 'DESTINATION'}"


class StationDatabase:
    """CRUD for the stations / station_lines / station_travel tables."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or _default_db_path()
        self._init_tables()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self) -> None:
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL UNIQUE,
                lat         REAL,
                lng         REAL,
                radius_m    REAL,
                to_skoyen_min INTEGER,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migration: drop legacy `type` column if it exists
        cur.execute("PRAGMA table_info(stations)")
        station_cols = {row[1] for row in cur.fetchall()}
        if "type" in station_cols:
            try:
                cur.execute("ALTER TABLE stations DROP COLUMN type")
            except Exception:
                pass  # SQLite < 3.35 — leave it; it won't be written

        cur.execute("""
            CREATE TABLE IF NOT EXISTS station_lines (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id  INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                line        TEXT    NOT NULL,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_id, line)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS station_travel (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                station_line_id  INTEGER NOT NULL REFERENCES station_lines(id) ON DELETE CASCADE,
                destination      TEXT    NOT NULL,
                minutes          INTEGER,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_line_id, destination)
            )
        """)

        # Indexes for common query patterns
        cur.execute("CREATE INDEX IF NOT EXISTS idx_station_lines_station_id ON station_lines(station_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_station_travel_line_id   ON station_travel(station_line_id)")

        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Upserts
    # ------------------------------------------------------------------

    def upsert_station(
        self,
        name: str,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_m: Optional[float] = None,
        to_skoyen_min: Optional[int] = None,
    ) -> int:
        """Insert or update a station; returns the station id."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stations (name, lat, lng, radius_m, to_skoyen_min, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(name) DO UPDATE SET
                lat           = COALESCE(excluded.lat,           stations.lat),
                lng           = COALESCE(excluded.lng,           stations.lng),
                radius_m      = COALESCE(excluded.radius_m,      stations.radius_m),
                to_skoyen_min = COALESCE(excluded.to_skoyen_min, stations.to_skoyen_min),
                updated_at    = CURRENT_TIMESTAMP
            """,
            (name, lat, lng, radius_m, to_skoyen_min),
        )
        conn.commit()
        cur.execute("SELECT id FROM stations WHERE name = ?", (name,))
        row = cur.fetchone()
        conn.close()
        return row["id"]

    def upsert_station_line(self, station_id: int, line: str) -> int:
        """Insert or touch a station_line; returns the station_line id."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO station_lines (station_id, line, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(station_id, line) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """,
            (station_id, line),
        )
        conn.commit()
        cur.execute(
            "SELECT id FROM station_lines WHERE station_id = ? AND line = ?",
            (station_id, line),
        )
        row = cur.fetchone()
        conn.close()
        return row["id"]

    def upsert_station_travel(
        self,
        station_line_id: int,
        destination: str,
        minutes: Optional[int],
    ) -> None:
        """Insert or update travel minutes for a (station_line, destination) pair."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO station_travel (station_line_id, destination, minutes, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(station_line_id, destination) DO UPDATE SET
                minutes    = excluded.minutes,
                updated_at = CURRENT_TIMESTAMP
            """,
            (station_line_id, destination, minutes),
        )
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_stations_missing_coords(self) -> List[Tuple[int, str]]:
        """Return list of (id, name) for stations that lack LAT or LNG."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name FROM stations WHERE lat IS NULL OR lng IS NULL ORDER BY name"
        )
        rows = [(r["id"], r["name"]) for r in cur.fetchall()]
        conn.close()
        return rows

    def set_station_coords(self, name: str, lat: float, lng: float) -> None:
        """Update LAT/LNG for a station identified by name."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "UPDATE stations SET lat = ?, lng = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
            (lat, lng, name),
        )
        conn.commit()
        conn.close()

    def get_all_stations(self) -> List[Dict[str, Any]]:
        """Return all stations as plain dicts."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT * FROM stations ORDER BY name")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_station_lines(self, station_id: int) -> List[Dict[str, Any]]:
        """Return all station_lines for a given station as plain dicts."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM station_lines WHERE station_id = ? ORDER BY line",
            (station_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_travel_for_station(
        self, station_id: int, destination: str
    ) -> Dict[str, Optional[int]]:
        """Return {line: minutes} for a station and destination."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sl.line, st.minutes
            FROM station_lines sl
            LEFT JOIN station_travel st
                   ON st.station_line_id = sl.id AND st.destination = ?
            WHERE sl.station_id = ?
            ORDER BY sl.line
            """,
            (destination, station_id),
        )
        result = {r["line"]: r["minutes"] for r in cur.fetchall()}
        conn.close()
        return result

    def get_all_for_export(self, destination: str = "Sandvika") -> List[Dict[str, Any]]:
        """
        Return a flat list of dicts suitable for writing to the Stations sheet.

        Columns: Name, LAT, LNG, Line, TO_<DESTINATION>

        One row is returned per (station, line) tuple.
        """
        conn = self._connect()
        cur = conn.cursor()
        travel_col = _destination_column_name(destination)

        cur.execute(
            """
            SELECT
                s.name AS station_name,
                s.lat AS lat,
                s.lng AS lng,
                sl.line AS line,
                st.minutes AS minutes
            FROM station_lines sl
            JOIN stations s
              ON s.id = sl.station_id
            LEFT JOIN station_travel st
              ON st.station_line_id = sl.id
             AND st.destination = ?
            ORDER BY s.name, sl.line
            """,
            (destination,),
        )
        rows = cur.fetchall()
        conn.close()

        export_rows: List[Dict[str, Any]] = []
        for r in rows:
            export_rows.append(
                {
                    "Name": r["station_name"],
                    "LAT": r["lat"] if r["lat"] is not None else "",
                    "LNG": r["lng"] if r["lng"] is not None else "",
                    "Line": r["line"],
                    travel_col: r["minutes"] if r["minutes"] is not None else "",
                }
            )

        return export_rows

    def count_stations(self) -> int:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM stations")
        n = cur.fetchone()["n"]
        conn.close()
        return n

    def count_station_lines(self) -> int:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM station_lines")
        n = cur.fetchone()["n"]
        conn.close()
        return n

    def count_station_travel(self) -> int:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM station_travel")
        n = cur.fetchone()["n"]
        conn.close()
        return n
