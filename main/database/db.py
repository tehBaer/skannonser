"""
Database module for storing property listings.
Replaces CSV-based storage with SQLite database.
"""
import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any
import os
from .overrides import PropertyOverrides


def _to_float_or_none(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _get_coord_bounds() -> tuple[float, float, float, float]:
    lat_min, lat_max, lng_min, lng_max = 57.0, 72.0, 4.0, 32.0
    try:
        from main.config.filters import COORD_LAT_MIN, COORD_LAT_MAX, COORD_LNG_MIN, COORD_LNG_MAX
        lat_min = float(COORD_LAT_MIN)
        lat_max = float(COORD_LAT_MAX)
        lng_min = float(COORD_LNG_MIN)
        lng_max = float(COORD_LNG_MAX)
    except Exception:
        try:
            from config.filters import COORD_LAT_MIN, COORD_LAT_MAX, COORD_LNG_MIN, COORD_LNG_MAX
            lat_min = float(COORD_LAT_MIN)
            lat_max = float(COORD_LAT_MAX)
            lng_min = float(COORD_LNG_MIN)
            lng_max = float(COORD_LNG_MAX)
        except Exception:
            pass
    return lat_min, lat_max, lng_min, lng_max


def _is_in_bounds(lat: float, lng: float, lat_min: float, lat_max: float, lng_min: float, lng_max: float) -> bool:
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _normalize_coordinates(lat: Any, lng: Any) -> tuple[Optional[float], Optional[float], bool]:
    """Return (lat, lng, swapped) when valid; otherwise (None, None, False)."""
    lat_v = _to_float_or_none(lat)
    lng_v = _to_float_or_none(lng)
    if lat_v is None or lng_v is None:
        return None, None, False

    lat_min, lat_max, lng_min, lng_max = _get_coord_bounds()
    if _is_in_bounds(lat_v, lng_v, lat_min, lat_max, lng_min, lng_max):
        return lat_v, lng_v, False

    # Common failure mode: lat/lng swapped.
    if _is_in_bounds(lng_v, lat_v, lat_min, lat_max, lng_min, lng_max):
        return lng_v, lat_v, True

    return None, None, False


class PropertyDatabase:
    """Handles all database operations for property listings."""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection."""
        if db_path is None:
            # Default to database folder (same directory as this file)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, 'properties.db')
        
        self.db_path = db_path
        self.overrides = PropertyOverrides(db_path)
        self._init_db()
    
    def _init_db(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create eiendom (property) table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eiendom (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                tilgjengelighet TEXT,
                adresse TEXT,
                postnummer TEXT,
                pris INTEGER,
                url TEXT,
                image_url TEXT,
                image_hosted_url TEXT,
                info_usable_area INTEGER,
                info_usable_i_area INTEGER,
                info_primary_area INTEGER,
                info_gross_area INTEGER,
                info_usable_e_area INTEGER,
                info_usable_b_area INTEGER,
                info_open_area INTEGER,
                info_plot_area INTEGER,
                info_plot_ownership TEXT,
                info_property_type TEXT,
                info_construction_year INTEGER,
                pris_kvm INTEGER,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                stale BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create eiendom_processed table for location-related features
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eiendom_processed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                adresse_cleaned TEXT,
                lat REAL,
                lng REAL,
                pendl_morn_brj INTEGER,
                bil_morn_brj INTEGER,
                pendl_dag_brj INTEGER,
                bil_dag_brj INTEGER,
                pendl_morn_mvv INTEGER,
                bil_morn_mvv INTEGER,
                pendl_dag_mvv INTEGER,
                bil_dag_mvv INTEGER,
                pendl_morn_cntr INTEGER,
                bil_morn_cntr INTEGER,
                pendl_dag_cntr INTEGER,
                bil_dag_cntr INTEGER,
                travel_copy_from_finnkode TEXT,
                google_maps_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (finnkode) REFERENCES eiendom(finnkode)
            )
        ''')

        # Ensure new columns exist for existing eiendom databases
        cursor.execute("PRAGMA table_info(eiendom)")
        existing_eiendom_columns = {row[1] for row in cursor.fetchall()}

        # Normalize legacy activity/search columns to the canonical stale name.
        if "stale" not in existing_eiendom_columns:
            if "found_in_last_search" in existing_eiendom_columns:
                try:
                    cursor.execute("ALTER TABLE eiendom RENAME COLUMN found_in_last_search TO stale")
                except sqlite3.OperationalError:
                    cursor.execute("ALTER TABLE eiendom ADD COLUMN stale BOOLEAN DEFAULT 1")
                    cursor.execute(
                        "UPDATE eiendom SET stale = COALESCE(found_in_last_search, 1)"
                    )
            elif "is_active" in existing_eiendom_columns:
                try:
                    cursor.execute("ALTER TABLE eiendom RENAME COLUMN is_active TO stale")
                except sqlite3.OperationalError:
                    cursor.execute("ALTER TABLE eiendom ADD COLUMN stale BOOLEAN DEFAULT 1")
                    cursor.execute(
                        "UPDATE eiendom SET stale = COALESCE(is_active, 1)"
                    )
            else:
                cursor.execute("ALTER TABLE eiendom ADD COLUMN stale BOOLEAN DEFAULT 1")

            cursor.execute("PRAGMA table_info(eiendom)")
            existing_eiendom_columns = {row[1] for row in cursor.fetchall()}
        eiendom_columns_to_add = {
            "image_url": "TEXT",
            "image_hosted_url": "TEXT",
            "info_usable_area": "INTEGER",
            "info_usable_i_area": "INTEGER",
            "info_primary_area": "INTEGER",
            "info_gross_area": "INTEGER",
            "info_usable_e_area": "INTEGER",
            "info_usable_b_area": "INTEGER",
            "info_open_area": "INTEGER",
            "info_plot_area": "INTEGER",
            "info_plot_ownership": "TEXT",
            "info_property_type": "TEXT",
            "info_construction_year": "INTEGER",
        }
        for column_name, column_type in eiendom_columns_to_add.items():
            if column_name not in existing_eiendom_columns:
                cursor.execute(f"ALTER TABLE eiendom ADD COLUMN {column_name} {column_type}")

        # Ensure new columns exist for existing databases
        cursor.execute("PRAGMA table_info(eiendom_processed)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        columns_to_add = {
            "lat": "REAL",
            "lng": "REAL",
            "bil_morn_brj": "INTEGER",
            "pendl_dag_brj": "INTEGER",
            "bil_dag_brj": "INTEGER",
            "pendl_morn_mvv": "INTEGER",
            "bil_morn_mvv": "INTEGER",
            "pendl_dag_mvv": "INTEGER",
            "bil_dag_mvv": "INTEGER",
            "pendl_morn_cntr": "INTEGER",
            "bil_morn_cntr": "INTEGER",
            "pendl_dag_cntr": "INTEGER",
            "bil_dag_cntr": "INTEGER",
            "travel_copy_from_finnkode": "TEXT",
        }
        for column_name, column_type in columns_to_add.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE eiendom_processed ADD COLUMN {column_name} {column_type}")

        # One-time migration: old MVV values become CNTR baseline.
        cursor.execute('''
            UPDATE eiendom_processed
            SET pendl_morn_cntr = COALESCE(pendl_morn_cntr, pendl_morn_mvv),
                bil_morn_cntr = COALESCE(bil_morn_cntr, bil_morn_mvv),
                pendl_dag_cntr = COALESCE(pendl_dag_cntr, pendl_dag_mvv),
                bil_dag_cntr = COALESCE(bil_dag_cntr, bil_dag_mvv)
            WHERE pendl_morn_mvv IS NOT NULL
               OR bil_morn_mvv IS NOT NULL
               OR pendl_dag_mvv IS NOT NULL
               OR bil_dag_mvv IS NOT NULL
        ''')

        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_finnkode ON eiendom(finnkode)')
        cursor.execute('DROP INDEX IF EXISTS idx_eiendom_active')
        cursor.execute('DROP INDEX IF EXISTS idx_eiendom_found_in_last_search')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_stale ON eiendom(stale)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_exported ON eiendom(exported_to_sheets)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_processed_finnkode ON eiendom_processed(finnkode)')
        
        # Create manual_overrides table for properties that need custom values
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS manual_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                pris INTEGER,
                adresse TEXT,
                postnummer TEXT,
                override_reason TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Ensure new override columns exist for existing databases.
        cursor.execute("PRAGMA table_info(manual_overrides)")
        existing_override_columns = {row[1] for row in cursor.fetchall()}
        override_columns_to_add = {
            "adresse": "TEXT",
            "postnummer": "TEXT",
        }
        for column_name, column_type in override_columns_to_add.items():
            if column_name not in existing_override_columns:
                cursor.execute(f"ALTER TABLE manual_overrides ADD COLUMN {column_name} {column_type}")
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_manual_overrides_finnkode ON manual_overrides(finnkode)')
        
        conn.commit()
        conn.close()
    
    def get_connection(self):
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute('PRAGMA journal_mode=WAL')
        return conn
    
    def insert_or_update_eiendom(self, df: pd.DataFrame):
        """Insert or update property listings from a DataFrame."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Map DataFrame columns to database columns
        column_mapping = {
            'Finnkode': 'finnkode',
            'Tilgjengelighet': 'tilgjengelighet',
            'Adresse': 'adresse',
            'Postnummer': 'postnummer',
            'Pris': 'pris',
            'URL': 'url',
            'IMAGE_URL': 'image_url',
            'IMAGE_HOSTED_URL': 'image_hosted_url',
            'Bruksareal': 'info_usable_area',
            'Internt bruksareal (BRA-i)': 'info_usable_i_area',
            'Primærrom': 'info_primary_area',
            'Bruttoareal': 'info_gross_area',
            'Eksternt bruksareal (BRA-e)': 'info_usable_e_area',
            'Innglasset balkong (BRA-b)': 'info_usable_b_area',
            'Balkong/Terrasse (TBA)': 'info_open_area',
            'Tomteareal': 'info_plot_area',
            'Eierskap, tomt': 'info_plot_ownership',
            'Boligtype': 'info_property_type',
            'PRIS KVM': 'pris_kvm'
        }
        
        inserted = 0
        updated = 0
        
        for _, row in df.iterrows():
            finnkode = str(row.get('Finnkode', '')).strip()
            if not finnkode:
                continue
            
            # Check if record exists
            cursor.execute('SELECT id FROM eiendom WHERE finnkode = ?', (finnkode,))
            existing = cursor.fetchone()
            
            data = {
                'finnkode': finnkode,
                'tilgjengelighet': row.get('Tilgjengelighet', ''),
                'adresse': row.get('Adresse', ''),
                'postnummer': row.get('Postnummer', ''),
                'pris': self._to_int(row.get('Pris')),
                'url': row.get('URL', ''),
                'image_url': row.get('IMAGE_URL', ''),
                'image_hosted_url': row.get('IMAGE_HOSTED_URL', ''),
                'info_usable_area': self._to_int(row.get('Bruksareal')),
                'info_usable_i_area': self._to_int(row.get('Internt bruksareal (BRA-i)')),
                'info_primary_area': self._to_int(row.get('Primærrom')),
                'info_gross_area': self._to_int(row.get('Bruttoareal')),
                'info_usable_e_area': self._to_int(row.get('Eksternt bruksareal (BRA-e)')),
                'info_usable_b_area': self._to_int(row.get('Innglasset balkong (BRA-b)')),
                'info_open_area': self._to_int(row.get('Balkong/Terrasse (TBA)')),
                'info_plot_area': self._to_int(row.get('Tomteareal')),
                'info_plot_ownership': row.get('Eierskap, tomt', ''),
                'info_property_type': row.get('Boligtype', ''),
                'info_construction_year': self._to_int(row.get('Byggeår')),
                'pris_kvm': self._to_int(row.get('PRIS KVM')),
            }
            
            # Check for manual overrides
            data = self.overrides.apply_overrides_to_data(finnkode, data)
            
            # Get pendl_morn_brj if present (for location table)
            pendl_morn_brj = row.get('PENDL MORN BRJ', None) if pd.notna(row.get('PENDL MORN BRJ')) else None
            
            # Get bil_morn_brj if present (for location table)
            bil_morn_brj = row.get('BIL MORN BRJ', None) if pd.notna(row.get('BIL MORN BRJ')) else None

            # Get return times if present (for location table)
            pendl_dag_brj = row.get('PENDL DAG BRJ', None) if pd.notna(row.get('PENDL DAG BRJ')) else None
            bil_dag_brj = row.get('BIL DAG BRJ', None) if pd.notna(row.get('BIL DAG BRJ')) else None
            
            # Get MVV (Oslo Sentralstasjon) times if present
            pendl_morn_mvv = row.get('PENDL MORN MVV', None) if pd.notna(row.get('PENDL MORN MVV')) else None
            bil_morn_mvv = row.get('BIL MORN MVV', None) if pd.notna(row.get('BIL MORN MVV')) else None
            pendl_dag_mvv = row.get('PENDL DAG MVV', None) if pd.notna(row.get('PENDL DAG MVV')) else None
            bil_dag_mvv = row.get('BIL DAG MVV', None) if pd.notna(row.get('BIL DAG MVV')) else None
            pendl_morn_cntr = row.get('PENDL MORN CNTR', None) if pd.notna(row.get('PENDL MORN CNTR')) else None
            bil_morn_cntr = row.get('BIL MORN CNTR', None) if pd.notna(row.get('BIL MORN CNTR')) else None
            pendl_dag_cntr = row.get('PENDL DAG CNTR', None) if pd.notna(row.get('PENDL DAG CNTR')) else None
            bil_dag_cntr = row.get('BIL DAG CNTR', None) if pd.notna(row.get('BIL DAG CNTR')) else None
            travel_copy_from_finnkode = (
                row.get('TRAVEL_COPY_FROM_FINNKODE', None)
                if pd.notna(row.get('TRAVEL_COPY_FROM_FINNKODE'))
                else None
            )
            
            if existing:
                # Update existing record
                cursor.execute('''
                    UPDATE eiendom 
                    SET tilgjengelighet = ?, adresse = ?, postnummer = ?, 
                        pris = ?, url = ?,
                        image_url = ?,
                        image_hosted_url = ?,
                        info_usable_area = ?, info_usable_i_area = ?, info_primary_area = ?,
                                                info_gross_area = ?, info_usable_e_area = ?, info_usable_b_area = ?,
                                                info_open_area = ?, info_plot_area = ?, info_plot_ownership = ?, info_property_type = ?, info_construction_year = ?,
                        pris_kvm = ?,
                        stale = 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE finnkode = ?
                ''', (data['tilgjengelighet'], data['adresse'], data['postnummer'],
                    data['pris'], data['url'], data['image_url'], data['image_hosted_url'],
                      data['info_usable_area'], data['info_usable_i_area'], data['info_primary_area'],
                                            data['info_gross_area'], data['info_usable_e_area'], data['info_usable_b_area'],
                                            data['info_open_area'], data['info_plot_area'], data['info_plot_ownership'], data['info_property_type'], data['info_construction_year'],
                      data['pris_kvm'],
                      finnkode))
                updated += 1
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO eiendom 
                    (finnkode, tilgjengelighet, adresse, postnummer, pris, url,
                     image_url,
                     image_hosted_url,
                     info_usable_area, info_usable_i_area, info_primary_area,
                                         info_gross_area, info_usable_e_area, info_usable_b_area,
                                     info_open_area, info_plot_area, info_plot_ownership, info_property_type, info_construction_year,
                     pris_kvm)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (finnkode, data['tilgjengelighet'], data['adresse'], data['postnummer'],
                                    data['pris'], data['url'], data['image_url'], data['image_hosted_url'],
                      data['info_usable_area'], data['info_usable_i_area'], data['info_primary_area'],
                                            data['info_gross_area'], data['info_usable_e_area'], data['info_usable_b_area'],
                                      data['info_open_area'], data['info_plot_area'], data['info_plot_ownership'], data['info_property_type'], data['info_construction_year'],
                      data['pris_kvm']))
                inserted += 1
            
            # Also insert/update processed data with commute times and Google Maps URL
            conn.commit()  # Commit property update first
            self.insert_or_update_eiendom_processed(
                finnkode=finnkode,
                adresse=data['adresse'],
                postnummer=data['postnummer'],
                pendl_morn_brj=pendl_morn_brj,
                bil_morn_brj=bil_morn_brj,
                pendl_dag_brj=pendl_dag_brj,
                bil_dag_brj=bil_dag_brj,
                pendl_morn_mvv=pendl_morn_mvv,
                bil_morn_mvv=bil_morn_mvv,
                pendl_dag_mvv=pendl_dag_mvv,
                bil_dag_mvv=bil_dag_mvv,
                pendl_morn_cntr=pendl_morn_cntr,
                bil_morn_cntr=bil_morn_cntr,
                pendl_dag_cntr=pendl_dag_cntr,
                bil_dag_cntr=bil_dag_cntr,
                travel_copy_from_finnkode=travel_copy_from_finnkode,
            )
        
        conn.commit()
        conn.close()
        
        print(f"Database updated: {inserted} inserted, {updated} updated")
        return inserted, updated
    
    def mark_inactive(self, table: str, active_finnkodes: List[str]):
        """Mark listings as inactive if they're no longer in the active list."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if active_finnkodes:
            placeholders = ','.join('?' * len(active_finnkodes))
            cursor.execute(f'''
                UPDATE {table}
                SET stale = 0, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode NOT IN ({placeholders}) AND stale = 1
            ''', active_finnkodes)
        else:
            cursor.execute(f'''
                UPDATE {table}
                SET stale = 0, updated_at = CURRENT_TIMESTAMP
                WHERE stale = 1
            ''')
        
        deactivated = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"Marked {deactivated} listings as inactive")
        return deactivated
    
    def get_active_listings(self, table: str, as_dataframe: bool = True):
        """Get all active listings from a table."""
        conn = self.get_connection()
        
        if as_dataframe:
            df = pd.read_sql_query(f'SELECT * FROM {table} WHERE stale = 1 ORDER BY scraped_at DESC', conn)
            conn.close()
            return df
        else:
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM {table} WHERE stale = 1 ORDER BY scraped_at DESC')
            rows = cursor.fetchall()
            conn.close()
            return rows
    
    def get_new_listings_for_export(self, table: str) -> pd.DataFrame:
        """Get listings that haven't been exported to Google Sheets yet."""
        conn = self.get_connection()
        
        df = pd.read_sql_query(f'''
            SELECT * FROM {table} 
            WHERE stale = 1 AND exported_to_sheets = 0 
            ORDER BY scraped_at DESC
        ''', conn)
        conn.close()
        
        return df
    
    def mark_as_exported(self, table: str, finnkodes: List[str]):
        """Mark listings as exported to Google Sheets."""
        if not finnkodes:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        placeholders = ','.join('?' * len(finnkodes))
        cursor.execute(f'''
            UPDATE {table}
            SET exported_to_sheets = 1, updated_at = CURRENT_TIMESTAMP
            WHERE finnkode IN ({placeholders})
        ''', finnkodes)
        
        marked = cursor.rowcount
        conn.commit()
        conn.close()
        
        return marked
    
    def update_eiendom_status(self, finnkode: str, new_status: str):
        """
        Update tilgjengelighet (status) for a property listing.

        Note:
            stale is managed by search/scrape matching logic
            (insert_or_update_eiendom + mark_inactive), not by status refresh.
        
        Args:
            finnkode: The FINN code for the listing
            new_status: The new status (e.g., 'Solgt', None for active)
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE eiendom
            SET tilgjengelighet = ?, updated_at = CURRENT_TIMESTAMP
            WHERE finnkode = ?
        ''', (new_status, finnkode))
        
        conn.commit()
        conn.close()
    
    def get_eiendom_for_sheets(self) -> pd.DataFrame:
        """Get property listings formatted for Google Sheets export (includes all listings: active, unlisted/inactive, and sold)."""
        conn = self.get_connection()

        # Optional filters
        try:
            from main.config.filters import MAX_PRICE, MIN_BRA_I
        except ImportError:
            try:
                from config.filters import MAX_PRICE, MIN_BRA_I
            except ImportError:
                MAX_PRICE = None
                MIN_BRA_I = None
        
        # Get all listings regardless of status
        # Uses cleaned addresses from eiendom_processed table when available
        query = '''
            SELECT 
                e.finnkode as "Finnkode",
                e.tilgjengelighet as "Tilgjengelighet",
                e.stale as "stale",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.pris as "Pris",
                e.url as "URL",
                e.image_url as "IMAGE_URL",
                e.image_hosted_url as "IMAGE_HOSTED_URL",
                e.info_usable_area as "Bruksareal",
                e.info_usable_i_area as "Internt bruksareal (BRA-i)",
                e.info_primary_area as "Primærrom",
                e.info_gross_area as "Bruttoareal",
                e.info_usable_e_area as "Eksternt bruksareal (BRA-e)",
                e.info_usable_b_area as "Innglasset balkong (BRA-b)",
                e.info_open_area as "Balkong/Terrasse (TBA)",
                e.info_plot_area as "Tomteareal",
                e.info_plot_ownership as "Eierskap, tomt",
                e.info_property_type as "Boligtype",
                e.info_construction_year as "Byggeår",
                ep.lat as "LAT",
                ep.lng as "LNG",
                e.pris_kvm as "PRIS KVM",
                COALESCE(ep.pendl_morn_brj, ep_src.pendl_morn_brj) as "PENDL MORN BRJ",
                COALESCE(ep.bil_morn_brj, ep_src.bil_morn_brj) as "BIL MORN BRJ",
                COALESCE(ep.pendl_dag_brj, ep_src.pendl_dag_brj) as "PENDL DAG BRJ",
                COALESCE(ep.bil_dag_brj, ep_src.bil_dag_brj) as "BIL DAG BRJ",
                COALESCE(ep.pendl_morn_mvv, ep_src.pendl_morn_mvv) as "PENDL MORN MVV",
                COALESCE(ep.bil_morn_mvv, ep_src.bil_morn_mvv) as "BIL MORN MVV",
                COALESCE(ep.pendl_dag_mvv, ep_src.pendl_dag_mvv) as "PENDL DAG MVV",
                COALESCE(ep.bil_dag_mvv, ep_src.bil_dag_mvv) as "BIL DAG MVV",
                COALESCE(ep.pendl_morn_cntr, ep_src.pendl_morn_cntr) as "PENDL MORN CNTR",
                COALESCE(ep.bil_morn_cntr, ep_src.bil_morn_cntr) as "BIL MORN CNTR",
                COALESCE(ep.pendl_dag_cntr, ep_src.pendl_dag_cntr) as "PENDL DAG CNTR",
                COALESCE(ep.bil_dag_cntr, ep_src.bil_dag_cntr) as "BIL DAG CNTR",
                ep.travel_copy_from_finnkode as "TRAVEL_COPY_FROM_FINNKODE",
                ep.google_maps_url as "GOOGLE_MAPS_URL"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
            WHERE 1=1
        '''

        params = []
        if MAX_PRICE is not None:
            query += " AND e.pris <= ?"
            params.append(MAX_PRICE)
        if MIN_BRA_I is not None:
            query += " AND CAST(e.info_usable_i_area AS REAL) >= ?"
            params.append(MIN_BRA_I)

        query += " ORDER BY e.stale DESC, e.scraped_at DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert numeric columns back to int (pandas reads them as float64)
        # Keep commute-time columns empty when missing (no fillna(0)).
        numeric_columns = ['Pris', 'PRIS KVM']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        return df

    def get_eiendom_for_status_refresh(self, only_inactive: bool = False) -> pd.DataFrame:
        """Get listings for FINN status refresh checks.

        Args:
            only_inactive: If True, only include listings with stale = 0.
        """
        if only_inactive:
            return self.get_stale_eiendom_for_status_refresh(require_url=True)

        conn = self.get_connection()
        query = '''
            SELECT
                e.finnkode as "Finnkode",
                e.url as "URL",
                e.tilgjengelighet as "Tilgjengelighet",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.stale as "stale"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            WHERE e.url IS NOT NULL AND TRIM(e.url) != ''
        '''

        params = []
        if only_inactive:
            query += " AND e.stale = 0"

        query += " ORDER BY e.stale ASC, e.scraped_at DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def get_stale_eiendom_for_status_refresh(self, require_url: bool = True) -> pd.DataFrame:
        """Get stale listings (stale=0) for FINN status refresh checks.

        Args:
            require_url: If True, only include rows with a non-empty URL.
        """
        conn = self.get_connection()
        query = '''
            SELECT
                e.finnkode as "Finnkode",
                e.url as "URL",
                e.tilgjengelighet as "Tilgjengelighet",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.stale as "stale"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            WHERE e.stale = 0
        '''

        if require_url:
            query += " AND e.url IS NOT NULL AND TRIM(e.url) != ''"

        query += " ORDER BY e.scraped_at DESC"

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_stale_eiendom_for_sheets(self) -> pd.DataFrame:
        """Get sold/inactive stale listings formatted for Google Sheets export.

        Scope is intentionally strict for the dedicated Sold tab:
        - stale = 0
        - Tilgjengelighet in {Solgt, Inaktiv} (case-insensitive)

        Uses the same visible columns as get_eiendom_for_sheets(), but without
        optional MAX_PRICE/MIN_BRA_I export filters so sold scope stays canonical.
        """
        conn = self.get_connection()

        query = '''
            SELECT
                e.finnkode as "Finnkode",
                e.tilgjengelighet as "Tilgjengelighet",
                e.stale as "stale",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.pris as "Pris",
                e.url as "URL",
                e.image_url as "IMAGE_URL",
                e.image_hosted_url as "IMAGE_HOSTED_URL",
                e.info_usable_area as "Bruksareal",
                e.info_usable_i_area as "Internt bruksareal (BRA-i)",
                e.info_primary_area as "Primærrom",
                e.info_gross_area as "Bruttoareal",
                e.info_usable_e_area as "Eksternt bruksareal (BRA-e)",
                e.info_usable_b_area as "Innglasset balkong (BRA-b)",
                e.info_open_area as "Balkong/Terrasse (TBA)",
                e.info_plot_area as "Tomteareal",
                e.info_plot_ownership as "Eierskap, tomt",
                e.info_property_type as "Boligtype",
                e.info_construction_year as "Byggeår",
                ep.lat as "LAT",
                ep.lng as "LNG",
                e.pris_kvm as "PRIS KVM",
                COALESCE(ep.pendl_morn_brj, ep_src.pendl_morn_brj) as "PENDL MORN BRJ",
                COALESCE(ep.bil_morn_brj, ep_src.bil_morn_brj) as "BIL MORN BRJ",
                COALESCE(ep.pendl_dag_brj, ep_src.pendl_dag_brj) as "PENDL DAG BRJ",
                COALESCE(ep.bil_dag_brj, ep_src.bil_dag_brj) as "BIL DAG BRJ",
                COALESCE(ep.pendl_morn_mvv, ep_src.pendl_morn_mvv) as "PENDL MORN MVV",
                COALESCE(ep.bil_morn_mvv, ep_src.bil_morn_mvv) as "BIL MORN MVV",
                COALESCE(ep.pendl_dag_mvv, ep_src.pendl_dag_mvv) as "PENDL DAG MVV",
                COALESCE(ep.bil_dag_mvv, ep_src.bil_dag_mvv) as "BIL DAG MVV",
                COALESCE(ep.pendl_morn_cntr, ep_src.pendl_morn_cntr) as "PENDL MORN CNTR",
                COALESCE(ep.bil_morn_cntr, ep_src.bil_morn_cntr) as "BIL MORN CNTR",
                COALESCE(ep.pendl_dag_cntr, ep_src.pendl_dag_cntr) as "PENDL DAG CNTR",
                COALESCE(ep.bil_dag_cntr, ep_src.bil_dag_cntr) as "BIL DAG CNTR",
                ep.travel_copy_from_finnkode as "TRAVEL_COPY_FROM_FINNKODE",
                ep.google_maps_url as "GOOGLE_MAPS_URL"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
                        WHERE e.stale = 0
                            AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')
            ORDER BY e.scraped_at DESC
        '''

        df = pd.read_sql_query(query, conn)
        conn.close()

        numeric_columns = ['Pris', 'PRIS KVM']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        return df

    def get_unlisted_eiendom_for_sheets(self) -> pd.DataFrame:
        """Get unlisted property listings formatted for Google Sheets export."""
        conn = self.get_connection()

        # Optional filters
        try:
            from main.config.filters import MAX_PRICE, MIN_BRA_I
        except ImportError:
            try:
                from config.filters import MAX_PRICE, MIN_BRA_I
            except ImportError:
                MAX_PRICE = None
                MIN_BRA_I = None
        
        # Get unlisted listings (not in search anymore, but not explicitly sold)
        query = '''
            SELECT 
                e.finnkode as "Finnkode",
                e.tilgjengelighet as "Tilgjengelighet",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.pris as "Pris",
                e.url as "URL",
                e.image_url as "IMAGE_URL",
                e.image_hosted_url as "IMAGE_HOSTED_URL",
                e.info_usable_area as "Bruksareal",
                e.info_usable_i_area as "Internt bruksareal (BRA-i)",
                e.info_primary_area as "Primærrom",
                e.info_gross_area as "Bruttoareal",
                e.info_usable_e_area as "Eksternt bruksareal (BRA-e)",
                e.info_usable_b_area as "Innglasset balkong (BRA-b)",
                e.info_open_area as "Balkong/Terrasse (TBA)",
                e.info_plot_area as "Tomteareal",
                e.info_plot_ownership as "Eierskap, tomt",
                e.info_property_type as "Boligtype",
                ep.lat as "LAT",
                ep.lng as "LNG",
                e.pris_kvm as "PRIS KVM",
                COALESCE(ep.pendl_morn_brj, ep_src.pendl_morn_brj) as "PENDL MORN BRJ",
                COALESCE(ep.bil_morn_brj, ep_src.bil_morn_brj) as "BIL MORN BRJ",
                COALESCE(ep.pendl_dag_brj, ep_src.pendl_dag_brj) as "PENDL DAG BRJ",
                COALESCE(ep.bil_dag_brj, ep_src.bil_dag_brj) as "BIL DAG BRJ",
                COALESCE(ep.pendl_morn_mvv, ep_src.pendl_morn_mvv) as "PENDL MORN MVV",
                COALESCE(ep.bil_morn_mvv, ep_src.bil_morn_mvv) as "BIL MORN MVV",
                COALESCE(ep.pendl_dag_mvv, ep_src.pendl_dag_mvv) as "PENDL DAG MVV",
                COALESCE(ep.bil_dag_mvv, ep_src.bil_dag_mvv) as "BIL DAG MVV",
                COALESCE(ep.pendl_morn_cntr, ep_src.pendl_morn_cntr) as "PENDL MORN CNTR",
                COALESCE(ep.bil_morn_cntr, ep_src.bil_morn_cntr) as "BIL MORN CNTR",
                COALESCE(ep.pendl_dag_cntr, ep_src.pendl_dag_cntr) as "PENDL DAG CNTR",
                COALESCE(ep.bil_dag_cntr, ep_src.bil_dag_cntr) as "BIL DAG CNTR",
                ep.travel_copy_from_finnkode as "TRAVEL_COPY_FROM_FINNKODE",
                ep.google_maps_url as "GOOGLE_MAPS_URL"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
            WHERE e.stale = 0 AND (e.tilgjengelighet IS NULL OR e.tilgjengelighet != 'Solgt')
        '''

        params = []
        if MAX_PRICE is not None:
            query += " AND e.pris <= ?"
            params.append(MAX_PRICE)
        if MIN_BRA_I is not None:
            query += " AND CAST(e.info_usable_i_area AS REAL) >= ?"
            params.append(MIN_BRA_I)

        query += " ORDER BY e.scraped_at DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert numeric columns back to int (pandas reads them as float64)
        numeric_columns = ['Pris', 'PRIS KVM']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        return df

    def get_eiendom_missing_coordinates(self) -> pd.DataFrame:
        """Get listings that are missing lat/lng in processed table."""
        conn = self.get_connection()

        query = '''
            SELECT
                e.finnkode as "Finnkode",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.url as "URL",
                e.stale as "stale",
                e.tilgjengelighet as "Tilgjengelighet",
                ep.lat as "LAT",
                ep.lng as "LNG"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            WHERE ep.lat IS NULL OR ep.lng IS NULL
            ORDER BY e.stale DESC, e.scraped_at DESC
        '''

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def set_eiendom_coordinates(self, finnkode: str, lat: float, lng: float) -> bool:
        """Set lat/lng for a listing in eiendom_processed, creating row if needed."""
        if not finnkode:
            return False

        lat_norm, lng_norm, swapped = _normalize_coordinates(lat, lng)
        if lat_norm is None or lng_norm is None:
            print(f"Skipping invalid coordinates for #{finnkode}: lat={lat}, lng={lng}")
            return False
        if swapped:
            print(f"Swapped latitude/longitude for #{finnkode}: lat={lat_norm}, lng={lng_norm}")

        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id FROM eiendom_processed WHERE finnkode = ?', (str(finnkode),))
        existing = cursor.fetchone()

        if existing:
            cursor.execute(
                '''
                UPDATE eiendom_processed
                SET lat = ?, lng = ?, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
                ''',
                (lat_norm, lng_norm, str(finnkode)),
            )
        else:
            cursor.execute(
                '''
                INSERT INTO eiendom_processed (finnkode, lat, lng)
                VALUES (?, ?, ?)
                ''',
                (str(finnkode), lat_norm, lng_norm),
            )

        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed
    
    def _generate_google_maps_url(self, adresse: str, postnummer: str) -> str:
        """Generate a Google Maps search URL from address and postal code."""
        if pd.isna(adresse) or pd.isna(postnummer):
            return ""
        
        # Format: replace spaces with + for URL encoding
        adresse_str = str(adresse).strip()
        postnummer_str = str(postnummer).strip()
        
        search_query = f"{adresse_str}+{postnummer_str}".replace(" ", "+")
        return f"https://www.google.com/maps/place/{search_query}"
    
    def insert_or_update_eiendom_processed(self, finnkode: str, adresse: str,
                                         postnummer: str,
                                         lat: float = None, lng: float = None,
                                         pendl_morn_brj: str = None, bil_morn_brj: str = None,
                                         pendl_dag_brj: str = None, bil_dag_brj: str = None,
                                         pendl_morn_mvv: str = None, bil_morn_mvv: str = None,
                                         pendl_dag_mvv: str = None, bil_dag_mvv: str = None,
                                         pendl_morn_cntr: str = None, bil_morn_cntr: str = None,
                                         pendl_dag_cntr: str = None, bil_dag_cntr: str = None,
                                         travel_copy_from_finnkode: str = None):
        """Insert or update processed location data for a property."""
        conn = self.get_connection()
        cursor = conn.cursor()

        lat_norm, lng_norm, swapped = _normalize_coordinates(lat, lng)
        if lat is not None or lng is not None:
            if lat_norm is None or lng_norm is None:
                print(f"Ignoring invalid coordinates for #{finnkode}: lat={lat}, lng={lng}")
            elif swapped:
                print(f"Swapped latitude/longitude for #{finnkode}: lat={lat_norm}, lng={lng_norm}")
        
        adresse_cleaned = self._clean_address(adresse)
        google_maps_url = self._generate_google_maps_url(adresse_cleaned, postnummer)
        
        # Check if record exists
        cursor.execute('SELECT id FROM eiendom_processed WHERE finnkode = ?', (finnkode,))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE eiendom_processed
                SET adresse_cleaned = ?,
                    lat = COALESCE(?, lat),
                    lng = COALESCE(?, lng),
                    pendl_morn_brj = ?, bil_morn_brj = ?,
                    pendl_dag_brj = ?, bil_dag_brj = ?,
                    pendl_morn_mvv = ?, bil_morn_mvv = ?,
                    pendl_dag_mvv = ?, bil_dag_mvv = ?,
                    pendl_morn_cntr = ?, bil_morn_cntr = ?,
                    pendl_dag_cntr = ?, bil_dag_cntr = ?,
                    travel_copy_from_finnkode = ?,
                    google_maps_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
            ''', (adresse_cleaned, lat_norm, lng_norm, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                                    pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv,
                                    pendl_morn_cntr, bil_morn_cntr, pendl_dag_cntr, bil_dag_cntr,
                                    travel_copy_from_finnkode,
                  google_maps_url, finnkode))
        else:
            cursor.execute('''
                INSERT INTO eiendom_processed
                (finnkode, adresse_cleaned, lat, lng, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                                 pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv,
                                 pendl_morn_cntr, bil_morn_cntr, pendl_dag_cntr, bil_dag_cntr,
                                 travel_copy_from_finnkode, google_maps_url)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (finnkode, adresse_cleaned, lat_norm, lng_norm, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                                    pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv,
                                    pendl_morn_cntr, bil_morn_cntr, pendl_dag_cntr, bil_dag_cntr,
                                    travel_copy_from_finnkode, google_maps_url))
        
        conn.commit()
        conn.close()
    
    def get_processed_data(self, finnkode: str = None) -> pd.DataFrame:
        """Get processed data for properties."""
        conn = self.get_connection()
        
        if finnkode:
            query = '''
                SELECT * FROM eiendom_processed WHERE finnkode = ?
            '''
            df = pd.read_sql_query(query, conn, params=(finnkode,))
        else:
            query = 'SELECT * FROM eiendom_processed ORDER BY updated_at DESC'
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    
    def drop_and_recreate_processed_table(self):
        """Drop the old eiendom_processed table and recreate it with new schema."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Drop the old processed table if it exists
        cursor.execute('DROP TABLE IF EXISTS eiendom_processed')
        
        # Recreate with new schema
        cursor.execute('''
            CREATE TABLE eiendom_processed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                lat REAL,
                lng REAL,
                pendlevei TEXT,
                google_maps_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (finnkode) REFERENCES eiendom(finnkode)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_processed_finnkode ON eiendom_processed(finnkode)')
        
        conn.commit()
        conn.close()
        
        print("✓ eiendom_processed table recreated with new schema")
    
    def migrate_pendlevei_to_processed_table(self):
        """Migrate pendlevei data from eiendom to eiendom_processed table."""
        conn = self.get_connection()
        
        # Get all active properties with pendlevei data
        query = '''
            SELECT finnkode, adresse, postnummer, pendlevei 
            FROM eiendom 
            WHERE stale = 1
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        migrated = 0
        for _, row in df.iterrows():
            finnkode = row['finnkode']
            adresse = row['adresse']
            postnummer = row['postnummer']
            pendlevei = row['pendlevei']
            
            # No kjøretid in old eiendom table, so pass None
            self.insert_or_update_eiendom_processed(
                finnkode,
                adresse,
                postnummer,
                pendl_morn_brj=pendlevei,
                bil_morn_brj=None
            )
            migrated += 1
        
        print(f"Migrated {migrated} properties to processed table")
        return migrated
    
    def _to_float(self, value) -> Optional[float]:
        """Safely convert value to float."""
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _to_int(self, value) -> Optional[int]:
        """Safely convert value to int."""
        if pd.isna(value):
            return None
        try:
            return int(round(float(value)))
        except (ValueError, TypeError):
            return None
    
    def _clean_address(self, address: str) -> str:
        """Clean address by removing text after house number.
        
        Examples:
            'Brynsveien 146 - Prosjekt' -> 'Brynsveien 146'
            'Jarenlia 107 (Bolignr. J-02)' -> 'Jarenlia 107'
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address with suffix removed
        """
        if not address or pd.isna(address):
            return address
        
        address = str(address).strip()
        
        # Split by common delimiters that indicate suffix text
        # Common patterns: " - ", " (", " [", " /"
        delimiters = [' - ', ' (', ' [', ' /']
        
        for delimiter in delimiters:
            if delimiter in address:
                # Keep everything before the first occurrence of the delimiter
                address = address.split(delimiter)[0].strip()
        
        return address
    
    def set_override(
        self,
        finnkode: str,
        pris: int = None,
        reason: str = None,
        adresse: str = None,
        postnummer: str = None,
    ):
        """Set manual override for a property (adresse/postnummer/pris)."""
        return self.overrides.set_override(
            finnkode=finnkode,
            pris=pris,
            reason=reason,
            adresse=adresse,
            postnummer=postnummer,
        )
    
    def get_override(self, finnkode: str):
        """Get override values for a property if they exist."""
        return self.overrides.get_override(finnkode)
    
    def list_overrides(self):
        """List all active overrides."""
        return self.overrides.list_overrides()
    
    def remove_override(self, finnkode: str):
        """Remove manual override for a property."""
        return self.overrides.remove_override(finnkode)
    
    def get_stats(self, table: str) -> Dict[str, Any]:
        """Get statistics about listings in a table."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        total = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE stale = 1')
        listed = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE stale = 0 AND tilgjengelighet != \'Solgt\'')
        unlisted = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE exported_to_sheets = 0 AND stale = 1')
        not_exported = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'listed': listed,
            'unlisted': unlisted,
            'not_exported': not_exported
        }


# Convenience function for backwards compatibility
def load_csv_to_db(csv_path: str, table: str, db: PropertyDatabase = None):
    """Load data from CSV file into database (migration helper)."""
    if db is None:
        db = PropertyDatabase()
    
    df = pd.read_csv(csv_path)
    
    if table == 'eiendom':
        db.insert_or_update_eiendom(df)
    else:
        raise ValueError(f"Table {table} not yet supported in this helper")
    
    return db


if __name__ == "__main__":
    # Test database creation
    db = PropertyDatabase()
    print("Database initialized successfully")
    print(f"Database location: {db.db_path}")
    
    # Print stats for all tables
    for table in ['eiendom', 'leie', 'jobbe']:
        stats = db.get_stats(table)
        print(f"\n{table.upper()} stats:")
        print(f"  Total: {stats['total']}")
        print(f"  Listed: {stats['listed']}")
        print(f"  Unlisted: {stats['unlisted']}")
        print(f"  Not exported: {stats['not_exported']}")
