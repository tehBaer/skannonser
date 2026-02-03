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
                areal INTEGER,
                pris_kvm INTEGER,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create eiendom_processed table for location-related features
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eiendom_processed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                adresse_cleaned TEXT,
                pendl_morn_brj INTEGER,
                bil_morn_brj INTEGER,
                pendl_dag_brj INTEGER,
                bil_dag_brj INTEGER,
                pendl_morn_mvv INTEGER,
                bil_morn_mvv INTEGER,
                pendl_dag_mvv INTEGER,
                bil_dag_mvv INTEGER,
                google_maps_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (finnkode) REFERENCES eiendom(finnkode)
            )
        ''')

        # Ensure new columns exist for existing databases
        cursor.execute("PRAGMA table_info(eiendom_processed)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        columns_to_add = {
            "bil_morn_brj": "INTEGER",
            "pendl_dag_brj": "INTEGER",
            "bil_dag_brj": "INTEGER",
            "pendl_morn_mvv": "INTEGER",
            "bil_morn_mvv": "INTEGER",
            "pendl_dag_mvv": "INTEGER",
            "bil_dag_mvv": "INTEGER",
        }
        for column_name, column_type in columns_to_add.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE eiendom_processed ADD COLUMN {column_name} {column_type}")
        
        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_finnkode ON eiendom(finnkode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_active ON eiendom(is_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_exported ON eiendom(exported_to_sheets)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_processed_finnkode ON eiendom_processed(finnkode)')
        
        # Create manual_overrides table for properties that need custom values
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS manual_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                areal INTEGER,
                pris INTEGER,
                override_reason TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
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
            'AREAL': 'areal',
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
                'areal': self._to_int(row.get('AREAL')),
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
            
            if existing:
                # Update existing record
                cursor.execute('''
                    UPDATE eiendom 
                    SET tilgjengelighet = ?, adresse = ?, postnummer = ?, 
                        pris = ?, url = ?, areal = ?, pris_kvm = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE finnkode = ?
                ''', (data['tilgjengelighet'], data['adresse'], data['postnummer'],
                      data['pris'], data['url'], data['areal'], data['pris_kvm'],
                      finnkode))
                updated += 1
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO eiendom 
                    (finnkode, tilgjengelighet, adresse, postnummer, pris, url, areal, pris_kvm)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (finnkode, data['tilgjengelighet'], data['adresse'], data['postnummer'],
                      data['pris'], data['url'], data['areal'], data['pris_kvm']))
                inserted += 1
            
            # Also insert/update processed data with commute times and Google Maps URL
            conn.commit()  # Commit property update first
            self.insert_or_update_eiendom_processed(
                finnkode,
                data['adresse'],
                data['postnummer'],
                pendl_morn_brj,
                bil_morn_brj,
                pendl_dag_brj,
                bil_dag_brj,
                pendl_morn_mvv,
                bil_morn_mvv,
                pendl_dag_mvv,
                bil_dag_mvv
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
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode NOT IN ({placeholders}) AND is_active = 1
            ''', active_finnkodes)
        else:
            cursor.execute(f'''
                UPDATE {table}
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE is_active = 1
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
            df = pd.read_sql_query(f'SELECT * FROM {table} WHERE is_active = 1 ORDER BY scraped_at DESC', conn)
            conn.close()
            return df
        else:
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM {table} WHERE is_active = 1 ORDER BY scraped_at DESC')
            rows = cursor.fetchall()
            conn.close()
            return rows
    
    def get_new_listings_for_export(self, table: str) -> pd.DataFrame:
        """Get listings that haven't been exported to Google Sheets yet."""
        conn = self.get_connection()
        
        df = pd.read_sql_query(f'''
            SELECT * FROM {table} 
            WHERE is_active = 1 AND exported_to_sheets = 0 
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
    
    def get_eiendom_for_sheets(self) -> pd.DataFrame:
        """Get property listings formatted for Google Sheets export."""
        conn = self.get_connection()

        # Optional price filter
        try:
            from main.config.filters import MAX_PRICE
        except ImportError:
            try:
                from config.filters import MAX_PRICE
            except ImportError:
                MAX_PRICE = None
        
        # Get active listings with the exact column names for Sheets
        # Uses cleaned addresses from eiendom_processed table when available
        query = '''
            SELECT 
                e.finnkode as "Finnkode",
                e.tilgjengelighet as "Tilgjengelighet",
                COALESCE(ep.adresse_cleaned, e.adresse) as "ADRESSE",
                e.postnummer as "Postnummer",
                e.pris as "Pris",
                e.url as "URL",
                e.areal as "AREAL",
                e.pris_kvm as "PRIS KVM",
                ep.pendl_morn_brj as "PENDL MORN BRJ",
                ep.bil_morn_brj as "BIL MORN BRJ",
                ep.pendl_dag_brj as "PENDL DAG BRJ",
                ep.bil_dag_brj as "BIL DAG BRJ",
                ep.pendl_morn_mvv as "PENDL MORN MVV",
                ep.bil_morn_mvv as "BIL MORN MVV",
                ep.pendl_dag_mvv as "PENDL DAG MVV",
                ep.bil_dag_mvv as "BIL DAG MVV",
                ep.google_maps_url as "GOOGLE_MAPS_URL"
            FROM eiendom e
            LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
            WHERE e.is_active = 1
        '''

        params = []
        if MAX_PRICE is not None:
            query += " AND e.pris <= ?"
            params.append(MAX_PRICE)

        query += " ORDER BY e.scraped_at DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert numeric columns back to int (pandas reads them as float64)
        # Keep commute-time columns empty when missing (no fillna(0)).
        numeric_columns = ['Pris', 'AREAL', 'PRIS KVM']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        return df
    
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
                                         postnummer: str, pendl_morn_brj: str = None, bil_morn_brj: str = None,
                                         pendl_dag_brj: str = None, bil_dag_brj: str = None,
                                         pendl_morn_mvv: str = None, bil_morn_mvv: str = None,
                                         pendl_dag_mvv: str = None, bil_dag_mvv: str = None):
        """Insert or update processed location data for a property."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        adresse_cleaned = self._clean_address(adresse)
        google_maps_url = self._generate_google_maps_url(adresse_cleaned, postnummer)
        
        # Check if record exists
        cursor.execute('SELECT id FROM eiendom_processed WHERE finnkode = ?', (finnkode,))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE eiendom_processed
                SET adresse_cleaned = ?, pendl_morn_brj = ?, bil_morn_brj = ?,
                    pendl_dag_brj = ?, bil_dag_brj = ?,
                    pendl_morn_mvv = ?, bil_morn_mvv = ?,
                    pendl_dag_mvv = ?, bil_dag_mvv = ?,
                    google_maps_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
            ''', (adresse_cleaned, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                  pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv,
                  google_maps_url, finnkode))
        else:
            cursor.execute('''
                INSERT INTO eiendom_processed
                (finnkode, adresse_cleaned, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                 pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv, google_maps_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (finnkode, adresse_cleaned, pendl_morn_brj, bil_morn_brj, pendl_dag_brj, bil_dag_brj,
                  pendl_morn_mvv, bil_morn_mvv, pendl_dag_mvv, bil_dag_mvv, google_maps_url))
        
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
            WHERE is_active = 1
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
            self.insert_or_update_eiendom_processed(finnkode, adresse, postnummer, pendlevei, None)
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
    
    def set_override(self, finnkode: str, areal: int = None, pris: int = None, reason: str = None):
        """Set manual override for a property's areal and/or pris."""
        return self.overrides.set_override(finnkode, areal, pris, reason)
    
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
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE is_active = 1')
        listed = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE is_active = 0 AND tilgjengelighet != \'Solgt\'')
        unlisted = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE exported_to_sheets = 0 AND is_active = 1')
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
        print(f"  Active: {stats['active']}")
        print(f"  Inactive: {stats['inactive']}")
        print(f"  Not exported: {stats['not_exported']}")
