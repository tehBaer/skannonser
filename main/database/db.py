"""
Database module for storing property listings.
Replaces CSV-based storage with SQLite database.
"""
import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any
import os


class PropertyDatabase:
    """Handles all database operations for property listings."""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection."""
        if db_path is None:
            # Default to database folder (same directory as this file)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, 'properties.db')
        
        self.db_path = db_path
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
                pendlevei TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create rental (leie) table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leie (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                tilgjengelighet TEXT,
                adresse TEXT,
                postnummer TEXT,
                leiepris REAL,
                depositum REAL,
                url TEXT,
                areal REAL,
                pris_kvm REAL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create jobs (jobbe) table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobbe (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                title TEXT,
                company TEXT,
                location TEXT,
                url TEXT,
                job_type TEXT,
                deadline TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_finnkode ON eiendom(finnkode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_active ON eiendom(is_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eiendom_exported ON eiendom(exported_to_sheets)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leie_finnkode ON leie(finnkode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leie_active ON leie(is_active)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobbe_finnkode ON jobbe(finnkode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobbe_active ON jobbe(is_active)')
        
        conn.commit()
        conn.close()
    
    def get_connection(self):
        """Get a database connection."""
        return sqlite3.connect(self.db_path)
    
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
                'pendlevei': row.get('PENDLEVEI', None) if pd.notna(row.get('PENDLEVEI')) else None
            }
            
            if existing:
                # Update existing record
                cursor.execute('''
                    UPDATE eiendom 
                    SET tilgjengelighet = ?, adresse = ?, postnummer = ?, 
                        pris = ?, url = ?, areal = ?, pris_kvm = ?, pendlevei = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE finnkode = ?
                ''', (data['tilgjengelighet'], data['adresse'], data['postnummer'],
                      data['pris'], data['url'], data['areal'], data['pris_kvm'],
                      data['pendlevei'], finnkode))
                updated += 1
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO eiendom 
                    (finnkode, tilgjengelighet, adresse, postnummer, pris, url, areal, pris_kvm, pendlevei)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (finnkode, data['tilgjengelighet'], data['adresse'], data['postnummer'],
                      data['pris'], data['url'], data['areal'], data['pris_kvm'], data['pendlevei']))
                inserted += 1
        
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
        
        # Get active listings with the exact column names for Sheets
        query = '''
            SELECT 
                finnkode as "Finnkode",
                tilgjengelighet as "Tilgjengelighet",
                adresse as "Adresse",
                postnummer as "Postnummer",
                pris as "Pris",
                url as "URL",
                areal as "AREAL",
                pris_kvm as "PRIS KVM",
                pendlevei as "PENDLEVEI"
            FROM eiendom
            WHERE is_active = 1
            ORDER BY scraped_at DESC
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert integer columns back to int (pandas reads them as float64)
        int_columns = ['Pris', 'AREAL', 'PRIS KVM']
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        return df
    
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
    
    def get_stats(self, table: str) -> Dict[str, Any]:
        """Get statistics about listings in a table."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        total = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE is_active = 1')
        active = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE is_active = 0')
        inactive = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE exported_to_sheets = 0 AND is_active = 1')
        not_exported = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'active': active,
            'inactive': inactive,
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
