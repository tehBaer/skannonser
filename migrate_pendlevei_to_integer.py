#!/usr/bin/env python3
"""
Migration script to convert pendlevei column from TEXT to INTEGER in eiendom_processed table
"""

import sqlite3
import os

# Database path
db_path = os.path.join(os.path.dirname(__file__), 'main', 'database', 'properties.db')

def migrate_pendlevei_to_integer():
    """Convert pendlevei column from TEXT to INTEGER"""
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    print("🔄 Starting migration: pendlevei TEXT → INTEGER")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check current schema
        cursor.execute("PRAGMA table_info(eiendom_processed)")
        columns = cursor.fetchall()
        print("\n📋 Current schema:")
        for col in columns:
            print(f"  {col[1]}: {col[2]}")
        
        # Create new table with INTEGER pendlevei
        print("\n🔨 Creating new table with INTEGER pendlevei...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eiendom_processed_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                adresse_cleaned TEXT,
                pendlevei INTEGER,
                google_maps_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (finnkode) REFERENCES eiendom(finnkode)
            )
        ''')
        
        # Copy data, converting pendlevei to INTEGER
        print("📋 Copying data and converting pendlevei values...")
        cursor.execute('''
            INSERT INTO eiendom_processed_new (id, finnkode, adresse_cleaned, pendlevei, google_maps_url, updated_at)
            SELECT 
                id, 
                finnkode, 
                adresse_cleaned, 
                CAST(pendlevei AS INTEGER),
                google_maps_url, 
                updated_at
            FROM eiendom_processed
        ''')
        
        rows_migrated = cursor.rowcount
        print(f"✅ Migrated {rows_migrated} rows")
        
        # Drop old table
        print("🗑️  Dropping old table...")
        cursor.execute('DROP TABLE eiendom_processed')
        
        # Rename new table
        print("🔄 Renaming new table...")
        cursor.execute('ALTER TABLE eiendom_processed_new RENAME TO eiendom_processed')
        
        # Verify new schema
        cursor.execute("PRAGMA table_info(eiendom_processed)")
        columns = cursor.fetchall()
        print("\n✅ New schema:")
        for col in columns:
            print(f"  {col[1]}: {col[2]}")
        
        # Check sample data
        cursor.execute('SELECT finnkode, pendlevei FROM eiendom_processed WHERE pendlevei IS NOT NULL LIMIT 5')
        samples = cursor.fetchall()
        print("\n📊 Sample data:")
        for sample in samples:
            print(f"  Finnkode: {sample[0]}, Pendlevei: {sample[1]} (type: {type(sample[1]).__name__})")
        
        conn.commit()
        print("\n✅ Migration completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Migration failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_pendlevei_to_integer()
