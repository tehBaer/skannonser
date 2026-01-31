#!/usr/bin/env python3
"""
Debug script to check if Price data is in the primary database.
"""
import sys
import os
import sqlite3

# Add project root to path for imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from main.database.db import PropertyDatabase


def check_price_data():
    """Check if price data exists in the database."""
    db = PropertyDatabase()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("=" * 100)
    print("Checking Price Data in Primary DB")
    print("=" * 100)
    print()
    
    # Check column exists
    cursor.execute("PRAGMA table_info(eiendom)")
    columns = [column[1] for column in cursor.fetchall()]
    print(f"Columns in eiendom table: {columns}")
    print()
    
    # Check if 'pris' column exists
    if 'pris' in columns:
        print("✓ 'pris' column exists")
    else:
        print("✗ 'pris' column NOT found")
        print("  Available columns:", columns)
        conn.close()
        return
    
    # Get stats on price data
    print("\nPrice Data Statistics:")
    print("-" * 100)
    
    cursor.execute('''
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN pris IS NOT NULL THEN 1 END) as records_with_price,
            COUNT(CASE WHEN pris IS NULL THEN 1 END) as records_without_price,
            MIN(pris) as min_price,
            MAX(pris) as max_price,
            AVG(pris) as avg_price
        FROM eiendom
        WHERE is_active = 1
    ''')
    
    stats = cursor.fetchone()
    if stats:
        total, with_price, without_price, min_p, max_p, avg_p = stats
        print(f"  Total active records:          {total}")
        print(f"  Records with price:            {with_price}")
        print(f"  Records without price:         {without_price}")
        print(f"  Min price:                     {min_p:,}" if min_p else "  Min price:                     None")
        print(f"  Max price:                     {max_p:,}" if max_p else "  Max price:                     None")
        print(f"  Avg price:                     {avg_p:,.0f}" if avg_p else "  Avg price:                     None")
    
    # Sample records with price data
    print("\n\nSample Records WITH Price Data:")
    print("-" * 100)
    cursor.execute('''
        SELECT finnkode, adresse, pris, areal, pris_kvm
        FROM eiendom
        WHERE is_active = 1 AND pris IS NOT NULL
        LIMIT 5
    ''')
    
    rows = cursor.fetchall()
    for finnkode, adresse, pris, areal, pris_kvm in rows:
        print(f"  {finnkode}: {adresse[:40]:<40} | Price: {pris:>10,} | Area: {areal:>5} | Price/kvm: {pris_kvm:>5}")
    
    # Sample records WITHOUT price data
    print("\n\nSample Records WITHOUT Price Data:")
    print("-" * 100)
    cursor.execute('''
        SELECT finnkode, adresse, pris, areal, pris_kvm
        FROM eiendom
        WHERE is_active = 1 AND pris IS NULL
        LIMIT 5
    ''')
    
    rows = cursor.fetchall()
    for finnkode, adresse, pris, areal, pris_kvm in rows:
        print(f"  {finnkode}: {adresse[:40]:<40} | Price: {str(pris):<10} | Area: {areal:>5} | Price/kvm: {pris_kvm:>5}")
    
    conn.close()
    
    print("\n" + "=" * 100)
    print("✓ Price data check completed")
    print("=" * 100)


if __name__ == '__main__':
    check_price_data()
