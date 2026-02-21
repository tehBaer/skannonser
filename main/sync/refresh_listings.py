"""
Re-download and update all listings that appear in Google Sheets.
This allows detection of status changes (e.g., listings that became sold/inactive).
Run update_rows_in_sheet.py afterwards to sync changes to sheets.
"""
import sys
import os
import time
from typing import Dict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.extractors.ad_html_loader import load_or_fetch_ad_html
    from main.extractors.parsing_helpers_common import getStatus
except ImportError:
    from database.db import PropertyDatabase
    from extractors.ad_html_loader import load_or_fetch_ad_html
    from extractors.parsing_helpers_common import getStatus


def refresh_listing(finnkode: str, url: str, project_name: str = "data/eiendom") -> Dict:
    """
    Re-download and extract data for a single listing.
    
    Args:
        finnkode: The FINN code for the listing
        project_name: The project folder name for storing HTML files
        
    Returns:
        Dict with updated data (finnkode and tilgjengelighet/status)
    """

    try:
        # Force re-download by setting force_save=True
        soup = load_or_fetch_ad_html(url, project_name, auto_save_new=True, force_save=True)
        
        # Extract the status
        tilgjengelig = getStatus(soup)
        
        return {
            'finnkode': finnkode,
            'tilgjengelighet': tilgjengelig,
            'success': True,
            'error': None
        }
        
    except Exception as e:
        return {
            'finnkode': finnkode,
            'tilgjengelighet': None,
            'success': False,
            'error': str(e)
        }


def refresh_all_listings(db_path: str = None, delay: float = 0.2, limit: int = None):
    """
    Re-download all listings that would appear in Google Sheets.
    
    Args:
        db_path: Optional path to database file
        delay: Delay between requests in seconds (default: 0.2)
        limit: Optional limit on number of listings to refresh (for testing)
    """
    print(f"\n{'='*60}")
    print(f"Refreshing listings from FINN.no")
    print(f"{'='*60}\n")
    
    # Initialize database
    db = PropertyDatabase(db_path)
    
    # Get all listings that would appear in sheets
    df = db.get_eiendom_for_sheets()
    
    if df.empty:
        print("No listings to refresh")
        return
    
    total = len(df) if limit is None else min(limit, len(df))
    print(f"Found {len(df)} listings in database")
    if limit:
        print(f"Limiting to {limit} listings for testing")
        df = df.head(limit)
    
    print(f"Will refresh {total} listings...\n")
    
    # Track results
    updated_count = 0
    status_changed_count = 0
    error_count = 0
    status_changes = []
    
    # Ensure html_extracted directory exists
    os.makedirs("data/eiendom/html_extracted", exist_ok=True)
    
    # Process each listing
    for index, row in df.iterrows():
        finnkode = str(row['Finnkode']).strip()
        url = row['URL']
        old_status = row.get('Tilgjengelighet', '')
        
        current_num = index + 1
        print(f"[{current_num}/{total}] Refreshing {finnkode}...", end=" ")
        
        # Refresh the listing
        result = refresh_listing(finnkode, url)
        
        if result['success']:
            new_status = result['tilgjengelighet']
            
            # Normalize statuses for comparison
            old_norm = str(old_status).strip() if old_status else ''
            new_norm = str(new_status).strip() if new_status else ''
            
            # Update database
            db.update_eiendom_status(finnkode, new_status)
            updated_count += 1
            
            # Check if status changed
            if old_norm != new_norm:
                status_changed_count += 1
                status_changes.append({
                    'finnkode': finnkode,
                    'adresse': row.get('ADRESSE', 'N/A'),
                    'old_status': old_norm or '(none)',
                    'new_status': new_norm or '(none)'
                })
                print(f"✓ Status changed: '{old_norm or '(none)'}' → '{new_norm or '(none)'}'")
            else:
                print(f"✓ No change ({new_norm or '(none)'})")
        else:
            error_count += 1
            print(f"✗ Error: {result['error']}")
        
        # Delay to avoid overwhelming the server
        if current_num < total:
            time.sleep(delay)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Refresh Summary")
    print(f"{'='*60}")
    print(f"Total processed: {total}")
    print(f"Successfully updated: {updated_count}")
    print(f"Status changes detected: {status_changed_count}")
    print(f"Errors: {error_count}")
    
    if status_changes:
        print(f"\n{'='*60}")
        print(f"Status Changes Detected")
        print(f"{'='*60}")
        for change in status_changes:
            print(f"\n{change['adresse']} ({change['finnkode']})")
            print(f"  {change['old_status']} → {change['new_status']}")
    
    print(f"\n{'='*60}")
    print(f"Next step: Run update_rows_in_sheet.py to sync changes to Google Sheets")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Refresh listings from FINN.no')
    parser.add_argument('--limit', type=int, help='Limit number of listings to refresh (for testing)')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between requests in seconds (default: 0.2)')
    
    args = parser.parse_args()
    
    refresh_all_listings(limit=args.limit, delay=args.delay)
