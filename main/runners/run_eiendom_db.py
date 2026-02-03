"""
Database-enabled version of eiendom scraper.
Stores data in SQLite database instead of CSV files.
"""
import pandas as pd
import sys
import os

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from main.crawl import extract_URLs
    from main.extractors.extraction_eiendom import extractEiendomDataFromAds
    from main.post_process import post_process_eiendom
    from main.runners.run_helper import ensure_venv
    from main.database.db import PropertyDatabase
except ImportError:
    from crawl import extract_URLs
    from extractors.extraction_eiendom import extractEiendomDataFromAds
    from post_process import post_process_eiendom
    from run_helper import ensure_venv
    from database.db import PropertyDatabase


def run_eiendom_scrape(db_path: str = None):
    """
    Run the eiendom scraper and store results in database.
    
    Args:
        db_path: Optional path to database file. If None, uses default location.
    """
    ensure_venv()
    
    # Initialize database
    db = PropertyDatabase(db_path)
    print(f"Using database: {db.db_path}")
    
    projectName = 'data/eiendom'

    # Step 1: Extract URLs from the search results
    print("\n" + "="*40)
    print("Step 1: URLs")
    print("="*40)
    
    # urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.515814226086547+59.830255688429475%2C10.718914241615323+59.89350518832623%2C10.860312986603077+59.90510937383482%2C10.816607919971034+59.96564316999155%2C10.233016736110244+60.03634039140428%2C10.376986367371302+59.84059035321431%2C10.515814226086547+59.830255688429475&property_type=4&property_type=1'
    urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.931112810853534+59.91640688425295%2C10.442541860909444+60.168376907054125%2C10.202261065854913+60.02462948131998%2C10.19692149263156+59.80380210029858%2C10.394485701898361+59.6827306506911%2C10.653455003235536+59.88025405163344%2C10.931112810853534+59.91640688425295&property_type=4&property_type=1'
    regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
    
    urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv")
    # urls = pd.read_csv(f'{projectName}/0_URLs.csv')  # for debugging quickly
    
    print(f"Found {len(urls)} URLs")
    
    # Step 2: Extract data from each ad
    print("\n" + "="*40)
    print("Step 2: Ads")
    print("="*40)
    
    extractEiendomDataFromAds(projectName, urls, "A_live.csv")
    
    # Step 3: Post-process the data
    print("\n" + "="*60)
    print("Step 3: Post-processing data")
    print("="*60)
    
    live_data = pd.read_csv(f'{projectName}/A_live.csv')
    post_process_eiendom(live_data, projectName, "AB_processed.csv")
    
    # Step 4: Load processed data into database
    print("\n" + "="*60)
    print("Step 4: Storing data in database")
    print("="*60)
    
    processed_data = pd.read_csv(f'{projectName}/AB_processed.csv')
    
    # Insert or update records in database
    inserted, updated = db.insert_or_update_eiendom(processed_data)
    
    # Mark listings that are no longer active
    active_finnkodes = [str(fk).strip() for fk in processed_data['Finnkode'].tolist()]
    deactivated = db.mark_inactive('eiendom', active_finnkodes)
    
    # Print statistics
    print("\n" + "="*60)
    print("Database Statistics")
    print("="*60)
    stats = db.get_stats('eiendom')
    print(f"Total listings in database: {stats['total']}")
    print(f"Active listings: {stats['active']}")
    print(f"Inactive listings: {stats['inactive']}")
    print(f"Not yet exported to Sheets: {stats['not_exported']}")
    
    print("\n" + "="*60)
    print("Scraping completed successfully!")
    print("="*60)
    
    return db


if __name__ == "__main__":
    run_eiendom_scrape()
