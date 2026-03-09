"""
Database-enabled version of eiendom scraper.
Stores data in SQLite database instead of CSV files.
"""
import pandas as pd
import sys
import os


def build_finn_polylocation(points):
    """Build FINN polylocation string from (lng, lat) point tuples.

    FINN expects pairs in `lng+lat` format separated by `%2C`.
    """
    if len(points) < 3:
        raise ValueError("Polygon must contain at least 3 points")

    polygon_points = list(points)
    if polygon_points[0] != polygon_points[-1]:
        polygon_points.append(polygon_points[0])

    return "%2C".join(f"{lng}+{lat}" for lng, lat in polygon_points)

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from main.crawl import extract_URLs
    from main.extractors.extraction_eiendom import extractEiendomDataFromAds
    from main.post_process import post_process_eiendom
    from main.database.db import PropertyDatabase
except ImportError:
    from crawl import extract_URLs
    from extractors.extraction_eiendom import extractEiendomDataFromAds
    from post_process import post_process_eiendom
    from database.db import PropertyDatabase


def run_eiendom_scrape(
    db_path: str = None,
    calculate_location_features: bool = True,
    calculate_google_directions: bool = None,
):
    """
    Run the eiendom scraper and store results in database.
    
    Args:
        db_path: Optional path to database file. If None, uses default location.
        calculate_location_features: Backwards-compatible toggle for Google travel-time API calculations.
        calculate_google_directions: Whether to run paid Google Directions calculations.
            If None, defaults to calculate_location_features.
    """
    # Initialize database
    db = PropertyDatabase(db_path)
    print(f"Using database: {db.db_path}")
    
    projectName = 'data/eiendom'

    # Step 1: Extract URLs from the search results
    print("\n" + "="*40)
    print("Step 1: URLs")
    print("="*40)
    
    # urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.515814226086547+59.830255688429475%2C10.718914241615323+59.89350518832623%2C10.860312986603077+59.90510937383482%2C10.816607919971034+59.96564316999155%2C10.233016736110244+60.03634039140428%2C10.376986367371302+59.84059035321431%2C10.515814226086547+59.830255688429475&property_type=4&property_type=1'
    # urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.931112810853534+59.91640688425295%2C10.442541860909444+60.168376907054125%2C10.202261065854913+60.02462948131998%2C10.19692149263156+59.80380210029858%2C10.394485701898361+59.6827306506911%2C10.653455003235536+59.88025405163344%2C10.931112810853534+59.91640688425295&property_type=4&property_type=1&property_type=2&property_type=11&lifecycle=1'
    # urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.931112810853534+59.91640688425295%2C10.442541860909444+60.168376907054125%2C10.202261065854913+60.02462948131998%2C10.19692149263156+59.80380210029858%2C10.394485701898361+59.6827306506911%2C10.653455003235536+59.88025405163344%2C10.931112810853534+59.91640688425295&property_type=4&property_type=1&property_type=2&property_type=11&lifecycle=1&is_new_property=false&price_to=8000000&property_type=3&area_from=40'
    finn_url_base = (
        'https://www.finn.no/realestate/homes/search.html?filters='
        '&property_type=4&property_type=1&property_type=2&property_type=11'
        '&lifecycle=1&is_new_property=false&price_to=6500000'
        '&property_type=3&area_from=50'
    )
    # Editable polygon points: (lng, lat). Tweak these directly for precise adjustments.
    finn_polygon_points = [
        (10.656738281250, 59.884802942124),
        (10.536789920973, 59.797487966246),
        (10.545723856072, 59.709734171804),
        (10.332641601563, 59.700380312509),
        (9.971542814941, 59.874465805403),
        (10.533142089844, 60.194784612969),
        (11.281037167969, 60.101825866222),
        (10.947750935529, 59.714239974969),
        (10.721282958984, 59.712097173323),
        (10.715468622953, 59.849132221282),
    ]
    urlBase = f"{finn_url_base}&polylocation={build_finn_polylocation(finn_polygon_points)}"
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
    processed_data = post_process_eiendom(
        live_data,
        projectName,
        db,
        calculate_location_features=calculate_location_features,
        calculate_google_directions=calculate_google_directions,
    )
    
    # Step 4: Store data in database
    print("\n" + "="*60)
    print("Step 4: Storing data in database")
    print("="*60)
    
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
    print(f"Listed: {stats['listed']}")
    print(f"Unlisted: {stats['unlisted']}")
    print(f"Not yet exported to Sheets: {stats['not_exported']}")
    
    print("\n" + "="*60)
    print("Scraping completed successfully!")
    print("="*60)
    
    return db


if __name__ == "__main__":
    run_eiendom_scrape()
