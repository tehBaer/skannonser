"""
Database-enabled version of eiendom scraper.
Stores data in SQLite database instead of CSV files.
"""
import pandas as pd
import sys
import os
import argparse


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


def get_finn_scrape_config():
    project_name = 'data/eiendom'
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
    url_base = f"{finn_url_base}&polylocation={build_finn_polylocation(finn_polygon_points)}"
    regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
    return project_name, url_base, regex


def run_eiendom_crawl():
    project_name, url_base, regex = get_finn_scrape_config()
    print("\n" + "=" * 40)
    print("FINN Crawl: URLs")
    print("=" * 40)
    urls = extract_URLs(url_base, regex, project_name, "0_URLs.csv")
    print(f"Found {len(urls)} URLs")
    return urls


def run_eiendom_extract(urls: pd.DataFrame = None):
    project_name, _, _ = get_finn_scrape_config()
    print("\n" + "=" * 40)
    print("FINN Extraction: Ads")
    print("=" * 40)
    if urls is None:
        urls = pd.read_csv(f'{project_name}/0_URLs.csv')
    return extractEiendomDataFromAds(project_name, urls, "A_live.csv")


def run_eiendom_postprocess_and_store(
    db_path: str = None,
    calculate_location_features: bool = True,
    calculate_google_directions: bool = None,
):
    db = PropertyDatabase(db_path)
    print(f"Using database: {db.db_path}")

    project_name, _, _ = get_finn_scrape_config()
    print("\n" + "=" * 60)
    print("FINN Post-processing data")
    print("=" * 60)
    live_data = pd.read_csv(f'{project_name}/A_live.csv')
    processed_data = post_process_eiendom(
        live_data,
        project_name,
        db,
        calculate_location_features=calculate_location_features,
        calculate_google_directions=calculate_google_directions,
    )

    print("\n" + "=" * 60)
    print("FINN Store data in database")
    print("=" * 60)
    inserted, updated = db.insert_or_update_eiendom(processed_data)

    active_finnkodes = [str(fk).strip() for fk in processed_data['Finnkode'].tolist()]
    db.mark_inactive('eiendom', active_finnkodes)

    print("\n" + "=" * 60)
    print("Database Statistics")
    print("=" * 60)
    stats = db.get_stats('eiendom')
    print(f"Total listings in database: {stats['total']}")
    print(f"Listed: {stats['listed']}")
    print(f"Unlisted: {stats['unlisted']}")
    print(f"Not yet exported to Sheets: {stats['not_exported']}")
    print(f"Inserted/Updated this run: {inserted}/{updated}")

    return db

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
    urls = run_eiendom_crawl()
    run_eiendom_extract(urls)
    db = run_eiendom_postprocess_and_store(
        db_path=db_path,
        calculate_location_features=calculate_location_features,
        calculate_google_directions=calculate_google_directions,
    )
    print("\n" + "=" * 60)
    print("Scraping completed successfully!")
    print("=" * 60)
    return db


def parse_args():
    parser = argparse.ArgumentParser(description='Run FINN pipeline by step')
    parser.add_argument(
        '--step',
        choices=['crawl', 'extract', 'process', 'full'],
        default='full',
        help='Pipeline step to run (default: full)',
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.step == 'crawl':
        run_eiendom_crawl()
    elif args.step == 'extract':
        run_eiendom_extract()
    elif args.step == 'process':
        run_eiendom_postprocess_and_store()
    else:
        run_eiendom_scrape()
