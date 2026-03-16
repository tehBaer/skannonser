"""
Database-enabled version of eiendom scraper.
Stores data in SQLite database instead of CSV files.
"""
import pandas as pd
import sys
import os
import argparse
import subprocess
import re
from urllib.parse import urlencode


def _env_bool(name: str):
    """Parse optional boolean env var; returns True/False or None when unset/invalid."""
    raw = os.getenv(name)
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if value in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(str(raw).strip())
    except Exception:
        return default
    return value if value > 0 else default


def _env_wants_true(name: str) -> bool:
    return str(os.getenv(name, '')).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _run_coords_fill_if_needed() -> int:
    coords_script = os.path.join(project_root, 'main', 'tools', 'fill_missing_coordinates.py')
    rpm = _env_float('COORDS_RPM', 120.0)
    include_inactive = _env_wants_true('COORDS_INCLUDE_INACTIVE')
    require_confirm = _env_bool('COORDS_CONFIRM')
    if require_confirm is None:
        require_confirm = True

    count_cmd = [
        sys.executable,
        coords_script,
        '--limit',
        '0',
        '--rpm',
        f'{rpm:g}',
        '--count-only',
    ]
    if include_inactive:
        count_cmd.append('--include-inactive')

    count_proc = subprocess.run(
        count_cmd,
        cwd=project_root,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        check=False,
    )
    count_output = (count_proc.stdout or '') + (count_proc.stderr or '')
    match = re.search(r'^Candidates:\s*(\d+)\s*$', count_output, flags=re.MULTILINE)
    candidate_count = int(match.group(1)) if match else 0

    print(f'[COORDS] candidates={candidate_count}')
    if candidate_count == 0:
        print('[COORDS] skip (no missing LAT/LNG)')
        return 0

    if require_confirm:
        answer = input('Run coords fill now (geocode missing LAT/LNG)? [y/N]: ').strip().lower()
        if answer not in {'y', 'yes'}:
            print('[COORDS] skip (user declined)')
            return 0

    fill_cmd = [
        sys.executable,
        coords_script,
        '--limit',
        '0',
        '--rpm',
        f'{rpm:g}',
        '--allow-failures',
    ]
    if include_inactive:
        fill_cmd.append('--include-inactive')

    subprocess.run(
        fill_cmd,
        cwd=project_root,
        env=os.environ.copy(),
        check=True,
    )
    return candidate_count


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
    try:
        from main.config.filters import get_finn_search_filter_params
    except ImportError:
        from config.filters import get_finn_search_filter_params

    finn_filter_params = get_finn_search_filter_params()
    filter_suffix = ''
    if finn_filter_params:
        filter_suffix = '&' + urlencode(finn_filter_params)

    finn_url_base = (
        'https://www.finn.no/realestate/homes/search.html?filters='
        '&property_type=4&property_type=1&property_type=2&property_type=11'
        '&lifecycle=1&is_new_property=false'
        '&property_type=3'
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
    url_base = f"{finn_url_base}{filter_suffix}&polylocation={build_finn_polylocation(finn_polygon_points)}"
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
    # Optional env override used by Makefile targets that want explicit travel API control.
    env_google_directions = _env_bool('EIENDOM_CALCULATE_GOOGLE_DIRECTIONS')
    if calculate_google_directions is None and env_google_directions is not None:
        calculate_google_directions = env_google_directions

    db = PropertyDatabase(db_path)
    print(f"Using database: {db.db_path}")

    project_name, _, _ = get_finn_scrape_config()
    print("\n" + "=" * 60)
    print("FINN Store base data")
    print("=" * 60)
    live_data = pd.read_csv(f'{project_name}/A_live.csv')
    donor_seed_df = db.get_travel_donor_seed()
    processed_data = post_process_eiendom(
        live_data,
        project_name,
        db,
        calculate_location_features=calculate_location_features,
        calculate_google_directions=False,
        donor_seed_df=donor_seed_df,
    )

    inserted, updated = db.insert_or_update_eiendom(processed_data)
    active_finnkodes = [str(fk).strip() for fk in processed_data['Finnkode'].tolist()]
    db.mark_inactive('eiendom', active_finnkodes)

    print(f"Inserted/Updated base rows: {inserted}/{updated}")

    print("\n" + "=" * 60)
    print("FINN Fill missing coordinates")
    print("=" * 60)
    _run_coords_fill_if_needed()

    if calculate_google_directions:
        print("\n" + "=" * 60)
        print("FINN Post-processing travel data")
        print("=" * 60)
        donor_seed_df = db.get_travel_donor_seed()
        processed_data = post_process_eiendom(
            live_data,
            project_name,
            db,
            calculate_location_features=calculate_location_features,
            calculate_google_directions=True,
            donor_seed_df=donor_seed_df,
        )
        inserted, updated = db.insert_or_update_eiendom(processed_data)
        active_finnkodes = [str(fk).strip() for fk in processed_data['Finnkode'].tolist()]
        db.mark_inactive('eiendom', active_finnkodes)
        print(f"Inserted/Updated after travel pass: {inserted}/{updated}")

    print("\n" + "=" * 60)
    print("Database Statistics")
    print("=" * 60)
    stats = db.get_stats('eiendom')
    print(f"Total listings in database: {stats['total']}")
    print(f"Listed: {stats['listed']}")
    print(f"Unlisted: {stats['unlisted']}")
    print(f"Not yet exported to Sheets: {stats['not_exported']}")
    print(f"Inserted/Updated this run: {inserted}/{updated}")

    print("\n" + "=" * 60)
    print("DNB Travel Backfill")
    print("=" * 60)
    env = os.environ.copy()
    env.setdefault('TRAVEL_REQUESTS_PER_MINUTE', '60')

    backfill_cmd = [
        sys.executable,
        os.path.join(project_root, 'scripts', 'backfill_dnbeiendom_travel_to_sheet.py'),
        '--target',
        'all',
    ]
    subprocess.run(backfill_cmd, cwd=project_root, env=env, check=True)
    print("✓ DNB travel backfill completed")

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
