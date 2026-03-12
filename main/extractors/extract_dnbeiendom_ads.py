import os
import sys
import time
import random
import json
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Ensure project root is importable when run as a script path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
 
try:
    from main.extractors.ad_html_loader import load_or_fetch_ad_html
except Exception:
    from extractors.ad_html_loader import load_or_fetch_ad_html

def parse_listing_jsonld(soup):
    scripts = soup.find_all('script', type='application/ld+json')
    for s in scripts:
        text = s.string
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue

        # data may be list or dict
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get('@type') == 'RealEstateListing':
                return entry
            if entry.get('@type') == 'ItemList' and entry.get('itemListElement'):
                for li in entry.get('itemListElement'):
                    item = li.get('item') or li.get('itemListElement') or li
                    if isinstance(item, dict) and item.get('@type') == 'RealEstateListing':
                        return item
    return None


def extract_fields_from_entry(entry: dict):
    out = {}
    out['URL'] = entry.get('url') or entry.get('@id')
    out['Title'] = entry.get('name')
    out['Description'] = entry.get('description')

    image = entry.get('image')
    if isinstance(image, list):
        out['IMAGE_URL'] = image[0]
    else:
        out['IMAGE_URL'] = image

    about = entry.get('about') or {}
    addr = about.get('address') or {}
    out['StreetAddress'] = addr.get('streetAddress')
    out['Locality'] = addr.get('addressLocality')
    out['Region'] = addr.get('addressRegion')
    out['PostalCode'] = addr.get('postalCode')

    geo = about.get('geo') or {}
    out['Latitude'] = geo.get('latitude')
    out['Longitude'] = geo.get('longitude')

    floor = about.get('floorSize') or {}
    out['FloorSize'] = floor.get('value')

    out['NumberOfRooms'] = about.get('numberOfRooms')
    out['NumberOfBedrooms'] = about.get('numberOfBedrooms')

    offers = entry.get('offers') or {}
    # offers may contain priceSpecification list
    price = None
    if isinstance(offers, dict):
        specs = offers.get('priceSpecification')
        if isinstance(specs, list):
            # try to find Prisantydning first
            for s in specs:
                name = s.get('name', '').lower()
                if 'prisantydning' in name or 'price' in name:
                    price = s.get('price')
                    break
            if price is None and specs:
                price = specs[0].get('price')
        else:
            price = offers.get('price')
    out['Price'] = price

    return out


def extract_all(url_csv_path: str, output_folder: str):
    df_urls = pd.read_csv(url_csv_path)
    urls = df_urls['URL'].tolist()

    os.makedirs(output_folder, exist_ok=True)
    # Ensure directory for cached per-ad HTML exists
    os.makedirs(os.path.join(output_folder, 'html_extracted'), exist_ok=True)
    results = []
    failures = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; dnbscraper/1.0; +https://dnbeiendom.no)'
    }

    for idx, url in enumerate(urls, start=1):
        try:
            # Try loading from cache or fetching+saving via loader; fallback to direct request.
            try:
                soup = load_or_fetch_ad_html(url, output_folder, auto_save_new=True, force_save=False)
            except Exception:
                resp = requests.get(url, headers=headers, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.content, 'html.parser')

            entry = parse_listing_jsonld(soup)
            if entry:
                out = extract_fields_from_entry(entry)
                results.append(out)
                print(f"{idx}/{len(urls)}: OK {out.get('URL')}")
            else:
                # fallback: still save URL as failed
                failures.append({'URL': url, 'Index': idx, 'Error': 'No JSON-LD RealEstateListing found'})
                print(f"{idx}/{len(urls)}: FAILED no JSON-LD {url}")
        except Exception as e:
            failures.append({'URL': url, 'Index': idx, 'Error': str(e)})
            print(f"{idx}/{len(urls)}: ERROR {url} -> {e}")

        time.sleep(random.uniform(200, 800) / 1000)

    df_out = pd.DataFrame(results)
    df_out.to_csv(os.path.join(output_folder, 'A_live.csv'), index=False)
    if failures:
        pd.DataFrame(failures).to_csv(os.path.join(output_folder, 'A_failed.csv'), index=False)

    print(f"Done. Extracted {len(results)} records, {len(failures)} failures.")
    return df_out, failures


def parse_args():
    parser = argparse.ArgumentParser(description='Extract DNB ad data from URL CSV')
    parser.add_argument('--input', default='data/dnbeiendom/0_URLs.csv', help='Input URL CSV path')
    parser.add_argument('--output-folder', default='data/dnbeiendom', help='Output folder')
    parser.add_argument('--fallback-input', default='data/dnbeiendom/0_URLs_from_saved.csv', help='Fallback input CSV if --input is missing')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    csv_in = args.input
    if not os.path.exists(csv_in) and os.path.exists(args.fallback_input):
        csv_in = args.fallback_input
        print(f"Primary input not found, using fallback: {csv_in}")
    extract_all(csv_in, args.output_folder)
