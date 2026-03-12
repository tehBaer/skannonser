import os
import json
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

SEARCH_URL = (
    'https://dnbeiendom.no/bolig?'
    'estateStatus=project_false&'
    'locations=BUSKERUD_ae0fe87e-0ba2-46b7-9164-5ee26c4fc85b&'
    'locations=AKERSHUS_fe2e9e2c-620e-4190-9af0-a5baa93abc1f&'
    'locations=OSLO_e6cde8d6-578c-4d73-b94e-08d59bb7ce4c'
)
LISTING_PATH_PREFIX = '/bolig/'
PROJECT_DIR = 'data/dnbeiendom'
OUTPUT_FILE = '0_URLs.csv'
MAX_PAGES = 200


def _set_page(search_url, page):
    parsed = urlparse(search_url)
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != 'page']
    query_pairs.append(('page', str(page)))
    new_query = urlencode(query_pairs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _extract_listing_urls_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    found = set()

    # Primary source: JSON-LD ItemList embedded in search page.
    for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
        script_content = script.string or script.get_text() or ''
        script_content = script_content.strip()
        if not script_content:
            continue

        try:
            payload = json.loads(script_content)
        except Exception:
            continue

        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get('@type') != 'ItemList':
                continue

            item_elements = entry.get('itemListElement') or []
            for li in item_elements:
                if not isinstance(li, dict):
                    continue
                item = li.get('item') if isinstance(li.get('item'), dict) else li
                url = item.get('url') or item.get('@id')
                if not isinstance(url, str):
                    continue

                absolute = urljoin('https://dnbeiendom.no', url)
                parsed = urlparse(absolute)
                if parsed.netloc != 'dnbeiendom.no':
                    continue
                if not parsed.path.startswith(LISTING_PATH_PREFIX):
                    continue

                # Canonicalize: normalize case, remove fragments, use unquoted path
                path = parsed.path.rstrip('/')
                # Unquote to catch double-encoded variants
                path = path.replace('%', '').lower() if '%' in path else path.lower().rstrip('/')
                canonical = f"https://dnbeiendom.no{path}"
                found.add(canonical)

    if found:
        return found

    # Fallback source: anchor tags.
    for tag in soup.find_all('a', href=True):
        href = tag['href'].strip()
        absolute = urljoin('https://dnbeiendom.no', href)
        parsed = urlparse(absolute)
        if parsed.netloc != 'dnbeiendom.no':
            continue
        if not parsed.path.startswith(LISTING_PATH_PREFIX):
            continue
        # Keep canonical URL without querystring/fragment to avoid duplicates.
        path = parsed.path.rstrip('/')
        path = path.replace('%', '').lower() if '%' in path else path.lower().rstrip('/')
        canonical = f"https://dnbeiendom.no{path}"
        found.add(canonical)
    return found


def fetch_urls_from_search(search_url, project_dir, output_filename, max_pages=MAX_PAGES):
    print("\n" + "=" * 40)
    print("DNB Crawl: URLs")
    print("=" * 40)

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; dnbscraper/1.0; +https://dnbeiendom.no)'
    }
    all_urls = set()
    consecutive_empty_pages = 0
    consecutive_no_new_pages = 0

    for page in range(1, max_pages + 1):
        page_url = _set_page(search_url, page)
        response = requests.get(page_url, headers=headers, timeout=30)
        response.raise_for_status()

        page_urls = _extract_listing_urls_from_html(response.text)
        new_count = len(page_urls - all_urls)
        all_urls.update(page_urls)

        print(f"Page {page}: found {len(page_urls)} listing links ({new_count} new) [cumulative: {len(all_urls)}]")

        if len(page_urls) == 0:
            consecutive_empty_pages += 1
        else:
            consecutive_empty_pages = 0

        if new_count == 0:
            consecutive_no_new_pages += 1
        else:
            consecutive_no_new_pages = 0

        # Stop when pages no longer produce new URLs.
        if consecutive_empty_pages >= 2:
            print(f"Stopping at page {page} after consecutive empty pages.")
            break
        if consecutive_no_new_pages >= 2:
            print(f"Stopping at page {page} after consecutive pages without new URLs.")
            break

    matched = sorted(all_urls)

    if not matched:
        raise RuntimeError('No listing URLs found in search results; check search URL or page parsing.')

    os.makedirs(project_dir, exist_ok=True)
    df = pd.DataFrame(matched, columns=['URL'])
    df.to_csv(os.path.join(project_dir, output_filename), index=False)
    print(f"Found {len(matched)} unique URLs from search results")
    print(f"Saved to {project_dir}/{output_filename}")
    return df


if __name__ == '__main__':
    fetch_urls_from_search(SEARCH_URL, PROJECT_DIR, OUTPUT_FILE)
