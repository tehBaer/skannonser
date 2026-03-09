import os
import random
import time
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup


def _parse_count(value: str):
    """Normalize count strings like '1 142'/'1.142'/'1\u00a0142' to int."""
    digits_only = re.sub(r'\D', '', value)
    if not digits_only:
        return None
    try:
        return int(digits_only)
    except ValueError:
        return None


def extract_total_matches(page_text: str):
    """Extract total FINN result count from visible text and embedded payload."""
    patterns = [
        # SEO text is typically very stable: "Du finner 1 142 boliger til salgs ..."
        r'Du finner\s+([\d\s\u00a0.,]+)\s+boliger\s+til\s+salgs',
        # Generic UI fallback when a visible count is present.
        r'([\d\s\u00a0.,]+)\s+treff',
        # Filter payload often contains: "Til salgs",1142
        r'"Til salgs"\s*,\s*"?([\d\s\u00a0.,]+)"?',
        # Legacy/alternate payload form with direct num_results value.
        r'"num_results"\s*[:=]\s*"?([\d\s\u00a0.,]+)"?',
        # Legacy/alternate payload form with direct match_count value.
        r'"match_count"\s*[:=]\s*"?([\d\s\u00a0.,]+)"?',
    ]
    for pattern in patterns:
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        if not m:
            continue
        parsed = _parse_count(m.group(1))
        if parsed is not None:
            return parsed
    return None


def parse_resultpage(urlBase, term, folder, page: int = 1, df=None, isNAV: bool = False, seen_urls=None):
    append = ''
    if page != 1:
        if isNAV:
            append = f'&from={100 * (page - 1)}'
        else:
            append = f'&page={page}'
    url = urlBase + append

    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Save the HTML content to a file inside the folder
    with open(os.path.join(folder, 'page' + str(page) + '.html'), 'w', encoding='utf-8') as file:
        file.write(soup.prettify())

    # Extract all hrefs from <a> tags, then filter with regex
    hrefs = [a.get('href') for a in soup.find_all('a', href=True)]
    pattern = re.compile(term)
    matches = {href for href in hrefs if pattern.match(href) and len(href) <= 100}

    full_urls = [
        match if match.startswith('http')
        else f"https://arbeidsplassen.nav.no{match}" if isNAV
        else f"https://www.finn.no{match}"
        for match in sorted(matches)
    ]

    if seen_urls is None:
        seen_urls = set()

    page_new_urls = [u for u in full_urls if u not in seen_urls]
    seen_urls.update(page_new_urls)

    total_matches = extract_total_matches(response.text)

    # Store the URLs in a pandas DataFrame
    new_df = pd.DataFrame(page_new_urls, columns=['URL'])

    # Append new URLs to the existing DataFrame
    if df is not None:
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df
    return df, len(matches), seen_urls, total_matches


def extract_URLs(url, searchTerm, projectname, outputFileName: str, isNAV: bool = False):
    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=['URL'])

    # Create a folder in the parent directory of this file if it doesn't exist
    os.makedirs(projectname, exist_ok=True)

    # Create a folder inside the previous folder for the HTMLs
    os.makedirs(os.path.join(projectname, 'html_crawled'), exist_ok=True)

    page = 1
    seen_urls = set()
    total_expected = None
    while True:
        folder = os.path.join(projectname, 'html_crawled')
        df, match_count, seen_urls, parsed_total = parse_resultpage(url, searchTerm, folder, page, df, isNAV, seen_urls)
        if total_expected is None and parsed_total is not None:
            total_expected = parsed_total

        if match_count == 0:
            print("No more results found. Stopping.")
            break

        total_label = str(total_expected) if total_expected is not None else "?"
        print(f"{len(df)}/{total_label} (page {page})")

        page += 1
        time.sleep(random.uniform(200, 500) / 1000)

    # Save the DataFrame as a CSV file inside the folder
    df.to_csv(os.path.join(projectname, outputFileName), index=False)
    print(f"Crawling completed. Saved to {projectname}/{outputFileName}")
    return df


def getURLsFromPredefinedSearch():
    urlBase = 'https://www.finn.no/realestate/lettings/search.html?lat=59.922591746076556&lon=10.73632512241602&radius=7000&price_to=18500&price_from=13000&start_month=202507&start_month=202508&stored-id=79416555&start_month=202509&area_from=30'
    regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
    extract_URLs(urlBase, regex, "leie", "0_URLs.csv")


if __name__ == "__main__":
    getURLsFromPredefinedSearch()
