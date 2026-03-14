import os
import random
import time
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup


# Note: removed logic that attempted to extract the total result count from
# FINN pages because it proved unreliable; we only crawl pages until no new
# matches are found and report the number of URLs collected per page.


def parse_resultpage(urlBase, term, folder, page: int = 1, isNAV: bool = False):
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

    # We no longer attempt to parse a total match count from the page.
    return full_urls, len(matches)


def extract_URLs(url, searchTerm, projectname, outputFileName: str, isNAV: bool = False):
    # Create a folder in the parent directory of this file if it doesn't exist
    os.makedirs(projectname, exist_ok=True)

    # Create a folder inside the previous folder for the HTMLs
    os.makedirs(os.path.join(projectname, 'html_crawled'), exist_ok=True)

    output_path = os.path.join(projectname, outputFileName)
    existing_urls = set()
    if os.path.exists(output_path):
        try:
            existing_df = pd.read_csv(output_path)
            if 'URL' in existing_df.columns:
                existing_urls = {
                    str(v).strip() for v in existing_df['URL'].dropna().tolist() if str(v).strip()
                }
                print(f"Loaded {len(existing_urls)} existing URLs from {output_path}")
        except Exception as e:
            print(f"Warning: could not read existing URLs from {output_path}: {e}")

    page = 1
    # Tracks all URLs found in this crawl run.
    all_urls = set()
    # Tracks URLs seen across historical output + current run for true "new" counts.
    seen_urls = set(existing_urls)
    while True:
        folder = os.path.join(projectname, 'html_crawled')
        page_urls, match_count = parse_resultpage(url, searchTerm, folder, page, isNAV)

        if match_count == 0:
            print("No more results found. Stopping.")
            break

        page_urls_set = set(page_urls)
        new_count = len(page_urls_set - seen_urls)
        seen_urls.update(page_urls_set)
        all_urls.update(page_urls_set)

        print(
            f"Page {page}: found {len(page_urls_set)} listing links "
            f"({new_count} new vs saved data) [cumulative: {len(all_urls)}]"
        )

        page += 1
        time.sleep(random.uniform(200, 500) / 1000)

    df = pd.DataFrame(sorted(all_urls), columns=['URL'])

    # Save the DataFrame as a CSV file inside the folder
    df.to_csv(output_path, index=False)
    print(f"Found {len(df)} unique URLs from search results")
    print(f"Crawling completed. Saved to {projectname}/{outputFileName}")
    return df


def getURLsFromPredefinedSearch():
    urlBase = 'https://www.finn.no/realestate/lettings/search.html?lat=59.922591746076556&lon=10.73632512241602&radius=7000&price_to=18500&price_from=13000&start_month=202507&start_month=202508&stored-id=79416555&start_month=202509&area_from=30'
    regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
    extract_URLs(urlBase, regex, "leie", "0_URLs.csv")


if __name__ == "__main__":
    getURLsFromPredefinedSearch()
