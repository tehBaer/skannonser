import os
import random
import time
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


def _parse_count(value: str):
    digits_only = re.sub(r'\D', '', value)
    if not digits_only:
        return None
    try:
        return int(digits_only)
    except ValueError:
        return None


def parse_resultpage_dn(urlBase, folder, page: int = 1, df=None, seen_urls=None):
    append = ''
    if page != 1:
        append = f'&page={page}'

    if '?' in urlBase:
        url = urlBase + append
    else:
        url = urlBase + (('?' + append.lstrip('&')) if append else '')

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Save HTML for inspection
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, f'page{page}.html'), 'w', encoding='utf-8') as fh:
        fh.write(soup.prettify())

    hrefs = [a.get('href') for a in soup.find_all('a', href=True)]

    matches = set()
    for href in hrefs:
        if not href:
            continue
        # Accept links that point to listings on dnbeiendom, or relative /bolig paths
        if 'dnbeiendom.no' in href and '/bolig' in href:
            matches.add(href)
        elif href.startswith('/bolig') or '/bolig?' in href or '/bolig/' in href:
            matches.add(href)

    full_urls = []
    for match in sorted(matches):
        if match.startswith('http'):
            full_urls.append(match)
        else:
            full_urls.append(f'https://dnbeiendom.no{match}')

    if seen_urls is None:
        seen_urls = set()

    page_new_urls = [u for u in full_urls if u not in seen_urls]
    seen_urls.update(page_new_urls)

    new_df = pd.DataFrame(page_new_urls, columns=['URL'])
    if df is not None:
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df

    # Try to extract total matches from page text (best-effort)
    total_matches = None
    m = re.search(r"(\d[\d\s\u00a0.,]+)\s+(annonser|boliger|treff)", response.text, flags=re.IGNORECASE)
    if m:
        total_matches = _parse_count(m.group(1))

    # Return the number of NEW URLs found on this page (so caller can stop when 0)
    return df, len(page_new_urls), seen_urls, total_matches


def extract_dnbeiendom_urls(urlBase, projectname, outputFileName: str, max_pages: int = None):
    df = pd.DataFrame(columns=['URL'])

    os.makedirs(projectname, exist_ok=True)
    os.makedirs(os.path.join(projectname, 'html_crawled'), exist_ok=True)

    page = 1
    seen_urls = set()
    total_expected = None
    while True:
        folder = os.path.join(projectname, 'html_crawled')
        df, match_count, seen_urls, parsed_total = parse_resultpage_dn(urlBase, folder, page, df, seen_urls)
        if total_expected is None and parsed_total is not None:
            total_expected = parsed_total

        if match_count == 0:
            print('No more results found. Stopping.')
            break

        total_label = str(total_expected) if total_expected is not None else '?'
        print(f"{len(df)}/{total_label} (page {page})")

        page += 1
        if max_pages is not None and page > max_pages:
            print(f"Reached max_pages={max_pages}. Stopping.")
            break
        time.sleep(random.uniform(200, 500) / 1000)

    df.to_csv(os.path.join(projectname, outputFileName), index=False)
    print(f"Crawling completed. Saved to {projectname}/{outputFileName}")
    return df


if __name__ == '__main__':
    url = 'https://dnbeiendom.no/bolig?estateStatus=project_false&locations=BUSKERUD_ae0fe87e-0ba2-46b7-9164-5ee26c4fc85b&locations=AKERSHUS_fe2e9e2c-620e-4190-9af0-a5baa93abc1f&locations=OSLO_e6cde8d6-578c-4d73-b94e-08d59bb7ce4c'
    # Cap pages to 39 per user's observation
    extract_dnbeiendom_urls(url, 'data/dnbeiendom', '0_URLs.csv', max_pages=39)
