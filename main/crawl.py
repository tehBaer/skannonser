import os
import random
import time
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup


def parse_resultpage(urlBase, term, folder, page: int = 1, df=None, isNAV: bool = False):
    append = ''
    if page != 1:
        if isNAV:
            append = f'&from={100 * (page - 1)}'
        else:
            append = f'&page={page}'
    url = urlBase + append

    print("Analyzing result page: ", url)
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
        for match in matches
    ]
    # for match in matches:
        # print(f'  Found match: {match}')

    # Count the unique matches
    print(f'Number of unique matches on page {page}: {len(matches)}')

    # Store the URLs in a pandas DataFrame
    new_df = pd.DataFrame(full_urls, columns=['URL'])

    # Append new URLs to the existing DataFrame
    if df is not None:
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df
    return df, len(matches)


def extract_URLs(url, searchTerm, projectname, outputFileName: str, isNAV: bool = False):
    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=['URL'])

    # Create a folder in the parent directory of this file if it doesn't exist
    os.makedirs(projectname, exist_ok=True)

    # Create a folder inside the previous folder for the HTMLs
    os.makedirs(os.path.join(projectname, 'html_crawled'), exist_ok=True)

    page = 1
    while True:
        folder = os.path.join(projectname, 'html_crawled')
        df, match_count = parse_resultpage(url, searchTerm, folder, page, df, isNAV)
        if match_count == 0:
            print("No more results found. Stopping.")
            break
        page += 1
        time.sleep(random.uniform(200, 500) / 1000)

    # Save the DataFrame as a CSV file inside the folder
    df.to_csv(os.path.join(projectname, outputFileName), index=False)
    print("Crawling completed. Results saved to ", os.path.join(projectname, outputFileName))
    return df


def getURLsFromPredefinedSearch():
    urlBase = 'https://www.finn.no/realestate/lettings/search.html?lat=59.922591746076556&lon=10.73632512241602&radius=7000&price_to=18500&price_from=13000&start_month=202507&start_month=202508&stored-id=79416555&start_month=202509&area_from=30'
    regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
    extract_URLs(urlBase, regex, "leie", "0_URLs.csv")


if __name__ == "__main__":
    getURLsFromPredefinedSearch()
