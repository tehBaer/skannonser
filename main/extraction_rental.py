import os
import subprocess
import pandas as pd
from pandas import DataFrame
from main.extraction import load_or_fetch_ad_html
from main.parsing_helpers_rental import *

# Ensure the path to the virtual environment activation script is correct
# subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)


def extract_rental_data(url, index, projectName, auto_save_new=True, force_save=False):
    try:
        soup = load_or_fetch_ad_html(url, projectName, auto_save_new, force_save)
    except Exception as e:
        print(f"Error fetching content for URL {url}: {e}")
        #     throw exception
        raise
    address, area = getAddress(soup)
    sizes = getAllSizes(soup)
    prices = getRentPrice(soup)
    date = getDate(soup)

    statuses = ["warning", "negative"]
    tilgjengelig = None

    for status in statuses:
        searchString = f"!text-m mb-24 py-4 px-8 border-0 rounded-4 text-xs inline-flex bg-[--w-color-badge-{status}-background] s-text"
        element = soup.find('div', class_=searchString)
        if element:
            tilgjengelig = element.get_text(strip=True)
            break

    data = {
        # 'Index': index,
        'Finnkode': url.split('finnkode=')[1],
        'Tilgjengelighet': tilgjengelig,
        'Adresse': address,
        'Postnummer': area,
        'Leiepris': prices.get('monthly'),
        'Depositum': prices.get('deposit'),
        'URL': url,
        'Primærrom': sizes.get('info-primary-area'),
        'Internt bruksareal (BRA-i)': sizes.get('info-usable-i-area'),
        'Bruksareal': sizes.get('info-usable-area'),
        'Eksternt bruksareal (BRA-e)': sizes.get('info-usable-e-area'),
        'Balkong/Terrasse (TBA)': sizes.get('info-open-area'),
        'Bruttoareal': sizes.get('info-gross-area'),
        # 'Innflytting': date.get('start'),
        # 'Utflytting': date.get('end'),
    }
    print(f'Index {index}: {data}')

    return data


def extractRentalDataFromAds(projectName: str, urls: DataFrame, outputFileName: str):
    # Create the directory if it doesn't exist
    os.makedirs(projectName, exist_ok=True)

    collectedData = []

    # Loop through each URL and extract rental data
    try:
        # Create a folder inside the previous folder for the htmls
        os.makedirs(f'{projectName}/html_extracted', exist_ok=True)
        for index, url in enumerate(urls['URL']):
            try:
                data = extract_rental_data(url, index, projectName)
                collectedData.append(data)
            except Exception as e:
                print(f'Error processing URL at index {index}: {url} - {e}')
    finally:
        pass
        # Save the combined data to a new CSV file in the output directory
        df = pd.DataFrame(collectedData)
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
        print(f"Data extraction completed. {len(collectedData)} records saved to {projectName}/{outputFileName}")
        return df


if __name__ == "__main__":
    extractRentalDataFromAds('leie', pd.read_csv('leie/live_URLs.csv'), 'live_data.csv')
