import os
import pandas as pd
from pandas import DataFrame

try:
    from main.extractors.ad_html_loader import load_or_fetch_ad_html
    from main.extractors.parsing_helpers_rental import *
except ImportError:
    from extractors.ad_html_loader import load_or_fetch_ad_html
    from extractors.parsing_helpers_rental import *


def extract_eiendom_data(url, index, projectName, total=None, auto_save_new=True, force_save=False):
    try:
        soup = load_or_fetch_ad_html(url, projectName, auto_save_new, force_save)
    except Exception as e:
        print(f"Error fetching content for URL {url}: {e}")
        raise

    address, area = getAddress(soup)
    sizes = getAllSizes(soup)
    buy_price = getBuyPrice(soup)
    tilgjengelig = getStatus(soup)

    finnkode = url.split('finnkode=')[1]
    data = {
        'Finnkode': finnkode,
        'Tilgjengelighet': tilgjengelig,
        'Adresse': address,
        'Postnummer': area,
        'Pris': buy_price,
        'URL': url,
        'Primærrom': sizes.get('info-primary-area'),
        'Internt bruksareal (BRA-i)': sizes.get('info-usable-i-area'),
        'Bruksareal': sizes.get('info-usable-area'),
        'Eksternt bruksareal (BRA-e)': sizes.get('info-usable-e-area'),
        'Balkong/Terrasse (TBA)': sizes.get('info-open-area'),
        'Bruttoareal': sizes.get('info-gross-area'),
    }
    if total:
        print(f"{index}/{total}: {finnkode}")
    else:
        print(f"{index}: {finnkode}")

    return data


def extractEiendomDataFromAds(projectName: str, urls: DataFrame, outputFileName: str):
    # Create the directory if it doesn't exist
    os.makedirs(projectName, exist_ok=True)

    collectedData = []
    failedUrls = []
    total_urls = len(urls)

    # Loop through each URL and extract eiendom data
    try:
        # Create a folder inside the previous folder for the htmls
        os.makedirs(f'{projectName}/html_extracted', exist_ok=True)
        for index, url in enumerate(urls['URL'], start=1):
            try:
                data = extract_eiendom_data(url, index, projectName, total=total_urls)
                collectedData.append(data)
            except Exception as e:
                finnkode = url.split('finnkode=')[1] if 'finnkode=' in url else 'unknown'
                error_type = type(e).__name__
                error_msg = str(e)
                
                print(f'\n❌ Error processing URL at index {index}: {url}')
                print(f'   Finnkode: {finnkode}')
                print(f'   Error type: {error_type}')
                print(f'   Error: {error_msg}')
                
                # Check if it's a planned property
                is_planned = '/realestate/planned/' in url
                if is_planned:
                    print(f'   Note: This is a PLANNED property (not yet built)')
                
                failedUrls.append({
                    'URL': url,
                    'Finnkode': finnkode,
                    'Index': index,
                    'Error_Type': error_type,
                    'Error_Message': error_msg,
                    'Is_Planned': is_planned
                })
    finally:
        # Save the combined data to a new CSV file in the output directory
        df = pd.DataFrame(collectedData)
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
        
        # Save failed URLs to a separate file
        if failedUrls:
            failed_df = pd.DataFrame(failedUrls)
            failed_df.to_csv(f'{projectName}/A_failed.csv', index=False)
            print(f"\n⚠️  {len(failedUrls)} URLs failed - saved to {projectName}/A_failed.csv")
            print(f"   Planned properties: {sum(1 for f in failedUrls if f['Is_Planned'])}")
            print(f"   Regular properties: {sum(1 for f in failedUrls if not f['Is_Planned'])}")
        
        print(f"\n✓ Data extraction completed. {len(collectedData)} records saved to {projectName}/{outputFileName}")
        return df


if __name__ == "__main__":
    extractEiendomDataFromAds('data/eiendom', pd.read_csv('data/eiendom/0_URLs.csv'), 'A_live.csv')
