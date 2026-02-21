import os
import subprocess
import pandas as pd
from pandas import DataFrame

try:
    from main.extractors.extraction import load_or_fetch_ad_html
    from main.extractors.parsing_helpers_jobs_FINN import FinnParser
    from main.extractors.parsing_helpers_jobs_NAV import NAVParser
    from main.extractors.parsing_helpers_rental import *
except ImportError:
    from extraction import load_or_fetch_ad_html
    from parsing_helpers_jobs_FINN import FinnParser
    from parsing_helpers_jobs_NAV import NAVParser
    from parsing_helpers_rental import *


def extract_job_data_NAV(url, index, projectName, auto_save_new=True, force_save=False):
    try:
        soup = load_or_fetch_ad_html(url, projectName, auto_save_new, force_save, isNAV=True)
    except Exception as e:
        print(f"Error fetching content for URL {url}: {e}")
        #     throw exception
        raise
    parser = NAVParser(soup)
    data = {
        # 'Index': index,
        'Finnkode': url.rstrip('/').split('/')[-1],
        'URL': url,
        'Selskap': parser.get_company(),
        'Tittel': parser.get_ad_title(),
        'Stillingstittel': parser.get_job_title(),
        'Posisjoner' : parser.get_job_positions(),
        'Søknadsfrist' : parser.get_deadline(),
        'Innhold' : parser.get_textcontent(),
        # 'Industri' : parser.get_industry(),
    }
    print(f'Index {index}: {data}')

    return data


def extractJobDataFromAds_NAV(projectName: str, urls: DataFrame, outputFileName: str):
    # Create the directory if it doesn't exist
    os.makedirs(projectName, exist_ok=True)

    collectedData = []

    # Loop through each URL and extract job data
    try:
        # Create a folder inside the previous folder for the htmls
        os.makedirs(f'{projectName}/html_extracted', exist_ok=True)
        for index, url in enumerate(urls['URL']):
            try:
                data = extract_job_data_NAV(url, index, projectName)
                collectedData.append(data)
                pass
            except Exception as e:
                print(f'Error processing URL at index {index}: {url} - {e}')
    finally:
        # Save the combined data to a new CSV file in the output directory
        df = pd.DataFrame(collectedData)
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
        print(f"Data extraction completed. {len(collectedData)} records saved to {projectName}/{outputFileName}")
