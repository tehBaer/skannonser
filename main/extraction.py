import os
import re
import time

import requests
from bs4 import BeautifulSoup

from enum import Enum


# class AdType(Enum):
#     PROPERTY = 1
#     JOB = 2


def download_and_save_ad_html(url, projectName, finnkode):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    # Save the HTML content to a file inside the folder
    html_file_path = f'{projectName}/html_extracted/{finnkode}.html'
    with open(html_file_path, 'w', encoding='utf-8') as file:
        file.write(str(soup))
        return soup


def load_or_fetch_ad_html(url, projectName, auto_save_new=True, force_save=False, isNAV=False):
    """
    Fetches ad data from the given URL and saves the HTML content if specified.
    :param url: The URL of the ad to extract data from.
    :param projectName: The name of the project folder to save the HTML content.
    :param force_save: If True, forces re-fetching of the ad data.
    :return: A dictionary containing extracted ad data.
    """
    if isNAV:
        match = re.search(r'stilling/([\w-]+)$', url)
    else:
        match = re.search(r'(\d+)(?!.*\d)', url)

    if not match:
        raise ValueError(f"Could not extract UID from URL: {url}")
    uid = match.group(1)
    html_file_path = f'{projectName}/html_extracted/{uid}.html'
    exists = os.path.exists(html_file_path)

    if (force_save):
        time.sleep(0.1)
        print(f"Force-saving HTML content for {uid}.")
        return download_and_save_ad_html(url, projectName, uid)

    elif (exists or not auto_save_new):
        with open(html_file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file.read(), 'html.parser')
            return soup
    else:
        time.sleep(0.1)
        print(f"Saving HTML content for {uid}.")
        return download_and_save_ad_html(url, projectName, uid)
