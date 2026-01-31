import subprocess
import pandas as pd
import requests
from bs4 import BeautifulSoup

from main.parsing_helpers_rental import getSize, getRentPrice

subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)

url = 'https://www.finn.no/realestate/lettings/ad.html?finnkode=328084574'


def extract_rental_data(url):
    # Step 1: Send a GET request to the URL
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Step 2: Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Save the HTML content to a file
    with open('output.html', 'w', encoding='utf-8') as file:
        file.write(soup.prettify())

    # Step 3: Extract the relevant data
    properties = []
    # price = OLD_getBuyPrice(soup)
    # address, area = getAddress(soup)
    size = getSize(soup)
    price = getRentPrice(soup)

    # properties.append({'Prisantydning': price, 'Adresse': address, 'Postnummer': area, 'Størrelse': size})
    properties.append({'Prisantydning': price})

    # Step 4: Store the extracted data in a pandas DataFrame
    df = pd.DataFrame(properties)

    return df


output = extract_rental_data(url)
# Step 5: Save the DataFrame as a CSV file

output.to_csv('test.csv', index=False)
