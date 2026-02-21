import re

try:
    from main.extractors.parsing_helpers_common import getSize, getSizeHelper, getAllSizes
except ImportError:
    from extractors.parsing_helpers_common import getSize, getSizeHelper, getAllSizes


def getBuyPrice(soup):
    # First try regular property price
    pricing_section = soup.find('div', {'data-testid': 'pricing-total-price'})
    if pricing_section:
        total_price_match = re.search(r'([\d\xa0\s]+) kr', pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace('\xa0', '').replace(' ', '')
            try:
                return int(price_str)
            except ValueError:
                pass

    # Try "Prisantydning" for properties without total price
    pricing_section = soup.find('div', {'data-testid': 'pricing-incicative-price'})
    if pricing_section:
        total_price_match = re.search(r'([\d\xa0\s]+) kr', pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace('\xa0', '').replace(' ', '')
            try:
                return int(price_str)
            except ValueError:
                pass

    # Check if it's a planned property ("Pris kommer")
    # For planned properties, we can't extract a numeric price yet
    nøkkelinfo = soup.find('section', {'aria-label': 'Nøkkelinfo'})
    if nøkkelinfo and 'Pris kommer' in nøkkelinfo.get_text():
        return None  # Price not yet available for planned properties

    return None


def getAddress(soup):
    # Try regular property address format first
    address_element = soup.find('span', {'data-testid': 'object-address'})
    if address_element:
        full_address = address_element.get_text().strip()
        if ',' in full_address:
            address, area_part = map(str.strip, full_address.split(',', 1))
            area = GetArea(area_part)
        else:
            address = None
            area = GetArea(full_address)
        return address, area

    # Try planned property format (title contains address)
    title_element = soup.find('h1')
    if title_element:
        title = title_element.get_text().strip()
        # For planned properties, title is often just the address
        # Extract postnummer from anywhere in the page
        full_text = soup.get_text()
        postnummer_match = re.search(r'\b(\d{4})\s+', full_text)
        area = postnummer_match.group(1) if postnummer_match else None
        return title, area

    return None, None


def GetArea(part):
    area = part.strip()
    area_match = re.search(r'(\d+)', area)
    area = area_match.group(1) if area_match else None
    return area


def getPriceHelper(pricing_section, term):
    text = pricing_section.get_text().strip() if pricing_section else ""

    rent_price_match = re.search(rf'{term}\s*([\d\xa0\s]+)', text)
    if rent_price_match:
        rent_price = removeSpaces(rent_price_match.group(1))
        return rent_price


def getRentPrice(soup):
    pricing_sections = soup.find('div', {'data-testid': 'pricing-common-monthly-cost'})
    rent_price = getPriceHelper(pricing_sections, "Månedsleie") if pricing_sections else None

    deposit_sections = soup.find('div', {'data-testid': 'pricing-deposit'})
    deposit_price = getPriceHelper(deposit_sections, "Depositum")

    return {
        'monthly': rent_price,
        'deposit': deposit_price
    }


def getDate(soup):
    output = ""
    timespan_element = soup.find('div', {'data-testid': 'info-timespan'})
    if timespan_element:
        date_element = timespan_element.find('dd', class_='m-0 font-bold')
        if date_element:
            output = date_element.get_text(strip=True)
    if output:
        output = output.strip().split('-')
        return {
            'start': output[0],
            'end': output[1] if len(output) > 1 else None
        }
    return None


def removeSpaces(string):
    return string.replace('\xa0', '').replace(' ', '')


def getStatus(soup):
    """
    Extracts the status from the property listing.
    Returns the status text if found (e.g., 'Solgt'), otherwise None.

    The element appears as: <div class="...bg-[--w-color-badge-warning-background]...">Solgt</div>
    """
    statuses = ["warning", "negative", "info"]
    status_text = None

    for status in statuses:
        searchString = f"!text-m mb-24 py-4 px-8 border-0 rounded-4 text-xs inline-flex bg-[--w-color-badge-{status}-background] s-text"
        element = soup.find('div', class_=searchString)
        if element:
            status_text = element.get_text(strip=True)
            break

    return status_text
