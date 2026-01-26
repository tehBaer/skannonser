import re


def getBuyPrice(soup):
    pricing_section = soup.find('div', {'data-testid': 'pricing-total-price'})
    total_price_match = re.search(r'([\d\xa0\s]+) kr', pricing_section.get_text())
    total_price = None
    if total_price_match:
        total_price = total_price_match.group(1).replace('\xa0', '').replace(' ', '')
    return total_price


def getAddress(soup):
    address_element = soup.find('span', {'data-testid': 'object-address'})
    if address_element:
        full_address = address_element.get_text().strip()
        if ',' in full_address:
            address, area_part = map(str.strip, full_address.split(',', 1))
            area = GetArea(area_part)
        else:
            address = None
            area = GetArea(full_address)
    else:
        address = None
        area = None
    return address, area


def GetArea(part):
    area = part.strip()
    area_match = re.search(r'(\d+)', area)
    area = area_match.group(1) if area_match else None
    return area


def getSize(soup):
    element = soup.find('div', {'data-testid': 'info-usable-area'})
    output = getSizeHelper(soup, element)
    if not output:
        element = soup.find('div', {'data-testid': 'info-usable-i-area'})
        output = getSizeHelper(soup, element)
    return output


def getSizeHelper(soup, element):
    usable_area = element.get_text().strip() if element else ""
    # print(usable_area)
    if usable_area:
        usable_area_match = re.search(r'(\d+)\s*m²', usable_area)
        usable_area = usable_area_match.group(1) if usable_area_match else ""
    return usable_area


def getAllSizes(soup):
    sizes = {}
    test_ids = [
        'info-usable-area',
        'info-usable-i-area',
        'info-primary-area',
        'info-gross-area',
        'info-usable-e-area'
        'info-open-area'
    ]

    for test_id in test_ids:
        element = soup.find('div', {'data-testid': test_id})
        sizes[test_id] = getSizeHelper(soup, element)

    return sizes


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
            output= date_element.get_text(strip=True)
    if output:
    #     strip it, and split it on "-"
        output = output.strip().split('-')
        return {
            'start': output[0],
            'end': output[1] if len(output) > 1 else None
        }
    return None


def removeSpaces(string):
    return string.replace('\xa0', '').replace(' ', '')
