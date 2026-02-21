import re


def getSizeHelper(soup, element):
    usable_area = element.get_text().strip() if element else ""
    if usable_area:
        usable_area_match = re.search(r'([\d\s\xa0]+)\s*m²', usable_area)
        if usable_area_match:
            usable_area = usable_area_match.group(1).replace('\xa0', '').replace(' ', '')
        else:
            usable_area = ""
    return usable_area


def getSize(soup):
    element = soup.find('div', {'data-testid': 'info-usable-area'})
    output = getSizeHelper(soup, element)
    if not output:
        element = soup.find('div', {'data-testid': 'info-usable-i-area'})
        output = getSizeHelper(soup, element)
    return output


def getAllSizes(soup):
    sizes = {}
    test_ids = [
        'info-usable-area',
        'info-usable-i-area',
        'info-primary-area',
        'info-gross-area',
        'info-usable-e-area',
        'info-open-area',
        'info-usable-b-area',
        'info-plot-area'
    ]

    for test_id in test_ids:
        element = soup.find('div', {'data-testid': test_id})
        sizes[test_id] = getSizeHelper(soup, element)

    return sizes


def getBuyPrice(soup):
    pricing_section = soup.find('div', {'data-testid': 'pricing-total-price'})
    if pricing_section:
        total_price_match = re.search(r'([\d\xa0\s]+) kr', pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace('\xa0', '').replace(' ', '')
            try:
                return int(price_str)
            except ValueError:
                pass

    pricing_section = soup.find('div', {'data-testid': 'pricing-incicative-price'})
    if pricing_section:
        total_price_match = re.search(r'([\d\xa0\s]+) kr', pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace('\xa0', '').replace(' ', '')
            try:
                return int(price_str)
            except ValueError:
                pass

    nøkkelinfo = soup.find('section', {'aria-label': 'Nøkkelinfo'})
    if nøkkelinfo and 'Pris kommer' in nøkkelinfo.get_text():
        return None

    return None


def getAddress(soup):
    address_element = soup.find('span', {'data-testid': 'object-address'})
    if address_element:
        full_address = address_element.get_text().strip()
        if ',' in full_address:
            address, area_part = map(str.strip, full_address.split(',', 1))
            area = getArea(area_part)
        else:
            address = None
            area = getArea(full_address)
        return address, area

    title_element = soup.find('h1')
    if title_element:
        title = title_element.get_text().strip()
        full_text = soup.get_text()
        postnummer_match = re.search(r'\b(\d{4})\s+', full_text)
        area = postnummer_match.group(1) if postnummer_match else None
        return title, area

    return None, None


def getArea(part):
    area = part.strip()
    area_match = re.search(r'(\d+)', area)
    area = area_match.group(1) if area_match else None
    return area


def getStatus(soup):
    statuses = ["warning", "negative", "info"]
    status_text = None

    for status in statuses:
        searchString = f"!text-m mb-24 py-4 px-8 border-0 rounded-4 text-xs inline-flex bg-[--w-color-badge-{status}-background] s-text"
        element = soup.find('div', class_=searchString)
        if element:
            status_text = element.get_text(strip=True)
            break

    return status_text


def getConstructionYear(soup):
    element = soup.find('div', {'data-testid': 'info-construction-year'})
    if not element:
        return ""
    match = re.search(r'(\d{4})', element.get_text())
    return match.group(1) if match else ""


def getPlotOwnership(soup):
    element = soup.find('div', {'data-testid': 'info-plot-area'})
    if not element:
        return ""
    match = re.search(r'\(([^)]+)\)', element.get_text())
    return match.group(1).strip() if match else ""