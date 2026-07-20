"""FINN ad HTML -> NormalizedListing.

Direct port of every field extractor in
`main/extractors/parsing_helpers_common.py` plus the assembly logic of
`main/extractors/extraction_eiendom.py:extract_eiendom_data` (lines 13-56).
Output is byte-identical to legacy's for the same HTML.

`finnkode` and `url` are NOT re-derived from the HTML (legacy pulled
`Finnkode` out of its own `url` argument via `url.split('finnkode=')[1]`,
line 29 of `extraction_eiendom.py`) -- the caller (the crawler) already has
both, robustly parsed, so `parse_ad` just takes them as arguments.
"""
import re

from bs4 import BeautifulSoup

from skannonser.ingest.base import NormalizedListing

# ---------------------------------------------------------------------------
# Field extractors -- ports of main/extractors/parsing_helpers_common.py
# ---------------------------------------------------------------------------


def _get_size_helper(element) -> str:
    usable_area = element.get_text().strip() if element else ""
    if usable_area:
        usable_area_match = re.search(r"([\d\s\xa0]+)\s*m²", usable_area)
        if usable_area_match:
            usable_area = usable_area_match.group(1).replace("\xa0", "").replace(" ", "")
        else:
            usable_area = ""
    return usable_area


def _get_size(soup) -> str:
    """Port of legacy `getSize` -- unused by `extract_eiendom_data`'s
    assembly (which only calls `getAllSizes`), kept for fidelity with the
    brief's "port every field extractor" instruction."""
    element = soup.find("div", {"data-testid": "info-usable-area"})
    output = _get_size_helper(element)
    if not output:
        element = soup.find("div", {"data-testid": "info-usable-i-area"})
        output = _get_size_helper(element)
    return output


def _get_all_sizes(soup) -> dict:
    sizes = {}
    test_ids = [
        "info-usable-area",
        "info-usable-i-area",
        "info-primary-area",
        "info-gross-area",
        "info-usable-e-area",
        "info-open-area",
        "info-usable-b-area",
        "info-plot-area",
    ]

    for test_id in test_ids:
        element = soup.find("div", {"data-testid": test_id})
        sizes[test_id] = _get_size_helper(element)

    return sizes


def _get_buy_price(soup):
    pricing_section = soup.find("div", {"data-testid": "pricing-total-price"})
    if pricing_section:
        total_price_match = re.search(r"([\d\xa0\s]+) kr", pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace("\xa0", "").replace(" ", "")
            try:
                return int(price_str)
            except ValueError:
                pass

    pricing_section = soup.find("div", {"data-testid": "pricing-incicative-price"})
    if pricing_section:
        total_price_match = re.search(r"([\d\xa0\s]+) kr", pricing_section.get_text())
        if total_price_match:
            price_str = total_price_match.group(1).replace("\xa0", "").replace(" ", "")
            try:
                return int(price_str)
            except ValueError:
                pass

    nøkkelinfo = soup.find("section", {"aria-label": "Nøkkelinfo"})
    if nøkkelinfo and "Pris kommer" in nøkkelinfo.get_text():
        return None

    return None


def _get_area(part: str):
    area = part.strip()
    # Norwegian post numbers are 4 digits and may begin with 0 (e.g. 0581).
    area_match = re.search(r"\b(\d{4})\b", area)
    return area_match.group(1) if area_match else None


def _get_address(soup):
    address_element = soup.find("span", {"data-testid": "object-address"})
    if address_element:
        full_address = address_element.get_text().strip()
        if "," in full_address:
            address, area_part = map(str.strip, full_address.split(",", 1))
            area = _get_area(area_part)
        else:
            address = None
            area = _get_area(full_address)
        return address, area

    title_element = soup.find("h1")
    if title_element:
        title = title_element.get_text().strip()
        full_text = soup.get_text()
        postnummer_match = re.search(r"\b(\d{4})\s+", full_text)
        area = postnummer_match.group(1) if postnummer_match else None
        return title, area

    return None, None


def _get_status(soup):
    statuses = ["warning", "negative", "info"]
    status_text = None

    for status in statuses:
        search_string = (
            f"!text-m mb-24 py-4 px-8 border-0 rounded-4 text-xs inline-flex "
            f"bg-[--w-color-badge-{status}-background] s-text"
        )
        element = soup.find("div", class_=search_string)
        if element:
            status_text = element.get_text(strip=True)
            break

    return status_text


def _get_construction_year(soup):
    element = soup.find("div", {"data-testid": "info-construction-year"})
    if not element:
        return ""
    match = re.search(r"(\d{4})", element.get_text())
    return match.group(1) if match else ""


def _get_plot_ownership(soup):
    element = soup.find("div", {"data-testid": "info-plot-area"})
    if not element:
        return ""
    match = re.search(r"\(([^)]+)\)", element.get_text())
    return match.group(1).strip() if match else ""


def _get_property_type(soup):
    element = soup.find("div", {"data-testid": "info-property-type"})
    if not element:
        return ""

    value_element = element.find("dd")
    if value_element:
        return value_element.get_text(strip=True)

    return element.get_text(strip=True)


def _get_image_url(soup):
    """Extract primary listing image URL from FINN ad HTML."""
    if not soup:
        return ""

    # Preferred: social preview metadata.
    meta_selectors = [
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "og:image"}),
        ("meta", {"name": "twitter:image"}),
    ]
    for tag_name, attrs in meta_selectors:
        meta = soup.find(tag_name, attrs=attrs)
        if not meta:
            continue
        content = (meta.get("content") or "").strip()
        if content.startswith("http://") or content.startswith("https://"):
            return content

    # Fallback: image URLs embedded in scripts.
    script_text = soup.get_text(" ", strip=False)
    if script_text:
        match = re.search(r"https://images\.finncdn\.no[^\"'\s>]+", script_text)
        if match:
            return match.group(0)

    return ""


# ---------------------------------------------------------------------------
# Assembly -- port of extraction_eiendom.py:extract_eiendom_data lines 13-56
# ---------------------------------------------------------------------------


def parse_ad(html: str, finnkode: str, url: str) -> NormalizedListing:
    soup = BeautifulSoup(html, "html.parser")

    address, area = _get_address(soup)
    sizes = _get_all_sizes(soup)
    construction_year = _get_construction_year(soup)
    plot_ownership = _get_plot_ownership(soup)
    property_type = _get_property_type(soup)
    image_url = _get_image_url(soup)
    buy_price = _get_buy_price(soup)
    tilgjengelig = _get_status(soup)

    row = {
        "Finnkode": finnkode,
        "Tilgjengelighet": tilgjengelig,
        "Adresse": address,
        "Postnummer": area,
        "Pris": buy_price,
        "URL": url,
        "IMAGE_URL": image_url,
        "Primærrom": sizes.get("info-primary-area"),
        "Internt bruksareal (BRA-i)": sizes.get("info-usable-i-area"),
        "Bruksareal": sizes.get("info-usable-area"),
        "Eksternt bruksareal (BRA-e)": sizes.get("info-usable-e-area"),
        "Innglasset balkong (BRA-b)": sizes.get("info-usable-b-area"),
        "Balkong/Terrasse (TBA)": sizes.get("info-open-area"),
        "Tomteareal": sizes.get("info-plot-area"),
        "Eierskap, tomt": plot_ownership,
        "Boligtype": property_type,
        "Bruttoareal": sizes.get("info-gross-area"),
        "Byggeår": construction_year,
    }

    return NormalizedListing(**row)
