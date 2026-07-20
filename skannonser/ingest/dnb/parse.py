"""DNB Eiendom listing-page JSON-LD parser.

Direct port of `parse_listing_jsonld` and `extract_fields_from_entry`
(`main/extractors/extract_dnbeiendom_ads.py:21-106`). Output dict keys and
values match legacy exactly, including its `IMAGE_URL` quirk: when the
JSON-LD `image` field is a list, legacy takes `image[0]` as-is -- on real
DNB pages that list holds `ImageObject` dicts (not bare URL strings), so
`IMAGE_URL` ends up holding a dict, not a URL. That is legacy's actual
behavior on real data (verified against a live fetched listing page), so it
is preserved here rather than "fixed."

`url` is accepted for interface symmetry with the FINN parser
(`skannonser.ingest.dnb.crawl`'s caller already has it, robustly, from the
crawl step) but is NOT used to populate the `URL` field -- legacy derives
that solely from the JSON-LD entry (`entry.get('url') or entry.get('@id')`),
never from the fetch URL, so this port does the same for byte-identical
output. It is deliberately unused for that reason.
"""

import json

from bs4 import BeautifulSoup

_TYPE_MAP = {
    "Apartment": "Leilighet",
    "House": "Enebolig",
    "Accommodation": "Fritidsbolig",
    "Landform": "Tomt",
    "Place": "Annet",
}


def _parse_listing_jsonld(soup: BeautifulSoup) -> dict | None:
    scripts = soup.find_all("script", type="application/ld+json")
    for s in scripts:
        text = s.string
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue

        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("@type") == "RealEstateListing":
                return entry
            if entry.get("@type") == "ItemList" and entry.get("itemListElement"):
                for li in entry.get("itemListElement"):
                    item = li.get("item") or li.get("itemListElement") or li
                    if isinstance(item, dict) and item.get("@type") == "RealEstateListing":
                        return item
    return None


def _extract_fields_from_entry(entry: dict) -> dict:
    out = {}
    out["URL"] = entry.get("url") or entry.get("@id")
    out["Title"] = entry.get("name")
    out["Description"] = entry.get("description")

    image = entry.get("image")
    if isinstance(image, list):
        out["IMAGE_URL"] = image[0]
    else:
        out["IMAGE_URL"] = image

    about = entry.get("about") or {}
    addr = about.get("address") or {}
    out["StreetAddress"] = addr.get("streetAddress")
    out["Locality"] = addr.get("addressLocality")
    out["Region"] = addr.get("addressRegion")
    out["PostalCode"] = addr.get("postalCode")

    about_type = about.get("@type", "")
    out["PropertyType"] = _TYPE_MAP.get(about_type, about_type or "")

    geo = about.get("geo") or {}
    out["Latitude"] = geo.get("latitude")
    out["Longitude"] = geo.get("longitude")

    floor = about.get("floorSize") or {}
    out["FloorSize"] = floor.get("value")

    out["NumberOfRooms"] = about.get("numberOfRooms")
    out["NumberOfBedrooms"] = about.get("numberOfBedrooms")

    offers = entry.get("offers") or {}
    price = None
    if isinstance(offers, dict):
        specs = offers.get("priceSpecification")
        if isinstance(specs, list):
            for spec in specs:
                name = spec.get("name", "").lower()
                if "prisantydning" in name or "price" in name:
                    price = spec.get("price")
                    break
            if price is None and specs:
                price = specs[0].get("price")
        else:
            price = offers.get("price")
    out["Price"] = price

    return out


def parse_listing(html: str, url: str) -> dict | None:
    """Parse a DNB listing page's JSON-LD `RealEstateListing` into legacy's
    `extract_fields_from_entry` dict shape, or `None` if no such entry is
    present (mirrors legacy's "no JSON-LD" failure path in `extract_all`).
    """
    del url  # unused -- see module docstring
    soup = BeautifulSoup(html, "html.parser")
    entry = _parse_listing_jsonld(soup)
    if entry is None:
        return None
    return _extract_fields_from_entry(entry)
