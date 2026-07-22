from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.dnb import crawl, parse

FIXTURES = Path(__file__).parent / "fixtures" / "dnb"
SEARCH_PAGE = Path("data/dnbeiendom/html_crawled/page1.html")

# The exact search URL legacy `_build_search_url()` produced, frozen from legacy
# at deletion, 2026-07-22 (hardcoded region GUIDs/estate types + filter suffix
# from config constants; no env input). Golden the ported builder must match.
LEGACY_DNB_SEARCH_URL = (
    "https://dnbeiendom.no/bolig?estateStatus=project_false"
    "&locations=BUSKERUD_ae0fe87e-0ba2-46b7-9164-5ee26c4fc85b"
    "&locations=AKERSHUS_fe2e9e2c-620e-4190-9af0-a5baa93abc1f"
    "&locations=OSLO_e6cde8d6-578c-4d73-b94e-08d59bb7ce4c"
    "&estateTypes=Leilighet&estateTypes=Enebolig&estateTypes=Tomannsbolig"
    "&estateTypes=Rekkehus&estateTypes=Landbruk&estateTypes=Sm%C3%A5bruk"
    "&priceSuggestion=max_7500000&primaryRoomArea=min_70"
)

# The sorted listing URLs legacy `_extract_listing_urls_from_html()` returned on
# the archived real search page (data/dnbeiendom/html_crawled/page1.html),
# frozen from legacy at deletion, 2026-07-22.
LEGACY_DNB_URLS_REAL = [
    "https://dnbeiendom.no/bolig/akershus/bærum/bekkestua/gamle-ringeriksvei-21c/404260037",
    "https://dnbeiendom.no/bolig/akershus/bærum/høvik/fjordveien-88/418260029",
    "https://dnbeiendom.no/bolig/akershus/lillestrøm/fetsund/rognebærlia-43/303260038",
    "https://dnbeiendom.no/bolig/akershus/lillestrøm/skedsmokorset/prost-holms-vei-261/303260019",
    "https://dnbeiendom.no/bolig/akershus/lørenskog/doktor-wendts-gate-12/304260071",
    "https://dnbeiendom.no/bolig/akershus/lørenskog/fjellhamar/svend-foyns-vei-5/304260022",
    "https://dnbeiendom.no/bolig/akershus/nannestad/gaupevegen-23/302250252",
    "https://dnbeiendom.no/bolig/akershus/nittedal/laboratorieveien-40/309260039",
    "https://dnbeiendom.no/bolig/akershus/nordre-follo/langhus/haugbro-terrasse-128/207260036",
    "https://dnbeiendom.no/bolig/akershus/nordre-follo/ski/gamle-asvei-38b/304260044",
    "https://dnbeiendom.no/bolig/akershus/rælingen/blystadlia/elgtrakket-11a/304260112",
    "https://dnbeiendom.no/bolig/akershus/rælingen/fjerdingby/veslejordet-3/303260121",
    "https://dnbeiendom.no/bolig/akershus/ullensaker/jessheim/fjellvegen-42/303250383",
    "https://dnbeiendom.no/bolig/akershus/ullensaker/jessheim/søndre-dølibekken-6/302260013",
    "https://dnbeiendom.no/bolig/buskerud/drammen/krokstadelva/egil-halandsvei-32/622260022",
    "https://dnbeiendom.no/bolig/buskerud/drammen/lauritz-hervigs-vei-37c/512260101",
    "https://dnbeiendom.no/bolig/buskerud/modum/vikersund/badeveien-10a/513250138",
    "https://dnbeiendom.no/bolig/buskerud/sigdal/eggedal/risleliveien-107/513260002",
    "https://dnbeiendom.no/bolig/buskerud/øvre-eiker/skotselv/ringeriksveien-794/513260010",
    "https://dnbeiendom.no/bolig/oslo/bjerke/veitvetveien-28/324250149",
    "https://dnbeiendom.no/bolig/oslo/gamle-oslo/sverres-gate-2/415260065",
    "https://dnbeiendom.no/bolig/oslo/helsfyr-sinsen/knut-alvssons-vei-13/325260061",
    "https://dnbeiendom.no/bolig/oslo/helsfyr-sinsen/st-jørgens-vei-14/331260049",
    "https://dnbeiendom.no/bolig/oslo/sagene-torshov/sandakerveien-104/306260031",
]

# The full field dict legacy `extract_fields_from_entry()` produced from the
# JSON-LD in fixtures/dnb/listing1.html, frozen from legacy at deletion,
# 2026-07-22. Includes legacy's IMAGE_URL quirk of keeping the raw ImageObject
# dict (not a URL string) — the ported parser must reproduce it verbatim.
LEGACY_DNB_ROW = {
    "URL": "https://dnbeiendom.no/bolig/Akershus/As/Dagny-fridrichsens-vei-44/208250077",
    "Title": "Dagny Fridrichsens vei 44, ÅS",
    "Description": "Lekkert, påkostet rekkehus fra 2024 | Fin uteplass | Garasjeplass med el-billader| Felles treningsstudio | Sentralt",
    "IMAGE_URL": {
        "@type": "ImageObject",
        "url": "https://dnb-nextgen-cdn-prod.azureedge.net/property-images/fbd1cfe7-ac6c-4216-5fc4-08de2074a218/fbd1cfe7-ac6c-4216-5fc4-08de2074a218.jpg?hash=B9630CEE8CE65BA3D6D98BF92D51AA81DDE59F0A545629FD8A197A1C3FC5664C",
        "caption": "Senior Eiendomsmegler Ole-Kristian Sverre har gleden av å presentere Dagny Fridrichsens vei 44! ",
    },
    "StreetAddress": "Dagny Fridrichsens vei 44",
    "Locality": "ÅS",
    "Region": "AKERSHUS",
    "PostalCode": "1435",
    "PropertyType": "Enebolig",
    "Latitude": 59.651226,
    "Longitude": 10.80955,
    "FloorSize": 81,
    "NumberOfRooms": None,
    "NumberOfBedrooms": 2,
    "Price": 4750000,
}


def test_search_url_contains_all_region_guids():
    d = load_domain()
    url = crawl.build_search_url(d)
    for guid in d.dnb.region_guids:
        assert guid in url


def test_search_url_matches_legacy():
    """The ported build_search_url must reproduce the byte-for-byte URL the
    legacy `_build_search_url()` emitted (frozen literal LEGACY_DNB_SEARCH_URL)."""
    assert crawl.build_search_url(load_domain()) == LEGACY_DNB_SEARCH_URL


def test_extract_urls_from_real_search_page():
    html = SEARCH_PAGE.read_text(errors="replace")
    urls = crawl.extract_listing_urls(html)
    assert len(urls) >= 5
    assert all(u.startswith("https://") for u in urls)


def test_extract_urls_matches_legacy_on_real_page():
    """The ported extractor must return exactly the sorted URL set legacy
    `_extract_listing_urls_from_html()` produced on the archived real page
    (frozen literal LEGACY_DNB_URLS_REAL)."""
    html = SEARCH_PAGE.read_text(errors="replace")
    new_urls = crawl.extract_listing_urls(html)

    assert new_urls == LEGACY_DNB_URLS_REAL


def test_parse_listing_jsonld():
    html = (FIXTURES / "listing1.html").read_text(errors="replace")
    row = parse.parse_listing(html, "https://dnbeiendom.no/x")
    assert row is not None
    assert row.get("Latitude") and row.get("Longitude")
    assert row.get("StreetAddress")
    assert row.get("Price")
    assert row.get("PropertyType") == "Enebolig"


def test_parse_listing_matches_legacy_extract_fields():
    """Pin skannonser.ingest.dnb.parse.parse_listing's output dict against the
    frozen legacy extract_fields_from_entry result (LEGACY_DNB_ROW) on the same
    JSON-LD entry -- same keys, same values, including legacy's IMAGE_URL quirk
    of storing the raw ImageObject dict rather than a URL string."""
    html = (FIXTURES / "listing1.html").read_text(errors="replace")
    new_row = parse.parse_listing(html, "https://dnbeiendom.no/x")

    assert new_row == LEGACY_DNB_ROW


def test_parse_listing_returns_none_without_jsonld():
    assert parse.parse_listing("<html><body>no jsonld here</body></html>", "https://dnbeiendom.no/x") is None
