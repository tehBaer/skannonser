"""
Central filters for exports and API usage.

Set MAX_PRICE to an integer (e.g. 7000000) to exclude expensive ads from:
- Location API calls
- Google Sheets exports
- DB-backed export/sync queries

Set to None to disable price filtering.

Set URL_MAX_PRICE to control crawler/search URL price caps (FINN/DNB).
This is intentionally separate from MAX_PRICE so you can crawl a wider set,
then export/sync a stricter subset from DB.
Set to None to disable URL price filtering.

Set INCLUDE_UNLISTED to True to include listings that are no longer in search
results (unlisted). Set to False to exclude them from database and exports.

Set MIN_BRA_I to an integer (e.g. 50) to require minimum
"Internt bruksareal (BRA-i)" in DB-backed exports.
Set to None to disable BRA-i filtering.
"""

SHEETS_MAX_PRICE = 7500000
URL_MAX_PRICE = 7500000
MIN_BRA_I = 50
INCLUDE_UNLISTED = True


# Shared search URL filters used by both FINN and DNB crawlers.
def get_finn_search_filter_params():
    """Return FINN query params derived from shared price/area filters."""
    params = {}
    if URL_MAX_PRICE is not None:
        params['price_to'] = str(int(URL_MAX_PRICE))
    if MIN_BRA_I is not None:
        params['area_from'] = str(int(MIN_BRA_I))
    return params


def get_dnb_search_filter_params():
    """Return DNB query params derived from shared price/area filters."""
    params = {}
    if URL_MAX_PRICE is not None:
        params['priceSuggestion'] = f"max_{int(URL_MAX_PRICE)}"
    if MIN_BRA_I is not None:
        params['primaryRoomArea'] = f"min_{int(MIN_BRA_I)}"
    return params

# Reuse travel-time calculations for nearby listings by linking them to a donor
# Finnkode (TRAVEL_COPY_FROM_FINNKODE) instead of making duplicate API calls.
# Example: if set to 120, listings within 120 meters can share one donor listing.
# Set to 0 to disable proximity-based reuse.
TRAVEL_REUSE_WITHIN_METERS = 300

# Guard rails for coordinate quality (defaults target Norway envelope).
COORD_LAT_MIN = 57.0
COORD_LAT_MAX = 72.0
COORD_LNG_MIN = 4.0
COORD_LNG_MAX = 32.0

# Reject clearly unrealistic travel durations from API responses and donor reuse.
MAX_TRAVEL_MINUTES = 360
