"""
Central filters for exports and API usage.

Set MAX_PRICE to an integer (e.g. 7000000) to exclude expensive ads from:
- Location API calls
- Google Sheets exports

Set to None to disable price filtering.

Set INCLUDE_UNLISTED to True to include listings that are no longer in search
results (unlisted). Set to False to exclude them from database and exports.
"""

MAX_PRICE = 7500000
INCLUDE_UNLISTED = True

# Reuse travel-time calculations for nearby listings by linking them to a donor
# Finnkode (TRAVEL_COPY_FROM_FINNKODE) instead of making duplicate API calls.
# Example: if set to 120, listings within 120 meters can share one donor listing.
# Set to 0 to disable proximity-based reuse.
TRAVEL_REUSE_WITHIN_METERS = 500
