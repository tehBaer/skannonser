"""
Central filters for exports and API usage.

Set MAX_PRICE to an integer (e.g. 7000000) to exclude expensive ads from:
- Location API calls
- Google Sheets exports

Set to None to disable price filtering.

Set INCLUDE_UNLISTED to True to include listings that are no longer in search
results (unlisted). Set to False to exclude them from database and exports.
"""

MAX_PRICE = 8500000
INCLUDE_UNLISTED = True
