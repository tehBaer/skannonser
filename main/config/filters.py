"""
Central filters for exports and API usage.

Set MAX_PRICE to an integer (e.g. 7000000) to exclude expensive ads from:
- Location API calls
- Google Sheets exports

Set to None to disable price filtering.
"""

MAX_PRICE = 8500000
