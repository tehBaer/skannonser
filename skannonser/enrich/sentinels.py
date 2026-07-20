"""Travel sentinel codes — negative integers stored in DB/sheet to mark known
failures. Rows with these values are NOT retried by the pipeline.

Single home for these constants/helpers (ported from `main/post_process.py:10-49`,
which duplicated them alongside an equivalent copy in `main/location_features.py`).
"""

TRAVEL_NO_ROUTES = -1
TRAVEL_UNREALISTIC = -2
TRAVEL_API_ERROR = -3
_TRAVEL_SENTINELS = frozenset({TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC, TRAVEL_API_ERROR})

_SENTINEL_LABELS = {
    TRAVEL_NO_ROUTES: "no routes",
    TRAVEL_UNREALISTIC: "unrealistic",
    TRAVEL_API_ERROR: "API error",
}


def is_travel_sentinel(value) -> bool:
    """Return True if value is a known travel-time failure code."""
    try:
        if value is None:
            return False
        return int(value) in _TRAVEL_SENTINELS
    except (TypeError, ValueError):
        return False


def sentinel_label(value) -> str:
    try:
        return _SENTINEL_LABELS.get(int(value), "failed")
    except (TypeError, ValueError):
        return "failed"
