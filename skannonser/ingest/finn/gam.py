"""The GAM ad-targeting key/value pairs FINN ships on every ad page.

Shared by `parse.py` (legacy `NormalizedListing` port) and `parse_details.py`
(the group-A/B/C enrichment): typed data FINN publishes itself, which is the
only remaining source for several values it no longer renders into the
key-info markup. Null-tolerant by contract -- any structural surprise yields
`{}` rather than raising, so a caller can always treat a miss as "absent".
"""
import json

__all__ = ["gam_targeting"]


def gam_targeting(soup) -> dict[str, list]:
    """`{key: value_list}` from the `advertising-initial-state` JSON blob.
    `{}` when the script tag is missing, unparseable, or not shaped as
    expected."""
    script = soup.find("script", {"id": "advertising-initial-state"})
    if script is None or not script.string:
        return {}
    try:
        data = json.loads(script.string)
        targeting = data["config"]["adServer"]["gam"]["targeting"]
        return {
            t["key"]: t["value"]
            for t in targeting
            if isinstance(t, dict) and "key" in t and isinstance(t.get("value"), list)
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}
