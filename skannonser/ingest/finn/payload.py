"""Decode the serialised app state a FINN ad page ships.

Two formats live side by side in the on-disk cache:

- current pages: ``window.__reactRouterContext.streamController.enqueue("…")``
  -- React Router's turbo-stream. The payload is a JSON *string* containing a
  FLAT array; objects inside it are ``{"_<keyIdx>": <valueIdx>}`` where BOTH
  sides are indices into that same array. It therefore needs a resolver pass,
  not a plain ``json.loads``. The root value is index 0.
- older pages: ``window.__remixContext = {…}`` -- ordinary nested JSON, with
  the payload one level deeper under ``state``.

Both land at ``objectData.ad``. Every entry point returns ``None`` rather than
raising, so an unfamiliar third format degrades to "no data" instead of
breaking the backfill.
"""
import html as html_mod
import json
import re
from typing import NamedTuple

from bs4 import BeautifulSoup

# Turbo-stream encodes a few JS values as negative pseudo-indices. Only the
# two booleans carry meaning for us; everything else collapses to None.
_NEGATIVE = {-7: False, -8: True}

_ENQUEUE = re.compile(r'enqueue\((".*")\)', re.S)
_REMIX = re.compile(r"window\.__remixContext\s*=\s*(\{.*\})", re.S)

_BREAK = re.compile(r"<br\s*/?>|</p>|</li>|</h\d>", re.I)
_TAG = re.compile(r"<[^>]+>")

# `seen` only stops cyclic references (the same index revisited); a plain
# chain of ~5000 distinct nested lists/dicts never revisits an index, so it
# sails past that guard and blows the interpreter's call stack instead. Cap
# recursion depth explicitly -- deeper than this is not a payload FINN would
# ever produce, only a hostile or corrupt one.
_MAX_DEPTH = 500


class Section(NamedTuple):
    heading: str
    text: str


def _largest_script(html: str) -> str:
    """The payload always lives in the page's biggest inline <script>."""
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return ""
    bodies = [s.string or "" for s in soup.find_all("script")]
    return max(bodies, key=len) if bodies else ""


def _resolve(arr: list, index, seen: frozenset, depth: int = 0) -> object:
    """Walk the turbo-stream index graph into ordinary Python values."""
    # bool is a subclass of int; True/False are never valid indices.
    if isinstance(index, bool) or not isinstance(index, int):
        return None
    if index < 0:
        return _NEGATIVE.get(index)
    if index in seen or index >= len(arr):
        return None
    if depth >= _MAX_DEPTH:
        return None
    value = arr[index]
    if isinstance(value, dict):
        seen = seen | {index}
        out = {}
        for raw_key, raw_val in value.items():
            try:
                key_index = int(str(raw_key).lstrip("_"))
            except ValueError:
                continue
            key = _resolve(arr, key_index, seen, depth + 1)
            if key is not None:
                out[str(key)] = _resolve(arr, raw_val, seen, depth + 1)
        return out
    if isinstance(value, list):
        seen = seen | {index}
        return [_resolve(arr, i, seen, depth + 1) for i in value]
    return value


def _from_turbostream(script: str) -> dict | None:
    match = _ENQUEUE.search(script)
    if not match:
        return None
    try:
        arr = json.loads(json.loads(match.group(1)))
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    if not isinstance(arr, list) or not arr:
        return None
    root = _resolve(arr, 0, frozenset())
    return root if isinstance(root, dict) else None


def _from_remix(script: str) -> dict | None:
    match = _REMIX.search(script)
    if not match:
        return None
    try:
        root = json.loads(match.group(1).rstrip().rstrip(";"))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(root, dict):
        return None
    state = root.get("state")
    return state if isinstance(state, dict) else root


def decode_ad(html: str) -> dict | None:
    """The ad's ``objectData.ad`` mapping, or None if the page has no
    recognisable payload."""
    if not html:
        return None
    script = _largest_script(html)
    if not script:
        return None
    root = _from_turbostream(script) or _from_remix(script)
    if not isinstance(root, dict):
        return None
    loader = root.get("loaderData")
    if not isinstance(loader, dict):
        return None
    for key, value in loader.items():
        if "ad[.html]" not in key and "homes.ad" not in key:
            continue
        if not isinstance(value, dict):
            continue
        object_data = value.get("objectData")
        if not isinstance(object_data, dict):
            continue
        ad = object_data.get("ad")
        if isinstance(ad, dict):
            return ad
    return None


def sections(ad: dict | None) -> list[Section]:
    """The salgsoppgave's ``generalText`` as plaintext (heading, text) pairs.

    Block-level tags become newlines so label structure survives; everything
    else is stripped and HTML entities are unescaped.
    """
    if not isinstance(ad, dict):
        return []
    raw_sections = ad.get("generalText")
    if not isinstance(raw_sections, list):
        return []
    out: list[Section] = []
    for item in raw_sections:
        if not isinstance(item, dict):
            continue
        body = str(item.get("textUnsafe") or "")
        body = _BREAK.sub("\n", body)
        body = _TAG.sub(" ", body)
        body = html_mod.unescape(body)
        body = re.sub(r"[ \t]+", " ", body)
        body = re.sub(r"\n\s*\n+", "\n", body).strip()
        heading = html_mod.unescape(str(item.get("heading") or "")).strip()
        out.append(Section(heading, body))
    return out
