"""Shared, path-safe identifier helpers used by BOTH the web API
(``skannonser/web/api.py``) and the nightly thumbnail cache
(``skannonser/enrich/thumbs.py``) -- kept in one place so the two call
sites can never drift on how a listing's identifier (and the filename/URL
derived from it) is computed.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

# Same charset for both an Eie ``finnkode`` (plain digits today, but not
# contractually guaranteed) and a synthetic ``dnb:<hash>`` id: alphanumerics
# plus ``:``, ``_``, ``-``. Deliberately excludes ``.``/``/`` -- so no
# identifier that passes this can ever encode a path-traversal segment
# (``..``) or a directory separator. Used to validate any identifier that
# becomes part of a filesystem path or URL path segment: the annotations
# CRUD routes (``skannonser/web/api.py``) and the ``/thumbs/{identifier}.jpg``
# static-file route (``skannonser/web/app.py``).
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9:_-]{1,128}$")


def dnb_identifier(url: Any) -> str:
    """Stable, path-safe synthetic id for a DNB row derived from its url
    hash: ``f"dnb:{sha1(url)[:16]}"`` unconditionally.

    See ``skannonser/web/api.py``'s "DNB IDENTIFIER DECISION" module
    docstring for the full rationale -- ``url`` is ``dnbeiendom``'s
    identity/upsert-match key (UNIQUE, never changes for a given crawled
    listing), so this derivation is provably stable across scrapes
    regardless of whether ``dnb_id`` is ever populated. A raw url can't be
    used directly as a path segment (embedded ``/`` would break path-param
    matching, both in the API and in the thumbnail filename/route).
    """
    digest = hashlib.sha1(str(url or "").encode("utf-8")).hexdigest()[:16]
    return f"dnb:{digest}"


__all__ = ["IDENTIFIER_RE", "dnb_identifier"]
