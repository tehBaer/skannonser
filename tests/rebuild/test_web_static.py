"""Static-serving + no-CDN guarantees for the map-core frontend (Phase 5
Task 6).

No browser automation here (that's the Task 9 walkthrough) -- these tests
assert only that the app serves the right bytes with the right content-types,
and that the authored static files pull in ZERO external resource/CDN
dependencies. The one hard rule the frontend must keep: nothing on the page
loads from a third party at render time (no CDN scripts/styles/fonts, no
third-party tiles beyond OpenStreetMap), so the UI works offline and leaks
nothing on load.

NO-CDN ALLOWLIST NOTE: the popup contract mandates a "Google Maps via
lat/lng" hyperlink and a "Finn via url" hyperlink. Those are user-navigation
`href`s (activated only by an explicit click), NOT passive resource loads, so
`www.google.com` is an allowed *host* for an anchor URL. What must never
appear is an external host in a resource position -- `<script src>`,
`<link href>`/stylesheet, `@import`, or `url(...)`. Those are asserted
separately and strictly (only same-origin + the OSM tile host).
"""

import re
import warnings

from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="Using `httpx` with `starlette.testclient` is deprecated",
        category=StarletteDeprecationWarning,
    )
    from fastapi.testclient import TestClient

from skannonser.store import connection, migrations
from skannonser.web.app import STATIC_DIR, create_app


def _migrated_db(tmp_path):
    db_path = tmp_path / "migrated.db"
    conn = connection.connect(db_path)
    migrations.migrate(conn)
    conn.close()
    return db_path


def _client(tmp_path):
    return TestClient(create_app(_migrated_db(tmp_path)))


# Hosts an authored static file may legitimately reference at all.
#  * tile.openstreetmap.org -- the raster tile source (map base) + attribution.
#  * www.google.com -- the popup's "Google Maps" user-navigation hyperlink.
#  * www.finn.no -- the missing-coords panel's "open the Finn ad" hyperlink
#    (Task 7). Like the Google Maps link it is a user-navigation `href`
#    (click-only), NOT a passive resource load -- see the NO-CDN ALLOWLIST NOTE.
_ALLOWED_HTTP_HOSTS = {"tile.openstreetmap.org", "www.google.com", "www.finn.no"}
# Hosts allowed in a *resource* position (script/link/@import/url()).
_ALLOWED_RESOURCE_HOSTS = {"tile.openstreetmap.org"}

_URL_RE = re.compile(r"https?://([A-Za-z0-9.\-]+)")


def _authored_static_files():
    """Every authored static asset (top-level .js/.html/.css), EXCLUDING the
    vendored MapLibre bundle under vendor/ (a minified third-party library
    that legitimately embeds doc/license URLs -- it's not authored here and
    is served, not fetched from a CDN)."""
    files = []
    for pattern in ("*.js", "*.html", "*.css"):
        files.extend(STATIC_DIR.glob(pattern))  # top-level only, not vendor/
    return files


# ---------------------------------------------------------------------------
# Serving
# ---------------------------------------------------------------------------

def test_index_served_at_root(tmp_path):
    resp = _client(tmp_path).get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    body = resp.text
    assert '<div id="map"></div>' in body
    assert 'id="sidebar"' in body
    # Loads the vendor lib + the app entrypoint as a module.
    assert "/vendor/maplibre-gl.js" in body
    assert 'type="module"' in body and "/app.js" in body


def test_index_has_task7_sidebar_sections(tmp_path):
    """The Task 7 sidebar sections (filters, per-boligtype visibility, station
    overlays + commute filter, missing-coords) and their mount points are
    present in index.html."""
    body = _client(tmp_path).get("/").text
    # Metric filters + dim, boligtype visibility, stations, missing-coords.
    for anchor in (
        'id="metric-filters"',
        'id="boligtype-filter"',
        'id="stations-panel"',
        'id="toggle-stations"',
        'id="toggle-hide-outside"',
        'id="sandvika-max"',
        'id="toggle-transfer"',
        'id="line-toggles"',
        'id="missing-coords"',
    ):
        assert anchor in body, anchor
    # Task 7 modules are loaded (via app.js's static imports, but assert the
    # files are referenced by the app module graph entrypoint at minimum).
    assert 'src="/app.js"' in body


def test_vendor_js_served_with_js_content_type(tmp_path):
    resp = _client(tmp_path).get("/vendor/maplibre-gl.js")
    assert resp.status_code == 200
    assert "javascript" in resp.headers["content-type"].lower()
    assert len(resp.content) > 100_000  # the real ~800KB bundle, not a stub


def test_vendor_css_served_with_css_content_type(tmp_path):
    resp = _client(tmp_path).get("/vendor/maplibre-gl.css")
    assert resp.status_code == 200
    assert "text/css" in resp.headers["content-type"].lower()
    assert len(resp.content) > 10_000


def test_app_modules_served(tmp_path):
    client = _client(tmp_path)
    for name in ("app.js", "map.js", "popup.js", "filters.js", "stations.js"):
        resp = client.get("/" + name)
        assert resp.status_code == 200, name
        assert "javascript" in resp.headers["content-type"].lower(), name


def test_style_css_served(tmp_path):
    resp = _client(tmp_path).get("/style.css")
    assert resp.status_code == 200
    assert "text/css" in resp.headers["content-type"].lower()


# ---------------------------------------------------------------------------
# No-CDN guarantees (grep-style, over authored files only)
# ---------------------------------------------------------------------------

def test_authored_static_files_reference_only_allowlisted_hosts(tmp_path):
    """Every http(s) host mentioned anywhere in an authored static file must
    be on the allowlist -- OSM tiles/attribution, or the Google Maps user
    hyperlink. Any other external host is a forbidden dependency."""
    offenders = {}
    for path in _authored_static_files():
        text = path.read_text(encoding="utf-8")
        for host in _URL_RE.findall(text):
            if host not in _ALLOWED_HTTP_HOSTS:
                offenders.setdefault(path.name, set()).add(host)
    assert not offenders, f"unexpected external hosts: {offenders}"


def test_osm_tile_url_present_in_map_js(tmp_path):
    """Positive check: the OSM raster tile URL is actually wired up (guards
    against the allowlist test passing simply because no URLs exist)."""
    text = (STATIC_DIR / "map.js").read_text(encoding="utf-8")
    assert "https://tile.openstreetmap.org/{z}/{x}/{y}.png" in text


def test_no_external_resource_loads(tmp_path):
    """The strict anti-CDN check: no external host in a *resource* position.
    Scans HTML for `src=`/`href=` (script/link/stylesheet) and CSS for
    `@import` / `url(...)`; the only external host permitted in those
    positions is the OSM tile host."""
    for path in _authored_static_files():
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".html":
            for attr in re.findall(r'(?:src|href)\s*=\s*"([^"]+)"', text):
                m = _URL_RE.match(attr)
                if m:
                    assert (
                        m.group(1) in _ALLOWED_RESOURCE_HOSTS
                    ), f"{path.name}: external resource {attr!r}"
        if path.suffix == ".css":
            for ref in re.findall(r'url\(([^)]+)\)', text) + re.findall(
                r'@import\s+["\']([^"\']+)', text
            ):
                m = _URL_RE.match(ref.strip("\"'"))
                if m:
                    assert (
                        m.group(1) in _ALLOWED_RESOURCE_HOSTS
                    ), f"{path.name}: external css resource {ref!r}"


def test_index_html_scripts_and_links_are_same_origin(tmp_path):
    """index.html must load its script/link assets from same-origin paths
    (leading `/`), never an absolute external URL."""
    text = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    refs = re.findall(r'(?:src|href)\s*=\s*"([^"]+)"', text)
    assert refs, "expected at least the vendor + app asset references"
    for ref in refs:
        assert ref.startswith("/"), f"non-same-origin asset ref: {ref!r}"
