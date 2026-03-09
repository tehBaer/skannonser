#!/usr/bin/env python3
"""Visual editor for FINN `polylocation` polygons.

This generates a local HTML file with a Leaflet map where you can:
- Drag vertices
- Add vertices by clicking the map
- Remove a vertex with right-click
- Copy updated polylocation / full FINN URL / Python tuple list

Default input is read from `main/runners/run_eiendom_db.py` by parsing:
- `finn_url_base`
- `finn_polygon_points`
"""

from __future__ import annotations

import argparse
import ast
import html
import json
import tempfile
import webbrowser
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


DEFAULT_SOURCE_FILE = Path("main/runners/run_eiendom_db.py")


def build_finn_polylocation(points: Iterable[tuple[float, float]]) -> str:
    """Build FINN polylocation value from (lng, lat) tuples."""
    points_list = list(points)
    if len(points_list) < 3:
        raise ValueError("Polygon must contain at least 3 points")

    if points_list[0] != points_list[-1]:
        points_list.append(points_list[0])

    return "%2C".join(f"{lng}+{lat}" for lng, lat in points_list)


def load_defaults_from_source(source_file: Path) -> tuple[str, list[tuple[float, float]]]:
    """Parse `finn_url_base` and `finn_polygon_points` from source file."""
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    module = ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))

    url_base = None
    polygon_points = None

    for node in ast.walk(module):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue

        name = node.targets[0].id
        if name == "finn_url_base":
            try:
                value = ast.literal_eval(node.value)
            except Exception:
                continue
            if isinstance(value, str):
                url_base = value

        if name == "finn_polygon_points":
            try:
                value = ast.literal_eval(node.value)
            except Exception:
                continue
            if isinstance(value, list):
                parsed = []
                for item in value:
                    if (
                        isinstance(item, tuple)
                        and len(item) == 2
                        and isinstance(item[0], (int, float))
                        and isinstance(item[1], (int, float))
                    ):
                        parsed.append((float(item[0]), float(item[1])))
                if parsed:
                    polygon_points = parsed

    if not url_base:
        raise ValueError(f"Could not parse `finn_url_base` from {source_file}")
    if not polygon_points:
        raise ValueError(f"Could not parse `finn_polygon_points` from {source_file}")

    return url_base, polygon_points


def with_polylocation(url_base: str, polylocation_value: str) -> str:
  """Set or replace polylocation in URL while preserving FINN's expected encoding."""
  marker = "polylocation="
  if marker in url_base:
    prefix, remainder = url_base.split(marker, 1)
    if "&" in remainder:
      _, suffix = remainder.split("&", 1)
      return f"{prefix}{marker}{polylocation_value}&{suffix}"
    return f"{prefix}{marker}{polylocation_value}"

  separator = "&" if urlparse(url_base).query else "?"
  return f"{url_base}{separator}{marker}{polylocation_value}"


def build_html(url_base: str, points: list[tuple[float, float]]) -> str:
    """Render standalone HTML for local visual editing."""
    # Leaflet expects [lat, lng]
    leaflet_points = [[lat, lng] for lng, lat in points]
    points_json = json.dumps(leaflet_points)
    url_base_json = json.dumps(url_base)

    return f"""<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>FINN Polygon Visual Editor</title>
    <link
      rel=\"stylesheet\"
      href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\"
      integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\"
      crossorigin=\"\"
    />
    <script
      src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"
      integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\"
      crossorigin=\"\"
    ></script>
    <style>
      html, body {{ margin: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
      .layout {{ display: grid; grid-template-columns: 360px 1fr; height: 100%; }}
      .panel {{ padding: 12px; border-right: 1px solid #d7dde3; overflow: auto; background: #f8fafc; }}
      .panel h1 {{ margin: 0 0 8px; font-size: 18px; }}
      .hint {{ margin: 0 0 10px; color: #475569; font-size: 13px; line-height: 1.4; }}
      .row {{ margin-top: 10px; }}
      .label {{ display: block; margin-bottom: 4px; font-size: 12px; color: #334155; font-weight: 600; }}
      textarea {{ width: 100%; box-sizing: border-box; min-height: 82px; padding: 8px; border: 1px solid #cbd5e1; border-radius: 8px; background: #fff; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
      .btns {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }}
      button {{ border: 1px solid #94a3b8; background: #fff; color: #0f172a; border-radius: 8px; padding: 6px 10px; cursor: pointer; font-size: 12px; }}
      button:hover {{ background: #f1f5f9; }}
      .status {{ margin-top: 8px; font-size: 12px; color: #0f766e; min-height: 16px; }}
      #map {{ height: 100%; width: 100%; }}
      .foot {{ margin-top: 8px; color: #64748b; font-size: 11px; }}
    </style>
  </head>
  <body>
    <div class=\"layout\">
      <aside class=\"panel\">
        <h1>FINN Polygon Editor</h1>
        <p class=\"hint\">Drag points to tweak. Click map to add point at end. Right-click a point to remove it (min 3).</p>

        <div class=\"row\">
          <label class=\"label\">Vertex count</label>
          <div id=\"count\">-</div>
        </div>

        <div class=\"row\">
          <label class=\"label\">`polylocation` value</label>
          <textarea id=\"polylocation\" readonly></textarea>
          <div class=\"btns\"><button id=\"copy-polylocation\">Copy polylocation</button></div>
        </div>

        <div class=\"row\">
          <label class=\"label\">Full FINN URL</label>
          <textarea id=\"full-url\" readonly></textarea>
          <div class=\"btns\"><button id=\"copy-url\">Copy URL</button></div>
        </div>

        <div class=\"row\">
          <label class=\"label\">Python `finn_polygon_points` snippet</label>
          <textarea id=\"python-points\" readonly></textarea>
          <div class=\"btns\"><button id=\"copy-python\">Copy Python points</button></div>
        </div>

        <div class=\"status\" id=\"status\"></div>
        <div class=\"foot\">Generated locally. Base URL source: <code>{html.escape(url_base)}</code></div>
      </aside>
      <main id=\"map\"></main>
    </div>

    <script>
      const BASE_URL = {url_base_json};
      let points = {points_json}; // [lat, lng]

      const map = L.map('map', {{ zoomControl: true }});
      L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
      }}).addTo(map);

      let polygonLayer = null;
      let markerLayer = null;

      function formatPythonPoints() {{
        const rows = points.map(([lat, lng]) => `    (${{lng.toFixed(12)}}, ${{lat.toFixed(12)}}),`);
        return "finn_polygon_points = [\\n" + rows.join("\\n") + "\\n]";
      }}

      function polylocationValue() {{
        const closed = points.slice();
        const first = closed[0];
        const last = closed[closed.length - 1];
        if (!first || !last || first[0] !== last[0] || first[1] !== last[1]) {{
          closed.push(first);
        }}

        return closed
          .map(([lat, lng]) => `${{lng}}+${{lat}}`)
          .join('%2C');
      }}

      function fullUrl() {{
        const p = new URL(BASE_URL);
        p.searchParams.set('polylocation', polylocationValue());
        return p.toString();
      }}

      function updateOutputs() {{
        document.getElementById('count').textContent = String(points.length);
        document.getElementById('polylocation').value = polylocationValue();
        document.getElementById('full-url').value = fullUrl();
        document.getElementById('python-points').value = formatPythonPoints();
      }}

      function render() {{
        if (polygonLayer) map.removeLayer(polygonLayer);
        if (markerLayer) map.removeLayer(markerLayer);

        polygonLayer = L.polygon(points, {{ color: '#2563eb', weight: 3, fillOpacity: 0.1 }}).addTo(map);
        markerLayer = L.layerGroup().addTo(map);

        points.forEach((pt, idx) => {{
          const marker = L.circleMarker(pt, {{
            radius: 6,
            color: '#1d4ed8',
            fillColor: '#3b82f6',
            fillOpacity: 1,
            weight: 2,
          }}).addTo(markerLayer);

          marker.bindTooltip(String(idx + 1), {{ permanent: true, direction: 'top', offset: [0, -8] }});

          marker.on('mousedown', () => map.dragging.disable());
          marker.on('mouseup', () => map.dragging.enable());

          marker.on('contextmenu', (e) => {{
            e.originalEvent.preventDefault();
            if (points.length <= 3) {{
              setStatus('Need at least 3 vertices.');
              return;
            }}
            points.splice(idx, 1);
            render();
          }});

          marker.on('mousedown', (ev) => {{
            const start = ev.latlng;
            const startPoint = points[idx].slice();

            function onMove(moveEv) {{
              const ll = moveEv.latlng;
              points[idx] = [ll.lat, ll.lng];
              render();
            }}

            function onUp() {{
              map.off('mousemove', onMove);
              map.off('mouseup', onUp);
              map.dragging.enable();
              if (start.lat !== startPoint[0] || start.lng !== startPoint[1]) {{
                setStatus(`Moved point ${{idx + 1}}.`);
              }}
            }}

            map.on('mousemove', onMove);
            map.on('mouseup', onUp);
          }});
        }});

        if (points.length > 0) {{
          map.fitBounds(polygonLayer.getBounds(), {{ padding: [30, 30] }});
        }}

        updateOutputs();
      }}

      function setStatus(msg) {{
        document.getElementById('status').textContent = msg;
      }}

      async function copyFromTextarea(id, okLabel) {{
        const el = document.getElementById(id);
        el.select();
        el.setSelectionRange(0, el.value.length);
        try {{
          await navigator.clipboard.writeText(el.value);
          setStatus(okLabel);
        }} catch (err) {{
          document.execCommand('copy');
          setStatus(okLabel + ' (fallback copy)');
        }}
      }}

      map.on('click', (e) => {{
        points.push([e.latlng.lat, e.latlng.lng]);
        setStatus('Added new vertex at end.');
        render();
      }});

      document.getElementById('copy-polylocation').addEventListener('click', () => copyFromTextarea('polylocation', 'Copied polylocation.'));
      document.getElementById('copy-url').addEventListener('click', () => copyFromTextarea('full-url', 'Copied URL.'));
      document.getElementById('copy-python').addEventListener('click', () => copyFromTextarea('python-points', 'Copied Python points.'));

      render();
    </script>
  </body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open visual editor for FINN polygon coordinates")
    parser.add_argument(
        "--source-file",
        type=Path,
        default=DEFAULT_SOURCE_FILE,
        help=f"Python file containing finn_url_base and finn_polygon_points (default: {DEFAULT_SOURCE_FILE})",
    )
    parser.add_argument(
        "--url-base",
        help="Optional override for finn_url_base. If set, source file is still used for points unless --points-json is set.",
    )
    parser.add_argument(
        "--points-json",
        type=Path,
        help="Optional JSON file with [[lng, lat], ...] points to override parsed points.",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Only generate HTML file and print path; do not open browser.",
    )
    parser.add_argument(
      "--print-url-only",
      action="store_true",
      help="Print only the generated FINN URL and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    src_url_base, src_points = load_defaults_from_source(args.source_file)
    url_base = args.url_base or src_url_base
    points = src_points

    if args.points_json:
        raw = json.loads(args.points_json.read_text(encoding="utf-8"))
        parsed = []
        for item in raw:
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError("points-json must contain [[lng, lat], ...]")
            lng, lat = item
            parsed.append((float(lng), float(lat)))
        points = parsed

    if len(points) < 3:
        raise ValueError("Need at least 3 points")

    preview_poly = build_finn_polylocation(points)
    preview_url = with_polylocation(url_base, preview_poly)

    if args.print_url_only:
      print(preview_url)
      return 0

    html_content = build_html(url_base=url_base, points=points)

    out_dir = Path(tempfile.gettempdir())
    out_file = out_dir / "finn_polygon_editor.html"
    out_file.write_text(html_content, encoding="utf-8")

    print(f"Wrote editor: {out_file}")
    print(f"Preview URL (from current points):\n{preview_url}\n")
    print("How to use:")
    print("1. Drag points on the map. Click map to add vertex. Right-click a point to remove.")
    print("2. Copy either 'Full FINN URL' or 'Python finn_polygon_points snippet'.")
    print("3. Paste back into main/runners/run_eiendom_db.py and run your scraper.")

    if not args.no_open:
        webbrowser.open(out_file.as_uri())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
