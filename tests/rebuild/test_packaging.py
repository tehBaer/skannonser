"""Phase 5 final-review Fix 1: pyproject.toml's package-data globs must cover
every git-tracked file under skannonser/web/static/, so a non-editable
`pip install .` (e.g. the Dockerfile's build) ships the full static tree --
including nested directories like static/vendor/ -- not just the top level.

WHY this test exists: setuptools' package-data glob patterns are resolved
like `glob.glob()` -- a bare `*` does NOT cross a `/` boundary. A pattern of
just `"static/*"` therefore matches `static/app.js` but silently skips
anything under `static/vendor/` (the vendored MapLibre bundle), so a built
wheel/sdist installed non-editable ships with the map dead (no maplibre-gl.js
served, 404s on `/vendor/maplibre-gl.js`). This was caught in the phase-5
final review; this test would have caught it up front and guards against it
recurring if someone adds a new nested static asset without a matching glob.
"""

import subprocess
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_PKG_DIR = REPO_ROOT / "skannonser" / "web"


def _git_tracked_static_files_relative_to_web_pkg():
    """Every file git tracks under skannonser/web/static/, as a path
    relative to skannonser/web/ (the dir package-data globs are resolved
    against for the "skannonser.web" package)."""
    result = subprocess.run(
        ["git", "ls-files", "skannonser/web/static"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        Path(line).relative_to("skannonser/web").as_posix()
        for line in result.stdout.splitlines()
        if line
    }


def _files_matched_by_globs(globs):
    """Files matched by the package-data glob patterns, using the same
    non-recursive (doesn't cross '/') glob semantics setuptools uses."""
    matched = set()
    for pattern in globs:
        for path in WEB_PKG_DIR.glob(pattern):
            if path.is_file():
                matched.add(path.relative_to(WEB_PKG_DIR).as_posix())
    return matched


def test_pyproject_package_data_covers_every_tracked_static_file():
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    globs = pyproject["tool"]["setuptools"]["package-data"]["skannonser.web"]

    tracked = _git_tracked_static_files_relative_to_web_pkg()
    assert tracked, "expected at least one git-tracked file under skannonser/web/static/"
    # Canary: guards against this test passing vacuously if the vendor dir
    # is ever removed from the tree instead of from git.
    assert any(rel.startswith("static/vendor/") for rel in tracked), (
        "expected a static/vendor/* file to be tracked (e.g. maplibre-gl.js) "
        "-- if this legitimately changed, the test itself needs updating"
    )

    matched = _files_matched_by_globs(globs)

    uncovered = sorted(tracked - matched)
    assert not uncovered, (
        "static file(s) not covered by any package-data glob in pyproject.toml "
        "-- setuptools globs don't cross directory boundaries, so a nested dir "
        f"needs its own pattern (e.g. \"static/vendor/*\"): {uncovered}"
    )
