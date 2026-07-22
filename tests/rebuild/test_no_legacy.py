"""Structural proofs that the legacy system is gone (Phase 6 Task 5).

The legacy `main/`, `scripts/`, and `apps_script/` trees plus the old
Makefile/requirements were deleted; there is now exactly one codebase. These
tests lock that in so the legacy system cannot silently creep back via a
re-added tracked file or a stray `import main`.
"""
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.splitlines()


def test_legacy_trees_untracked():
    tracked = _tracked_files()
    offenders = [
        p
        for p in tracked
        if p.startswith(("main/", "scripts/", "apps_script/"))
        or p in {"Makefile", "requirements.txt"}
        or p.endswith("Stations (2).csv")
    ]
    assert offenders == [], f"legacy paths still tracked by git: {offenders}"


def test_no_import_of_legacy_main_package():
    roots = [REPO_ROOT / "skannonser", REPO_ROOT / "tests" / "rebuild"]
    offenders = []
    for root in roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="replace")
            for lineno, line in enumerate(text.splitlines(), start=1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue  # frozen-provenance comments may cite legacy paths
                if stripped.startswith(("import main", "from main")):
                    # tolerate identifiers like `import main_helper`
                    tail = stripped[len("import main"):] if stripped.startswith("import main") else stripped[len("from main"):]
                    if tail[:1] in ("", " ", ".", "\t"):
                        offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {stripped}")
    assert offenders == [], f"legacy `main` imports still present: {offenders}"


def test_pyproject_has_no_legacy_references():
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    for token in ["main/", "main.", '"main"', "scripts/", "apps_script"]:
        assert token not in text, f"pyproject.toml still references legacy: {token!r}"
