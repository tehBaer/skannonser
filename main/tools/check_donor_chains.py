#!/usr/bin/env python3
"""Check donor-link integrity in eiendom_processed travel data.

This tool is read-only. It reports:
- multi-hop donor chains (A <- B <- C)
- self-links (A -> A)
- cycles (A -> B -> A)
- broken donor references (donor finnkode missing in table)
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


def _normalize_finnkode(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        value = float(text)
        if value.is_integer():
            return str(int(value))
    except Exception:
        pass
    return text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check for donor-of-donor chains and donor-link integrity issues",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Maximum findings to print per section (default: 50)",
    )
    parser.add_argument(
        "--csv",
        help="Optional CSV path to write all findings",
    )
    parser.add_argument(
        "--fail-on-findings",
        action="store_true",
        help="Exit with code 1 when any integrity issue is found",
    )
    return parser.parse_args()


def _resolve_chain(start: str, link_map: dict[str, str]) -> tuple[list[str], str]:
    """Resolve donor path for start and classify result kind.

    Returns:
      (path, kind)
      kind in {"root", "broken", "self", "cycle"}
    """
    current = start
    path = [current]
    visited = {current}

    while True:
        donor = link_map.get(current, "")
        if not donor:
            return path, "root"
        if donor == current:
            path.append(donor)
            return path, "self"
        if donor in visited:
            path.append(donor)
            return path, "cycle"
        path.append(donor)
        visited.add(donor)
        if donor not in link_map:
            return path, "broken"
        current = donor


def main() -> int:
    args = parse_args()

    db = PropertyDatabase()
    seed = db.get_travel_donor_seed()
    if seed.empty:
        print("No rows in travel donor seed.")
        return 0

    work = seed[["Finnkode", "TRAVEL_COPY_FROM_FINNKODE"]].copy()
    work["Finnkode"] = work["Finnkode"].apply(_normalize_finnkode)
    work["TRAVEL_COPY_FROM_FINNKODE"] = work["TRAVEL_COPY_FROM_FINNKODE"].apply(_normalize_finnkode)

    work = work.loc[work["Finnkode"] != ""].copy()

    # Only explicit donor links participate in chain integrity checks.
    links = work.loc[work["TRAVEL_COPY_FROM_FINNKODE"] != ""].copy()
    if links.empty:
        print("No donor links found (TRAVEL_COPY_FROM_FINNKODE is empty for all rows).")
        return 0

    link_map = dict(zip(links["Finnkode"], links["TRAVEL_COPY_FROM_FINNKODE"]))

    findings: list[dict[str, object]] = []
    chain_rows: list[dict[str, object]] = []
    self_rows: list[dict[str, object]] = []
    cycle_rows: list[dict[str, object]] = []
    broken_rows: list[dict[str, object]] = []

    for listing in sorted(link_map.keys()):
        path, kind = _resolve_chain(listing, link_map)
        hops = max(len(path) - 1, 0)

        row = {
            "listing_finnkode": listing,
            "immediate_donor": link_map.get(listing, ""),
            "hops": hops,
            "kind": kind,
            "path": " -> ".join(path),
        }
        findings.append(row)

        if kind == "self":
            self_rows.append(row)
        elif kind == "cycle":
            cycle_rows.append(row)
        elif kind == "broken":
            broken_rows.append(row)
        elif kind == "root" and hops >= 2:
            chain_rows.append(row)

    total_links = len(link_map)
    issue_count = len(chain_rows) + len(self_rows) + len(cycle_rows) + len(broken_rows)

    print("Donor Link Integrity Report")
    print("===========================")
    print(f"Rows with donor links: {total_links}")
    print(f"Multi-hop chains (>=2 hops): {len(chain_rows)}")
    print(f"Self-links: {len(self_rows)}")
    print(f"Cycles: {len(cycle_rows)}")
    print(f"Broken donor refs: {len(broken_rows)}")

    def _print_section(title: str, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        print(f"\n{title} (showing up to {max(args.top, 0)}):")
        show = pd.DataFrame(rows).head(max(args.top, 0))
        print(show.to_string(index=False))

    _print_section("Multi-hop chains", chain_rows)
    _print_section("Self-links", self_rows)
    _print_section("Cycles", cycle_rows)
    _print_section("Broken donor refs", broken_rows)

    if args.csv:
        out = pd.DataFrame(findings)
        out.to_csv(args.csv, index=False)
        print(f"\nWrote findings CSV: {args.csv}")

    if issue_count == 0:
        print("\nNo donor chain integrity issues found.")
        return 0

    if args.fail_on_findings:
        print("\nIntegrity issues found (failing due to --fail-on-findings).")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
