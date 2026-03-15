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
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Flatten multi-hop chains and clear invalid links in the database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --repair: show what would be changed without writing to the DB.",
    )
    return parser.parse_args()


def _flatten_to_root(
    start: str,
    link_map: dict[str, str],
    all_finnkodes: set[str],
) -> tuple[str, int, str]:
    """Walk the donor chain from ``start`` to its terminal node.

    Returns:
        (end_node, hops, kind)  where kind is one of:
        - 'ok':     reached a valid root (not in link_map, exists in DB)
        - 'self':   self-link (start -> start)
        - 'cycle':  cycle detected
        - 'broken': donor finnkode does not exist in eiendom_processed at all
    """
    if link_map.get(start) == start:
        return start, 0, "self"

    current = start
    visited_list: list[str] = [current]
    visited_set: set[str] = {current}

    while current in link_map:
        donor = link_map[current]
        if donor not in all_finnkodes:
            return donor, len(visited_list) - 1, "broken"
        if donor in visited_set:
            return donor, len(visited_list) - 1, "cycle"
        visited_list.append(donor)
        visited_set.add(donor)
        current = donor

    return current, len(visited_list) - 1, "ok"


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
        if not args.repair:
            return 0

    if args.repair:
        all_finnkodes = set(work["Finnkode"])
        updates: dict[str, str | None] = {}  # finnkode -> new_donor (None = clear)

        for listing in sorted(link_map.keys()):
            root, hops, kind = _flatten_to_root(listing, link_map, all_finnkodes)
            if kind == "ok" and hops <= 1:
                continue  # valid single-hop or already a root — nothing to do
            elif kind == "ok" and hops >= 2:
                updates[listing] = root  # flatten A->B->...->root to A->root
            else:  # self, cycle, broken
                updates[listing] = None  # clear invalid link

        n_updates = len(updates)
        if n_updates == 0:
            print("\nNo repairs needed.")
        else:
            n_flatten = sum(1 for v in updates.values() if v is not None)
            n_clear = sum(1 for v in updates.values() if v is None)
            action = "Would repair" if args.dry_run else "Repairing"
            print(f"\n{action} {n_updates} link(s): {n_flatten} to flatten, {n_clear} to clear.")
            for fk, new_donor in sorted(updates.items()):
                old = link_map.get(fk, "")
                if new_donor:
                    print(f"  flatten  {fk:<20}  {old!r} -> {new_donor!r}")
                else:
                    print(f"  clear    {fk:<20}  {old!r} -> NULL")
            if not args.dry_run:
                _conn = db.get_connection()
                _cursor = _conn.cursor()
                for fk, new_donor in updates.items():
                    if new_donor:
                        _cursor.execute(
                            "UPDATE eiendom_processed SET travel_copy_from_finnkode = ?"
                            " WHERE finnkode = ?",
                            (new_donor, fk),
                        )
                    else:
                        _cursor.execute(
                            "UPDATE eiendom_processed SET travel_copy_from_finnkode = NULL"
                            " WHERE finnkode = ?",
                            (fk,),
                        )
                _conn.commit()
                _conn.close()
                print(f"✓ Repaired {n_updates} donor link(s).")

        if not args.dry_run and n_updates > 0:
            return 0  # repairs applied — not a failure

    if args.fail_on_findings and issue_count > 0:
        print("\nIntegrity issues found (failing due to --fail-on-findings).")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
