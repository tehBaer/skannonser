#!/usr/bin/env python3
"""Manage per-listing address/postcode overrides in manual_overrides."""
import argparse
import os
import sys

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


def cmd_set(args) -> int:
    db = PropertyDatabase(args.db)
    db.set_override(
        finnkode=str(args.finnkode).strip(),
        adresse=args.adresse,
        postnummer=args.postnummer,
        reason=args.reason,
    )
    return 0


def cmd_remove(args) -> int:
    db = PropertyDatabase(args.db)
    db.remove_override(str(args.finnkode).strip())
    return 0


def cmd_list(args) -> int:
    db = PropertyDatabase(args.db)
    db.list_overrides()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage listing address/postcode overrides")
    parser.add_argument("--db", help="Optional path to properties.db")

    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="Set override for one listing")
    p_set.add_argument("finnkode", help="Listing Finnkode")
    p_set.add_argument("--adresse", required=True, help="Override address")
    p_set.add_argument("--postnummer", help="Override postal code")
    p_set.add_argument("--reason", default="manual address fix", help="Override reason")
    p_set.set_defaults(func=cmd_set)

    p_remove = sub.add_parser("remove", help="Remove override for one listing")
    p_remove.add_argument("finnkode", help="Listing Finnkode")
    p_remove.set_defaults(func=cmd_remove)

    p_list = sub.add_parser("list", help="List all overrides")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
