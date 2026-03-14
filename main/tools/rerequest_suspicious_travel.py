#!/usr/bin/env python3
"""Re-request travel values only for listings flagged as suspicious.

Workflow:
- Read findings CSV from validate_travel_values.py
- Filter to rows at/above suspicion score threshold
- Group by travel target (BRJ/MVV)
- Force only those target values to missing for flagged Finnkoder
- Recompute travel for flagged rows only
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

import pandas as pd

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
    from main.post_process import post_process_eiendom
except ImportError:
    from database.db import PropertyDatabase
    from post_process import post_process_eiendom


TARGET_MAP = {
    "PENDL RUSH BRJ": "brj",
    "PENDL RUSH MVV": "mvv",
    "BRJ": "brj",
    "MVV": "mvv",
    "brj": "brj",
    "mvv": "mvv",
}

TRAVEL_COLUMN_BY_TARGET = {
    "brj": "PENDL RUSH BRJ",
    "mvv": "PENDL RUSH MVV",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-request travel values only for suspicious findings"
    )
    parser.add_argument(
        "--findings-csv",
        required=True,
        help="CSV from validate_travel_values.py --csv",
    )
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv"],
        default="all",
        help="Which suspicious targets to re-request",
    )
    parser.add_argument(
        "--score-threshold",
        type=int,
        default=2,
        help="Minimum suspicion score to include from findings CSV",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive listings when recomputing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be re-requested, do not call APIs or write DB",
    )
    return parser.parse_args()


def _normalize_target(raw: object) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    return TARGET_MAP.get(text)


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


def _load_requested_finnkodes(args: argparse.Namespace) -> dict[str, set[str]]:
    df = pd.read_csv(args.findings_csv)
    if df.empty:
        return {"brj": set(), "mvv": set()}

    if "Finnkode" not in df.columns:
        raise ValueError("Findings CSV is missing required column: Finnkode")
    if "target" not in df.columns:
        raise ValueError("Findings CSV is missing required column: target")

    if "suspicion_score" in df.columns:
        scores = pd.to_numeric(df["suspicion_score"], errors="coerce")
        df = df.loc[scores >= float(args.score_threshold)].copy()

    if df.empty:
        return {"brj": set(), "mvv": set()}

    df["_target_norm"] = df["target"].apply(_normalize_target)
    df["_finnkode_norm"] = df["Finnkode"].apply(_normalize_finnkode)
    df = df.loc[df["_target_norm"].notna() & (df["_finnkode_norm"] != "")].copy()

    if args.target in {"brj", "mvv"}:
        df = df.loc[df["_target_norm"] == args.target].copy()

    grouped = {"brj": set(), "mvv": set()}
    for _, row in df.iterrows():
        grouped[row["_target_norm"]].add(row["_finnkode_norm"])
    return grouped


def _restrict_input_rows(df: pd.DataFrame, finnkodes: Iterable[str], include_inactive: bool) -> pd.DataFrame:
    out = df.copy()
    out["Finnkode"] = out["Finnkode"].apply(_normalize_finnkode)
    out = out.loc[out["Finnkode"].isin(set(finnkodes))].copy()

    if not include_inactive and "active" in out.columns:
        active_mask = pd.to_numeric(out["active"], errors="coerce").fillna(1).astype(int) == 1
        out = out.loc[active_mask].copy()

    if "ADRESSE" in out.columns and "Adresse" not in out.columns:
        out["Adresse"] = out["ADRESSE"]

    if "TRAVEL_COPY_FROM_FINNKODE" not in out.columns:
        out["TRAVEL_COPY_FROM_FINNKODE"] = pd.NA

    return out


def _run_target_refresh(
    db: PropertyDatabase,
    df_source: pd.DataFrame,
    target: str,
    finnkodes: set[str],
    include_inactive: bool,
    dry_run: bool,
) -> int:
    if not finnkodes:
        return 0

    col = TRAVEL_COLUMN_BY_TARGET[target]
    work = _restrict_input_rows(df_source, finnkodes, include_inactive)
    if work.empty:
        print(f"[{target}] No matching DB rows for requested Finnkoder")
        return 0

    # Force targeted recalculation for flagged rows only.
    work[col] = pd.NA
    work["TRAVEL_COPY_FROM_FINNKODE"] = pd.NA

    print(f"[{target}] Requested Finnkoder: {len(finnkodes)}")
    print(f"[{target}] Rows eligible for re-request: {len(work)}")

    if dry_run:
        return len(work)

    processed = post_process_eiendom(
        work,
        projectName="data/eiendom",
        db=db,
        calculate_location_features=True,
        calculate_google_directions=True,
        travel_targets=target,
    )

    inserted, updated = db.insert_or_update_eiendom(processed)
    print(f"[{target}] Database rows touched: {inserted} inserted, {updated} updated")
    return len(work)


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.findings_csv):
        print(f"Findings CSV not found: {args.findings_csv}")
        return 1

    try:
        requested = _load_requested_finnkodes(args)
    except Exception as exc:
        print(f"Failed to parse findings CSV: {exc}")
        return 1

    total_flagged = len(requested["brj"] | requested["mvv"])
    print(f"Flagged Finnkoder after score filter: {total_flagged}")
    print(f"  BRJ flagged: {len(requested['brj'])}")
    print(f"  MVV flagged: {len(requested['mvv'])}")

    if total_flagged == 0:
        print("No suspicious rows to re-request.")
        return 0

    db = PropertyDatabase()
    source_df = db.get_eiendom_for_sheets()
    if source_df.empty:
        print("No listings available in DB source for re-request.")
        return 0

    touched_total = 0
    if args.target in {"all", "brj"}:
        touched_total += _run_target_refresh(
            db=db,
            df_source=source_df,
            target="brj",
            finnkodes=requested["brj"],
            include_inactive=args.include_inactive,
            dry_run=args.dry_run,
        )

    if args.target in {"all", "mvv"}:
        touched_total += _run_target_refresh(
            db=db,
            df_source=source_df,
            target="mvv",
            finnkodes=requested["mvv"],
            include_inactive=args.include_inactive,
            dry_run=args.dry_run,
        )

    print(f"Done. Total targeted row passes: {touched_total}")
    if args.dry_run:
        print("Dry-run mode: no API calls and no DB writes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
