#!/usr/bin/env python3
"""Re-request travel values only for listings flagged as suspicious.

Workflow:
- Read findings CSV from validate_travel_values.py
- Filter to rows at/above suspicion score threshold
- Collapse donor-linked groups to a single representative donor Finnkode
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
    "MVV UNI RUSH": "mvv_uni",
    "BRJ": "brj",
    "MVV": "mvv",
    "MVV_UNI": "mvv_uni",
    "brj": "brj",
    "mvv": "mvv",
    "mvv_uni": "mvv_uni",
}

TRAVEL_COLUMN_BY_TARGET = {
    "brj": "PENDL RUSH BRJ",
    "mvv": "PENDL RUSH MVV",
    "mvv_uni": "MVV UNI RUSH",
}


def _to_int_or_none(value: object) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(round(float(value)))
    except Exception:
        return None


def _print_update_differences(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    requested_columns_by_finnkode: dict[str, set[str]],
) -> None:
    if before_df.empty or after_df.empty:
        print("No before/after data available for diff report.")
        return

    compare_cols = ["Finnkode", "Adresse"] + list(TRAVEL_COLUMN_BY_TARGET.values())
    before = before_df[[col for col in compare_cols if col in before_df.columns]].copy()
    after = after_df[[col for col in compare_cols if col in after_df.columns]].copy()
    before["Finnkode"] = before["Finnkode"].apply(_normalize_finnkode)
    after["Finnkode"] = after["Finnkode"].apply(_normalize_finnkode)

    before = before.rename(
        columns={
            "Adresse": "Adresse_before",
            "PENDL RUSH BRJ": "old_brj",
            "PENDL RUSH MVV": "old_mvv",
            "MVV UNI RUSH": "old_mvv_uni",
        }
    )
    after = after.rename(
        columns={
            "Adresse": "Adresse_after",
            "PENDL RUSH BRJ": "new_brj",
            "PENDL RUSH MVV": "new_mvv",
            "MVV UNI RUSH": "new_mvv_uni",
        }
    )

    merged = before.merge(after, on="Finnkode", how="inner")
    if merged.empty:
        print("No overlapping rows available for diff report.")
        return

    changed_rows: list[dict[str, object]] = []
    requested_cell_count = 0
    changed_cell_count = 0
    for _, row in merged.iterrows():
        finnkode = _normalize_finnkode(row.get("Finnkode"))
        requested_cols = requested_columns_by_finnkode.get(finnkode, set())
        adresse_after = row.get("Adresse_after")
        adresse_before = row.get("Adresse_before")
        if pd.isna(adresse_after):
            adresse_after = None
        if pd.isna(adresse_before):
            adresse_before = None
        adresse = str(adresse_after if adresse_after is not None else (adresse_before or ""))

        for col in sorted(requested_cols):
            requested_cell_count += 1
            if col == "PENDL RUSH BRJ":
                old_col, new_col, label = "old_brj", "new_brj", "BRJ"
            elif col == "PENDL RUSH MVV":
                old_col, new_col, label = "old_mvv", "new_mvv", "MVV"
            else:
                old_col, new_col, label = "old_mvv_uni", "new_mvv_uni", "MVV_UNI"
            old_value = _to_int_or_none(row.get(old_col))
            new_value = _to_int_or_none(row.get(new_col))
            if old_value != new_value:
                changed_cell_count += 1
                changed_rows.append(
                    {
                        "Finnkode": finnkode,
                        "Adresse": adresse,
                        "Target": label,
                        "Old": old_value,
                        "New": new_value,
                        "Delta": None
                        if old_value is None or new_value is None
                        else int(new_value - old_value),
                    }
                )

    changed_listing_count = len({item["Finnkode"] for item in changed_rows})
    print("Travel diff summary (requested targets):")
    print(f"  Changed cells: {changed_cell_count}/{requested_cell_count}")
    print(f"  Listings with at least one change: {changed_listing_count}")

    if not changed_rows:
        print("  No requested values changed.")
        return

    diff_df = pd.DataFrame(changed_rows)
    diff_df = diff_df.sort_values(["Target", "Finnkode"]).reset_index(drop=True)
    preview_count = min(len(diff_df), 30)
    print(f"  Showing {preview_count} changed target values:")
    print(diff_df.head(preview_count).to_string(index=False))


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
        choices=["all", "brj", "mvv", "mvv_uni"],
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


def _build_representative_map(db: PropertyDatabase) -> dict[str, str]:
    donor_seed = db.get_travel_donor_seed()
    if donor_seed is None or donor_seed.empty:
        return {}

    link_map: dict[str, str] = {}
    for _, row in donor_seed.iterrows():
        finnkode = _normalize_finnkode(row.get("Finnkode"))
        donor = _normalize_finnkode(row.get("TRAVEL_COPY_FROM_FINNKODE"))
        if finnkode and donor and donor != finnkode:
            link_map[finnkode] = donor

    representative_cache: dict[str, str] = {}

    def _resolve(finnkode: str) -> str:
        current = _normalize_finnkode(finnkode)
        if not current:
            return ""
        if current in representative_cache:
            return representative_cache[current]

        seen: set[str] = set()
        while True:
            donor = _normalize_finnkode(link_map.get(current, ""))
            if not donor or donor == current or donor in seen:
                break
            seen.add(current)
            current = donor

        representative = current
        representative_cache[_normalize_finnkode(finnkode)] = representative
        for item in seen:
            representative_cache[item] = representative
        return representative

    for finnkode in list(link_map):
        _resolve(finnkode)

    return representative_cache


def _load_requested_finnkodes(args: argparse.Namespace, representative_map: dict[str, str]) -> dict[str, set[str]]:
    df = pd.read_csv(args.findings_csv)
    if df.empty:
        return {"brj": set(), "mvv": set(), "mvv_uni": set()}

    if "Finnkode" not in df.columns:
        raise ValueError("Findings CSV is missing required column: Finnkode")
    if "target" not in df.columns:
        raise ValueError("Findings CSV is missing required column: target")

    if "suspicion_score" in df.columns:
        scores = pd.to_numeric(df["suspicion_score"], errors="coerce")
        df = df.loc[scores >= float(args.score_threshold)].copy()

    if df.empty:
        return {"brj": set(), "mvv": set(), "mvv_uni": set()}

    df["_target_norm"] = df["target"].apply(_normalize_target)
    df["_finnkode_norm"] = df["Finnkode"].apply(_normalize_finnkode)
    df["_reason_text"] = df.get("reason", "").astype(str)
    # Keep donor-distance findings on the originally flagged listing so we can
    # explicitly detach that child row from its donor before recomputing.
    donor_reason_mask = df["_reason_text"].str.contains("donor:", case=False, na=False)
    df["_representative_finnkode"] = df["_finnkode_norm"].apply(
        lambda value: representative_map.get(value, value)
    )
    df["_request_finnkode"] = df["_representative_finnkode"]
    df.loc[donor_reason_mask, "_request_finnkode"] = df.loc[donor_reason_mask, "_finnkode_norm"]
    df = df.loc[df["_target_norm"].notna() & (df["_finnkode_norm"] != "")].copy()

    if args.target in {"brj", "mvv", "mvv_uni"}:
        df = df.loc[df["_target_norm"] == args.target].copy()

    grouped = {"brj": set(), "mvv": set(), "mvv_uni": set()}
    for _, row in df.iterrows():
        grouped[row["_target_norm"]].add(row["_request_finnkode"])

    donor_direct_count = int(donor_reason_mask.sum())
    if donor_direct_count > 0:
        print(
            f"Targeting {donor_direct_count} donor-flagged finding rows by original Finnkode "
            f"(no representative collapse for those rows)."
        )
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


def _run_combined_refresh(
    db: PropertyDatabase,
    df_source: pd.DataFrame,
    requested: dict[str, set[str]],
    include_inactive: bool,
    dry_run: bool,
) -> int:
    requested_columns_by_finnkode: dict[str, set[str]] = {}
    for target, finnkodes in requested.items():
        col = TRAVEL_COLUMN_BY_TARGET[target]
        for finnkode in finnkodes:
            requested_columns_by_finnkode.setdefault(finnkode, set()).add(col)

    requested_finnkodes = set(requested_columns_by_finnkode)
    if not requested_finnkodes:
        return 0

    work = _restrict_input_rows(df_source, requested_finnkodes, include_inactive)
    if work.empty:
        print("No matching DB rows for requested Finnkoder")
        return 0

    before_values = work[[
        col for col in ["Finnkode", "Adresse", "PENDL RUSH BRJ", "PENDL RUSH MVV", "MVV UNI RUSH"] if col in work.columns
    ]].copy()

    work["_requested_columns"] = work["Finnkode"].map(requested_columns_by_finnkode)
    for col in TRAVEL_COLUMN_BY_TARGET.values():
        mask = work["_requested_columns"].apply(lambda cols: col in (cols or set()))
        work.loc[mask, col] = pd.NA
    work["TRAVEL_COPY_FROM_FINNKODE"] = pd.NA

    print(f"Requested unique target Finnkoder: {len(requested_finnkodes)}")
    print(f"Rows eligible for re-request: {len(work)}")
    print(f"  BRJ target Finnkoder: {len(requested['brj'])}")
    print(f"  MVV target Finnkoder: {len(requested['mvv'])}")
    print(f"  MVV UNI target Finnkoder: {len(requested['mvv_uni'])}")

    if dry_run:
        return len(work)

    non_empty_targets = [name for name, values in requested.items() if values]
    if len(non_empty_targets) == 1:
        travel_target = non_empty_targets[0]
    else:
        travel_target = "all"

    processed = post_process_eiendom(
        work,
        projectName="data/eiendom",
        db=db,
        calculate_location_features=True,
        calculate_google_directions=True,
        travel_targets=travel_target,
        skip_db_merge=True,
    )

    inserted, updated = db.insert_or_update_eiendom(processed)
    print(f"Database rows touched: {inserted} inserted, {updated} updated")
    _print_update_differences(before_values, processed, requested_columns_by_finnkode)
    return len(work)


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.findings_csv):
        print(f"Findings CSV not found: {args.findings_csv}")
        return 1

    db = PropertyDatabase()
    representative_map = _build_representative_map(db)

    try:
        requested = _load_requested_finnkodes(args, representative_map)
    except Exception as exc:
        print(f"Failed to parse findings CSV: {exc}")
        return 1

    total_flagged = len(requested["brj"] | requested["mvv"] | requested["mvv_uni"])
    print(f"Flagged Finnkoder after score filter: {total_flagged}")
    print(f"  BRJ flagged: {len(requested['brj'])}")
    print(f"  MVV flagged: {len(requested['mvv'])}")
    print(f"  MVV UNI flagged: {len(requested['mvv_uni'])}")

    if total_flagged == 0:
        print("No suspicious rows to re-request.")
        return 0

    source_df = db.get_eiendom_for_sheets()
    if source_df.empty:
        print("No listings available in DB source for re-request.")
        return 0

    targeted_request = {
        "brj": requested["brj"] if args.target in {"all", "brj"} else set(),
        "mvv": requested["mvv"] if args.target in {"all", "mvv"} else set(),
        "mvv_uni": requested["mvv_uni"] if args.target in {"all", "mvv_uni"} else set(),
    }

    touched_total = _run_combined_refresh(
        db=db,
        df_source=source_df,
        requested=targeted_request,
        include_inactive=args.include_inactive,
        dry_run=args.dry_run,
    )

    print(f"Done. Total targeted listings: {touched_total}")
    if args.dry_run:
        print("Dry-run mode: no API calls and no DB writes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
