#!/usr/bin/env python3
"""Validate stored travel values using local-coordinate and postcode heuristics.

This tool is read-only. It loads the same DB-backed listing scope used for sheets,
then flags suspicious travel values without calling external APIs.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import pandas as pd

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


TARGET_COLUMNS = {
    "brj": ["PENDL RUSH BRJ"],
    "mvv": ["PENDL RUSH MVV"],
    "mvv_uni": ["MVV UNI RUSH"],
    "all": ["PENDL RUSH BRJ", "PENDL RUSH MVV"],
}

DEFAULT_LIVE_SCOPE_CSV = "data/eiendom/A_live.csv"


def _load_defaults() -> tuple[float, float | None]:
    try:
        from main.config.filters import TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
    except ImportError:
        try:
            from config.filters import TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
        except ImportError:
            TRAVEL_REUSE_WITHIN_METERS = 750
            MAX_TRAVEL_MINUTES = 360

    radius = max(float(TRAVEL_REUSE_WITHIN_METERS or 0), 0.0)
    max_travel = float(MAX_TRAVEL_MINUTES) if MAX_TRAVEL_MINUTES is not None else None
    return radius, max_travel


def _to_float_or_none(value) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _normalize_postnummer(value) -> str:
    if value is None or pd.isna(value):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return raw.lower()
    if len(digits) <= 4:
        return digits.zfill(4)
    return digits


def _normalize_finnkode(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
    except Exception:
        pass
    return text


def _haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)

    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * radius_m * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(pd.Series(values).median())


def _mad(values: list[float], center: float) -> float:
    if not values:
        return 0.0
    return float(pd.Series([abs(v - center) for v in values]).median())


def _is_valid_travel(value: object, max_travel_minutes: float | None) -> bool:
    parsed = _to_float_or_none(value)
    if parsed is None:
        return False
    if parsed < 1:
        return False
    if max_travel_minutes is not None and parsed > max_travel_minutes:
        return False
    return True


def _format_reason(label: str, value: float, median: float, diff: float, group_size: int) -> str:
    value_i = int(round(value))
    median_i = int(round(median))
    diff_i = int(round(diff))
    direction = "higher" if value >= median else "lower"

    if label == "local":
        return f"Local: {value_i}m ({diff_i}m {direction} vs near med {median_i}, n={group_size})"

    if label == "postcode":
        return f"Postnr: {value_i}m ({diff_i}m {direction} vs med {median_i}, n={group_size})"

    return f"Outlier: {value_i}m ({diff_i}m from med {median_i}, n={group_size})"


def parse_args() -> argparse.Namespace:
    default_radius, default_max_travel = _load_defaults()
    parser = argparse.ArgumentParser(
        description="Validate stored travel values using coordinates, postcode, and donor checks",
    )
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv", "mvv_uni"],
        default="all",
        help="Which travel target(s) to validate",
    )
    parser.add_argument(
        "--radius-meters",
        type=float,
        default=default_radius if default_radius > 0 else 750.0,
        help="Radius for nearby-comparison checks (default: TRAVEL_REUSE_WITHIN_METERS or 750)",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive listings in validation",
    )
    parser.add_argument(
        "--live-scope-csv",
        default=DEFAULT_LIVE_SCOPE_CSV,
        help=(
            "CSV used to constrain validation to current live FINN scope "
            f"(default: {DEFAULT_LIVE_SCOPE_CSV})"
        ),
    )
    parser.add_argument(
        "--disable-live-scope",
        action="store_true",
        help="Disable live-scope filtering and validate all DB-backed sheet rows",
    )
    parser.add_argument(
        "--min-neighbors",
        type=int,
        default=5,
        help="Minimum nearby rows required for local outlier checks (default: 5)",
    )
    parser.add_argument(
        "--min-postcode-group",
        type=int,
        default=6,
        help="Minimum postcode cohort size required for postcode checks (default: 6)",
    )
    parser.add_argument(
        "--min-abs-diff",
        type=float,
        default=20.0,
        help="Minimum absolute deviation in minutes before a value is suspicious (default: 20)",
    )
    parser.add_argument(
        "--min-relative-diff",
        type=float,
        default=0.35,
        help="Minimum relative deviation versus comparison median (default: 0.35)",
    )
    parser.add_argument(
        "--mad-multiplier",
        type=float,
        default=2.5,
        help="Minimum multiple of MAD required for local/postcode outlier checks (default: 2.5)",
    )
    parser.add_argument(
        "--score-threshold",
        type=int,
        default=3,
        help="Only emit findings with at least this suspicion score (default: 3)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="How many flagged rows to print (default: 50)",
    )
    parser.add_argument(
        "--csv",
        help="Optional output CSV path for the full findings table",
    )
    parser.add_argument(
        "--full-table",
        action="store_true",
        help="Print full wide table (default prints compact readable table)",
    )
    parser.add_argument(
        "--max-travel-minutes",
        type=float,
        default=default_max_travel,
        help="Upper bound for valid stored travel values (default: MAX_TRAVEL_MINUTES)",
    )
    return parser.parse_args()


def _truncate_text(value: object, max_len: int) -> str:
    text = "" if value is None or pd.isna(value) else str(value)
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


def _print_findings_table(findings: pd.DataFrame, top: int, full_table: bool) -> None:
    show = findings.head(max(top, 0)).copy()
    if show.empty:
        return

    if full_table:
        print(show.to_string(index=False))
        return

    # Compact default output keeps only high-signal fields and truncates long text.
    show["target"] = show["target"].map({
        "PENDL RUSH BRJ": "BRJ",
        "PENDL RUSH MVV": "MVV",
        "MVV UNI RUSH": "MVV_UNI",
    }).fillna(show["target"])
    show["Adresse"] = show["Adresse"].apply(lambda v: _truncate_text(v, 28))
    if "GOOGLE_MAPS_URL" in show.columns:
        show["GOOGLE_MAPS_URL"] = show["GOOGLE_MAPS_URL"].apply(lambda v: _truncate_text(v, 56))
        show["reason"] = show["reason"].apply(lambda v: _truncate_text(v, 50))

    display_cols = [
        "suspicion_score",
        "target",
        "Adresse",
        "GOOGLE_MAPS_URL",
        "reason",
    ]
    existing_cols = [c for c in display_cols if c in show.columns]
    print(show[existing_cols].to_string(index=False))


def _load_live_scope_finnkodes(csv_path: str) -> set[str] | None:
    """Load current live FINN ids from CSV; return None when unavailable."""
    if not csv_path:
        return None

    path = Path(csv_path)
    if not path.exists():
        print(f"[WARN] Live scope CSV not found: {path}. Continuing without live-scope filtering.")
        return None

    try:
        source = pd.read_csv(path, dtype=str)
    except Exception as exc:
        print(f"[WARN] Could not read live scope CSV {path}: {exc}. Continuing without live-scope filtering.")
        return None

    finnkode_col = None
    for col in source.columns:
        if str(col).strip().lower() == "finnkode":
            finnkode_col = col
            break

    if not finnkode_col:
        print(f"[WARN] Live scope CSV {path} has no 'Finnkode' column. Continuing without live-scope filtering.")
        return None

    normalized = {
        _normalize_finnkode(value)
        for value in source[finnkode_col].tolist()
        if _normalize_finnkode(value)
    }
    if not normalized:
        print(f"[WARN] Live scope CSV {path} has no usable finnkoder. Continuing without live-scope filtering.")
        return None
    return normalized


def _prepare_source_dataframe(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, tuple[float | None, float | None]]]:
    db = PropertyDatabase()
    df = db.get_eiendom_for_sheets()
    if df.empty:
        return df, {}

    # Default behavior: keep validation aligned with latest crawl/search scope.
    # This avoids analyzing rows that are outside current URL filters/polygon.
    if not args.disable_live_scope and not args.include_inactive and "Finnkode" in df.columns:
        live_scope_ids = _load_live_scope_finnkodes(args.live_scope_csv)
        if live_scope_ids is not None:
            finnkode_norm = df["Finnkode"].apply(_normalize_finnkode)
            df = df.loc[finnkode_norm.isin(live_scope_ids)].copy()

    if not args.include_inactive and "active" in df.columns:
        active_mask = pd.to_numeric(df["active"], errors="coerce").fillna(1).astype(int) == 1
        df = df.loc[active_mask].copy()

    if df.empty:
        return df, {}

    if "ADRESSE" in df.columns and "Adresse" not in df.columns:
        df["Adresse"] = df["ADRESSE"]

    df["_row_id"] = range(len(df))
    df["_lat"] = df.get("LAT", pd.Series(index=df.index, dtype="float64")).apply(_to_float_or_none)
    df["_lng"] = df.get("LNG", pd.Series(index=df.index, dtype="float64")).apply(_to_float_or_none)
    df["_postnummer_norm"] = df.get("Postnummer", pd.Series(index=df.index, dtype="object")).apply(_normalize_postnummer)

    for col in TARGET_COLUMNS[args.target]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    donor_seed = db.get_travel_donor_seed()
    donor_coords: dict[str, tuple[float | None, float | None]] = {}
    donor_links: dict[str, str] = {}
    if donor_seed is not None and not donor_seed.empty:
        for _, row in donor_seed.iterrows():
            finnkode = _normalize_finnkode(row.get("Finnkode"))
            if not finnkode:
                continue
            donor_coords[finnkode] = (
                _to_float_or_none(row.get("LAT")),
                _to_float_or_none(row.get("LNG")),
            )
            donor_finnkode = _normalize_finnkode(row.get("TRAVEL_COPY_FROM_FINNKODE"))
            if donor_finnkode and donor_finnkode != finnkode:
                donor_links[finnkode] = donor_finnkode

    representative_cache: dict[str, str] = {}

    def _resolve_representative(finnkode: object) -> str:
        start = _normalize_finnkode(finnkode)
        if not start:
            return ""
        if start in representative_cache:
            return representative_cache[start]

        current = start
        seen: set[str] = set()
        while True:
            donor = donor_links.get(current, "")
            donor = _normalize_finnkode(donor)
            if not donor or donor == current or donor in seen:
                break
            seen.add(current)
            current = donor

        representative = current
        representative_cache[start] = representative
        for item in seen:
            representative_cache[item] = representative
        return representative

    df["_finnkode_norm"] = df.get("Finnkode", pd.Series(index=df.index, dtype="object")).apply(_normalize_finnkode)
    df["_group_representative"] = df["_finnkode_norm"].apply(_resolve_representative)
    df["_is_group_representative"] = df["_finnkode_norm"] == df["_group_representative"]
    return df, donor_coords


def _build_spatial_buckets(work_df: pd.DataFrame, radius_meters: float) -> tuple[dict[tuple[int, int], list[int]], float, float]:
    if radius_meters <= 0:
        return {}, 1.0, 1.0

    lat_series = pd.to_numeric(work_df["_lat"], errors="coerce")
    mean_lat = float(lat_series.dropna().mean()) if lat_series.notna().any() else 60.0
    lat_step = max(radius_meters / 111320.0, 0.0001)
    lng_step = max(radius_meters / (111320.0 * max(0.1, math.cos(math.radians(mean_lat)))), 0.0001)

    buckets: dict[tuple[int, int], list[int]] = {}
    lat_values = work_df["_lat"].tolist()
    lng_values = work_df["_lng"].tolist()
    for pos, (lat, lng) in enumerate(zip(lat_values, lng_values)):
        lat = _to_float_or_none(lat)
        lng = _to_float_or_none(lng)
        if lat is None or lng is None:
            continue
        key = (int(lat / lat_step), int(lng / lng_step))
        buckets.setdefault(key, []).append(pos)

    return buckets, lat_step, lng_step


def _candidate_positions(
    lat: float | None,
    lng: float | None,
    buckets: dict[tuple[int, int], list[int]],
    lat_step: float,
    lng_step: float,
) -> list[int]:
    lat = _to_float_or_none(lat)
    lng = _to_float_or_none(lng)
    if lat is None or lng is None or not buckets:
        return []

    lat_bucket = int(lat / lat_step)
    lng_bucket = int(lng / lng_step)
    positions: list[int] = []
    for lat_offset in (-1, 0, 1):
        for lng_offset in (-1, 0, 1):
            positions.extend(buckets.get((lat_bucket + lat_offset, lng_bucket + lng_offset), []))
    return positions


def _score_against_group(
    value: float,
    peers: list[float],
    min_abs_diff: float,
    min_relative_diff: float,
    mad_multiplier: float,
) -> tuple[bool, float | None, float | None, float | None]:
    median = _median(peers)
    if median is None:
        return False, None, None, None
    diff = abs(value - median)
    rel = diff / max(abs(median), 1.0)
    mad = _mad(peers, median)
    robust_threshold = mad_multiplier * max(mad, 1.0)
    suspicious = diff >= max(min_abs_diff, robust_threshold) and rel >= min_relative_diff
    return suspicious, median, diff, mad


def _build_findings(
    df: pd.DataFrame,
    donor_coords: dict[str, tuple[float | None, float | None]],
    args: argparse.Namespace,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    findings: list[dict[str, object]] = []
    targets = TARGET_COLUMNS[args.target]

    for travel_col in targets:
        valid_mask = df[travel_col].apply(lambda value: _is_valid_travel(value, args.max_travel_minutes))
        work_df = df.loc[valid_mask].copy()
        if work_df.empty:
            continue

        if "_group_representative" in work_df.columns:
            work_df["_group_sort_key"] = work_df.get("Finnkode", pd.Series(index=work_df.index, dtype="object")).astype(str)
            work_df.sort_values(
                by=["_group_representative", "_is_group_representative", "_group_sort_key"],
                ascending=[True, False, True],
                inplace=True,
            )
            work_df = work_df.drop_duplicates(subset=["_group_representative"], keep="first").copy()
            work_df.drop(columns=["_group_sort_key"], inplace=True, errors="ignore")

        records = work_df.to_dict("records")
        buckets, lat_step, lng_step = _build_spatial_buckets(work_df, args.radius_meters)

        postcode_groups: dict[str, list[tuple[int, float]]] = {}
        for row in records:
            post = row.get("_postnummer_norm", "")
            if not post:
                continue
            postcode_groups.setdefault(post, []).append((int(row["_row_id"]), float(row[travel_col])))

        for pos, row in enumerate(records):
            row_id = int(row["_row_id"])
            value = float(row[travel_col])
            lat = _to_float_or_none(row.get("_lat"))
            lng = _to_float_or_none(row.get("_lng"))
            post = row.get("_postnummer_norm", "")
            score = 0
            reasons: list[str] = []
            local_neighbor_count = 0
            postcode_group_size = 0
            donor_distance_m = None

            if lat is not None and lng is not None and args.radius_meters > 0:
                local_values = []
                for peer_pos in _candidate_positions(lat, lng, buckets, lat_step, lng_step):
                    if peer_pos == pos:
                        continue
                    peer = records[peer_pos]
                    peer_id = int(peer["_row_id"])
                    if peer_id == row_id:
                        continue
                    peer_lat = _to_float_or_none(peer.get("_lat"))
                    peer_lng = _to_float_or_none(peer.get("_lng"))
                    if peer_lat is None or peer_lng is None:
                        continue
                    if _haversine_meters(lat, lng, peer_lat, peer_lng) <= args.radius_meters:
                        local_values.append(float(peer[travel_col]))

                local_neighbor_count = len(local_values)
                if local_neighbor_count >= args.min_neighbors:
                    suspicious, median, diff, _ = _score_against_group(
                        value,
                        local_values,
                        args.min_abs_diff,
                        args.min_relative_diff,
                        args.mad_multiplier,
                    )
                    if suspicious and median is not None and diff is not None:
                        score += 3
                        reasons.append(_format_reason("local", value, median, diff, local_neighbor_count))

            if post:
                postcode_values = [peer_value for peer_id, peer_value in postcode_groups.get(post, []) if peer_id != row_id]
                postcode_group_size = len(postcode_values)
                if postcode_group_size >= args.min_postcode_group:
                    suspicious, median, diff, _ = _score_against_group(
                        value,
                        postcode_values,
                        args.min_abs_diff + 5.0,
                        args.min_relative_diff,
                        args.mad_multiplier,
                    )
                    if suspicious and median is not None and diff is not None:
                        score += 2
                        reasons.append(_format_reason("postcode", value, median, diff, postcode_group_size))

            donor_finnkode = str(row.get("TRAVEL_COPY_FROM_FINNKODE", "") or "").strip()
            if donor_finnkode and lat is not None and lng is not None:
                donor_lat, donor_lng = donor_coords.get(donor_finnkode, (None, None))
                donor_lat = _to_float_or_none(donor_lat)
                donor_lng = _to_float_or_none(donor_lng)
                if donor_lat is not None and donor_lng is not None:
                    donor_distance_m = _haversine_meters(lat, lng, donor_lat, donor_lng)
                    if donor_distance_m > args.radius_meters:
                        score += 3
                        reasons.append(
                            f"Donor: {int(round(donor_distance_m))}m > {int(round(args.radius_meters))}m ({donor_finnkode})"
                        )

            if score < args.score_threshold:
                continue

            findings.append(
                {
                    "suspicion_score": score,
                    "target": travel_col,
                    "Finnkode": row.get("Finnkode"),
                    "GROUP_REPRESENTATIVE_FINNKODE": row.get("_group_representative") or row.get("Finnkode"),
                    "Adresse": row.get("Adresse") or row.get("ADRESSE"),
                    "Postnummer": row.get("Postnummer"),
                    "GOOGLE_MAPS_URL": row.get("GOOGLE_MAPS_URL"),
                    "travel_minutes": int(round(value)),
                    "active": row.get("active"),
                    "neighbor_count": local_neighbor_count,
                    "postcode_group_size": postcode_group_size,
                    "donor_distance_m": int(round(donor_distance_m)) if donor_distance_m is not None else pd.NA,
                    "TRAVEL_COPY_FROM_FINNKODE": donor_finnkode or pd.NA,
                    "LAT": row.get("LAT"),
                    "LNG": row.get("LNG"),
                    "reason": " | ".join(reasons),
                }
            )

    if not findings:
        return pd.DataFrame()

    out = pd.DataFrame(findings)
    out.sort_values(
        by=["suspicion_score", "target", "travel_minutes"],
        ascending=[False, True, False],
        inplace=True,
    )
    out.reset_index(drop=True, inplace=True)
    return out


def main() -> int:
    args = parse_args()
    df, donor_coords = _prepare_source_dataframe(args)

    if df.empty:
        print("No listings available for validation.")
        return 0

    findings = _build_findings(df, donor_coords, args)

    total_rows = len(df)
    targets = ", ".join(TARGET_COLUMNS[args.target])
    print("=" * 72)
    print("Travel Value Validation Report")
    print("=" * 72)
    print(f"Listings scanned: {total_rows}")
    print(f"Targets: {targets}")
    print(f"Nearby radius: {int(round(args.radius_meters))} m")
    print(f"Score threshold: {args.score_threshold}")

    if findings.empty:
        print("No suspicious travel values found with current thresholds.")
        return 0

    print(f"Flagged findings: {len(findings)}")
    print()
    _print_findings_table(findings, args.top, args.full_table)

    if args.csv:
        findings.to_csv(args.csv, index=False)
        print()
        print(f"Saved full findings to: {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())