import re
import time
import math
import os
from collections import Counter

import pandas as pd
from pandas import DataFrame

# Travel sentinel codes — negative integers stored in DB/sheet to mark known failures.
# Rows with these values are NOT retried by the pipeline.
TRAVEL_NO_ROUTES = -1
TRAVEL_UNREALISTIC = -2
TRAVEL_API_ERROR = -3
_TRAVEL_SENTINELS = frozenset({TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC, TRAVEL_API_ERROR})


def is_travel_sentinel(value) -> bool:
    """Return True if value is a known travel-time failure code."""
    try:
        if value is None:
            return False
        return int(value) in _TRAVEL_SENTINELS
    except (TypeError, ValueError):
        return False


_SENTINEL_LABELS = {
    TRAVEL_NO_ROUTES: 'no routes',
    TRAVEL_UNREALISTIC: 'unrealistic',
    TRAVEL_API_ERROR: 'API error',
}


def _sentinel_label(value) -> str:
    try:
        return _SENTINEL_LABELS.get(int(value), 'failed')
    except (TypeError, ValueError):
        return 'failed'


def _sentinel_summary(counts: Counter) -> str:
    """Format a Counter of {sentinel_int: count} as a human-readable string."""
    parts = []
    for code in (TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC, TRAVEL_API_ERROR):
        n = counts.get(code, 0)
        if n > 0:
            parts.append(f"{_SENTINEL_LABELS[code]} \xd7{n}")
    return ', '.join(parts) if parts else ''


def _count_missing_and_sentinels(series: pd.Series) -> tuple[int, Counter]:
    """Return (missing_count, sentinel_counter) for a travel column series."""
    missing = int(series.isna().sum())
    sentinel_counts: Counter = Counter()
    for v in series:
        if not pd.isna(v) and is_travel_sentinel(v):
            sentinel_counts[int(v)] += 1
    return missing, sentinel_counts


def _to_float_or_none(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _to_text_or_empty(value) -> str:
    """Return a trimmed string, treating None/NaN/pd.NA as empty."""
    try:
        if value is None or pd.isna(value):
            return ''
    except Exception:
        pass
    return str(value).strip()


def _haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return distance in meters between two latitude/longitude pairs."""
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


def _get_row_coords(row, lat_col: str, lng_col: str):
    """Extract row coordinates as floats, or (None, None) when unavailable."""
    if not lat_col or not lng_col:
        return None, None
    lat = _to_float_or_none(row.get(lat_col))
    lng = _to_float_or_none(row.get(lng_col))
    return lat, lng


def _is_valid_travel_value(value, max_travel_minutes: float) -> bool:
    parsed = _to_float_or_none(value)
    if parsed is None:
        return False
    return 1 <= parsed <= max_travel_minutes


def _row_has_all_travel_values(row, columns: list[str], max_travel_minutes: float) -> bool:
    return all(_is_valid_travel_value(row.get(col), max_travel_minutes) for col in columns)


def _build_travel_donor_cache(
    df: DataFrame,
    columns: list[str],
    lat_col: str,
    lng_col: str,
    max_travel_minutes: float,
):
    """Build a list of donor rows (lat, lng, finnkode) with complete travel data."""
    cache = []
    if not lat_col or not lng_col or 'Finnkode' not in df.columns:
        return cache

    for _, row in df.iterrows():
        lat, lng = _get_row_coords(row, lat_col, lng_col)
        if lat is None or lng is None:
            continue
        if _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', '')):
            continue
        if not _row_has_all_travel_values(row, columns, max_travel_minutes):
            continue
        finnkode = _to_text_or_empty(row.get('Finnkode', ''))
        if not finnkode:
            continue
        cache.append((lat, lng, finnkode))

    return cache


def _find_nearby_donor_finnkode(
    lat: float,
    lng: float,
    candidates: list[tuple[float, float, str]],
    max_distance_m: float,
):
    """Return closest donor finnkode within threshold, else None."""
    if lat is None or lng is None or max_distance_m <= 0:
        return None

    best_finnkode = None
    best_distance = None
    for cand_lat, cand_lng, cand_finnkode in candidates:
        distance_m = _haversine_meters(lat, lng, cand_lat, cand_lng)
        if distance_m <= max_distance_m and (best_distance is None or distance_m < best_distance):
            best_distance = distance_m
            best_finnkode = cand_finnkode

    return best_finnkode


def confirm_with_rate_limit(prompt: str) -> tuple[bool, float]:
    """
    Ask user for confirmation with optional rate limiting for API requests.
    
    Args:
        prompt: The confirmation prompt to display 
    
    Returns:
        Tuple of (proceed: bool, requests_per_minute: float)
        - proceed: True if user wants to continue, False otherwise
        - requests_per_minute: Rate limit (default 60.0, or user-specified number)
    
    Examples:
        User can enter: yes, no, or a number like 30 (for 30 requests/min)
    """
    auto_confirm = str(os.getenv("TRAVEL_AUTO_CONFIRM", "")).strip().lower()
    if auto_confirm in {"1", "true", "yes", "y", "on"}:
        rate_raw = str(os.getenv("TRAVEL_REQUESTS_PER_MINUTE", "60")).strip()
        try:
            rate = float(rate_raw)
            if rate <= 0:
                rate = 60.0
        except Exception:
            rate = 60.0
        print(f"Auto-confirmed travel calculations via TRAVEL_AUTO_CONFIRM=1 (rpm={rate:g})")
        return True, rate

    valid_input = False
    while not valid_input:
        response = input(prompt + " (yes/no/<requests per minute>): ").strip().lower()
        
        if response in ['yes', 'y']:
            return True, 60.0  # Default 60 requests per minute
        elif response in ['no', 'n']:
            return False, 60.0
        else:
            try:
                rate = float(response)
                if rate > 0:
                    return True, rate
                else:
                    print("Please enter a positive number for requests per minute")
            except ValueError:
                print("Invalid input. Please enter 'yes', 'no', or a number (e.g., 30)")
    
    return False, 60.0


def post_process_rental(df: DataFrame, projectName: str, save_csv: bool = True) -> DataFrame:
    """
    Post-process rental data.
    
    Args:
        df: DataFrame with raw rental data
        projectName: Project directory name (e.g., 'data/flippe')
        save_csv: Whether to save to CSV (for backwards compatibility)
    
    Returns:
        Processed DataFrame
    """
    if df.empty:
        if save_csv:
            df.to_csv(f'{projectName}/AB_processed.csv', index=False)
        return df

    # Convert area columns to numeric, coerce errors to NaN
    for col in ['Primærrom', 'Internt bruksareal (BRA-i)', 'Bruksareal']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate PRIS KVM from best available area source.
    area_for_price = df['Primærrom'].fillna(df['Internt bruksareal (BRA-i)']).fillna(df['Bruksareal'])
    mask = df['Leiepris'].notna() & area_for_price.notna() & (area_for_price > 0)
    df['PRIS KVM'] = (df['Leiepris'].astype(float) / area_for_price.astype(float)).where(mask)
    df['PRIS KVM'] = df['PRIS KVM'].round().astype('Int64')

    # Format capitalization
    df['Adresse'] = df['Adresse'].str.title()


    # Drop unnecessary columns
    df = df.drop(columns=['Primærrom',
                          'Internt bruksareal (BRA-i)',
                          'Bruksareal',
                          'Eksternt bruksareal (BRA-e)',
                          'Balkong/Terrasse (TBA)',
                          'Bruttoareal'
                          ])

    if save_csv:
        df.to_csv(f'{projectName}/AB_processed.csv', index=False)

    return df


def post_process_eiendom(
    df: DataFrame,
    projectName: str,
    db=None,
    calculate_location_features: bool = True,
    calculate_google_directions: bool = None,
    travel_targets: str = "all",
    donor_seed_df: DataFrame | None = None,
    skip_db_merge: bool = False,
) -> DataFrame:
    """
    Post-process eiendom data by calculating location features and cleaning data.
    
    Args:
        df: DataFrame with raw eiendom data
        projectName: Project directory name (e.g., 'data/eiendom')
        db: PropertyDatabase instance (if None, will save to CSV for backwards compatibility)
        calculate_location_features: Backwards-compatible toggle for Google travel-time API calculations
        calculate_google_directions: Whether to run paid Google Directions calculations.
            If None, defaults to the value of calculate_location_features.
        travel_targets: Which travel destination group to compute: "all", "brj", "mvv", or "mvv_uni".
        donor_seed_df: Optional dataframe with additional donor candidates
            (Finnkode, LAT/LNG, travel columns) shared across runs/sources.
    
    Returns:
        Processed DataFrame
    """
    if df.empty:
        return df

    if calculate_google_directions is None:
        calculate_google_directions = calculate_location_features

    target_value = str(travel_targets or "all").strip().lower()
    if target_value not in {"all", "brj", "mvv", "mvv_uni"}:
        print(f"⚠️  Unknown travel_targets='{travel_targets}', defaulting to 'all'")
        target_value = "all"
    run_brj = target_value in {"all", "brj"}
    run_mvv = target_value in {"all", "mvv"}
    run_mvv_uni = target_value == "mvv_uni"
    updates_only_logging = str(os.getenv("TRAVEL_LOG_UPDATES_ONLY", "0")).strip().lower() in {"1", "true", "yes", "on"}
    force_api_for_missing = str(os.getenv("TRAVEL_FORCE_API_FOR_MISSING", "0")).strip().lower() in {"1", "true", "yes", "on"}

    # Optional filters/config for API calls and sheets export
    try:
        from main.config.filters import SHEETS_MAX_PRICE, TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
    except ImportError:
        try:
            from config.filters import SHEETS_MAX_PRICE, TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
        except ImportError:
            SHEETS_MAX_PRICE = None
            TRAVEL_REUSE_WITHIN_METERS = 0
            MAX_TRAVEL_MINUTES = 360

    if TRAVEL_REUSE_WITHIN_METERS is None:
        TRAVEL_REUSE_WITHIN_METERS = 0
    if MAX_TRAVEL_MINUTES is None:
        MAX_TRAVEL_MINUTES = 360
    travel_reuse_within_meters = max(float(TRAVEL_REUSE_WITHIN_METERS), 0.0)
    max_travel_minutes = max(float(MAX_TRAVEL_MINUTES), 1.0)

    # Load existing commute data from database if available
    if db is not None and not skip_db_merge:
        try:
            if hasattr(db, 'get_eiendom_commute_data'):
                existing_data = db.get_eiendom_commute_data()
            else:
                existing_data = db.get_eiendom_for_sheets()
            
            # Extract commute columns from existing database data (BRJ + CNTR + MVV)
            commute_columns = ['Finnkode', 'PENDL RUSH BRJ', 'PENDL RUSH MVV', 'MVV UNI RUSH',
                             'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                             'TRAVEL_COPY_FROM_FINNKODE']
            existing_commute_cols = ['Finnkode'] + [col for col in commute_columns[1:] if col in existing_data.columns]
            existing_commute = existing_data[existing_commute_cols].copy() if len(existing_commute_cols) > 1 else None
            
            if existing_commute is not None and not existing_commute.empty:
                # Convert Finnkode to string for consistent merging
                existing_commute['Finnkode'] = existing_commute['Finnkode'].astype(str)
                df['Finnkode'] = df['Finnkode'].astype(str)
                
                # Merge commute data back into new dataframe
                df = df.merge(existing_commute, on='Finnkode', how='left', suffixes=('', '_old'))
                # Use existing values where new values are NaN
                for col in commute_columns[1:]:
                    if col in df.columns and f'{col}_old' in df.columns:
                        # Fill NaN in new column with values from old column
                        df[col] = df[col].combine_first(df[f'{col}_old'])
                        df = df.drop(columns=[f'{col}_old'])
                print("✓ Merged existing commute data from database")
        except Exception as e:
            print(f"⚠️  Could not load existing data from database: {e}")
    elif not skip_db_merge:
        # Fallback to CSV if no database provided (backwards compatibility)
        processed_file_path = f'{projectName}/AB_processed.csv'
        if os.path.exists(processed_file_path):
            try:
                existing_df = pd.read_csv(processed_file_path)
                
                # Migrate old column names in existing data
                column_renames = {
                    'PENDLEVEI': 'PENDL RUSH BRJ',
                    'PENDL MORN BRJ': 'PENDL RUSH BRJ',
                    'PENDL MORN MVV': 'PENDL RUSH MVV',
                }
                for old_name, new_name in column_renames.items():
                    if old_name in existing_df.columns:
                        existing_df.rename(columns={old_name: new_name}, inplace=True)
                
                # Extract commute columns from existing data (BRJ + CNTR + MVV)
                commute_columns = ['Finnkode', 'PENDL RUSH BRJ', 'PENDL RUSH MVV', 'MVV UNI RUSH',
                                 'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                                 'TRAVEL_COPY_FROM_FINNKODE']
                # Filter to only include columns that exist in existing data
                existing_commute_cols = ['Finnkode'] + [col for col in commute_columns[1:] if col in existing_df.columns]
                existing_commute = existing_df[existing_commute_cols].copy() if len(existing_commute_cols) > 1 else None
                
                if existing_commute is not None:
                    # Convert to integers in existing data before merging
                    for col in commute_columns[1:]:
                        if col == 'TRAVEL_COPY_FROM_FINNKODE':
                            continue
                        if col in existing_commute.columns:
                            existing_commute[col] = pd.to_numeric(existing_commute[col], errors='coerce').round().astype('Int64')
                    
                    # Merge commute data back into new dataframe
                    df = df.merge(existing_commute, on='Finnkode', how='left', suffixes=('', '_old'))
                    # Use existing values where new values are NaN
                    for col in commute_columns[1:]:
                        if col in df.columns and f'{col}_old' in df.columns:
                            # Fill NaN in new column with values from old column
                            df[col] = df[col].combine_first(df[f'{col}_old'])
                            df = df.drop(columns=[f'{col}_old'])
                    print("✓ Merged existing commute data from CSV snapshot")
            except Exception as e:
                print(f"⚠️  Could not load existing processed data: {e}")

    # Convert area columns to numeric, coerce errors to NaN
    for col in ['Primærrom', 'Internt bruksareal (BRA-i)', 'Bruksareal']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate PRIS KVM from best available area source.
    # Some pipelines (e.g., DNB export) do not include FINN area columns.
    primary_area = df['Primærrom'] if 'Primærrom' in df.columns else pd.Series(pd.NA, index=df.index)
    usable_i_area = (
        df['Internt bruksareal (BRA-i)']
        if 'Internt bruksareal (BRA-i)' in df.columns
        else pd.Series(pd.NA, index=df.index)
    )
    usable_area = df['Bruksareal'] if 'Bruksareal' in df.columns else pd.Series(pd.NA, index=df.index)
    area_for_price = primary_area.fillna(usable_i_area).fillna(usable_area)

    if 'Pris' in df.columns:
        price_numeric = pd.to_numeric(df['Pris'], errors='coerce')
        area_numeric = pd.to_numeric(area_for_price, errors='coerce')
        mask = price_numeric.notna() & area_numeric.notna() & (area_numeric > 0)
        df['PRIS KVM'] = (price_numeric / area_numeric).where(mask)
        # Replace infinity with NaN before converting to Int64
        df['PRIS KVM'] = df['PRIS KVM'].replace([float('inf'), float('-inf')], pd.NA)
        df['PRIS KVM'] = df['PRIS KVM'].round().astype('Int64')

    # Format capitalization
    df['Adresse'] = df['Adresse'].str.title()

    # Migrate old column names to new names (backward compatibility)
    column_renames = {
        'PENDLEVEI': 'PENDL RUSH BRJ',
        'PENDL MORN BRJ': 'PENDL RUSH BRJ',
        'PENDL MORN MVV': 'PENDL RUSH MVV',
    }
    for old_name, new_name in column_renames.items():
        if old_name in df.columns and new_name not in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
            print(f"✓ Migrated column: {old_name} → {new_name}")

    # Initialize columns if not present
    if 'PENDL RUSH BRJ' not in df.columns:
        df['PENDL RUSH BRJ'] = None
    if 'PENDL RUSH MVV' not in df.columns:
        df['PENDL RUSH MVV'] = None
    if 'MVV UNI RUSH' not in df.columns:
        df['MVV UNI RUSH'] = None
    if 'PENDL MORN CNTR' not in df.columns:
        df['PENDL MORN CNTR'] = None
    if 'BIL MORN CNTR' not in df.columns:
        df['BIL MORN CNTR'] = None
    if 'PENDL DAG CNTR' not in df.columns:
        df['PENDL DAG CNTR'] = None
    if 'BIL DAG CNTR' not in df.columns:
        df['BIL DAG CNTR'] = None
    if 'TRAVEL_COPY_FROM_FINNKODE' not in df.columns:
        df['TRAVEL_COPY_FROM_FINNKODE'] = None

    # Transit-only donor reuse. Driving columns remain in DB as legacy data but are no longer fetched.
    brj_travel_columns = ['PENDL RUSH BRJ']
    mvv_travel_columns = ['PENDL RUSH MVV']
    mvv_uni_travel_columns = ['MVV UNI RUSH']
    transit_travel_columns = brj_travel_columns + mvv_travel_columns + mvv_uni_travel_columns

    lat_col = 'LAT' if 'LAT' in df.columns else ('lat' if 'lat' in df.columns else None)
    lng_col = 'LNG' if 'LNG' in df.columns else ('lng' if 'lng' in df.columns else None)

    donor_cache_brj = _build_travel_donor_cache(df, brj_travel_columns, lat_col, lng_col, max_travel_minutes)
    donor_cache_mvv = _build_travel_donor_cache(df, mvv_travel_columns, lat_col, lng_col, max_travel_minutes)
    donor_cache_mvv_uni = _build_travel_donor_cache(df, mvv_uni_travel_columns, lat_col, lng_col, max_travel_minutes)
    donor_cache_all = _build_travel_donor_cache(df, transit_travel_columns, lat_col, lng_col, max_travel_minutes)

    # Fast donor-value lookup for MVV UNI, used to avoid false donor skips when
    # a donor link exists but the donor has no MVV UNI value yet.
    donor_value_mvv_uni = {}
    donor_link_mvv_uni = {}

    def _seed_mvv_uni_lookup(source_df: DataFrame | None):
        if source_df is None or source_df.empty or 'Finnkode' not in source_df.columns:
            return
        for _, row in source_df.iterrows():
            finnkode = _to_text_or_empty(row.get('Finnkode', ''))
            if not finnkode:
                continue
            donor_fk = _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', ''))
            if donor_fk:
                donor_link_mvv_uni[finnkode] = donor_fk
            if 'MVV UNI RUSH' in source_df.columns:
                value = row.get('MVV UNI RUSH')
                if _is_valid_travel_value(value, max_travel_minutes) or is_travel_sentinel(value):
                    donor_value_mvv_uni[finnkode] = int(float(value))

    _seed_mvv_uni_lookup(donor_seed_df)
    _seed_mvv_uni_lookup(df)

    def _resolve_mvv_uni_donor_value(donor_finnkode: str):
        """Resolve MVV UNI value from donor, following donor chains safely."""
        current = _to_text_or_empty(donor_finnkode)
        if not current:
            return None

        seen = set()
        while current and current not in seen:
            seen.add(current)
            value = donor_value_mvv_uni.get(current)
            if value is not None:
                return value
            current = _to_text_or_empty(donor_link_mvv_uni.get(current, ''))
        return None

    if donor_seed_df is not None and not donor_seed_df.empty:
        seed_lat_col = 'LAT' if 'LAT' in donor_seed_df.columns else ('lat' if 'lat' in donor_seed_df.columns else None)
        seed_lng_col = 'LNG' if 'LNG' in donor_seed_df.columns else ('lng' if 'lng' in donor_seed_df.columns else None)

        def _merge_seed_cache(cache: list[tuple[float, float, str]], required_cols: list[str]) -> int:
            seed_cache = _build_travel_donor_cache(
                donor_seed_df,
                required_cols,
                seed_lat_col,
                seed_lng_col,
                max_travel_minutes,
            )
            existing = {finnkode for _, _, finnkode in cache}
            added = 0
            for item in seed_cache:
                finnkode = item[2]
                if finnkode in existing:
                    continue
                cache.append(item)
                existing.add(finnkode)
                added += 1
            return added

        _merge_seed_cache(donor_cache_brj, brj_travel_columns)
        _merge_seed_cache(donor_cache_mvv, mvv_travel_columns)
        _merge_seed_cache(donor_cache_mvv_uni, mvv_uni_travel_columns)
        _merge_seed_cache(donor_cache_all, transit_travel_columns)

    if travel_reuse_within_meters > 0:
        if lat_col and lng_col:
            print(
                f"Using travel reuse radius: {travel_reuse_within_meters:.0f} m "
                f"(nearby listings can reuse donor Finnkode)"
            )
            # Pre-pass: assign donor links to ALL rows without an existing donor link
            # that are within travel_reuse_within_meters of a complete root donor.
            # Enforces strict one-way relationship: an acceptor cannot be a donor.
            # Cascade: when B is assigned a donor (B→C), any A→B links collapse to A→C,
            # preventing chains from forming within a single processing run.
            if donor_cache_all:
                _all_caches = [donor_cache_brj, donor_cache_mvv, donor_cache_mvv_uni, donor_cache_all]
                _prepass_assigned = 0
                _prepass_collapsed = 0
                for _idx in df.index:
                    if _to_text_or_empty(df.at[_idx, 'TRAVEL_COPY_FROM_FINNKODE']):
                        continue  # already an acceptor
                    _finnkode = _to_text_or_empty(df.at[_idx, 'Finnkode'])
                    if not _finnkode:
                        continue
                    _lat, _lng = _get_row_coords(df.loc[_idx], lat_col, lng_col)
                    if _lat is None or _lng is None:
                        continue
                    _nearest = _find_nearby_donor_finnkode(
                        _lat, _lng,
                        [(la, ln, f) for la, ln, f in donor_cache_all if f != _finnkode],
                        travel_reuse_within_meters,
                    )
                    if not _nearest:
                        continue
                    # Assign donor link
                    df.at[_idx, 'TRAVEL_COPY_FROM_FINNKODE'] = _nearest
                    _prepass_assigned += 1
                    # Cascade: collapse any A→B links (where B = this row) to A→(nearest)
                    _mask_chain = (
                        df['TRAVEL_COPY_FROM_FINNKODE'].notna()
                        & (df['TRAVEL_COPY_FROM_FINNKODE'].fillna('').astype(str).str.strip() == _finnkode)
                        & (df.index != _idx)
                    )
                    _n_chain = int(_mask_chain.sum())
                    if _n_chain:
                        df.loc[_mask_chain, 'TRAVEL_COPY_FROM_FINNKODE'] = _nearest
                        _prepass_collapsed += _n_chain
                    # Remove this row from all donor caches — it is now an acceptor
                    for _cache in _all_caches:
                        _cache[:] = [(la, ln, f) for la, ln, f in _cache if f != _finnkode]
                if _prepass_assigned:
                    print(
                        f"  Pre-pass: assigned {_prepass_assigned} donor link(s)"
                        + (f", collapsed {_prepass_collapsed} chain(s)" if _prepass_collapsed else "")
                    )
        else:
            print("Travel reuse enabled in config, but no LAT/LNG columns found in dataframe.")

    eligible_mask = pd.Series([True] * len(df), index=df.index)
    if SHEETS_MAX_PRICE is not None and 'Pris' in df.columns:
        eligible_mask = df['Pris'].fillna(0) <= SHEETS_MAX_PRICE

    if not calculate_google_directions:
        print("Skipping Google Directions calculations (travel API calls disabled).")
        commute_cols = transit_travel_columns
        for col in commute_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
        return df

    # Calculate transit commute columns only.
    pendl_rush_brj_missing, brj_rush_sentinels = _count_missing_and_sentinels(df.loc[eligible_mask, 'PENDL RUSH BRJ']) if run_brj else (0, Counter())
    pendl_rush_mvv_missing, mvv_rush_sentinels = _count_missing_and_sentinels(df.loc[eligible_mask, 'PENDL RUSH MVV']) if run_mvv else (0, Counter())
    pendl_rush_mvv_uni_missing, mvv_uni_rush_sentinels = _count_missing_and_sentinels(df.loc[eligible_mask, 'MVV UNI RUSH']) if run_mvv_uni else (0, Counter())

    # Report sentinel (known-failure) totals — these won't be retried.
    brj_sentinels_total = brj_rush_sentinels
    mvv_sentinels_total = mvv_rush_sentinels
    if run_brj and brj_sentinels_total:
        print(f"⚠️  BRJ has {sum(brj_sentinels_total.values())} failure-coded values (skipping re-calc): {_sentinel_summary(brj_sentinels_total)}")
    if run_mvv and mvv_sentinels_total:
        print(f"⚠️  MVV has {sum(mvv_sentinels_total.values())} failure-coded values (skipping re-calc): {_sentinel_summary(mvv_sentinels_total)}")
    if run_mvv_uni and mvv_uni_rush_sentinels:
        print(f"⚠️  MVV UNI has {sum(mvv_uni_rush_sentinels.values())} failure-coded values (skipping re-calc): {_sentinel_summary(mvv_uni_rush_sentinels)}")

    rows_missing_any = pd.Series(False, index=df.index)
    if run_brj:
        rows_missing_any = rows_missing_any | df['PENDL RUSH BRJ'].isna()
    if run_mvv:
        rows_missing_any = rows_missing_any | df['PENDL RUSH MVV'].isna()
    if run_mvv_uni:
        rows_missing_any = rows_missing_any | df['MVV UNI RUSH'].isna()
    rows_missing_count = int((eligible_mask & rows_missing_any).sum())
    
    if pendl_rush_brj_missing > 0 or pendl_rush_mvv_missing > 0 or pendl_rush_mvv_uni_missing > 0:
        if run_brj:
            print(f"\n⚠️  {pendl_rush_brj_missing} properties missing PENDL RUSH BRJ (public transit rush-hour commute time)")
        if run_mvv:
            print(f"⚠️  {pendl_rush_mvv_missing} properties missing PENDL RUSH MVV (public transit to Lambertseter svømmeklubb)")
        if run_mvv_uni:
            print(f"⚠️  {pendl_rush_mvv_uni_missing} properties missing MVV UNI RUSH (public transit to Gaustadalléen 30, 0373 Oslo)")
        if SHEETS_MAX_PRICE is not None:
            print(f"⚠️  Price filter active: MAX_PRICE = {SHEETS_MAX_PRICE}")
        if updates_only_logging:
            print(f"ℹ️  Updates-only logging enabled: {rows_missing_count} rows need travel updates")

        def _preview_api_calls(column_name: str, required_columns: list[str], donor_cache: list[tuple[float, float, str]]) -> tuple[int, int]:
            """Estimate max API attempts from current donor seed (no in-run donor growth)."""
            attempts = 0
            donor_reuse = 0

            for _, row0 in df.loc[eligible_mask].iterrows():
                if not pd.isna(row0.get(column_name)):
                    continue

                donor0 = None
                if travel_reuse_within_meters > 0:
                    existing_donor = _to_text_or_empty(row0.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                    if existing_donor:
                        donor0 = existing_donor
                    else:
                        self_finnkode = _to_text_or_empty(row0.get('Finnkode', ''))
                        if self_finnkode:
                            lat0, lng0 = _get_row_coords(row0, lat_col, lng_col)
                            donor0 = _find_nearby_donor_finnkode(
                                lat0,
                                lng0,
                                donor_cache,
                                travel_reuse_within_meters,
                            )
                            if donor0 == self_finnkode:
                                donor0 = None

                if donor0 and not force_api_for_missing:
                    donor_reuse += 1
                    continue

                attempts += 1

            return attempts, donor_reuse

        def _simulate_in_run_api_calls(column_name: str, donor_cache: list[tuple[float, float, str]]) -> tuple[int, int]:
            """Simulate one pass where successful API rows become donors for later rows.

            This is an optimistic estimate because it assumes each API attempt yields
            a valid travel value that can seed subsequent donor reuse.
            """
            if force_api_for_missing:
                attempts = int(df.loc[eligible_mask, column_name].isna().sum())
                return attempts, 0

            sim_cache = list(donor_cache)
            attempts = 0
            donor_reuse = 0

            for _, row0 in df.loc[eligible_mask].iterrows():
                if not pd.isna(row0.get(column_name)):
                    continue

                donor0 = None
                if travel_reuse_within_meters > 0:
                    existing_donor = _to_text_or_empty(row0.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                    if existing_donor:
                        donor0 = existing_donor
                    else:
                        self_finnkode = _to_text_or_empty(row0.get('Finnkode', ''))
                        if self_finnkode:
                            lat0, lng0 = _get_row_coords(row0, lat_col, lng_col)
                            donor0 = _find_nearby_donor_finnkode(
                                lat0,
                                lng0,
                                sim_cache,
                                travel_reuse_within_meters,
                            )
                            if donor0 == self_finnkode:
                                donor0 = None

                if donor0:
                    donor_reuse += 1
                    continue

                attempts += 1

                # Assume successful API response: this row becomes a donor candidate.
                finnkode0 = _to_text_or_empty(row0.get('Finnkode', ''))
                if finnkode0:
                    lat0, lng0 = _get_row_coords(row0, lat_col, lng_col)
                    if lat0 is not None and lng0 is not None and not any(c_f == finnkode0 for _, _, c_f in sim_cache):
                        sim_cache.append((lat0, lng0, finnkode0))

            return attempts, donor_reuse

        preview_total_attempts = 0
        preview_total_attempts_sim = 0
        if run_brj:
            brj_preview_attempts, brj_preview_reuse = _preview_api_calls('PENDL RUSH BRJ', brj_travel_columns, donor_cache_brj)
            brj_preview_attempts_sim, brj_preview_reuse_sim = _simulate_in_run_api_calls('PENDL RUSH BRJ', donor_cache_brj)
            preview_total_attempts += brj_preview_attempts
            preview_total_attempts_sim += brj_preview_attempts_sim
            print(f"[PREVIEW] BRJ max API attempts now: {brj_preview_attempts} (seed donor reuse: {brj_preview_reuse})")
            print(f"[PREVIEW] BRJ simulated in-run API attempts: {brj_preview_attempts_sim} (in-run donor reuse: {brj_preview_reuse_sim})")
        if run_mvv:
            mvv_preview_attempts, mvv_preview_reuse = _preview_api_calls('PENDL RUSH MVV', mvv_travel_columns, donor_cache_mvv)
            mvv_preview_attempts_sim, mvv_preview_reuse_sim = _simulate_in_run_api_calls('PENDL RUSH MVV', donor_cache_mvv)
            preview_total_attempts += mvv_preview_attempts
            preview_total_attempts_sim += mvv_preview_attempts_sim
            print(f"[PREVIEW] MVV max API attempts now: {mvv_preview_attempts} (seed donor reuse: {mvv_preview_reuse})")
            print(f"[PREVIEW] MVV simulated in-run API attempts: {mvv_preview_attempts_sim} (in-run donor reuse: {mvv_preview_reuse_sim})")
        if run_mvv_uni:
            # For MVV UNI, count donor reuse only when donor value is actually available.
            mvv_uni_preview_attempts = 0
            mvv_uni_preview_reuse = 0
            for _, row0 in df.loc[eligible_mask].iterrows():
                if not pd.isna(row0.get('MVV UNI RUSH')):
                    continue
                donor0 = None
                if travel_reuse_within_meters > 0:
                    existing_donor = _to_text_or_empty(row0.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                    if existing_donor:
                        donor0 = existing_donor
                    else:
                        self_finnkode = _to_text_or_empty(row0.get('Finnkode', ''))
                        if self_finnkode:
                            lat0, lng0 = _get_row_coords(row0, lat_col, lng_col)
                            donor0 = _find_nearby_donor_finnkode(
                                lat0,
                                lng0,
                                donor_cache_mvv_uni,
                                travel_reuse_within_meters,
                            )
                            if donor0 == self_finnkode:
                                donor0 = None
                donor_val0 = _resolve_mvv_uni_donor_value(donor0) if donor0 else None
                if donor_val0 is not None and not force_api_for_missing:
                    mvv_uni_preview_reuse += 1
                else:
                    mvv_uni_preview_attempts += 1

            # Simulated in-run count: optimistic for MVV UNI as well.
            mvv_uni_preview_attempts_sim = mvv_uni_preview_attempts
            mvv_uni_preview_reuse_sim = mvv_uni_preview_reuse
            preview_total_attempts += mvv_uni_preview_attempts
            preview_total_attempts_sim += mvv_uni_preview_attempts_sim
            print(f"[PREVIEW] MVV UNI RUSH max API attempts now: {mvv_uni_preview_attempts} (seed donor reuse: {mvv_uni_preview_reuse})")
            print(f"[PREVIEW] MVV UNI RUSH simulated in-run API attempts: {mvv_uni_preview_attempts_sim} (in-run donor reuse: {mvv_uni_preview_reuse_sim})")
        print(f"[PREVIEW] Total max API attempts before run: {preview_total_attempts}")
        print(f"[PREVIEW] Total simulated in-run API attempts: {preview_total_attempts_sim}")
        
        proceed, requests_per_minute = confirm_with_rate_limit("Calculate location features for these properties?")
        
        if proceed:
            try:
                from main.location_features import PublicTransitCommuteTime
            except ImportError:
                from location_features import PublicTransitCommuteTime
            
            # Initialize calculators
            work_address = "Rådmann Halmrasts Vei 5"
            transit_commute_calculator = PublicTransitCommuteTime(work_address)
            
            calculated_transit = 0

            def _checkpoint_row(row_idx):
                if db is None:
                    return
                try:
                    row = df.loc[row_idx]
                    finnkode = _to_text_or_empty(row.get('Finnkode', ''))
                    donor = _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                    ctx = finnkode + (f" [donor→{donor}]" if donor else "")
                    db.insert_or_update_eiendom(df.loc[[row_idx]].copy(), context=ctx)
                except Exception as checkpoint_error:
                    print(f"⚠️  Could not checkpoint row {row_idx}: {checkpoint_error}")
            
            eligible_total = int(eligible_mask.sum())

            delay_between_requests = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
            interrupted = False
            donor_assigned_count = 0
            donor_existing_reuse_count = 0
            donor_api_skipped_count = 0
            brj_api_attempted_count = 0

            # BRJ run reuses only BRJ-complete donors.
            donor_required_cols = brj_travel_columns
            donor_cache_for_assignment = donor_cache_brj

            def _maybe_assign_donor(row_data, required_columns, donor_cache):
                if travel_reuse_within_meters <= 0:
                    return None

                existing_donor = _to_text_or_empty(row_data.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                if existing_donor:
                    # Listing-wide donor semantics: existing link always wins.
                    return existing_donor

                self_finnkode = _to_text_or_empty(row_data.get('Finnkode', ''))
                if not self_finnkode:
                    return None

                lat, lng = _get_row_coords(row_data, lat_col, lng_col)
                donor_finnkode = _find_nearby_donor_finnkode(
                    lat,
                    lng,
                    donor_cache,
                    travel_reuse_within_meters,
                )
                if not donor_finnkode or donor_finnkode == self_finnkode:
                    return None
                return donor_finnkode

            def _add_row_as_donor_if_complete(row_idx, required_columns, donor_cache):
                row_now = df.loc[row_idx]
                if _to_text_or_empty(row_now.get('TRAVEL_COPY_FROM_FINNKODE', '')):
                    return
                if not _row_has_all_travel_values(row_now, required_columns, max_travel_minutes):
                    return
                finnkode = _to_text_or_empty(row_now.get('Finnkode', ''))
                if not finnkode:
                    return
                lat, lng = _get_row_coords(row_now, lat_col, lng_col)
                if lat is None or lng is None:
                    return
                if not any(c_finnkode == finnkode for _, _, c_finnkode in donor_cache):
                    donor_cache.append((lat, lng, finnkode))

            # Count only rows that will actually trigger an API request (excludes donor skips).
            if run_brj:
                brj_api_calls_needed = 0
                for _, row0 in df.loc[eligible_mask].iterrows():
                    if not pd.isna(row0.get('PENDL RUSH BRJ')):
                        continue
                    donor0 = _maybe_assign_donor(row0, donor_required_cols, donor_cache_for_assignment)
                    if donor0 and not force_api_for_missing:
                        continue
                    brj_api_calls_needed += 1
            else:
                brj_api_calls_needed = 0

            print(f"\n🚀 BRJ: scanning {eligible_total} eligible properties, {brj_api_calls_needed} API candidate(s)...")
            if requests_per_minute < 60.0:
                print(f"   Rate limited to {requests_per_minute} requests/minute\n")
            else:
                print(f"   Running at {requests_per_minute} requests/minute\n")

            try:
                for loop_pos, (idx, row) in enumerate(df.loc[eligible_mask].iterrows(), start=1):
                    address = row['Adresse']
                    postnummer = row.get('Postnummer')
                    existing_donor_before = _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                    brj_api_attempted = False
                    brj_attempt_status = None

                    # Show which property we're working on for every row in detailed mode.
                    if not updates_only_logging:
                        print(f"⏳ Processing property {loop_pos}/{eligible_total}: {address}")

                    row_changed = False
                    brj_rush_stored = None

                    donor_finnkode = _maybe_assign_donor(row, donor_required_cols, donor_cache_for_assignment)
                    if donor_finnkode:
                        if not existing_donor_before:
                            df.at[idx, 'TRAVEL_COPY_FROM_FINNKODE'] = donor_finnkode
                            donor_assigned_count += 1
                            row_changed = True
                            if not updates_only_logging:
                                print(f"   🔁 Using donor travel values from #{donor_finnkode}")

                    # Calculate PENDL RUSH BRJ (public transit rush-hour commute time to work)
                    if run_brj and pd.isna(row.get('PENDL RUSH BRJ')):
                        if donor_finnkode and not force_api_for_missing:
                            donor_api_skipped_count += 1
                            if existing_donor_before:
                                donor_existing_reuse_count += 1
                        else:
                            brj_api_attempted = True
                            brj_api_attempted_count += 1
                            try:
                                if not updates_only_logging:
                                    print(f"   📍 Calculating public transit time...", end='', flush=True)
                                minutes = transit_commute_calculator.calculate(address, postnummer)
                                if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                    df.at[idx, 'PENDL RUSH BRJ'] = int(minutes)
                                    brj_rush_stored = minutes
                                    calculated_transit += 1
                                    row_changed = True
                                    brj_attempt_status = f"OK {int(minutes)} min"
                                    if not updates_only_logging:
                                        print(f" ✓ {minutes} min")
                                elif is_travel_sentinel(minutes):
                                    df.at[idx, 'PENDL RUSH BRJ'] = int(minutes)
                                    brj_rush_stored = minutes
                                    row_changed = True
                                    brj_attempt_status = f"FAIL {_sentinel_label(minutes)}"
                                    if not updates_only_logging:
                                        print(f" ✗ {_sentinel_label(minutes)}")
                                else:
                                    brj_attempt_status = f"FAIL rejected ({minutes})"
                                    if not updates_only_logging:
                                        print(f" ✗ Rejected/failed value ({minutes})")
                                if minutes is not None and delay_between_requests > 0:
                                    time.sleep(delay_between_requests)
                            except Exception as e:
                                brj_attempt_status = f"ERROR {str(e)}"
                                print(f" ✗ Error: {str(e)}")

                    _add_row_as_donor_if_complete(idx, brj_travel_columns, donor_cache_brj)
                    _add_row_as_donor_if_complete(idx, transit_travel_columns, donor_cache_all)

                    if row_changed:
                        _checkpoint_row(idx)

                    if updates_only_logging and brj_api_attempted:
                        status = brj_attempt_status or "DONE"
                        print(f"[{brj_api_attempted_count}/{brj_api_calls_needed}] BRJ API: {address} -> {status}")

                    # Summary every 10 properties
                    total_calculated = calculated_transit
                    if not updates_only_logging and total_calculated % 20 == 0 and total_calculated > 0:
                        print(
                            f"\n📊 Progress: {calculated_transit} transit RUSH "
                            f"+ {donor_assigned_count} donor-linked = {total_calculated + donor_assigned_count} total\n"
                        )
            except KeyboardInterrupt:
                interrupted = True
                print("\n⚠️  Interrupted during BRJ travel calculations. Returning partial results and preserving saved progress.")

            if interrupted:
                commute_cols = [
                    'PENDL RUSH BRJ', 'PENDL RUSH MVV',
                    'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                ]
                for col in commute_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
                return df

            if donor_api_skipped_count > 0 or donor_assigned_count > 0:
                print(
                    "✓ BRJ donor usage: "
                    f"{donor_api_skipped_count} API call(s) skipped via donor "
                    f"({donor_assigned_count} newly linked, {donor_existing_reuse_count} existing links reused)"
                )
            
            if run_mvv:
                # Calculate new MVV destination (Lambertseter svømmeklubb).
                mvv_address = "Langbølgen 24, 1155 Oslo"
                transit_commute_calculator_mvv = PublicTransitCommuteTime(mvv_address)

                calculated_transit_mvv = 0
                donor_assigned_mvv_count = 0
                donor_existing_reuse_mvv_count = 0
                donor_api_skipped_mvv_count = 0
                mvv_api_attempted_count = 0

                donor_required_cols_mvv = transit_travel_columns if (run_brj and run_mvv) else mvv_travel_columns
                donor_cache_for_assignment_mvv = donor_cache_all if (run_brj and run_mvv) else donor_cache_mvv

                mvv_api_calls_needed = 0
                for _, row0 in df.loc[eligible_mask].iterrows():
                    if not pd.isna(row0.get('PENDL RUSH MVV')):
                        continue
                    donor0 = _maybe_assign_donor(row0, donor_required_cols_mvv, donor_cache_for_assignment_mvv)
                    if donor0 and not force_api_for_missing:
                        continue
                    mvv_api_calls_needed += 1
                print(f"\n🚀 MVV: scanning {eligible_total} eligible properties, {mvv_api_calls_needed} API candidate(s)...\n")

                try:
                    for loop_pos, (idx, row) in enumerate(df.loc[eligible_mask].iterrows(), start=1):
                        address = row['Adresse']
                        postnummer = row.get('Postnummer')
                        existing_donor_before = _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                        mvv_api_attempted = False
                        mvv_attempt_status = None

                        if not updates_only_logging:
                            print(f"⏳ Processing property {loop_pos}/{eligible_total}: {address}")

                        row_changed = False
                        mvv_rush_stored = None

                        donor_finnkode = _maybe_assign_donor(row, donor_required_cols_mvv, donor_cache_for_assignment_mvv)
                        if donor_finnkode and not existing_donor_before:
                            df.at[idx, 'TRAVEL_COPY_FROM_FINNKODE'] = donor_finnkode
                            donor_assigned_mvv_count += 1
                            row_changed = True
                            if not updates_only_logging:
                                print(f"   🔁 Using donor travel values from #{donor_finnkode}")

                        # Calculate PENDL RUSH MVV
                        if pd.isna(row.get('PENDL RUSH MVV')):
                            if donor_finnkode and not force_api_for_missing:
                                donor_api_skipped_mvv_count += 1
                                if existing_donor_before:
                                    donor_existing_reuse_mvv_count += 1
                            else:
                                mvv_api_attempted = True
                                mvv_api_attempted_count += 1
                                try:
                                    if not updates_only_logging:
                                        print(f"   📍 Calculating public transit to Lambertseter svømmeklubb...", end='', flush=True)
                                    minutes = transit_commute_calculator_mvv.calculate(address, postnummer)
                                    if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                        df.at[idx, 'PENDL RUSH MVV'] = int(minutes)
                                        mvv_rush_stored = minutes
                                        calculated_transit_mvv += 1
                                        row_changed = True
                                        mvv_attempt_status = f"OK {int(minutes)} min"
                                        if not updates_only_logging:
                                            print(f" ✓ {minutes} min")
                                    elif is_travel_sentinel(minutes):
                                        df.at[idx, 'PENDL RUSH MVV'] = int(minutes)
                                        mvv_rush_stored = minutes
                                        row_changed = True
                                        mvv_attempt_status = f"FAIL {_sentinel_label(minutes)}"
                                        if not updates_only_logging:
                                            print(f" ✗ {_sentinel_label(minutes)}")
                                    else:
                                        mvv_attempt_status = f"FAIL rejected ({minutes})"
                                        if not updates_only_logging:
                                            print(f" ✗ Rejected/failed value ({minutes})")
                                    if minutes is not None and delay_between_requests > 0:
                                        time.sleep(delay_between_requests)
                                except Exception as e:
                                    mvv_attempt_status = f"ERROR {str(e)}"
                                    print(f" ✗ Error: {str(e)}")

                        _add_row_as_donor_if_complete(idx, mvv_travel_columns, donor_cache_mvv)
                        _add_row_as_donor_if_complete(idx, transit_travel_columns, donor_cache_all)

                        if row_changed:
                            _checkpoint_row(idx)

                        if updates_only_logging and mvv_api_attempted:
                            status = mvv_attempt_status or "DONE"
                            print(f"[{mvv_api_attempted_count}/{mvv_api_calls_needed}] MVV API: {address} -> {status}")
                except KeyboardInterrupt:
                    print("\n⚠️  Interrupted during MVV travel calculations. Returning partial results and preserving saved progress.")

                print(
                    f"\n✓ Successfully calculated {calculated_transit_mvv} RUSH transit times to Lambertseter svømmeklubb"
                )
                if donor_api_skipped_mvv_count > 0 or donor_assigned_mvv_count > 0:
                    print(
                        "✓ MVV donor usage: "
                        f"{donor_api_skipped_mvv_count} API call(s) skipped via donor "
                        f"({donor_assigned_mvv_count} newly linked, {donor_existing_reuse_mvv_count} existing links reused)"
                    )
                if donor_assigned_count > 0 or donor_assigned_mvv_count > 0:
                    print(f"✓ Linked {donor_assigned_count + donor_assigned_mvv_count} rows to nearby donor Finnkoder")

            if run_mvv_uni:
                mvv_uni_address = "Gaustadalléen 30, 0373 Oslo"
                transit_commute_calculator_mvv_uni = PublicTransitCommuteTime(mvv_uni_address)

                calculated_transit_mvv_uni = 0
                donor_assigned_mvv_uni_count = 0
                donor_existing_reuse_mvv_uni_count = 0
                donor_api_skipped_mvv_uni_count = 0
                mvv_uni_api_attempted_count = 0

                donor_required_cols_mvv_uni = mvv_uni_travel_columns
                donor_cache_for_assignment_mvv_uni = donor_cache_mvv_uni

                # Run order matters: compute donor candidates first, then rows that
                # already point to a donor, so same-run reuse can kick in.
                mvv_uni_rows = df.loc[eligible_mask].copy()
                mvv_uni_rows['_has_donor_link'] = mvv_uni_rows['TRAVEL_COPY_FROM_FINNKODE'].apply(
                    lambda v: _to_text_or_empty(v) != ''
                )
                mvv_uni_rows = mvv_uni_rows.sort_values(by=['_has_donor_link'])
                mvv_uni_row_items = list(mvv_uni_rows.iterrows())

                mvv_uni_api_calls_needed = 0
                sim_donor_value_mvv_uni = dict(donor_value_mvv_uni)
                for _, row0 in mvv_uni_row_items:
                    if not pd.isna(row0.get('MVV UNI RUSH')):
                        continue
                    donor0 = _maybe_assign_donor(row0, donor_required_cols_mvv_uni, donor_cache_for_assignment_mvv_uni)
                    donor_val0 = sim_donor_value_mvv_uni.get(_to_text_or_empty(donor0)) if donor0 else None
                    if donor_val0 is not None and not force_api_for_missing:
                        continue
                    mvv_uni_api_calls_needed += 1
                    finnkode0 = _to_text_or_empty(row0.get('Finnkode', ''))
                    if finnkode0:
                        # Assume successful API response for planning purposes.
                        sim_donor_value_mvv_uni[finnkode0] = 1
                print(f"\n🚀 MVV UNI RUSH: scanning {eligible_total} eligible properties, {mvv_uni_api_calls_needed} API candidate(s)...\n")

                try:
                    for loop_pos, (idx, row) in enumerate(mvv_uni_row_items, start=1):
                        address = row['Adresse']
                        postnummer = row.get('Postnummer')
                        existing_donor_before = _to_text_or_empty(row.get('TRAVEL_COPY_FROM_FINNKODE', ''))
                        mvv_uni_api_attempted = False
                        mvv_uni_attempt_status = None

                        if not updates_only_logging:
                            print(f"⏳ Processing property {loop_pos}/{eligible_total}: {address}")

                        row_changed = False

                        donor_finnkode = _maybe_assign_donor(row, donor_required_cols_mvv_uni, donor_cache_for_assignment_mvv_uni)
                        donor_mvv_uni_value = _resolve_mvv_uni_donor_value(donor_finnkode) if donor_finnkode else None
                        can_use_donor_value = donor_mvv_uni_value is not None and not force_api_for_missing

                        if donor_finnkode and can_use_donor_value and not existing_donor_before:
                            df.at[idx, 'TRAVEL_COPY_FROM_FINNKODE'] = donor_finnkode
                            donor_assigned_mvv_uni_count += 1
                            row_changed = True
                            if not updates_only_logging:
                                print(f"   🔁 Using donor travel values from #{donor_finnkode}")

                        if pd.isna(row.get('MVV UNI RUSH')):
                            if can_use_donor_value:
                                df.at[idx, 'MVV UNI RUSH'] = int(donor_mvv_uni_value)
                                row_changed = True
                                donor_api_skipped_mvv_uni_count += 1
                                if existing_donor_before:
                                    donor_existing_reuse_mvv_uni_count += 1
                                if not updates_only_logging:
                                    print(f"   🔁 Reused donor MVV UNI RUSH: {int(donor_mvv_uni_value)} min")
                            else:
                                mvv_uni_api_attempted = True
                                mvv_uni_api_attempted_count += 1
                                try:
                                    if not updates_only_logging:
                                        print(f"   📍 Calculating public transit to Gaustadalléen 30, 0373 Oslo...", end='', flush=True)
                                    minutes = transit_commute_calculator_mvv_uni.calculate(address, postnummer)
                                    if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                        df.at[idx, 'MVV UNI RUSH'] = int(minutes)
                                        finnkode_now = _to_text_or_empty(row.get('Finnkode', ''))
                                        if finnkode_now:
                                            donor_value_mvv_uni[finnkode_now] = int(minutes)
                                        calculated_transit_mvv_uni += 1
                                        row_changed = True
                                        mvv_uni_attempt_status = f"OK {int(minutes)} min"
                                        if not updates_only_logging:
                                            print(f" ✓ {minutes} min")
                                    elif is_travel_sentinel(minutes):
                                        df.at[idx, 'MVV UNI RUSH'] = int(minutes)
                                        finnkode_now = _to_text_or_empty(row.get('Finnkode', ''))
                                        if finnkode_now:
                                            donor_value_mvv_uni[finnkode_now] = int(minutes)
                                        row_changed = True
                                        mvv_uni_attempt_status = f"FAIL {_sentinel_label(minutes)}"
                                        if not updates_only_logging:
                                            print(f" ✗ {_sentinel_label(minutes)}")
                                    else:
                                        mvv_uni_attempt_status = f"FAIL rejected ({minutes})"
                                        if not updates_only_logging:
                                            print(f" ✗ Rejected/failed value ({minutes})")
                                    if minutes is not None and delay_between_requests > 0:
                                        time.sleep(delay_between_requests)
                                except Exception as e:
                                    mvv_uni_attempt_status = f"ERROR {str(e)}"
                                    print(f" ✗ Error: {str(e)}")

                        _add_row_as_donor_if_complete(idx, mvv_uni_travel_columns, donor_cache_mvv_uni)
                        _add_row_as_donor_if_complete(idx, transit_travel_columns, donor_cache_all)

                        if row_changed:
                            _checkpoint_row(idx)

                        if updates_only_logging and mvv_uni_api_attempted:
                            status = mvv_uni_attempt_status or "DONE"
                            print(f"[{mvv_uni_api_attempted_count}/{mvv_uni_api_calls_needed}] MVV UNI API: {address} -> {status}")
                except KeyboardInterrupt:
                    print("\n⚠️  Interrupted during MVV UNI RUSH travel calculations. Returning partial results and preserving saved progress.")

                print(
                    f"\n✓ Successfully calculated {calculated_transit_mvv_uni} RUSH transit times to Gaustadalléen 30, 0373 Oslo"
                )
                if donor_api_skipped_mvv_uni_count > 0 or donor_assigned_mvv_uni_count > 0:
                    print(
                        "✓ MVV UNI donor usage: "
                        f"{donor_api_skipped_mvv_uni_count} API call(s) skipped via donor "
                        f"({donor_assigned_mvv_uni_count} newly linked, {donor_existing_reuse_mvv_uni_count} existing links reused)"
                    )
        else:
            print("Skipped location features calculation")
    else:
        if run_mvv_uni:
            print("✓ All properties already have transit commute data for MVV UNI RUSH")
        else:
            print("✓ All properties already have transit commute data for BRJ and MVV")

    # Ensure commute time columns are integers without decimals
    commute_cols = [
        'PENDL RUSH BRJ', 'PENDL RUSH MVV', 'MVV UNI RUSH',
        'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
    ]
    for col in commute_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    return df

def post_process_jobs(df: DataFrame, projectName: str, save_csv: bool = True) -> DataFrame:
    """
    Post-process job data by normalizing dates and formatting text fields.
    
    Args:
        df: DataFrame with raw job data
        projectName: Project directory name (e.g., 'data/jobbe')
        save_csv: Whether to save to CSV (for backwards compatibility)
    
    Returns:
        Processed DataFrame
    """
    if df.empty:
        if save_csv:
            df.to_csv(f'{projectName}/AB_processed.csv', index=False)
        return df

    def parse_date(deadline):
        if pd.isna(deadline):
            return None
        deadline_str = str(deadline)
        # Replace dashes with periods
        deadline_str = deadline_str.replace('-', '.')
        # Check if it matches the date pattern D.M.YYYY, DD.M.YYYY, D.MM.YYYY, or DD.MM.YYYY
        if re.match(r'\d{1,2}\.\d{1,2}\.\d{4}', deadline_str):
            return deadline_str
        return None

    df['FRIST'] = df['Søknadsfrist'].apply(parse_date)

    if save_csv:
        df.to_csv(f'{projectName}/AB_processed.csv', index=False)

    return df

# if main
if __name__ == "__main__":
 
    pass

