import re
import time
import math

import pandas as pd
from pandas import DataFrame


def _to_float_or_none(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


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
        if str(row.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip():
            continue
        if not _row_has_all_travel_values(row, columns, max_travel_minutes):
            continue
        finnkode = str(row.get('Finnkode', '')).strip()
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
        travel_targets: Which travel destination group to compute: "all", "brj", or "mvv".
    
    Returns:
        Processed DataFrame
    """
    if df.empty:
        return df

    if calculate_google_directions is None:
        calculate_google_directions = calculate_location_features

    target_value = str(travel_targets or "all").strip().lower()
    if target_value not in {"all", "brj", "mvv"}:
        print(f"⚠️  Unknown travel_targets='{travel_targets}', defaulting to 'all'")
        target_value = "all"
    run_brj = target_value in {"all", "brj"}
    run_mvv = target_value in {"all", "mvv"}

    # Optional filters/config for API calls and sheets export
    try:
        from main.config.filters import MAX_PRICE, TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
    except ImportError:
        try:
            from config.filters import MAX_PRICE, TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
        except ImportError:
            MAX_PRICE = None
            TRAVEL_REUSE_WITHIN_METERS = 0
            MAX_TRAVEL_MINUTES = 360

    if TRAVEL_REUSE_WITHIN_METERS is None:
        TRAVEL_REUSE_WITHIN_METERS = 0
    if MAX_TRAVEL_MINUTES is None:
        MAX_TRAVEL_MINUTES = 360
    travel_reuse_within_meters = max(float(TRAVEL_REUSE_WITHIN_METERS), 0.0)
    max_travel_minutes = max(float(MAX_TRAVEL_MINUTES), 1.0)

    # Load existing commute data from database if available
    if db is not None:
        try:
            existing_data = db.get_eiendom_for_sheets()
            
            # Extract commute columns from existing database data (BRJ + CNTR + MVV)
            commute_columns = ['Finnkode', 'PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                             'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                             'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV',
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
    else:
        # Fallback to CSV if no database provided (backwards compatibility)
        import os
        processed_file_path = f'{projectName}/AB_processed.csv'
        if os.path.exists(processed_file_path):
            try:
                existing_df = pd.read_csv(processed_file_path)
                
                # Migrate old column names in existing data
                column_renames = {
                    'PENDLEVEI': 'PENDL MORN BRJ',
                    'KJØRETID': 'BIL MORN BRJ',
                    'PENDLEVEI_RETUR_16': 'PENDL DAG BRJ',
                    'KJØRETID_RETUR_16': 'BIL DAG BRJ'
                }
                for old_name, new_name in column_renames.items():
                    if old_name in existing_df.columns:
                        existing_df.rename(columns={old_name: new_name}, inplace=True)
                
                # Extract commute columns from existing data (BRJ + CNTR + MVV)
                commute_columns = ['Finnkode', 'PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                                 'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                                 'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV',
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
    area_for_price = df['Primærrom'].fillna(df['Internt bruksareal (BRA-i)']).fillna(df['Bruksareal'])
    mask = df['Pris'].notna() & area_for_price.notna() & (area_for_price > 0)
    df['PRIS KVM'] = (df['Pris'].astype(float) / area_for_price.astype(float)).where(mask)
    # Replace infinity with NaN before converting to Int64
    df['PRIS KVM'] = df['PRIS KVM'].replace([float('inf'), float('-inf')], pd.NA)
    df['PRIS KVM'] = df['PRIS KVM'].round().astype('Int64')

    # Format capitalization
    df['Adresse'] = df['Adresse'].str.title()

    # Migrate old column names to new names (backward compatibility)
    column_renames = {
        'PENDLEVEI': 'PENDL MORN BRJ',
        'KJØRETID': 'BIL MORN BRJ',
        'PENDLEVEI_RETUR_16': 'PENDL DAG BRJ',
        'KJØRETID_RETUR_16': 'BIL DAG BRJ'
    }
    for old_name, new_name in column_renames.items():
        if old_name in df.columns and new_name not in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
            print(f"✓ Migrated column: {old_name} → {new_name}")

    # Initialize columns if not present
    if 'PENDL MORN BRJ' not in df.columns:
        df['PENDL MORN BRJ'] = None
    if 'BIL MORN BRJ' not in df.columns:
        df['BIL MORN BRJ'] = None
    if 'PENDL DAG BRJ' not in df.columns:
        df['PENDL DAG BRJ'] = None
    if 'BIL DAG BRJ' not in df.columns:
        df['BIL DAG BRJ'] = None
    if 'PENDL MORN CNTR' not in df.columns:
        df['PENDL MORN CNTR'] = None
    if 'BIL MORN CNTR' not in df.columns:
        df['BIL MORN CNTR'] = None
    if 'PENDL DAG CNTR' not in df.columns:
        df['PENDL DAG CNTR'] = None
    if 'BIL DAG CNTR' not in df.columns:
        df['BIL DAG CNTR'] = None
    if 'PENDL MORN MVV' not in df.columns:
        df['PENDL MORN MVV'] = None
    if 'BIL MORN MVV' not in df.columns:
        df['BIL MORN MVV'] = None
    if 'PENDL DAG MVV' not in df.columns:
        df['PENDL DAG MVV'] = None
    if 'BIL DAG MVV' not in df.columns:
        df['BIL DAG MVV'] = None
    if 'TRAVEL_COPY_FROM_FINNKODE' not in df.columns:
        df['TRAVEL_COPY_FROM_FINNKODE'] = None

    # Transit-only donor reuse. Driving columns remain in DB as legacy data but are no longer fetched.
    brj_travel_columns = ['PENDL MORN BRJ', 'PENDL DAG BRJ']
    mvv_travel_columns = ['PENDL MORN MVV', 'PENDL DAG MVV']
    transit_travel_columns = brj_travel_columns + mvv_travel_columns

    lat_col = 'LAT' if 'LAT' in df.columns else ('lat' if 'lat' in df.columns else None)
    lng_col = 'LNG' if 'LNG' in df.columns else ('lng' if 'lng' in df.columns else None)

    donor_cache_brj = _build_travel_donor_cache(df, brj_travel_columns, lat_col, lng_col, max_travel_minutes)
    donor_cache_mvv = _build_travel_donor_cache(df, mvv_travel_columns, lat_col, lng_col, max_travel_minutes)

    # Debugging: show donor cache sizes to help diagnose missing donor assignments
    try:
        print(f"Debug: donor_cache_brj size: {len(donor_cache_brj)}; sample: {[c[2] for c in donor_cache_brj[:5]]}")
        print(f"Debug: donor_cache_mvv size: {len(donor_cache_mvv)}; sample: {[c[2] for c in donor_cache_mvv[:5]]}")
    except Exception:
        # Keep debug prints best-effort and non-fatal
        pass

    if travel_reuse_within_meters > 0:
        if lat_col and lng_col:
            print(
                f"Using travel reuse radius: {travel_reuse_within_meters:.0f} m "
                f"(nearby listings can reuse donor Finnkode)"
            )
        else:
            print("Travel reuse enabled in config, but no LAT/LNG columns found in dataframe.")
    
    eligible_mask = pd.Series([True] * len(df), index=df.index)
    if MAX_PRICE is not None and 'Pris' in df.columns:
        eligible_mask = df['Pris'].fillna(0) <= MAX_PRICE

    if not calculate_google_directions:
        print("Skipping Google Directions calculations (travel API calls disabled).")
        commute_cols = transit_travel_columns
        for col in commute_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
        return df

    # Calculate transit commute columns only.
    pendl_morn_missing = df.loc[eligible_mask, 'PENDL MORN BRJ'].isna().sum() if run_brj else 0
    pendl_dag_missing = df.loc[eligible_mask, 'PENDL DAG BRJ'].isna().sum() if run_brj else 0
    pendl_morn_mvv_missing = df.loc[eligible_mask, 'PENDL MORN MVV'].isna().sum() if run_mvv else 0
    pendl_dag_mvv_missing = df.loc[eligible_mask, 'PENDL DAG MVV'].isna().sum() if run_mvv else 0
    
    if pendl_morn_missing > 0 or pendl_dag_missing > 0 or pendl_morn_mvv_missing > 0 or pendl_dag_mvv_missing > 0:
        if run_brj:
            print(f"\n⚠️  {pendl_morn_missing} properties missing PENDL MORN BRJ (public transit morning commute time)")
            print(f"⚠️  {pendl_dag_missing} properties missing PENDL DAG BRJ (public transit return at 16:00)")
        if run_mvv:
            print(f"⚠️  {pendl_morn_mvv_missing} properties missing PENDL MORN MVV (public transit to Lambertseter svømmeklubb)")
            print(f"⚠️  {pendl_dag_mvv_missing} properties missing PENDL DAG MVV (public transit return from Lambertseter svømmeklubb)")
        if MAX_PRICE is not None:
            print(f"⚠️  Price filter active: MAX_PRICE = {MAX_PRICE}")
        
        proceed, requests_per_minute = confirm_with_rate_limit("Calculate location features for these properties?")
        
        if proceed:
            try:
                from main.location_features import PublicTransitCommuteTime
            except ImportError:
                from location_features import PublicTransitCommuteTime
            
            # Initialize calculators
            work_address = "Rådmann Halmrasts Vei 5"
            # PENDL MORN BRJ = public transit commute time to work
            transit_commute_calculator = PublicTransitCommuteTime(work_address)
            
            calculated_transit = 0
            calculated_transit_return = 0

            def _checkpoint_row(row_idx):
                if db is None:
                    return
                try:
                    row = df.loc[row_idx]
                    finnkode = str(row.get('Finnkode', '') or '').strip()
                    donor = str(row.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip()
                    ctx = finnkode + (f" [donor→{donor}]" if donor else "")
                    db.insert_or_update_eiendom(df.loc[[row_idx]].copy(), context=ctx)
                except Exception as checkpoint_error:
                    print(f"⚠️  Could not checkpoint row {row_idx}: {checkpoint_error}")
            
            print(f"\n🚀 Starting calculations for {len(df)} properties...")
            print(f"   (This may take a while - each property needs 2 API calls)")
            if requests_per_minute < 60.0:
                print(f"   Rate limited to {requests_per_minute} requests/minute\n")
            else:
                print(f"   Running at {requests_per_minute} requests/minute\n")
            
            delay_between_requests = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
            interrupted = False
            donor_assigned_count = 0

            def _maybe_assign_donor(row_data, required_columns, donor_cache):
                if travel_reuse_within_meters <= 0:
                    return None
                existing_donor = str(row_data.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip()
                if existing_donor:
                    return existing_donor

                self_finnkode = str(row_data.get('Finnkode', '') or '').strip()
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
                if str(row_now.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip():
                    return
                if not _row_has_all_travel_values(row_now, required_columns, max_travel_minutes):
                    return
                finnkode = str(row_now.get('Finnkode', '') or '').strip()
                if not finnkode:
                    return
                lat, lng = _get_row_coords(row_now, lat_col, lng_col)
                if lat is None or lng is None:
                    return
                if not any(c_finnkode == finnkode for _, _, c_finnkode in donor_cache):
                    donor_cache.append((lat, lng, finnkode))

            try:
                for idx, row in df.loc[eligible_mask].iterrows():
                    address = row['Adresse']
                    postnummer = row.get('Postnummer')
                    current_num = calculated_transit + calculated_transit_return

                    # Show which property we're working on
                    if current_num % 5 == 0 or current_num < 5:
                        print(f"⏳ Processing property {current_num + 1}/{len(df)}: {address}")

                    donor_finnkode = _maybe_assign_donor(row, brj_travel_columns, donor_cache_brj)
                    if donor_finnkode:
                        had_donor = str(row.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip()
                        if not had_donor:
                            df.at[idx, 'TRAVEL_COPY_FROM_FINNKODE'] = donor_finnkode
                            donor_assigned_count += 1
                            print(f"   🔁 Using donor travel values from #{donor_finnkode}")

                    # Calculate PENDL MORN BRJ (total public transit commute time to work)
                    if run_brj and pd.isna(row.get('PENDL MORN BRJ')):
                        if donor_finnkode:
                            pass
                        else:
                            try:
                                print(f"   📍 Calculating public transit time...", end='', flush=True)
                                minutes = transit_commute_calculator.calculate(address, postnummer)
                                if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                    df.at[idx, 'PENDL MORN BRJ'] = int(minutes)
                                    calculated_transit += 1
                                    print(f" ✓ {minutes} min")
                                    if delay_between_requests > 0:
                                        time.sleep(delay_between_requests)
                                else:
                                    print(f" ✗ Rejected/failed value ({minutes})")
                            except Exception as e:
                                print(f" ✗ Error: {str(e)}")

                    destination = f"{address}, {postnummer}, Norway" if pd.notna(postnummer) and postnummer else f"{address}, Norway"
                    work_addr_norway = f"{work_address}, Norway" if "Norway" not in work_address else work_address

                    # Calculate PENDL DAG BRJ (public transit return at 16:00 Monday)
                    if run_brj and pd.isna(row.get('PENDL DAG BRJ')):
                        if donor_finnkode:
                            pass
                        else:
                            try:
                                print(f"   🚌 Calculating public transit return (16:00)...", end='', flush=True)
                                minutes = transit_commute_calculator.calculate(
                                    address,
                                    postnummer,
                                    departure_time=16,
                                    origin_override=work_addr_norway,
                                    destination_override=destination
                                )
                                if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                    df.at[idx, 'PENDL DAG BRJ'] = int(minutes)
                                    calculated_transit_return += 1
                                    print(f" ✓ {minutes} min")
                                    if delay_between_requests > 0:
                                        time.sleep(delay_between_requests)
                                else:
                                    print(f" ✗ Rejected/failed value ({minutes})")
                            except Exception as e:
                                print(f" ✗ Error: {str(e)}")

                    _add_row_as_donor_if_complete(idx, brj_travel_columns, donor_cache_brj)

                    _checkpoint_row(idx)

                    # Summary every 10 properties
                    total_calculated = calculated_transit + calculated_transit_return
                    if total_calculated % 20 == 0 and total_calculated > 0:
                        print(
                            f"\n📊 Progress: {calculated_transit} transit "
                            f"+ {calculated_transit_return} transit_return "
                            f"+ {donor_assigned_count} donor-linked = {total_calculated + donor_assigned_count} total\n"
                        )
            except KeyboardInterrupt:
                interrupted = True
                print("\n⚠️  Interrupted during BRJ travel calculations. Returning partial results and preserving saved progress.")

            if interrupted:
                commute_cols = [
                    'PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                    'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
                    'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV'
                ]
                for col in commute_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
                return df
            
            if run_mvv:
                # Calculate new MVV destination (Lambertseter svømmeklubb).
                mvv_address = "Langbølgen 24, 1155 Oslo"
                transit_commute_calculator_mvv = PublicTransitCommuteTime(mvv_address)

                calculated_transit_mvv = 0
                calculated_transit_return_mvv = 0
                donor_assigned_mvv_count = 0

                print(f"\n🚀 Starting calculations for Lambertseter svømmeklubb (MVV)...\n")

                try:
                    for idx, row in df.loc[eligible_mask].iterrows():
                        address = row['Adresse']
                        postnummer = row.get('Postnummer')
                        current_num = calculated_transit_mvv + calculated_transit_return_mvv

                        if current_num % 10 == 0 or current_num < 5:
                            print(f"⏳ Processing property {current_num + 1}/{len(df[eligible_mask])}: {address}")

                        donor_finnkode = _maybe_assign_donor(row, mvv_travel_columns, donor_cache_mvv)
                        if donor_finnkode and not str(row.get('TRAVEL_COPY_FROM_FINNKODE', '') or '').strip():
                            df.at[idx, 'TRAVEL_COPY_FROM_FINNKODE'] = donor_finnkode
                            donor_assigned_mvv_count += 1
                            print(f"   🔁 Using donor travel values from #{donor_finnkode}")

                        # Calculate PENDL MORN MVV
                        if pd.isna(row.get('PENDL MORN MVV')):
                            if donor_finnkode:
                                pass
                            else:
                                try:
                                    print(f"   📍 Calculating public transit to Lambertseter svømmeklubb...", end='', flush=True)
                                    minutes = transit_commute_calculator_mvv.calculate(address, postnummer)
                                    if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                        df.at[idx, 'PENDL MORN MVV'] = int(minutes)
                                        calculated_transit_mvv += 1
                                        print(f" ✓ {minutes} min")
                                        if delay_between_requests > 0:
                                            time.sleep(delay_between_requests)
                                    else:
                                        print(f" ✗ Rejected/failed value ({minutes})")
                                except Exception as e:
                                    print(f" ✗ Error: {str(e)}")

                        destination_mvv = f"{address}, {postnummer}, Norway" if pd.notna(postnummer) and postnummer else f"{address}, Norway"

                        # Calculate PENDL DAG MVV
                        if pd.isna(row.get('PENDL DAG MVV')):
                            if donor_finnkode:
                                pass
                            else:
                                try:
                                    print(f"   🚌 Calculating public transit return from Lambertseter svømmeklubb (16:00)...", end='', flush=True)
                                    minutes = transit_commute_calculator_mvv.calculate(
                                        address,
                                        postnummer,
                                        departure_time=16,
                                        origin_override=mvv_address,
                                        destination_override=destination_mvv
                                    )
                                    if minutes is not None and _is_valid_travel_value(minutes, max_travel_minutes):
                                        df.at[idx, 'PENDL DAG MVV'] = int(minutes)
                                        calculated_transit_return_mvv += 1
                                        print(f" ✓ {minutes} min")
                                        if delay_between_requests > 0:
                                            time.sleep(delay_between_requests)
                                    else:
                                        print(f" ✗ Rejected/failed value ({minutes})")
                                except Exception as e:
                                    print(f" ✗ Error: {str(e)}")

                        _add_row_as_donor_if_complete(idx, mvv_travel_columns, donor_cache_mvv)

                        _checkpoint_row(idx)
                except KeyboardInterrupt:
                    print("\n⚠️  Interrupted during MVV travel calculations. Returning partial results and preserving saved progress.")

                print(
                    f"\n✓ Successfully calculated {calculated_transit_mvv} transit times to Lambertseter svømmeklubb "
                    f"and {calculated_transit_return_mvv} transit return times"
                )
                if donor_assigned_count > 0 or donor_assigned_mvv_count > 0:
                    print(f"✓ Linked {donor_assigned_count + donor_assigned_mvv_count} rows to nearby donor Finnkoder")
        else:
            print("Skipped location features calculation")
    else:
        print(f"✓ All properties already have transit commute data for BRJ and MVV")

    # Ensure commute time columns are integers without decimals
    commute_cols = [
        'PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
        'PENDL MORN CNTR', 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR',
        'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV'
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

