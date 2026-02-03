import re
import time

import pandas as pd
from pandas import DataFrame


def confirm_with_rate_limit(prompt: str) -> tuple[bool, float]:
    """
    Ask user for confirmation with optional rate limiting for API requests.
    
    Args:
        prompt: The confirmation prompt to display
    
    Returns:
        Tuple of (proceed: bool, requests_per_second: float)
        - proceed: True if user wants to continue, False otherwise
        - requests_per_second: Rate limit (default 1.0, or user-specified number)
    
    Examples:
        User can enter: yes, no, or a number like 5 (for 5 requests/sec)
    """
    valid_input = False
    while not valid_input:
        response = input(prompt + " (yes/no/<requests per second>): ").strip().lower()
        
        if response in ['yes', 'y']:
            return True, 1.0  # Default 1 request per second
        elif response in ['no', 'n']:
            return False, 1.0
        else:
            try:
                rate = float(response)
                if rate > 0:
                    return True, rate
                else:
                    print("Please enter a positive number for requests per second")
            except ValueError:
                print("Invalid input. Please enter 'yes', 'no', or a number (e.g., 5)")
    
    return False, 1.0


def post_process_rental(df: DataFrame, projectName: str, outputFileName: str, originalDF: DataFrame = None) -> DataFrame:
    if df.empty:
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
        return df

    # Convert area columns to numeric, coerce errors to NaN
    for col in ['Primærrom', 'Internt bruksareal (BRA-i)', 'Bruksareal']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill AREAL column
    df['AREAL'] = df['Primærrom'].fillna(df['Internt bruksareal (BRA-i)']).fillna(df['Bruksareal'])

    # # Convert AREAL to numeric, coercing errors to NaN
    # df['AREAL'] = pd.to_numeric(df['AREAL'], errors='coerce')
    #
    # # Convert AREAL and Depositum to integers
    # df['AREAL'] = df['AREAL'].round().astype('Int64')
    # df['Depositum'] = pd.to_numeric(df['Depositum'], errors='coerce').fillna(0).astype('Int64')


    # Calculate PRIS KVM only where both Leiepris and AREAL are present
    mask = df['Leiepris'].notna() & df['AREAL'].notna()
    df['PRIS KVM'] = (df['Leiepris'].astype(float) / df['AREAL'].astype(float)).where(mask)
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

    df.to_csv(f'{projectName}/{outputFileName}', index=False)

    return df

def post_process_eiendom(df: DataFrame, projectName: str, outputFileName: str, originalDF: DataFrame = None) -> DataFrame:
    if df.empty:
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
        return df

    # Optional price filter for API calls and sheets export
    try:
        from main.config.filters import MAX_PRICE
    except ImportError:
        try:
            from config.filters import MAX_PRICE
        except ImportError:
            MAX_PRICE = None

    # Load existing processed data to preserve commute times from CSV snapshot
    import os
    processed_file_path = f'{projectName}/{outputFileName}'
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
            
            # Extract commute columns from existing data (BRJ + MVV)
            commute_columns = ['Finnkode', 'PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                             'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV']
            # Filter to only include columns that exist in existing data
            existing_commute_cols = ['Finnkode'] + [col for col in commute_columns[1:] if col in existing_df.columns]
            existing_commute = existing_df[existing_commute_cols].copy() if len(existing_commute_cols) > 1 else None
            
            if existing_commute is not None:
                # Convert to integers in existing data before merging
                for col in commute_columns[1:]:
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
                print("✓ Merged existing commute data from snapshot")
        except Exception as e:
            print(f"⚠️  Could not load existing processed data: {e}")

    # Convert area columns to numeric, coerce errors to NaN
    for col in ['Primærrom', 'Internt bruksareal (BRA-i)', 'Bruksareal']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill AREAL column
    df['AREAL'] = df['Primærrom'].fillna(df['Internt bruksareal (BRA-i)']).fillna(df['Bruksareal'])

    # Calculate PRIS KVM only where both Pris and AREAL are present
    mask = df['Pris'].notna() & df['AREAL'].notna() & (df['AREAL'] > 0)
    df['PRIS KVM'] = (df['Pris'].astype(float) / df['AREAL'].astype(float)).where(mask)
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
    if 'PENDL MORN MVV' not in df.columns:
        df['PENDL MORN MVV'] = None
    if 'BIL MORN MVV' not in df.columns:
        df['BIL MORN MVV'] = None
    if 'PENDL DAG MVV' not in df.columns:
        df['PENDL DAG MVV'] = None
    if 'BIL DAG MVV' not in df.columns:
        df['BIL DAG MVV'] = None
    
    # Calculate PENDL MORN BRJ (total public transit commute time) and BIL MORN BRJ (driving time)
    eligible_mask = pd.Series([True] * len(df), index=df.index)
    if MAX_PRICE is not None and 'Pris' in df.columns:
        eligible_mask = df['Pris'].fillna(0) <= MAX_PRICE

    pendl_morn_missing = df.loc[eligible_mask, 'PENDL MORN BRJ'].isna().sum()
    bil_morn_missing = df.loc[eligible_mask, 'BIL MORN BRJ'].isna().sum()
    pendl_dag_missing = df.loc[eligible_mask, 'PENDL DAG BRJ'].isna().sum()
    bil_dag_missing = df.loc[eligible_mask, 'BIL DAG BRJ'].isna().sum()
    pendl_morn_mvv_missing = df.loc[eligible_mask, 'PENDL MORN MVV'].isna().sum()
    bil_morn_mvv_missing = df.loc[eligible_mask, 'BIL MORN MVV'].isna().sum()
    pendl_dag_mvv_missing = df.loc[eligible_mask, 'PENDL DAG MVV'].isna().sum()
    bil_dag_mvv_missing = df.loc[eligible_mask, 'BIL DAG MVV'].isna().sum()
    
    if pendl_morn_missing > 0 or bil_morn_missing > 0 or pendl_dag_missing > 0 or bil_dag_missing > 0 or \
       pendl_morn_mvv_missing > 0 or bil_morn_mvv_missing > 0 or pendl_dag_mvv_missing > 0 or bil_dag_mvv_missing > 0:
        print(f"\n⚠️  {pendl_morn_missing} properties missing PENDL MORN BRJ (public transit morning commute time)")
        print(f"⚠️  {bil_morn_missing} properties missing BIL MORN BRJ (driving morning commute time)")
        print(f"⚠️  {pendl_dag_missing} properties missing PENDL DAG BRJ (public transit return at 16:00)")
        print(f"⚠️  {bil_dag_missing} properties missing BIL DAG BRJ (driving return at 16:00)")
        print(f"⚠️  {pendl_morn_mvv_missing} properties missing PENDL MORN MVV (public transit to Oslo Sentralstasjon)")
        print(f"⚠️  {bil_morn_mvv_missing} properties missing BIL MORN MVV (driving to Oslo Sentralstasjon)")
        print(f"⚠️  {pendl_dag_mvv_missing} properties missing PENDL DAG MVV (public transit return from Oslo Sentralstasjon)")
        print(f"⚠️  {bil_dag_mvv_missing} properties missing BIL DAG MVV (driving return from Oslo Sentralstasjon)")
        if MAX_PRICE is not None:
            print(f"⚠️  Price filter active: MAX_PRICE = {MAX_PRICE}")
        
        proceed, requests_per_second = confirm_with_rate_limit("Calculate location features for these properties?")
        
        if proceed:
            try:
                from main.location_features import PublicTransitCommuteTime, CommutingTimeToWorkAddress
            except ImportError:
                from location_features import PublicTransitCommuteTime, CommutingTimeToWorkAddress
            
            # Initialize calculators
            work_address = "Rådmann Halmrasts Vei 5"
            # PENDL MORN BRJ = public transit commute time to work
            transit_commute_calculator = PublicTransitCommuteTime(work_address)
            # BIL MORN BRJ = driving time to work
            driving_calculator = CommutingTimeToWorkAddress(work_address)
            
            calculated_transit = 0
            calculated_driving = 0
            calculated_transit_return = 0
            calculated_driving_return = 0
            total_properties = pendl_morn_missing + bil_morn_missing
            
            print(f"\n🚀 Starting calculations for {len(df)} properties...")
            print(f"   (This may take a while - each property needs 2 API calls)")
            if requests_per_second < 1.0:
                print(f"   Rate limited to {requests_per_second} requests/second\n")
            else:
                print(f"   Running at {requests_per_second} requests/second\n")
            
            delay_between_requests = 1.0 / requests_per_second if requests_per_second > 0 else 0
            
            for idx, row in df.loc[eligible_mask].iterrows():
                address = row['Adresse']
                postnummer = row.get('Postnummer')
                current_num = calculated_transit + calculated_driving
                
                # Show which property we're working on
                if current_num % 5 == 0 or current_num < 5:
                    print(f"⏳ Processing property {current_num + 1}/{len(df)}: {address}")
                
                # Calculate PENDL MORN BRJ (total public transit commute time to work)
                if pd.isna(row.get('PENDL MORN BRJ')):
                    try:
                        print(f"   📍 Calculating public transit time...", end='', flush=True)
                        minutes = transit_commute_calculator.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'PENDL MORN BRJ'] = int(minutes)
                            calculated_transit += 1
                            print(f" ✓ {minutes} min")
                            if delay_between_requests > 0:
                                time.sleep(delay_between_requests)
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                # Calculate BIL MORN BRJ (driving time to work)
                if pd.isna(row.get('BIL MORN BRJ')):
                    try:
                        print(f"   🚗 Calculating driving time...", end='', flush=True)
                        minutes = driving_calculator.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'BIL MORN BRJ'] = int(minutes)
                            calculated_driving += 1
                            print(f" ✓ {minutes} min")
                            if delay_between_requests > 0:
                                time.sleep(delay_between_requests)
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")

                destination = f"{address}, {postnummer}, Norway" if pd.notna(postnummer) and postnummer else f"{address}, Norway"

                # Calculate PENDL DAG BRJ (public transit return at 16:00 Monday)
                if pd.isna(row.get('PENDL DAG BRJ')):
                    try:
                        print(f"   🚌 Calculating public transit return (16:00)...", end='', flush=True)
                        work_addr_norway = f"{work_address}, Norway" if "Norway" not in work_address else work_address
                        minutes = transit_commute_calculator.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=work_addr_norway,
                            destination_override=destination
                        )
                        if minutes is not None:
                            df.at[idx, 'PENDL DAG BRJ'] = int(minutes)
                            calculated_transit_return += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")

                # Calculate BIL DAG BRJ (driving return at 16:00 Monday)
                if pd.isna(row.get('BIL DAG BRJ')):
                    try:
                        print(f"   🚙 Calculating driving return (16:00)...", end='', flush=True)
                        minutes = driving_calculator.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=work_addr_norway,
                            destination_override=destination
                        )
                        if minutes is not None:
                            df.at[idx, 'BIL DAG BRJ'] = int(minutes)
                            calculated_driving_return += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                # Summary every 10 properties
                total_calculated = calculated_transit + calculated_driving + calculated_transit_return + calculated_driving_return
                if total_calculated % 20 == 0 and total_calculated > 0:
                    print(
                        f"\n📊 Progress: {calculated_transit} transit + {calculated_driving} driving "
                        f"+ {calculated_transit_return} transit_return + {calculated_driving_return} driving_return "
                        f"= {total_calculated} total\n"
                    )
            
            # Now calculate MVV (Oslo Sentralstasjon) times
            mvv_address = "Oslo Sentralstasjon"
            transit_commute_calculator_mvv = PublicTransitCommuteTime(mvv_address)
            driving_calculator_mvv = CommutingTimeToWorkAddress(mvv_address)
            
            calculated_transit_mvv = 0
            calculated_driving_mvv = 0
            calculated_transit_return_mvv = 0
            calculated_driving_return_mvv = 0
            
            print(f"\n🚀 Starting calculations for Oslo Sentralstasjon (MVV)...\n")
            
            for idx, row in df.loc[eligible_mask].iterrows():
                address = row['Adresse']
                postnummer = row.get('Postnummer')
                current_num = calculated_transit_mvv + calculated_driving_mvv
                
                if current_num % 10 == 0 or current_num < 5:
                    print(f"⏳ Processing property {current_num + 1}/{len(df[eligible_mask])}: {address}")
                
                # Calculate PENDL MORN MVV
                if pd.isna(row.get('PENDL MORN MVV')):
                    try:
                        print(f"   📍 Calculating public transit to Oslo S...", end='', flush=True)
                        minutes = transit_commute_calculator_mvv.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'PENDL MORN MVV'] = int(minutes)
                            calculated_transit_mvv += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                # Calculate BIL MORN MVV
                if pd.isna(row.get('BIL MORN MVV')):
                    try:
                        print(f"   🚗 Calculating driving to Oslo S...", end='', flush=True)
                        minutes = driving_calculator_mvv.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'BIL MORN MVV'] = int(minutes)
                            calculated_driving_mvv += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                destination_mvv = f"{address}, {postnummer}" if pd.notna(postnummer) and postnummer else address
                
                # Calculate PENDL DAG MVV
                if pd.isna(row.get('PENDL DAG MVV')):
                    try:
                        print(f"   🚌 Calculating public transit return from Oslo S (16:00)...", end='', flush=True)
                        minutes = transit_commute_calculator_mvv.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=mvv_address,
                            destination_override=destination_mvv
                        )
                        if minutes is not None:
                            df.at[idx, 'PENDL DAG MVV'] = int(minutes)
                            calculated_transit_return_mvv += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                # Calculate BIL DAG MVV
                if pd.isna(row.get('BIL DAG MVV')):
                    try:
                        print(f"   🚙 Calculating driving return from Oslo S (16:00)...", end='', flush=True)
                        minutes = driving_calculator_mvv.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=mvv_address,
                            destination_override=destination_mvv
                        )
                        if minutes is not None:
                            df.at[idx, 'BIL DAG MVV'] = int(minutes)
                            calculated_driving_return_mvv += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
            
            print(
                f"\n✓ Successfully calculated {calculated_transit_mvv} transit times to Oslo S, "
                f"{calculated_driving_mvv} driving times, {calculated_transit_return_mvv} transit return times, "
                f"and {calculated_driving_return_mvv} driving return times"
            )
        else:
            print("Skipped location features calculation")
    else:
        print(f"✓ All properties already have commute data for BRJ and MVV")

    # Ensure commute time columns are integers without decimals
    commute_cols = ['PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                    'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV']
    for col in commute_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # Drop unnecessary columns
    df = df.drop(columns=['Primærrom',
                          'Internt bruksareal (BRA-i)',
                          'Bruksareal',
                          'Eksternt bruksareal (BRA-e)',
                          'Balkong/Terrasse (TBA)',
                          'Bruttoareal'
                          ])

    df.to_csv(f'{projectName}/{outputFileName}', index=False)

    return df

def post_process_jobs(df: DataFrame, projectName: str, outputFileName: str, originalDF: DataFrame = None) -> DataFrame:
    """Post-process job data by normalizing dates and formatting text fields."""
    if df.empty:
        df.to_csv(f'{projectName}/{outputFileName}', index=False)
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

    df.to_csv(f'{projectName}/{outputFileName}', index=False)

    return df

# if main
if __name__ == "__main__":
 
    pass

