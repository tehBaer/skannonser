import re

import pandas as pd
from pandas import DataFrame


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

    # Initialize columns if not present
    if 'PENDLEVEI' not in df.columns:
        df['PENDLEVEI'] = None
    if 'KJØRETID' not in df.columns:
        df['KJØRETID'] = None
    if 'PENDLEVEI_RETUR_16' not in df.columns:
        df['PENDLEVEI_RETUR_16'] = None
    if 'KJØRETID_RETUR_16' not in df.columns:
        df['KJØRETID_RETUR_16'] = None
    
    # Calculate PENDLEVEI (total public transit commute time) and KJØRETID (driving time)
    eligible_mask = pd.Series([True] * len(df), index=df.index)
    if MAX_PRICE is not None and 'Pris' in df.columns:
        eligible_mask = df['Pris'].fillna(0) <= MAX_PRICE

    pendlevei_missing = df.loc[eligible_mask, 'PENDLEVEI'].isna().sum()
    kjoretid_missing = df.loc[eligible_mask, 'KJØRETID'].isna().sum()
    pendlevei_retur_missing = df.loc[eligible_mask, 'PENDLEVEI_RETUR_16'].isna().sum()
    kjoretid_retur_missing = df.loc[eligible_mask, 'KJØRETID_RETUR_16'].isna().sum()
    
    if pendlevei_missing > 0 or kjoretid_missing > 0 or pendlevei_retur_missing > 0 or kjoretid_retur_missing > 0:
        print(f"\n⚠️  {pendlevei_missing} properties missing PENDLEVEI (public transit commute time)")
        print(f"⚠️  {kjoretid_missing} properties missing KJØRETID (driving time)")
        print(f"⚠️  {pendlevei_retur_missing} properties missing PENDLEVEI_RETUR_16 (public transit return at 16:00)")
        print(f"⚠️  {kjoretid_retur_missing} properties missing KJØRETID_RETUR_16 (driving return at 16:00)")
        if MAX_PRICE is not None:
            print(f"⚠️  Price filter active: MAX_PRICE = {MAX_PRICE}")
        
        response = input(f"Calculate location features for these properties? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            try:
                from main.location_features import PublicTransitCommuteTime, CommutingTimeToWorkAddress
            except ImportError:
                from location_features import PublicTransitCommuteTime, CommutingTimeToWorkAddress
            
            # Initialize calculators
            work_address = "Rådmann Halmrasts Vei 5"
            # PENDLEVEI = public transit commute time to work
            transit_commute_calculator = PublicTransitCommuteTime(work_address)
            # KJØRETID = driving time to work
            driving_calculator = CommutingTimeToWorkAddress(work_address)
            
            calculated_transit = 0
            calculated_driving = 0
            calculated_transit_return = 0
            calculated_driving_return = 0
            total_properties = pendlevei_missing + kjoretid_missing
            
            print(f"\n🚀 Starting calculations for {len(df)} properties...")
            print(f"   (This may take a while - each property needs 2 API calls)\n")
            
            for idx, row in df.loc[eligible_mask].iterrows():
                address = row['Adresse']
                postnummer = row.get('Postnummer')
                current_num = calculated_transit + calculated_driving
                
                # Show which property we're working on
                if current_num % 5 == 0 or current_num < 5:
                    print(f"⏳ Processing property {current_num + 1}/{len(df)}: {address}")
                
                # Calculate PENDLEVEI (total public transit commute time to work)
                if pd.isna(row.get('PENDLEVEI')):
                    try:
                        print(f"   📍 Calculating public transit time...", end='', flush=True)
                        minutes = transit_commute_calculator.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'PENDLEVEI'] = int(minutes)
                            calculated_transit += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")
                
                # Calculate KJØRETID (driving time to work)
                if pd.isna(row.get('KJØRETID')):
                    try:
                        print(f"   🚗 Calculating driving time...", end='', flush=True)
                        minutes = driving_calculator.calculate(address, postnummer)
                        if minutes is not None:
                            df.at[idx, 'KJØRETID'] = int(minutes)
                            calculated_driving += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")

                destination = f"{address}, {postnummer}" if pd.notna(postnummer) and postnummer else address

                # Calculate PENDLEVEI_RETUR_16 (public transit return at 16:00 Monday)
                if pd.isna(row.get('PENDLEVEI_RETUR_16')):
                    try:
                        print(f"   🚌 Calculating public transit return (16:00)...", end='', flush=True)
                        minutes = transit_commute_calculator.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=work_address,
                            destination_override=destination
                        )
                        if minutes is not None:
                            df.at[idx, 'PENDLEVEI_RETUR_16'] = int(minutes)
                            calculated_transit_return += 1
                            print(f" ✓ {minutes} min")
                        else:
                            print(f" ✗ Failed (returned None)")
                    except Exception as e:
                        print(f" ✗ Error: {str(e)}")

                # Calculate KJØRETID_RETUR_16 (driving return at 16:00 Monday)
                if pd.isna(row.get('KJØRETID_RETUR_16')):
                    try:
                        print(f"   🚙 Calculating driving return (16:00)...", end='', flush=True)
                        minutes = driving_calculator.calculate(
                            address,
                            postnummer,
                            departure_time=16,
                            origin_override=work_address,
                            destination_override=destination
                        )
                        if minutes is not None:
                            df.at[idx, 'KJØRETID_RETUR_16'] = int(minutes)
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
            
            print(
                f"\n✓ Successfully calculated {calculated_transit} transit commute times, "
                f"{calculated_driving} driving times, {calculated_transit_return} transit return times, "
                f"and {calculated_driving_return} driving return times"
            )
        else:
            print("Skipped location features calculation")
    else:
        print(f"✓ All properties already have PENDLEVEI, KJØRETID, and return time data")

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

