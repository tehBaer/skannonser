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

    # Calculate commuting time (Pendlevei) with verification check
    if 'PENDLEVEI' not in df.columns:
        df['PENDLEVEI'] = None
    
    missing_count = df['PENDLEVEI'].isna().sum()
    
    if missing_count > 0:
        print(f"\n⚠️  {missing_count} properties are missing Pendlevei data")
        response = input(f"Calculate commute time for these properties using Google Directions API? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            try:
                from main.location_features import CommutingTimeToWorkAddress
            except ImportError:
                from location_features import CommutingTimeToWorkAddress
            
            print("Calculating commuting time to Pendlevei...")
            work_address = "Rådmann Halmrasts Vei 5"
            commute_calculator = CommutingTimeToWorkAddress(work_address)
            
            calculated = 0
            for idx, row in df.iterrows():
                # Skip if this entry already has a commute time
                if pd.notna(row.get('PENDLEVEI')):
                    continue
                
                address = row['Adresse']
                postnummer = row.get('Postnummer')
                
                try:
                    minutes = commute_calculator.calculate_minutes(address, postnummer)
                    if minutes is not None:
                        df.at[idx, 'PENDLEVEI'] = int(minutes)  # Ensure integer
                        calculated += 1
                        if calculated % 10 == 0:
                            print(f"  Calculated {calculated}/{missing_count}: {address} → {minutes} min")
                    else:
                        df.at[idx, 'PENDLEVEI'] = None
                except Exception as e:
                    print(f"  Error calculating commute for {address}: {e}")
                    df.at[idx, 'PENDLEVEI'] = None
            
            print(f"\n✓ Successfully calculated {calculated}/{missing_count} commute times")
        else:
            print("Skipped Pendlevei calculation")
    else:
        print(f"✓ All properties already have Pendlevei data")

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

