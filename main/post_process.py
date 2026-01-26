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
    # file_path = 'leie/live_data.csv'
    # df = pd.read_csv(file_path)
    # cleanData(df, 'leie', 'live_data_parsed.csv')

    post_process_rental(pd.read_csv('leie/saved_all_updated.csv'), 'leie', 'saved_all_updated_parsed.csv')
