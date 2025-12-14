from typing import List

from main.googleUtils import SPREADSHEET_ID, get_credentials, download_sheet_as_csv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import csv
import os


import pandas as pd

def align_to_sheet_layout(csv_to_align, sheet_downloaded_path, output_path):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Read only the headers from sheet_downloaded.csv
    sheet_df = pd.read_csv(sheet_downloaded_path, nrows=0)
    sheet_headers = [str(col) for col in sheet_df.columns]

    # Read live_missing.csv
    live_df = pd.read_csv(csv_to_align)
    live_df.columns = [str(col) for col in live_df.columns]

    # Create aligned DataFrame with the same columns as the sheet
    aligned = pd.DataFrame(columns=sheet_headers)
    for col in sheet_headers:
        if col in live_df.columns:
            aligned[col] = live_df[col]
        else:
            aligned[col] = ""

    # Ensure the number of rows matches live_df
    aligned = aligned.iloc[:len(live_df)].copy()

    # Write to CSV
    aligned.to_csv(output_path, index=False, header=True)


def try_filter_new_ads(path_csv_to_filter, path_sheets_downloaded, path_output, headers: List[str]) -> bool:
    try:
        # Load all columns from the aligned CSV
        live_df = pd.read_csv(path_csv_to_filter)

        # Load the sheets CSV - skip the first unnamed column
        sheets_df = pd.read_csv(
            path_sheets_downloaded,
            usecols=headers,  # Only read the columns you need
            on_bad_lines='skip'
        )

        # Check for required columns
        for col in headers:
            if col not in live_df.columns:
                raise ValueError(f'Missing required column in live data: {col}')

        # Find rows in live_df not in sheets_df based on Finnkode
        live_df['Finnkode'] = live_df['Finnkode'].astype(str).str.strip()
        sheets_df['Finnkode'] = sheets_df['Finnkode'].astype(str).str.strip()

        # Debug: print what we're comparing
        print(f"Live Finnkodes: {live_df['Finnkode'].tolist()}")
        print(f"Sheet Finnkodes: {sheets_df['Finnkode'].tolist()}")

        missing_ads = live_df[~live_df['Finnkode'].isin(sheets_df['Finnkode'])]
        print("Found", len(missing_ads), "missing ads.")

        if missing_ads.empty:
            print("No new rows to save. The output file will not be created.")
            return False

        # Save all columns (preserving alignment)
        missing_ads.to_csv(path_output, index=False)
        print(f"Missing rows saved to '{path_output}'.")
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def filter_new_property_ads(analyzed_path, saved_all_path, output_path, empty_columns_count):
    """Find rows in analyzed.csv not present in sheets.csv and save them to a new CSV."""
    try:
        # Load the CSV files
        analyzed_df = pd.read_csv(analyzed_path)
        sheets_df = pd.read_csv(
            saved_all_path,
            header=None,
            names=['Finnkode', 'Tilgjengelighet', 'Adresse', 'Postnummer', 'Leiepris',
                   'Depositum', 'URL',
                   # 'Innflytting', 'Utflytting',
                   'AREAL', 'PRIS KVM'],
            on_bad_lines='skip'
        )

        # Clean and standardize the Finnkode column
        analyzed_df['Finnkode'] = analyzed_df['Finnkode'].astype(str).str.strip()
        sheets_df['Finnkode'] = sheets_df['Finnkode'].astype(str).str.strip()

        # Align columns for comparison
        common_columns = analyzed_df.columns.intersection(sheets_df.columns)
        analyzed_df = analyzed_df[common_columns]
        sheets_df = sheets_df[common_columns]

        # Find rows in analyzed.csv not in sheets.csv
        missing_finnkode = analyzed_df[~analyzed_df['Finnkode'].isin(sheets_df['Finnkode'])]

        # Check if there are missing rows
        if missing_finnkode.empty:
            print("No new rows to save. The output file will not be created.")
            return

        # Add empty columns to the missing rows

        for i in range(empty_columns_count):
            missing_finnkode.insert(0, f'Empty{i + 1}', '')

        # Save missing rows to a new CSV file
        missing_finnkode.to_csv(output_path, index=False)
        print(f"Missing rows saved to '{output_path}'.")

    except Exception as e:
        print(f"An error occurred: {e}")


def prepend_missing_ads(service, sheet_name, missing_rows_path, range, empty_columns_count):
    """Prepend missing rows below the header of the specified sheet, ensuring numeric values are recognized as numbers."""
    try:
        # Read missing rows from the CSV file
        with open(missing_rows_path, "r", encoding="utf-8") as file:
            csv_reader = csv.reader(file)
            missing_rows = list(csv_reader)

        # Separate header and data
        header = missing_rows[0]
        missing_rows = missing_rows[1:]

        # Ensure each row has exactly `empty_columns_count` empty cells at the start
        padded_missing_rows = [
            ([""] * empty_columns_count + row[empty_columns_count:])[:len(header)]
            for row in missing_rows
        ]

        # Retrieve existing data from the sheet
        range_name = f"{sheet_name}!{range}"  # Adjust the range as needed
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
        existing_data = result.get("values", [])

        # Combine header, existing data, and padded missing rows
        updated_data = [existing_data[0]] + padded_missing_rows + existing_data[1:]

        # Write the updated data back to the sheet
        body = {"values": updated_data}
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",  # Use USER_ENTERED to preserve formatting
            body=body,
        ).execute()

        print(f"Missing rows have been prepended below the header in the sheet: {sheet_name}")

    except Exception as e:
        print(f"An error occurred: {e}")


def merge_above(emptyColCount, sheet_name, path_live_parsed, path_downloaded_spreadsheet, path_live_missing,
                range="A1:Z1000"):
    """Main function to export data to Google Sheets."""
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)

        download_sheet_as_csv(service, sheet_name, path_downloaded_spreadsheet, range)

        filter_new_property_ads(path_live_parsed, path_downloaded_spreadsheet, path_live_missing, emptyColCount)

        prepend_missing_ads(service, sheet_name, path_live_missing, range, emptyColCount)
        print(f"Data successfully updated.")
    except HttpError as err:
        print(err)

def check_missing_headers(df: pd.DataFrame, headers_to_use: List[str]) -> List[str]:
    missing_headers = [h for h in headers_to_use if h not in df.columns]
    return missing_headers

def try_verify_align_filter_merge_below(sheet_name, pAB_processed, p_sheet, pC_filtered, pB_aligned, headers_to_use: List[str]):
    verified = verify_headers(headers_to_use, pAB_processed, p_sheet, sheet_name)

    if not verified:
        print("Headers does not match. Aborting process.")
        return

    # Align sheet layout
    align_to_sheet_layout(pAB_processed, p_sheet, pB_aligned)

    # Filter missing ads
    filtered_successfully = try_filter_new_ads(pB_aligned, p_sheet, pC_filtered, headers_to_use)

    # Merge missing ads
    if (filtered_successfully):
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
        append_missing_ads(service, sheet_name, pC_filtered, headers_to_use)


def verify_headers(headers_to_use, p_csv_to_check, p_sheet, sheet_name) -> bool:
    """Checks for missing headers in both CSVs before merging below."""
    # Check headers
    live_df = pd.read_csv(p_csv_to_check)
    headers_missing_in_live = [h for h in headers_to_use if h not in live_df.columns]
    if headers_missing_in_live:
        print(f"Missing required headers in live data: {headers_missing_in_live}")
        return False
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
        download_sheet_as_csv(service, sheet_name, p_sheet)
    except HttpError as err:
        print(err)
        return False
    # Check headers in downloaded sheet
    try:
        sheet_df = pd.read_csv(p_sheet, on_bad_lines='skip')
    except pd.errors.ParserError as e:
        print(f"Parser error reading sheet: {e}")
        sheet_df = pd.read_csv(p_sheet, on_bad_lines='skip', engine='python', encoding='utf-8')

    headers_missing_in_sheet = [h for h in headers_to_use if h not in sheet_df.columns]
    if headers_missing_in_sheet:
        print(f"Missing required headers in sheet: {headers_missing_in_sheet}")
        return False
    return True


def append_missing_ads(service, sheet_name, pC_filtered, headers_to_use: List[str]):
    try:
        # Read only the specified columns from the CSV
        df = pd.read_csv(pC_filtered, usecols=headers_to_use)

        df = sanitize(df)

        # Convert to list of lists (header + data rows)
        missing_rows = [df.columns.tolist()] + df.values.tolist()

        # Separate header and data
        header = missing_rows[0]
        missing_rows = missing_rows[1:]

        # Retrieve existing data from the sheet to find the next available row
        range_name = f"{sheet_name}!A1:Z1000"
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
        existing_data = result.get("values", [])
        next_row_index = len(existing_data) + 1

        # Write the missing rows to the sheet starting from the next available row
        body = {"values": missing_rows}
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A{next_row_index}",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

        print(f"Appended {len(missing_rows)} missing rows to the sheet: {sheet_name}")

    except Exception as e:
        print(f"An error occurred: {e}")


def sanitize(df):
    # Clean the data: replace newlines and other problematic characters
    df = df.fillna('')  # Replace NaN with empty string
    df = df.applymap(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').strip() if isinstance(x, str) else x)
    return df


if __name__ == "__main__":
    merge_above(
        emptyColCount=3,
        sheet_name="Main",
        path_live_parsed="leie/live_data_parsed.csv",
        path_downloaded_spreadsheet="leie/sheet_downloaded.csv",
        path_live_missing="leie/live_missing.csv"
    )
