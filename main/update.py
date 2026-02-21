import pandas as pd

from main.post_process import post_process_rental
from main.googleUtils import download_sheet_as_csv, get_sheets_service, SPREADSHEET_ID
from main.extraction_rental import extract_rental_data


def find_new_unavailable(sheet_name: str, columns: str):
    """Fetch latest listing availability and persist `leie/saved_availability.csv`."""
    service = get_sheets_service()
    download_sheet_as_csv(service, sheet_name, "leie/_temp2.csv", columns)

    # Load the downloaded data into a DataFrame
    df_saved = pd.read_csv("leie/_temp2.csv")

    updated_rows = []

    for index, row in df_saved.iterrows():
        # if (index < 50):
        #     continue
        # If Tilgjengelighet is already marked as Utleid or Slettet, | the row
        if (row["Tilgjengelighet"] == 'Utleid' or row["Tilgjengelighet"] == "Slettet"):
            updated_rows.append({
                "Finnkode": row["Finnkode"],
                "Tilgjengelighet": row["Tilgjengelighet"],
            })
            print(f"Finnkode {row['Finnkode']} is already {row['Tilgjengelighet']}")
            continue
        try:
            # Extract data for the URL
            updated_data = extract_rental_data(row["URL"], index, "leie", True, True)
            updated_rows.append(updated_data)
        except Exception as e:
            print(f"Error processing URL at index {index}: {row['Finnkode']} - {e}")
            updated_rows.append(row)

    data = pd.DataFrame(updated_rows)
    dfdata = data[["Finnkode", "Tilgjengelighet"]]
    dfdata.to_csv("leie/saved_availability.csv", index=False)
    return dfdata


def paste_new_availability(data: pd.DataFrame, sheet_name: str, start_cell: str):
    """Write availability dataframe into an existing sheet range."""
    service = get_sheets_service()

    # Define spreadsheet ID and range name
    range_name = f"{sheet_name}!{start_cell}"  # Adjust the range as needed
    # Replace NaN values with an empty string
    data = data.fillna("")

    # Convert the DataFrame to a list of lists
    body = {
        "values": data.values.tolist()
    }

    # Update the Google Sheet
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    print(f"{result.get('updatedCells')} cells updated.")


def get_everything_updated(df_saved: pd.DataFrame):
    """Re-extract all listings and write merged dataset to local CSV outputs."""

    updated_rows = []
    for index, row in df_saved.iterrows():
        # temporary limit for testing
        # if not (600 < index):
        #     continue
        # if not (index<3):
        #     continue
        try:
            # Extract data for the URL
            updated_data = extract_rental_data(row["URL"], index, "leie")
            updated_rows.append(updated_data)
        except Exception:
            # print(f"Error processing URL at index {index}: {row['Finnkode']} - {e}")
            updated_rows.append({
                "Finnkode": row["Finnkode"],
                "Tilgjengelighet": "Slettet",
            })

    # Save the updated data to a new CSV file
    data = pd.DataFrame(updated_rows)
    data.to_csv("leie/xx.csv", index=False)

    cleaned_df = post_process_rental(data, 'leie', save_csv=False)

    # If a row has "Slettet", fill inn the values from df_saved instead
    for index, row in cleaned_df.iterrows():
        if row["Tilgjengelighet"] == "Slettet":
            original_row = df_saved[df_saved["Finnkode"] == row["Finnkode"]]
            if not original_row.empty:
                cleaned_df.loc[index, "Adresse"] = original_row.iloc[0]["Adresse"]
                cleaned_df.loc[index, "Leiepris"] = original_row.iloc[0]["Leiepris"]
                cleaned_df.loc[index, "Depositum"] = original_row.iloc[0]["Depositum"]
                cleaned_df.loc[index, "URL"] = original_row.iloc[0]["URL"]
                value = pd.to_numeric(original_row.iloc[0]["AREAL"], errors="coerce")
                cleaned_df.loc[index, "AREAL"] = value
                value = pd.to_numeric(original_row.iloc[0]["PRIS KVM"], errors="coerce")
                cleaned_df.loc[index, "PRIS KVM"] = value


    cleaned_df.to_csv("leie/saved_all_updated.csv", index=False)

    print("Updated data saved to leie/saved_all_updated.csv")


def FindNewUnavailable(sheet_name: str, columns: str):
    return find_new_unavailable(sheet_name, columns)


def PasteNewAvailability(data, sheet_name, startCell):
    return paste_new_availability(data, sheet_name, startCell)


if __name__ == "__main__":
    sheet_name = "Main"
    # data = find_new_unavailable(sheet_name, "D:L")
    data = pd.read_csv("leie/saved_availability.csv")
    paste_new_availability(data, sheet_name, "D2")


    # creds = get_credentials()
    # service = build("sheets", "v4", credentials=creds)
    # download_sheet_as_csv(service, "New", "leie/_temp3.csv", "C:L")
    # df_saved = pd.read_csv("leie/to_paste.csv")
    # get_everything_updated(df_saved)
