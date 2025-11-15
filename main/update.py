import pandas as pd

from main.post_process import post_process_property
from main.googleUtils import download_sheet_as_csv, get_credentials, SPREADSHEET_ID
from main.extraction_property import extract_property_data
from googleapiclient.discovery import build


def FindNewUnavailable(sheet_name: str, columns: str):
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
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
            print(f"Finnkode {row['Finnkode']} is already {row["Tilgjengelighet"]}")
            continue
        try:
            # Extract data for the URL
            updated_data = extract_property_data(row["URL"], index, "leie", True, True)
            updated_rows.append(updated_data)
        except Exception as e:
            print(f"Error processing URL at index {index}: {row['Finnkode']} - {e}")
            updated_rows.append(row)

    data = pd.DataFrame(updated_rows)
    dfdata = data[["Finnkode", "Tilgjengelighet"]]
    dfdata.to_csv("leie/saved_availability.csv", index=False)
    return dfdata


def PasteNewAvailability(data, sheet_name, startCell):
    # Initialize the service
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    # Define spreadsheet ID and range name
    range_name = f"{sheet_name}!{startCell}"  # Adjust the range as needed
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

    updated_rows = []
    for index, row in df_saved.iterrows():
        # temporary limit for testing
        # if not (600 < index):
        #     continue
        # if not (index<3):
        #     continue
        try:
            # Extract data for the URL
            updated_data = extract_property_data(row["URL"], index, "leie")
            updated_rows.append(updated_data)
        except Exception as e:
            # print(f"Error processing URL at index {index}: {row['Finnkode']} - {e}")
            updated_rows.append({
                "Finnkode": row["Finnkode"],
                "Tilgjengelighet": "Slettet",
            })

    # Save the updated data to a new CSV file
    data = pd.DataFrame(updated_rows)
    data.to_csv("leie/xx.csv", index=False)

    cleaned_df = post_process_property(data, 'leie', '_temp.csv')

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


if __name__ == "__main__":
    sheetName = "Main"
    # data = FindNewUnavailable(sheetName, "D:L")
    data = pd.read_csv("leie/saved_availability.csv")
    PasteNewAvailability(data, sheetName, "D2")


    # creds = get_credentials()
    # service = build("sheets", "v4", credentials=creds)
    # download_sheet_as_csv(service, "New", "leie/_temp3.csv", "C:L")
    # df_saved = pd.read_csv("leie/to_paste.csv")
    # get_everything_updated(df_saved)
