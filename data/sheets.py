import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from config.settings import DATA_RODIZIO_SHEET


def get_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json", scope
    )
    return gspread.authorize(creds)


def read_tab(tab_name):
    client = get_client()
    sheet = client.open(DATA_RODIZIO_SHEET).worksheet(tab_name)
    data = sheet.get_all_records()
    return pd.DataFrame(data)


def append_df(tab_name, df):
    client = get_client()
    sheet = client.open(DATA_RODIZIO_SHEET).worksheet(tab_name)
    sheet.append_rows(df.astype(str).values.tolist())
