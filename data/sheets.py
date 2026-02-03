import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = st.secrets["gcp_service_account"]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict,
        scope
    )

    return gspread.authorize(creds)

def read_tab(tab_name):
    client = get_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = sh.worksheet(tab_name)
    return ws.get_all_records()

def append_df(tab_name, df):
    client = get_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = sh.worksheet(tab_name)

    ws.append_rows(
        df.astype(str).values.tolist(),
        value_input_option="USER_ENTERED"
    )
