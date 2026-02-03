import pandas as pd
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

    try:
        ws = sh.worksheet(tab_name)
        records = ws.get_all_records()

        if not records:
            return pd.DataFrame()

        return pd.DataFrame(records)

    except Exception as e:
        st.error(f"Erro ao ler aba '{tab_name}': {e}")
        return pd.DataFrame()


def append_df(tab_name, df):
    if df.empty:
        return

    client = get_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = sh.worksheet(tab_name)

    ws.append_rows(
        df.astype(str).values.tolist(),
        value_input_option="USER_ENTERED"
    )
