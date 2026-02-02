import pandas as pd


def calcular_semana(df, coluna_data="data"):
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    df["semana"] = df[coluna_data].dt.isocalendar().week
    return df
