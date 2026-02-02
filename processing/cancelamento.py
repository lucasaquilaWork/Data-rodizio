import datetime
from utils.normalize import normalize_columns
from utils.validation import validar_colunas
from utils.dates import calcular_semana


def processar_cancelamento(df):
    # Normaliza colunas
    df = normalize_columns(df)

    # Valida APENAS o que o usuário sobe
    validar_colunas(df, [
        "driver_id",
        "driver_name",
        "data",
        "turno"
    ])

    # Garante tipo data
    df["data"] = df["data"].astype(str)

    # Calcula semana
    df = calcular_semana(df)

    # Data de importação (automática)
    df["data_importacao"] = datetime.datetime.now()

    return df
