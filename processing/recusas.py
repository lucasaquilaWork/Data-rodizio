import pandas as pd
from datetime import datetime

from utils.normalize import normalize_columns
from utils.validation import validar_colunas
from utils.dates import calcular_semana


def identificar_turno_recusa(valor: str):
    if not isinstance(valor, str):
        return None

    if "05:45" in valor:
        return "AM"
    if "12:30" in valor:
        return "SD"

    return None


def extrair_data(valor: str):
    """
    Extrai a data do Call-up Time Slot
    Ex: 2026-01-28 12:30 - 15:00 → 2026-01-28
    """
    try:
        return pd.to_datetime(valor[:10]).strftime("%Y-%m-%d")
    except Exception:
        return None


def extrair_driver_id(valor: str):
    """
    [1414170] FELIPE BOTELHO DA ROCHA → 1414170
    """
    if not isinstance(valor, str):
        return None

    if "[" in valor and "]" in valor:
        return valor.split("[")[1].split("]")[0].strip()

    return None


def extrair_driver_name(valor: str):
    """
    [1414170] FELIPE BOTELHO DA ROCHA → FELIPE BOTELHO DA ROCHA
    """
    if not isinstance(valor, str):
        return None

    if "]" in valor:
        return valor.split("]")[1].strip()

    return None


def processar_recusas(df: pd.DataFrame, base_motoristas: pd.DataFrame) -> pd.DataFrame:

    # -------------------------------
    # Normalização
    # -------------------------------
    df = normalize_columns(df)
    base_motoristas = normalize_columns(base_motoristas)

    # -------------------------------
    # Validação mínima
    # -------------------------------
    validar_colunas(df, [
        "notification_id",
        "call-up_time_slot",
        "driver"
    ])

    # -------------------------------
    # Extrações principais
    # -------------------------------
    df["driver_id"] = df["driver"].apply(extrair_driver_id)
    df["driver_name"] = df["driver"].apply(extrair_driver_name)

    df["turno_recusa"] = df["call-up_time_slot"].apply(identificar_turno_recusa)
    df["data"] = df["call-up_time_slot"].apply(extrair_data)

    # Remove linhas inválidas
    df = df[
        df["driver_id"].notna() &
        df["data"].notna() &
        df["turno_recusa"].notna()
    ]

    # -------------------------------
    # Semana
    # -------------------------------
    df = calcular_semana(df)
    # Padronizar driver_id como string nos dois DataFrames
    df["driver_id"] = df["driver_id"].astype(str)
    base_motoristas["driver_id"] = base_motoristas["driver_id"].astype(str)

    # -------------------------------
    # Merge com base_motoristas
    # -------------------------------
    df = df.merge(
        base_motoristas[["driver_id", "turno"]],
        on="driver_id",
        how="left"
    )

    df = df.rename(columns={"turno": "turno_base"})
    df["turno_base"] = df["turno_base"].fillna("N/D")

    # -------------------------------
    # Data de importação
    # -------------------------------
    df["data_importacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------------
    # Seleção final
    # -------------------------------
    return df[
        [
            "notification_id",
            "driver_id",
            "driver_name",
            "data",
            "semana",
            "turno_recusa",
            "turno_base",
            "data_importacao"
        ]
    ]
