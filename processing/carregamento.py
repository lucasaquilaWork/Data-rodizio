import pandas as pd
from datetime import datetime


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )
    return df


def identificar_turno_carregamento(create_time: str) -> str | None:
    """
    Regras:
    - AM: criação entre 00:00 e 04:59
    - SD: criação entre 08:00 e 10:59
    - Caso contrário: None
    """
    if not isinstance(create_time, str):
        return None

    try:
        hora = pd.to_datetime(create_time).hour
    except Exception:
        return None

    if 0 <= hora <= 4:
        return "AM"
    if 6 <= hora <= 12:
        return "SD"

    return None


# ------------------------------------------------------
# PROCESSAMENTO PRINCIPAL
# ------------------------------------------------------
def processar_carregamento(
    df_raw: pd.DataFrame,
    base_motoristas: pd.DataFrame
) -> pd.DataFrame:

    # -------------------------------
    # Normalização
    # -------------------------------
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    base_motoristas = normalize_columns(base_motoristas)

    # -------------------------------
    # Validações
    # -------------------------------
    obrigatorias_upload = [
        "Task ID",
        "Driver name",
        "Driver ID",
        "Vehicle Type",
        "Delivery Date",
        "Create Time"
    ]

    for col in obrigatorias_upload:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente no carregamento: {col}")

    if "driver_id" not in base_motoristas.columns:
        raise ValueError("base_motoristas precisa ter a coluna driver_id")

    if "turno" not in base_motoristas.columns:
        raise ValueError("base_motoristas precisa ter a coluna turno")

    # -------------------------------
    # Remover duplicados por Task ID
    # -------------------------------
    df = df.drop_duplicates(subset=["Task ID"])

    # -------------------------------
    # Seleção das colunas relevantes
    # -------------------------------
    df = df[
        [
            "Task ID",
            "Driver name",
            "Driver ID",
            "Vehicle Type",
            "Delivery Date",
            "Create Time"
        ]
    ].copy()

    # -------------------------------
    # Datas
    # -------------------------------
    df["data"] = pd.to_datetime(
        df["Delivery Date"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df["semana"] = pd.to_datetime(
        df["Delivery Date"],
        errors="coerce"
    ).dt.strftime("%G-W%V")

    # -------------------------------
    # Turno do carregamento
    # -------------------------------
    df["turno_carregamento"] = df["Create Time"].apply(
        identificar_turno_carregamento
    )

    # -------------------------------
    # Renomear colunas
    # -------------------------------
    df = df.rename(columns={
        "Task ID": "task_id",
        "Driver name": "driver_name",
        "Driver ID": "driver_id",
        "Vehicle Type": "vehicle_type"
    })

    df = normalize_columns(df)

    # -------------------------------
    # Enriquecer com base_motoristas
    # -------------------------------
    df = df.merge(
        base_motoristas[["driver_id", "turno"]].rename(
            columns={"turno": "turno_base"}
        ),
        on="driver_id",
        how="left"
    )

    df["turno_base"] = df["turno_base"].fillna("N/D")

    # -------------------------------
    # Fora do turno
    # -------------------------------
    df["fora_do_turno"] = (
        (df["turno_carregamento"].notna())
        & (df["turno_base"] != "N/D")
        & (df["turno_carregamento"] != df["turno_base"])
    )

    # -------------------------------
    # Metadados
    # -------------------------------
    df["data_importacao"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # -------------------------------
    # Retorno final
    # -------------------------------
    return df[
        [
            "task_id",
            "driver_id",
            "driver_name",
            "vehicle_type",
            "data",
            "turno_carregamento",
            "semana",
            "turno_base",
            "fora_do_turno",
            "data_importacao"
        ]
    ]
