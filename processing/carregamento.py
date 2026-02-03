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
    - SD: criação entre 06:00 e 12:59
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
    # Normalização inicial
    # -------------------------------
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    base_motoristas = normalize_columns(base_motoristas)

    # -------------------------------
    # Validações mínimas
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
    # Remove duplicidade bruta por Task ID
    # -------------------------------
    df = df.drop_duplicates(subset=["Task ID"], keep="first")

    # -------------------------------
    # Seleção de colunas relevantes
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
    data_parse = pd.to_datetime(df["Delivery Date"], errors="coerce")

    df["data"] = data_parse.dt.strftime("%Y-%m-%d")
    df["semana"] = data_parse.dt.strftime("%G-W%V")

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
    # Garante colunas mínimas (ANTI BUG)
    # -------------------------------
    colunas_minimas = [
        "task_id",
        "driver_id",
        "data",
        "semana",
        "turno_carregamento"
    ]

    for col in colunas_minimas:
        if col not in df.columns:
            df[col] = pd.NA

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
        df["turno_carregamento"].notna()
        & (df["turno_base"] != "N/D")
        & (df["turno_carregamento"] != df["turno_base"])
    )

    # -------------------------------
    # REMOVE DUPLICIDADES FINAIS (SAFE)
    # 1 AT / 1 motorista / 1 turno / 1 data
    # -------------------------------
    chaves = [
        "task_id",
        "driver_id",
        "data",
        "turno_carregamento"
    ]

    chaves_existentes = [c for c in chaves if c in df.columns]

    if len(chaves_existentes) == len(chaves):
        df = (
            df
            .drop_duplicates(
                subset=chaves_existentes,
                keep="first"
            )
            .reset_index(drop=True)
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
