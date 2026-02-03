import pandas as pd
from datetime import datetime


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


def identificar_turno_carregamento(create_time: str):
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


def processar_carregamento(
    df_raw: pd.DataFrame,
    base_motoristas: pd.DataFrame
) -> pd.DataFrame:

    # ===============================
    # NORMALIZAÇÃO INICIAL
    # ===============================
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    base_motoristas = normalize_columns(base_motoristas)

    # ===============================
    # VALIDAÇÕES
    # ===============================
    obrigatorias = [
        "Task ID",
        "Driver ID",
        "Driver name",
        "Vehicle Type",
        "Delivery Date",
        "Create Time"
    ]

    for col in obrigatorias:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {col}")

    # ===============================
    # NORMALIZA DATAS ANTES DE DEDUP
    # ===============================
    df["delivery_date_norm"] = pd.to_datetime(
        df["Delivery Date"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # ===============================
    # DEDUPLICAÇÃO REAL (RAIZ)
    # 1 AT / 1 DRIVER / 1 DIA
    # ===============================
    df = (
        df
        .drop_duplicates(
            subset=["Task ID", "Driver ID", "delivery_date_norm"],
            keep="first"
        )
        .reset_index(drop=True)
    )

    # ===============================
    # CAMPOS DERIVADOS
    # ===============================
    df["data"] = df["delivery_date_norm"]

    df["semana"] = pd.to_datetime(
        df["delivery_date_norm"],
        errors="coerce"
    ).dt.strftime("%G-W%V")

    df["turno_carregamento"] = df["Create Time"].apply(
        identificar_turno_carregamento
    )

    # ===============================
    # RENOMEAR + NORMALIZAR
    # ===============================
    df = df.rename(columns={
        "Task ID": "task_id",
        "Driver ID": "driver_id",
        "Driver name": "driver_name",
        "Vehicle Type": "vehicle_type"
    })

    df = normalize_columns(df)

    # ===============================
    # ENRIQUECE COM BASE MOTORISTAS
    # ===============================
    df = df.merge(
        base_motoristas[["driver_id", "turno"]].rename(
            columns={"turno": "turno_base"}
        ),
        on="driver_id",
        how="left"
    )

    df["turno_base"] = df["turno_base"].fillna("N/D")

    # ===============================
    # FORA DO TURNO
    # ===============================
    df["fora_do_turno"] = (
        df["turno_carregamento"].notna()
        & (df["turno_base"] != "N/D")
        & (df["turno_carregamento"] != df["turno_base"])
    )

    # ===============================
    # METADADOS
    # ===============================
    df["data_importacao"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # ===============================
    # RETORNO FINAL
    # ===============================
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
