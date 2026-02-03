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
    )
    return df


def identificar_turno(valor: str):
    """
    Retorna:
    - 'AM'
    - 'SD'
    - 'AM+SD'
    - None (quando não disponível)
    """
    if not isinstance(valor, str):
        return None

    v = valor.lower()

    if "not available" in v or "pending" in v:
        return None

    tem_am = "05:45" in v or "09:30" in v
    tem_sd = "12:30" in v or "15:00" in v

    if tem_am and tem_sd:
        return "AM+SD"
    if tem_am:
        return "AM"
    if tem_sd:
        return "SD"

    return None


# ------------------------------------------------------
# PROCESSAMENTO PRINCIPAL
# ------------------------------------------------------
def processar_disponibilidade(
    df_raw: pd.DataFrame,
    base_motoristas: pd.DataFrame,
    base_regiao: pd.DataFrame
) -> pd.DataFrame:

    # -------------------------------
    # Normalização inicial
    # -------------------------------
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    base_motoristas = normalize_columns(base_motoristas)
    base_regiao = normalize_columns(base_regiao)

    # -------------------------------
    # Validações mínimas
    # -------------------------------
    obrigatorias_upload = [
        "Driver ID",
        "Driver Name",
        "Cluster",
        "Vehicle Type",
        "No Show Time"
    ]

    for col in obrigatorias_upload:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente no upload: {col}")

    for col in ["driver_id", "cep_ofertado", "turno"]:
        if col not in base_motoristas.columns:
            raise ValueError(f"base_motoristas precisa ter a coluna {col}")

    for col in ["cluster", "cep_base"]:
        if col not in base_regiao.columns:
            raise ValueError(f"base_regiao precisa ter a coluna {col}")

    # -------------------------------
    # Identificar colunas de data
    # -------------------------------
    idx = df.columns.get_loc("No Show Time")
    colunas_data = df.columns[idx + 1:]

    if not colunas_data.any():
        raise ValueError("Nenhuma coluna de data encontrada após 'No Show Time'")

    # -------------------------------
    # Explodir por data + turno
    # -------------------------------
    registros = []

    for col_data in colunas_data:
        data_ref = pd.to_datetime(col_data, errors="coerce")
        if pd.isna(data_ref):
            continue

        temp = df[
            [
                "Driver ID",
                "Driver Name",
                "Cluster",
                "Vehicle Type",
                col_data
            ]
        ].copy()

        temp["turno_raw"] = temp[col_data].apply(identificar_turno)
        temp = temp[temp["turno_raw"].notna()]

        # 🔥 AM+SD vira duas linhas
        temp["turno_ofertado"] = temp["turno_raw"].apply(
            lambda x: ["AM", "SD"] if x == "AM+SD" else [x]
        )

        temp = temp.explode("turno_ofertado").drop(columns=["turno_raw", col_data])

        temp["data"] = data_ref.strftime("%Y-%m-%d")
        temp["semana"] = data_ref.strftime("%G-W%V")

        registros.append(temp)

    if not registros:
        return pd.DataFrame()

    hist = pd.concat(registros, ignore_index=True)

    # -------------------------------
    # Normalização de colunas
    # -------------------------------
    hist = hist.rename(columns={
        "Driver ID": "driver_id",
        "Driver Name": "driver_name",
        "Vehicle Type": "vehicle_type",
        "Cluster": "cluster"
    })

    hist = normalize_columns(hist)

    # -------------------------------
    # Merge base_motoristas
    # -------------------------------
    hist = hist.merge(
        base_motoristas[[
            "driver_id",
            "cep_ofertado",
            "turno"
        ]].rename(columns={"turno": "turno_base"}),
        on="driver_id",
        how="left"
    )

    hist["turno_base"] = hist["turno_base"].fillna("N/D")

    # -------------------------------
    # Merge base_regiao
    # -------------------------------
    hist = hist.merge(
        base_regiao[["cluster", "cep_base"]],
        on="cluster",
        how="left"
    )

    # -------------------------------
    # CEPs
    # -------------------------------
    hist["cep_motorista"] = (
        hist["cluster"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str[:2]
    )

    hist["cep_base"] = (
        hist["cep_ofertado"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str[:2]
    )

    hist["disponivel"] = hist["cep_motorista"] == hist["cep_base"]
    hist["fora_da_regiao"] = ~hist["disponivel"]

    # -------------------------------
    # Metadados
    # -------------------------------
    hist["data_importacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

   # =====================================================
    # 🚫 REMOVE DUPLICIDADES (SAFE)
    # =====================================================
    chaves = ["driver_id", "data", "turno_ofertado"]
    
    chaves_existentes = [c for c in chaves if c in hist.columns]
    
    if len(chaves_existentes) == 3:
        hist = (
            hist
            .drop_duplicates(
                subset=chaves_existentes,
                keep="first"
            )
            .reset_index(drop=True)
        )


    # -------------------------------
    # Retorno final
    # -------------------------------
    return hist[
        [
            "driver_id",
            "driver_name",
            "cluster",
            "vehicle_type",
            "cep_ofertado",
            "cep_base",
            "disponivel",
            "fora_da_regiao",
            "data",
            "semana",
            "turno_base",
            "turno_ofertado",
            "data_importacao"
        ]
    ]
