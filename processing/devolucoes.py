from utils.normalize import normalize_columns
from utils.validation import validar_colunas
from utils.dates import calcular_semana
from datetime import datetime


def processar_devolucoes(df, base_motoristas):
    df = normalize_columns(df)
    base_motoristas = normalize_columns(base_motoristas)

    # ✅ valida só o que vem do arquivo
    validar_colunas(df, [
        "driver_id",
        "qtd_pacotes",
        "data"
    ])

    # semana no mesmo padrão do resto do sistema
    df = calcular_semana(df)

    # vínculo com base de motoristas
    df = df.merge(
        base_motoristas[["driver_id", "turno"]]
        .rename(columns={"turno": "turno_base"}),
        on="driver_id",
        how="left"
    )

    # coluna gerada pelo sistema
    df["data_importacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df
