def validar_colunas(df, obrigatorias):
    faltando = set(obrigatorias) - set(df.columns)
    if faltando:
        raise ValueError(f"Colunas faltando: {faltando}")
