import pandas as pd
from utils.normalize import normalize_columns


def consolidar_rodizio(
    disp,
    carg,
    dev,
    canc,
    rec,
    base_motoristas=None,
):
    # ==================================================
    # NORMALIZA
    # ==================================================
    disp = normalize_columns(disp)
    carg = normalize_columns(carg)
    dev = normalize_columns(dev)
    canc = normalize_columns(canc)
    rec = normalize_columns(rec)

    if base_motoristas is not None:
        base_motoristas = normalize_columns(base_motoristas)

    # ==================================================
    # DRIVER_ID STRING (ANTI-FLOAT .0)
    # ==================================================
    for df_ in [disp, carg, dev, canc, rec, base_motoristas]:
        if df_ is not None and "driver_id" in df_.columns:
            df_["driver_id"] = (
                df_["driver_id"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
            )

    # ==================================================
    # BASE DO RODÍZIO = DISP + CADASTRO
    # ==================================================
    base_disp = disp[[
        "driver_id",
        "driver_name",
        "turno_ofertado"
    ]].copy()

    if base_motoristas is not None:
        base_cad = base_motoristas[[
            "driver_id",
            "driver_name",
            "turno"
        ]].copy()

        base_cad = base_cad.rename(columns={"turno": "turno_base"})
        base_cad["turno_ofertado"] = pd.NA

        base = pd.concat(
            [base_disp, base_cad],
            ignore_index=True
        )
    else:
        base = base_disp
        base["turno_base"] = pd.NA

    # ==================================================
    # DISPONIBILIDADE POR TURNO
    # ==================================================
    base["disp_am"] = (base["turno_ofertado"] == "AM").astype(int)
    base["disp_sd"] = (base["turno_ofertado"] == "SD").astype(int)

    # ==================================================
    # AGREGAÇÃO — 1 LINHA POR DRIVER
    # ==================================================
    disp_agg = base.groupby("driver_id", as_index=False).agg(
        driver_name=("driver_name", "first"),
        turno_base=("turno_base", "first"),
        disp_am=("disp_am", "sum"),
        disp_sd=("disp_sd", "sum"),
        disp_total=("turno_ofertado", "count")
    )

    # ==================================================
    # TURNO PREDOMINANTE / REFERÊNCIA
    # ==================================================
    disp_agg["turno_predominante"] = disp_agg.apply(
        lambda r: "AM" if r["disp_am"] >= r["disp_sd"] else "SD",
        axis=1
    )

    disp_agg["turno_referencia"] = disp_agg["turno_base"]
    disp_agg.loc[
        disp_agg["turno_referencia"].isna(),
        "turno_referencia"
    ] = disp_agg["turno_predominante"]

    # ==================================================
    # CARREGAMENTOS (GRANULAR + MERGE)
    # ==================================================
    if not carg.empty:
        carg = carg.merge(
            disp_agg[["driver_id", "turno_referencia"]],
            on="driver_id",
            how="left"
        )

        carg["carg_no_turno"] = (
            carg["turno_carregamento"] == carg["turno_referencia"]
        ).astype(int)

        carg["carg_am"] = (carg["turno_carregamento"] == "AM").astype(int)
        carg["carg_sd"] = (carg["turno_carregamento"] == "SD").astype(int)

        carg_agg = carg.groupby("driver_id", as_index=False).agg(
            carg_total=("task_id", "count"),
            carg_no_turno=("carg_no_turno", "sum"),
            carg_am=("carg_am", "sum"),
            carg_sd=("carg_sd", "sum")
        )
    else:
        carg_agg = pd.DataFrame(
            columns=[
                "driver_id",
                "carg_total",
                "carg_no_turno",
                "carg_am",
                "carg_sd"
            ]
        )

    # ==================================================
    # DEV / CANC / REC
    # ==================================================
    dev_agg = (
        dev.groupby("driver_id", as_index=False)
        .agg(devolucoes=("qtd_pacotes", "sum"))
        if not dev.empty else pd.DataFrame(columns=["driver_id", "devolucoes"])
    )

    canc_agg = (
        canc.groupby("driver_id", as_index=False)
        .size()
        .rename(columns={"size": "cancelamentos"})
        if not canc.empty else pd.DataFrame(columns=["driver_id", "cancelamentos"])
    )

    rec_agg = (
        rec.groupby("driver_id", as_index=False)
        .size()
        .rename(columns={"size": "recusas"})
        if not rec.empty else pd.DataFrame(columns=["driver_id", "recusas"])
    )

    # ==================================================
    # CONSOLIDAÇÃO FINAL
    # ==================================================
    df = (
        disp_agg
        .merge(carg_agg, on="driver_id", how="left")
        .merge(dev_agg, on="driver_id", how="left")
        .merge(canc_agg, on="driver_id", how="left")
        .merge(rec_agg, on="driver_id", how="left")
        .fillna(0)
    )

    # ==================================================
    # DISP NO PRÓPRIO TURNO
    # ==================================================
    df["disp_no_turno"] = 0
    df.loc[df["turno_referencia"] == "AM", "disp_no_turno"] = df["disp_am"]
    df.loc[df["turno_referencia"] == "SD", "disp_no_turno"] = df["disp_sd"]

    # ==================================================
    # TAXA DE APROVEITAMENTO
    # ==================================================
    df["taxa_aproveitamento_turno"] = (
        df["carg_no_turno"] /
        df["disp_no_turno"].replace(0, 1)
    ).round(2)

    df["taxa_aproveitamento_turno_pct"] = (
        df["taxa_aproveitamento_turno"] * 100
    ).round(1)

    # ==================================================
    # PRIORIDADE (QUEM RODOU MENOS PRIMEIRO)
    # ==================================================
    df["penalidade"] = (
        df["recusas"] * 2 +
        df["cancelamentos"] * 3 +
        df["devolucoes"] * 0.5
    )

    df["indice_prioridade"] = df["carg_total"] + df["penalidade"]

    # JOGA PRO FINAL QUEM NÃO SE DISPONIBILIZOU
    df.loc[df["disp_total"] == 0, "indice_prioridade"] += 1000

    # ==================================================
    # STATUS / ORIGEM
    # ==================================================
    df["origem_turno"] = "BASE_MOTORISTAS"
    df.loc[df["turno_base"].isna(), "origem_turno"] = "INFERIDO_PELA_DISP"

    df["status_rodizio"] = "ATIVO"
    df.loc[df["disp_total"] == 0, "status_rodizio"] = "SEM DISPONIBILIDADE"

    return (
        df.sort_values("indice_prioridade", ascending=True)
        .reset_index(drop=True)
    )
