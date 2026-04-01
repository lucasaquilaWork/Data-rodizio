import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import datetime

from data.sheets import read_tab, append_df
from processing.disponibilidade import processar_disponibilidade
from processing.carregamento import processar_carregamento
from processing.devolucoes import processar_devolucoes
from processing.cancelamento import processar_cancelamento
from processing.recusas import processar_recusas
from metrics.rodizio import consolidar_rodizio
from config.settings import *

# =====================================================
# CONFIG
# =====================================================
st.set_page_config(layout="wide")
st.title("📊 Rodízio Semanal")

# =====================================================
# HELPERS
# =====================================================
def ensure_df(df):
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame() if df is None else pd.DataFrame(df)


def ler_arquivo(file):
    return pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)


def preparar_para_sheets(df):
    """🔥 FUNÇÃO CRÍTICA - evita erro de JSON"""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df = df.replace([np.nan, None], "")
    df = df.replace([np.inf, -np.inf], "")
    df = df.astype(str)

    return df


def salvar_no_sheets(nome_tab, df):
    """Centraliza append + tratamento"""
    df = preparar_para_sheets(df)

    if df.empty:
        st.warning("⚠️ Nenhum dado válido para salvar")
        return

    try:
        append_df(nome_tab, df)
        st.success(f"✅ {len(df)} registros salvos com sucesso")
    except Exception as e:
        st.error("❌ Erro ao salvar no Google Sheets")
        st.exception(e)


def botao_modelo(df_modelo, nome_arquivo, label):
    buffer = BytesIO()
    df_modelo.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    st.download_button(label, buffer, nome_arquivo)


# =====================================================
# BASES
# =====================================================
try:
    base_motoristas = ensure_df(read_tab(BASE_MOTORISTAS_TAB))
    base_regiao = ensure_df(read_tab(BASE_REGIAO_TAB))
except:
    st.error("❌ Erro ao conectar com Google Sheets")
    st.stop()

# =====================================================
# MENU
# =====================================================
menu = st.sidebar.selectbox("Menu", [
    "Upload disponibilidade",
    "Upload carregamento",
    "Upload devolucoes",
    "Upload cancelamento",
    "Upload recusas",
    "Rodízio (visualização)"
])

# =====================================================
# UPLOAD
# =====================================================
arquivo = None

if menu == "Upload devolucoes":
    modelo = pd.DataFrame({
        "Driver ID": [""],
        "Driver Name": [""],
        "qtd_pacotes": [""],
        "data": [datetime.datetime.now().strftime("%d/%m/%Y")]
    })
    botao_modelo(modelo, "modelo_devolucoes.xlsx", "⬇️ Baixar modelo")

elif menu == "Upload cancelamento":
    modelo = pd.DataFrame({
        "Driver ID": [""],
        "Driver Name": [""],
        "Data": [datetime.datetime.now().strftime("%d/%m/%Y")],
        "Turno": ["AM"]
    })
    botao_modelo(modelo, "modelo_cancelamento.xlsx", "⬇️ Baixar modelo")

arquivo = st.file_uploader("Upload de arquivo", type=["csv", "xlsx"])

# =====================================================
# PROCESSAMENTO
# =====================================================
if arquivo and menu != "Rodízio (visualização)":
    df = ler_arquivo(arquivo)

    try:
        if menu == "Upload disponibilidade":
            df = processar_disponibilidade(df, base_motoristas, base_regiao)
            salvar_no_sheets(DISPONIBILIDADE_TAB, df)

        elif menu == "Upload carregamento":
            df_novo = processar_carregamento(df, base_motoristas)

            df_existente = ensure_df(read_tab(CARREGAMENTO_TAB))

            if not df_existente.empty:
                df_existente.columns = df_existente.columns.str.strip().str.lower()

                df_existente["task_id"] = df_existente["task_id"].astype(str).str.replace(r"\.0$", "", regex=True)
                df_novo["task_id"] = df_novo["task_id"].astype(str).str.replace(r"\.0$", "", regex=True)

                df_novo = df_novo[~df_novo["task_id"].isin(df_existente["task_id"])]

            if df_novo.empty:
                st.warning("⚠️ Nenhuma AT nova")
            else:
                salvar_no_sheets(CARREGAMENTO_TAB, df_novo)

        elif menu == "Upload devolucoes":
            df = processar_devolucoes(df, base_motoristas)
            salvar_no_sheets(DEVOLUCOES_TAB, df)

        elif menu == "Upload cancelamento":
            df = processar_cancelamento(df)
            salvar_no_sheets(CANCELAMENTO_TAB, df)

        elif menu == "Upload recusas":
            df = processar_recusas(df, base_motoristas)
            salvar_no_sheets(RECUSAS_TAB, df)

    except Exception as e:
        st.error("❌ Erro no processamento")
        st.exception(e)

# =====================================================
# NORMALIZAR SEMANA
# =====================================================
def normalizar_semana(df):
    if df.empty or "semana" not in df.columns:
        return df

    df = df.copy()

    def normalizar(valor, data):
        if pd.isna(valor):
            return None

        valor = str(valor).strip()

        if "-W" in valor:
            return valor

        try:
            ano = pd.to_datetime(data, dayfirst=True).year if pd.notna(data) else datetime.datetime.now().year
            semana = int(valor)
            return f"{ano}-W{str(semana).zfill(2)}"
        except:
            return None

    df["semana"] = df.apply(lambda r: normalizar(r["semana"], r.get("data")), axis=1)
    return df

# =====================================================
# RODÍZIO
# =====================================================
if menu == "Rodízio (visualização)":

    disp = normalizar_semana(ensure_df(read_tab(DISPONIBILIDADE_TAB)))
    carg = normalizar_semana(ensure_df(read_tab(CARREGAMENTO_TAB)))
    dev  = normalizar_semana(ensure_df(read_tab(DEVOLUCOES_TAB)))
    canc = normalizar_semana(ensure_df(read_tab(CANCELAMENTO_TAB)))
    rec  = normalizar_semana(ensure_df(read_tab(RECUSAS_TAB)))

    if disp.empty:
        st.warning("Nenhuma disponibilidade cadastrada")
        st.stop()

    semanas = sorted(disp["semana"].dropna().unique())
    semana_sel = st.selectbox("Selecione a semana", semanas)

    rodizio = consolidar_rodizio(
        disp[disp["semana"] == semana_sel],
        carg[carg["semana"] == semana_sel],
        dev[dev["semana"] == semana_sel],
        canc[canc["semana"] == semana_sel],
        rec[rec["semana"] == semana_sel],
        base_motoristas
    )

    st.subheader(f"📅 Rodízio – Semana {semana_sel}")
    st.dataframe(rodizio, use_container_width=True)

    st.download_button(
        "📥 Exportar CSV",
        rodizio.to_csv(index=False).encode("utf-8"),
        f"rodizio_{semana_sel}.csv"
    )
