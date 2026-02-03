import streamlit as st
import pandas as pd
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
# CONFIG STREAMLIT
# =====================================================
st.set_page_config(layout="wide")
st.title("📊 Rodízio Semanal")


# =====================================================
# FUNÇÕES AUXILIARES
# =====================================================
def ensure_df(df):
    if df is None:
        return pd.DataFrame()
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame(df)


def ler_arquivo(file):
    if file.name.endswith(".csv"):
        return pd.read_csv(file)
    return pd.read_excel(file)


def botao_modelo(df_modelo, nome_arquivo, label):
    buffer = BytesIO()
    df_modelo.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    st.download_button(
        label=label,
        data=buffer,
        file_name=nome_arquivo,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# =====================================================
# BASES FIXAS
# =====================================================
try:
    base_motoristas = ensure_df(read_tab(BASE_MOTORISTAS_TAB))
    base_regiao = ensure_df(read_tab(BASE_REGIAO_TAB))
except Exception as e:
    st.error("❌ Erro ao conectar na planilha Google")
    st.stop()


# =====================================================
# MENU
# =====================================================
menu = st.sidebar.selectbox(
    "Menu",
    [
        "Upload disponibilidade",
        "Upload carregamento",
        "Upload devolucoes",
        "Upload cancelamento",
        "Upload recusas",
        "Rodízio (visualização)"
    ]
)


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
    arquivo = st.file_uploader("Upload do arquivo", type=["csv", "xlsx"])

elif menu == "Upload cancelamento":
    modelo = pd.DataFrame({
        "Driver ID": [""],
        "Driver Name": [""],
        "Data": [datetime.datetime.now().strftime("%d/%m/%Y")],
        "Turno": ["AM"]
    })
    botao_modelo(modelo, "modelo_cancelamento.xlsx", "⬇️ Baixar modelo")
    arquivo = st.file_uploader("Upload do arquivo", type=["csv", "xlsx"])

else:
    arquivo = st.file_uploader("Upload de arquivo", type=["csv", "xlsx"])


# =====================================================
# PROCESSAMENTO
# =====================================================
if arquivo and menu != "Rodízio (visualização)":
    df = ler_arquivo(arquivo)

    if menu == "Upload disponibilidade":
        df = processar_disponibilidade(df, base_motoristas, base_regiao)
        append_df(DISPONIBILIDADE_TAB, df)

    elif menu == "Upload carregamento":
        df = processar_carregamento(df, base_motoristas)
        append_df(CARREGAMENTO_TAB, df)

    elif menu == "Upload devolucoes":
        df = processar_devolucoes(df, base_motoristas)
        append_df(DEVOLUCOES_TAB, df)

    elif menu == "Upload cancelamento":
        df = processar_cancelamento(df)
        append_df(CANCELAMENTO_TAB, df)

    elif menu == "Upload recusas":
        df = processar_recusas(df, base_motoristas)
        append_df(RECUSAS_TAB, df)

    st.success("✅ Arquivo processado e salvo com sucesso")


# =====================================================
# RODÍZIO
# =====================================================
if menu == "Rodízio (visualização)":

    disp = ensure_df(read_tab(DISPONIBILIDADE_TAB))
    carg = ensure_df(read_tab(CARREGAMENTO_TAB))
    dev  = ensure_df(read_tab(DEVOLUCOES_TAB))
    canc = ensure_df(read_tab(CANCELAMENTO_TAB))
    rec  = ensure_df(read_tab(RECUSAS_TAB))

    if disp.empty or "semana" not in disp.columns:
        st.warning("Nenhuma disponibilidade cadastrada")
        st.stop()

    semanas = sorted(disp["semana"].dropna().unique())
    semana_sel = st.selectbox("Selecione a semana", semanas)

    disp_w = disp[disp["semana"] == semana_sel]
    carg_w = carg[carg["semana"] == semana_sel] if "semana" in carg.columns else carg
    dev_w  = dev[dev["semana"] == semana_sel] if "semana" in dev.columns else dev
    rec_w  = rec[rec["semana"] == semana_sel] if "semana" in rec.columns else rec

    rodizio = consolidar_rodizio(
        disp,
        carg,
        dev,
        canc,
        rec,
        base_motoristas
    )

    st.subheader(f"📅 Rodízio – Semana {semana_sel}")
    st.dataframe(rodizio, use_container_width=True)

    st.download_button(
        "📥 Exportar rodízio (CSV)",
        data=rodizio.to_csv(index=False).encode("utf-8"),
        file_name=f"rodizio_semana_{semana_sel}.csv",
        mime="text/csv"
    )
