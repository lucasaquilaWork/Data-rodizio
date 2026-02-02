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


# ===============================
# CONFIG
# ===============================
st.set_page_config(layout="wide")
st.title("📊 Rodízio Semanal")


# ===============================
# BASES FIXAS
# ===============================
base_motoristas = read_tab(BASE_MOTORISTAS_TAB)
base_regiao = read_tab(BASE_REGIAO_TAB)


# ===============================
# MENU
# ===============================
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



# ===============================
# FUNÇÕES AUX
# ===============================
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


# ===============================
# UPLOAD + MODELOS
# ===============================
arquivo = None

if menu == "Upload devolucoes":

    st.subheader("📥 Modelo para upload de Devoluções")

    modelo = pd.DataFrame({
        "Driver ID": [""],
        "Driver Name": [""],
        "qtd_pacotes": [""],
        "data": [datetime.datetime.now().strftime("%d/%m/%Y")]
    })

    botao_modelo(
        modelo,
        "modelo_devolucoes.xlsx",
        "⬇️ Baixar modelo de devoluções"
    )

    st.divider()

    arquivo = st.file_uploader(
        "Upload do arquivo de devoluções (CSV ou XLSX)",
        type=["csv", "xlsx"]
    )

elif menu == "Upload cancelamento":

    st.subheader("📥 Modelo para upload de Cancelamento")

    modelo = pd.DataFrame({
        "Driver ID": [""],
        "Driver Name": [""],
        "Data": [datetime.datetime.now().strftime("%d/%m/%Y")],
        "Turno": ["AM"]
    })

    botao_modelo(
        modelo,
        "modelo_cancelamento.xlsx",
        "⬇️ Baixar modelo de cancelamento"
    )

    st.divider()

    arquivo = st.file_uploader(
        "Upload do arquivo de cancelamento (CSV ou XLSX)",
        type=["csv", "xlsx"]
    )

else:
    arquivo = st.file_uploader(
        "Upload de arquivo (CSV ou XLSX)",
        type=["csv", "xlsx"]
    )


# ===============================
# PROCESSAMENTO
# ===============================
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


# ===============================
# RODÍZIO / VISUALIZAÇÃO
# ===============================
if menu == "Rodízio (visualização)":

    disp = read_tab(DISPONIBILIDADE_TAB)
    carg = read_tab(CARREGAMENTO_TAB)
    dev = read_tab(DEVOLUCOES_TAB)
    canc = read_tab(CANCELAMENTO_TAB)
    rec = read_tab(RECUSAS_TAB)

    if disp.empty:
        st.warning("Nenhuma disponibilidade cadastrada")
        st.stop()

    semanas = sorted(disp["semana"].dropna().unique())

    semana_sel = st.selectbox(
        "Selecione a semana",
        semanas
    )

    disp_w = disp[disp["semana"] == semana_sel]
    carg_w = carg[carg["semana"] == semana_sel] if not carg.empty else carg
    dev_w = dev[dev["semana"] == semana_sel] if not dev.empty else dev
    rec_w = rec[rec["semana"] == semana_sel] if not rec.empty else rec
    canc_w = canc.copy()

    if disp_w.empty:
        st.warning("Nenhuma disponibilidade para essa semana")
        st.stop()

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
