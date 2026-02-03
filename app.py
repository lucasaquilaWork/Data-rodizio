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
        df_novo = processar_carregamento(df, base_motoristas)
    
        # 🔥 LER O QUE JÁ EXISTE
        df_existente = ensure_df(read_tab(CARREGAMENTO_TAB))
    
        if not df_existente.empty:
            df_existente.columns = (
                df_existente.columns
                .astype(str)
                .str.strip()
                .str.lower()
            )
    
            # GARANTE STRING LIMPA
            df_existente["task_id"] = (
                df_existente["task_id"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
            )
    
            df_novo["task_id"] = (
                df_novo["task_id"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
            )
    
            # 🚫 REMOVE ATs JÁ REGISTRADAS
            df_novo = df_novo[
                ~df_novo["task_id"].isin(df_existente["task_id"])
            ]
    
        if df_novo.empty:
            st.warning("⚠️ Nenhuma AT nova para registrar (todas já existiam)")
        else:
            append_df(CARREGAMENTO_TAB, df_novo)
            st.success(f"✅ {len(df_novo)} carregamentos novos registrados")


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

def normalizar_semana(df):
    if df.empty or "semana" not in df.columns:
        return df

    df = df.copy()

    def normalizar(valor, data):
        if pd.isna(valor):
            return None

        valor = str(valor).strip()

        # Se já estiver no formato 2026-W05
        if "-W" in valor:
            return valor

        # Tenta pegar o ano da coluna data
        ano = None
        if pd.notna(data):
            try:
                ano = pd.to_datetime(data, dayfirst=True).year
            except:
                pass

        if ano is None:
            ano = datetime.datetime.now().year

        try:
            semana = int(valor)
            return f"{ano}-W{str(semana).zfill(2)}"
        except:
            return None

    if "data" in df.columns:
        df["semana"] = df.apply(
            lambda r: normalizar(r["semana"], r["data"]),
            axis=1
        )
    else:
        df["semana"] = df["semana"].apply(
            lambda v: normalizar(v, None)
        )

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
        disp_w,
        carg_w,
        dev_w,
        canc[canc["semana"] == semana_sel] if "semana" in canc.columns else canc,
        rec_w,
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
