"""
app.py — Painel Executivo de Gestão Hospitalar & Ocupação de Leitos
===================================================================
Aplicação analítica interativa em Streamlit & Plotly consumindo os dados
da Camada Gold do HealthTech Data Lakehouse.

Peça 6 da Crafting Table: A Tela de Consumo & Analytics
"""

import os
import glob
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ===========================================================================
# CONFIGURAÇÕES DA PÁGINA STREAMLIT
# ===========================================================================
st.set_page_config(
    page_title="HealthTech Lakehouse — Gestão Hospitalar & UTI",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS para Design Dark Moderno e Glassmorphism
st.markdown("""
<style>
    .main {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    .metric-card {
        background: linear-gradient(135deg, rgba(22, 27, 34, 0.8), rgba(13, 17, 23, 0.9));
        border: 1px solid rgba(48, 54, 61, 0.8);
        border-radius: 12px;
        padding: 18px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        margin-bottom: 12px;
    }
    .metric-title {
        font-size: 0.85rem;
        font-weight: 600;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #58a6ff;
        margin-top: 4px;
    }
    .metric-alert {
        color: #ff7b72;
    }
    .metric-success {
        color: #3fb950;
    }
</style>
""", unsafe_allow_html=True)


# ===========================================================================
# FUNÇÃO DE CARGA DE DADOS DO DELTA LAKE (CAMADA GOLD)
# ===========================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_GOLD = os.path.join(BASE_DIR, "storage", "gold")

@st.cache_data(ttl=60)
def load_gold_data():
    """
    Carrega as tabelas analíticas da Camada Gold lendo os arquivos colunares Parquet
    gerados pelo Delta Lake.
    """
    path_fact = os.path.join(PATH_GOLD, "fact_hospital_occupancy")
    path_hospitals = os.path.join(PATH_GOLD, "dim_hospitals")
    path_specialties = os.path.join(PATH_GOLD, "dim_specialties")

    # Ler arquivos Parquet da Tabela Fato (recursivo pelas partições year_month)
    fact_files = glob.glob(f"{path_fact}/**/*.parquet", recursive=True)
    if not fact_files:
        return None, None, None

    df_fact = pd.concat([pd.read_parquet(f) for f in fact_files], ignore_index=True)

    # Ler Dimensões
    hosp_files = glob.glob(f"{path_hospitals}/*.parquet")
    df_hospitals = pd.concat([pd.read_parquet(f) for f in hosp_files], ignore_index=True) if hosp_files else pd.DataFrame()

    spec_files = glob.glob(f"{path_specialties}/*.parquet")
    df_specialties = pd.concat([pd.read_parquet(f) for f in spec_files], ignore_index=True) if spec_files else pd.DataFrame()

    return df_fact, df_hospitals, df_specialties


# ===========================================================================
# CABEÇALHO & SIDEBAR
# ===========================================================================
st.title("🏥 HealthTech Data Lakehouse")
st.caption("Painel Executivo de Ocupação Hospitalar, Leitos de UTI e Gestão de Custos Clínicos")

df_fact, df_hospitals, df_specialties = load_gold_data()

if df_fact is None or df_fact.empty:
    st.warning("⚠️ Nenhuma tabela analítica encontrada na Camada Gold (`storage/gold`). Execute o pipeline `python3 main.py` ou a DAG do Airflow primeiro.")
    st.stop()

# Sidebar de Filtros
st.sidebar.header("🔍 Filtros Clínicos")

# Filtro de Hospital
hospitais_disponiveis = ["Todos os Hospitais"] + sorted(df_fact["hospital_name"].dropna().unique().tolist())
hospital_selecionado = st.sidebar.selectbox("Filtrar por Unidade Hospitalar:", hospitais_disponiveis)

# Filtro de Especialidade Médica
especialidades_disponiveis = ["Todas as Especialidades"] + sorted(df_fact["specialty"].dropna().unique().tolist())
especialidade_selecionada = st.sidebar.selectbox("Filtrar por Especialidade Médica:", especialidades_disponiveis)

# Filtro de Tipo de Leito
leitos_disponiveis = ["Todos os Leitos"] + sorted(df_fact["bed_type"].dropna().unique().tolist())
leito_selecionado = st.sidebar.selectbox("Filtrar por Tipo de Leito:", leitos_disponiveis)

# Aplicação dos Filtros no DataFrame
df_filtered = df_fact.copy()
if hospital_selecionado != "Todos os Hospitais":
    df_filtered = df_filtered[df_filtered["hospital_name"] == hospital_selecionado]
if especialidade_selecionada != "Todas as Especialidades":
    df_filtered = df_filtered[df_filtered["specialty"] == especialidade_selecionada]
if leito_selecionado != "Todos os Leitos":
    df_filtered = df_filtered[df_filtered["bed_type"] == leito_selecionado]


# ===========================================================================
# CARDS DE MÉTRICAS E KPIS PRINCIPAIS
# ===========================================================================
total_pacientes = len(df_filtered)
total_uti = int(df_filtered["is_critical_care"].sum())
taxa_uti = (total_uti / total_pacientes * 100) if total_pacientes > 0 else 0
media_permanencia = df_filtered["length_of_stay_days"].mean() if total_pacientes > 0 else 0
custo_total = df_filtered["total_treatment_cost"].sum() if total_pacientes > 0 else 0

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">👥 Total de Internações</div>
        <div class="metric-value">{total_pacientes:,}</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    alert_class = "metric-alert" if taxa_uti > 30 else "metric-success"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">🚨 Ocupação UTI (Casos Críticos)</div>
        <div class="metric-value {alert_class}">{total_uti} <span style="font-size: 1.1rem;">({taxa_uti:.1f}%)</span></div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">⏱️ Média de Permanência</div>
        <div class="metric-value">{media_permanencia:.1f} <span style="font-size: 1.1rem;">dias</span></div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">💰 Custo Total do Tratamento</div>
        <div class="metric-value" style="color: #7ee787;">R$ {custo_total:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")


# ===========================================================================
# GRÁFICOS ANALÍTICOS (PLOTLY INTERATIVO)
# ===========================================================================
g1, g2 = st.columns(2)

with g1:
    st.subheader("🏨 Internações Críticas (UTI) por Hospital")
    df_uti_hosp = df_filtered[df_filtered["is_critical_care"] == True].groupby("hospital_name")["admission_id"].count().reset_index()
    df_uti_hosp.columns = ["Hospital", "Pacientes em UTI"]
    df_uti_hosp = df_uti_hosp.sort_values("Pacientes em UTI", ascending=True)

    fig_hosp = px.bar(
        df_uti_hosp,
        x="Pacientes em UTI",
        y="Hospital",
        orientation="h",
        color="Pacientes em UTI",
        color_continuous_scale="Reds",
        template="plotly_dark"
    )
    fig_hosp.update_layout(showlegend=False, height=350, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_hosp, use_container_width=True)

with g2:
    st.subheader("🩺 Custo Total e Volume por Especialidade")
    df_spec_agg = df_filtered.groupby("specialty").agg(
        total_cost=("total_treatment_cost", "sum"),
        total_patients=("admission_id", "count")
    ).reset_index().sort_values("total_cost", ascending=False)

    fig_spec = px.bar(
        df_spec_agg,
        x="specialty",
        y="total_cost",
        color="total_patients",
        color_continuous_scale="Blues",
        labels={"specialty": "Especialidade", "total_cost": "Custo Total (R$)", "total_patients": "Pacientes"},
        template="plotly_dark"
    )
    fig_spec.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_spec, use_container_width=True)

st.markdown("---")

g3, g4 = st.columns([3, 2])

with g3:
    st.subheader("📈 Diagnósticos Mais Frequentes (Top Códigos CID-10)")
    df_cid = df_filtered.groupby(["cid_code", "cid_description"])["admission_id"].count().reset_index()
    df_cid.columns = ["Código CID", "Descrição da Patologia", "Total"]
    df_cid["Diagnóstico"] = df_cid["Código CID"] + " - " + df_cid["Descrição da Patologia"]
    df_cid = df_cid.sort_values("Total", ascending=False).head(8)

    fig_cid = px.bar(
        df_cid,
        x="Total",
        y="Diagnóstico",
        orientation="h",
        color="Total",
        color_continuous_scale="Teal",
        template="plotly_dark"
    )
    fig_cid.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_cid, use_container_width=True)

with g4:
    st.subheader("🛏️ Proporção por Tipo de Leito")
    df_bed = df_filtered.groupby("bed_type")["admission_id"].count().reset_index()
    df_bed.columns = ["Tipo de Leito", "Total"]

    fig_bed = px.pie(
        df_bed,
        names="Tipo de Leito",
        values="Total",
        color="Tipo de Leito",
        color_discrete_map={"UTI": "#ff4b4b", "Semi-Intensiva": "#ffa600", "Enfermaria": "#00d2ff"},
        hole=0.45,
        template="plotly_dark"
    )
    fig_bed.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_bed, use_container_width=True)

# Tabela Detalhada com Paginação
with st.expander("📋 Visualizar Tabela Detalhada de Internações (Camada Gold)"):
    st.dataframe(
        df_filtered[[
            "admission_id", "patient_name", "hospital_name", "bed_type",
            "specialty", "cid_code", "cid_description", "admission_date",
            "discharge_date", "length_of_stay_days", "daily_cost", "total_treatment_cost"
        ]],
        use_container_width=True
    )
