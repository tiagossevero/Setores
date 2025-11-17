"""
================================================================================
ARGOS SETORES v5.0 - SISTEMA DE INTELIGÊNCIA TRIBUTÁRIA SETORIAL
================================================================================
Sistema Avançado de Análise Tributária com Machine Learning
Receita Estadual de Santa Catarina - NIAT

Desenvolvido por: tsevero
Data: 2025-11-17
Versão: 5.0 (Refatorado e Otimizado)

FUNCIONALIDADES PRINCIPAIS:
- Dashboard Executivo com KPIs Avançados
- Análises Setoriais Profundas com Visualizações 3D
- Machine Learning Expandido (Clustering, PCA, Previsões, Anomalias)
- Análises Temporais e Forecasting
- Sistema de Alertas Inteligente
- Relatórios Executivos Automatizados
- Exportação Multi-formato
- Visualizações Interativas Avançadas
================================================================================
"""

# ============================================================================
# IMPORTS E CONFIGURAÇÕES GLOBAIS
# ============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
from sqlalchemy import create_engine
import warnings
import ssl
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import io
from scipy import stats
from scipy.stats import chi2_contingency, pearsonr, spearmanr
import itertools

# Machine Learning
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier,
    IsolationForest,
    RandomForestRegressor
)
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    silhouette_score,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
    mean_squared_error,
    r2_score
)
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeClassifier

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÕES SSL E PÁGINA
# ============================================================================

# SSL Context (para conexão Impala)
try:
    createunverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = createunverified_https_context

# Configuração da Página Streamlit
st.set_page_config(
    page_title="ARGOS Setores v5.0 - Análise Tributária Avançada",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/receita-sc',
        'Report a bug': 'mailto:niat@sef.sc.gov.br',
        'About': """
        # ARGOS SETORES v5.0
        Sistema de Inteligência Tributária Setorial

        **Receita Estadual de Santa Catarina**

        Desenvolvido por NIAT - Núcleo de Inteligência e Análise Tributária
        """
    }
)

# ============================================================================
# ESTILOS CSS CUSTOMIZADOS
# ============================================================================

st.markdown("""
<style>
    /* ==== HEADER PRINCIPAL ==== */
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f77b4 0%, #ff7f0e 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 2rem;
    }

    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2c3e50;
        border-bottom: 3px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }

    /* ==== MÉTRICAS (KPIs) ==== */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border: 2px solid #2c3e50;
        border-radius: 15px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }

    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }

    div[data-testid="stMetric"] > label {
        font-weight: 700;
        color: #2c3e50;
        font-size: 1.1rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: 2.2rem;
        font-weight: bold;
        color: #1f77b4;
    }

    div[data-testid="stMetricDelta"] {
        font-size: 1rem;
        font-weight: 600;
    }

    /* ==== CARDS E CONTAINERS ==== */
    .info-card {
        background: white;
        border-left: 5px solid #1f77b4;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }

    .success-card {
        background: #d4edda;
        border-left: 5px solid #28a745;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }

    .warning-card {
        background: #fff3cd;
        border-left: 5px solid #ffc107;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }

    .danger-card {
        background: #f8d7da;
        border-left: 5px solid #dc3545;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }

    /* ==== TABELAS ==== */
    .dataframe {
        font-size: 0.95rem;
        border-collapse: collapse;
    }

    .dataframe thead tr th {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: bold;
        padding: 12px;
        text-align: left;
    }

    .dataframe tbody tr:nth-child(even) {
        background-color: #f8f9fa;
    }

    .dataframe tbody tr:hover {
        background-color: #e9ecef;
        cursor: pointer;
    }

    /* ==== SIDEBAR ==== */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }

    /* ==== BOTÕES ==== */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: bold;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        transition: all 0.3s;
    }

    .stButton>button:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }

    /* ==== SELECTBOX E INPUTS ==== */
    .stSelectbox > div > div {
        border-radius: 10px;
        border: 2px solid #e9ecef;
    }

    /* ==== TABS ==== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #f8f9fa;
        border-radius: 10px 10px 0 0;
        padding: 10px 20px;
        font-weight: 600;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }

    /* ==== EXPANDER ==== */
    .streamlit-expanderHeader {
        background-color: #f8f9fa;
        border-radius: 10px;
        font-weight: 600;
    }

    /* ==== PROGRESSO ==== */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }

    /* ==== BADGES ==== */
    .badge {
        display: inline-block;
        padding: 0.35em 0.65em;
        font-size: 0.85em;
        font-weight: 700;
        line-height: 1;
        text-align: center;
        white-space: nowrap;
        vertical-align: baseline;
        border-radius: 0.375rem;
    }

    .badge-success {
        color: #fff;
        background-color: #28a745;
    }

    .badge-warning {
        color: #212529;
        background-color: #ffc107;
    }

    .badge-danger {
        color: #fff;
        background-color: #dc3545;
    }

    .badge-info {
        color: #fff;
        background-color: #17a2b8;
    }

    /* ==== FOOTER ==== */
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: #2c3e50;
        color: white;
        text-align: center;
        padding: 10px;
        font-size: 0.85rem;
    }

    /* ==== SCROLLBAR ==== */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }

    ::-webkit-scrollbar-track {
        background: #f1f1f1;
    }

    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 5px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: #555;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# SISTEMA DE AUTENTICAÇÃO
# ============================================================================

SENHA = "tsevero852"  # ← ALTERE AQUI

def check_password():
    """Verificação de senha com interface moderna"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.markdown("""
        <div style='text-align: center; padding: 3rem;'>
            <h1 style='font-size: 3rem;'>🔐</h1>
            <h2>ARGOS SETORES v5.0</h2>
            <p style='color: #6c757d; font-size: 1.1rem;'>
                Sistema de Inteligência Tributária Setorial<br>
                Receita Estadual de Santa Catarina
            </p>
        </div>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input(
                "Digite a senha de acesso:",
                type="password",
                key="pwd_input",
                placeholder="••••••••"
            )

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("🔓 Entrar", use_container_width=True):
                    if senha_input == SENHA:
                        st.session_state.authenticated = True
                        st.session_state.login_time = datetime.now()
                        st.success("✅ Autenticação bem-sucedida!")
                        st.rerun()
                    else:
                        st.error("❌ Senha incorreta. Tente novamente.")

            with col_btn2:
                if st.button("ℹ️ Ajuda", use_container_width=True):
                    st.info("Entre em contato com o NIAT para obter credenciais de acesso.")

        st.markdown("""
        <div style='text-align: center; margin-top: 3rem; color: #6c757d; font-size: 0.9rem;'>
            <p>Desenvolvido por NIAT - Núcleo de Inteligência e Análise Tributária</p>
            <p>© 2025 Receita Estadual de Santa Catarina</p>
        </div>
        """, unsafe_allow_html=True)

        st.stop()

check_password()

# ============================================================================
# CONFIGURAÇÕES DE CONEXÃO
# ============================================================================

# Credenciais Impala
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'niat'

try:
    IMPALA_USER = st.secrets["impala_credentials"]["user"]
    IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]
except Exception as e:
    st.error(f"⚠️ Erro ao carregar credenciais: {e}")
    st.info("Configure o arquivo `.streamlit/secrets.toml` com as credenciais Impala.")
    st.stop()

# ============================================================================
# FUNÇÕES DE CONEXÃO E CARREGAMENTO DE DADOS
# ============================================================================

@st.cache_resource
def criar_engine():
    """Cria engine SQLAlchemy com cache permanente"""
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={
                'user': IMPALA_USER,
                'password': IMPALA_PASSWORD,
                'auth_mechanism': 'LDAP',
                'use_ssl': True
            }
        )
        return engine
    except Exception as e:
        st.error(f"❌ Erro ao criar engine: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_dados_completos() -> Dict[str, pd.DataFrame]:
    """
    Carrega todos os dados do sistema com cache de 1 hora

    Returns:
        Dict com DataFrames: {
            'benchmark_setorial', 'benchmark_porte', 'empresas',
            'empresa_vs_benchmark', 'evolucao_setor', 'evolucao_empresa',
            'anomalias', 'alertas', 'pagamentos'
        }
    """
    engine = criar_engine()
    if engine is None:
        st.error("❌ Falha na conexão com banco de dados.")
        return {}

    # Tabelas a carregar
    tabelas = {
        'benchmark_setorial': 'argos_benchmark_setorial',
        'benchmark_porte': 'argos_benchmark_setorial_porte',
        'empresas': 'argos_empresas',
        'empresa_vs_benchmark': 'argos_empresa_vs_benchmark',
        'evolucao_setor': 'argos_evolucao_temporal_setor',
        'evolucao_empresa': 'argos_evolucao_temporal_empresa',
        'anomalias': 'argos_anomalias_setoriais',
        'alertas': 'argos_alertas_empresas',
        'pagamentos': 'argos_pagamentos_empresa'
    }

    dados = {}
    logs = []

    try:
        with st.spinner("🔄 Carregando dados do banco... Aguarde..."):
            for key, table in tabelas.items():
                try:
                    query = f"SELECT * FROM {DATABASE}.{table}"
                    df = pd.read_sql(query, engine)

                    # Normalização de colunas
                    df.columns = [col.lower() for col in df.columns]

                    # Conversão de tipos
                    for col in df.select_dtypes(include=['object']).columns:
                        try:
                            df[col] = pd.to_numeric(df[col], errors='ignore')
                        except:
                            pass

                    dados[key] = df
                    logs.append(f"✅ {table}: {len(df):,} registros carregados")

                except Exception as e:
                    logs.append(f"❌ {table}: Erro - {str(e)[:100]}")
                    st.warning(f"⚠️ Falha ao carregar {table}: {e}")

        # Salvar logs no session_state
        st.session_state['logs_carregamento'] = logs
        st.session_state['data_load_time'] = datetime.now()

        return dados

    except Exception as e:
        st.error(f"❌ Erro geral no carregamento: {e}")
        return {}

# ============================================================================
# FUNÇÕES AUXILIARES E UTILITÁRIAS
# ============================================================================

def formatar_numero(valor: float, prefixo: str = "", sufixo: str = "", decimais: int = 0) -> str:
    """Formata números com separadores de milhar"""
    if pd.isna(valor):
        return "N/A"
    return f"{prefixo}{valor:,.{decimais}f}{sufixo}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_moeda(valor: float) -> str:
    """Formata valores monetários"""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_percentual(valor: float, decimais: int = 2) -> str:
    """Formata percentuais"""
    if pd.isna(valor):
        return "0,00%"
    return f"{valor * 100:,.{decimais}f}%".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_milhoes(valor: float, decimais: int = 1) -> str:
    """Formata valores em milhões"""
    if pd.isna(valor):
        return "R$ 0,0 Mi"
    milhoes = valor / 1_000_000
    return f"R$ {milhoes:,.{decimais}f} Mi".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_bilhoes(valor: float, decimais: int = 2) -> str:
    """Formata valores em bilhões"""
    if pd.isna(valor):
        return "R$ 0,00 Bi"
    bilhoes = valor / 1_000_000_000
    return f"R$ {bilhoes:,.{decimais}f} Bi".replace(",", "X").replace(".", ",").replace("X", ".")

def converter_periodo_str(periodo: int) -> str:
    """Converte período YYYYMM para 'Mês/YYYY'"""
    if pd.isna(periodo) or periodo == 0:
        return "N/A"
    try:
        periodo_str = str(int(periodo))
        ano = periodo_str[:4]
        mes = periodo_str[4:6]
        meses = {
            '01': 'Jan', '02': 'Fev', '03': 'Mar', '04': 'Abr',
            '05': 'Mai', '06': 'Jun', '07': 'Jul', '08': 'Ago',
            '09': 'Set', '10': 'Out', '11': 'Nov', '12': 'Dez'
        }
        return f"{meses.get(mes, mes)}/{ano}"
    except:
        return str(periodo)

def calcular_crescimento(valor_final: float, valor_inicial: float) -> float:
    """Calcula crescimento percentual"""
    if pd.isna(valor_inicial) or pd.isna(valor_final) or valor_inicial == 0:
        return 0
    return ((valor_final - valor_inicial) / abs(valor_inicial)) * 100

def badge_status(status: str) -> str:
    """Retorna HTML badge para status"""
    cores = {
        'MUITO_ABAIXO': ('danger', '🔴'),
        'ABAIXO': ('warning', '🟡'),
        'NORMAL': ('success', '🟢'),
        'ACIMA': ('info', '🔵'),
        'MUITO_ACIMA': ('info', '🟣'),
        'CRITICO': ('danger', '🔴'),
        'ALTO': ('warning', '🟠'),
        'MEDIO': ('warning', '🟡'),
        'BAIXO': ('info', '🔵'),
        'ALTA': ('danger', '🔴'),
        'MEDIA': ('warning', '🟡'),
        'BAIXA': ('success', '🟢'),
    }

    cor, emoji = cores.get(status, ('secondary', '⚪'))
    return f"<span class='badge badge-{cor}'>{emoji} {status}</span>"

def aplicar_gradiente_df(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    """Aplica gradiente de cores em DataFrame"""
    return df.style.background_gradient(
        subset=[coluna],
        cmap='RdYlGn' if 'aliq' in coluna.lower() else 'Blues'
    )

# ============================================================================
# FUNÇÕES DE VISUALIZAÇÃO AVANÇADAS
# ============================================================================

def criar_gauge_moderno(valor: float, titulo: str, minimo: float = 0, maximo: float = 100,
                        referencia: Optional[float] = None, unidade: str = "%") -> go.Figure:
    """
    Cria gauge moderno com indicadores visuais

    Args:
        valor: Valor a exibir
        titulo: Título do gauge
        minimo: Valor mínimo
        maximo: Valor máximo
        referencia: Valor de referência (opcional)
        unidade: Unidade de medida
    """
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=valor,
        title={'text': titulo, 'font': {'size': 20}},
        delta={'reference': referencia if referencia else valor, 'increasing': {'color': "green"}},
        number={'suffix': unidade, 'font': {'size': 32}},
        gauge={
            'axis': {'range': [minimo, maximo], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': "#1f77b4", 'thickness': 0.75},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [minimo, maximo * 0.33], 'color': '#d4edda'},
                {'range': [maximo * 0.33, maximo * 0.66], 'color': '#fff3cd'},
                {'range': [maximo * 0.66, maximo], 'color': '#f8d7da'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': referencia if referencia else maximo * 0.8
            }
        }
    ))

    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': "#2c3e50", 'family': "Arial"}
    )

    return fig

def criar_treemap(df: pd.DataFrame, path: List[str], values: str,
                  titulo: str = "TreeMap") -> go.Figure:
    """Cria TreeMap hierárquico"""
    fig = px.treemap(
        df,
        path=path,
        values=values,
        title=titulo,
        color=values,
        color_continuous_scale='Viridis',
        hover_data={values: ':,.0f'}
    )

    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=50, b=0)
    )

    return fig

def criar_sunburst(df: pd.DataFrame, path: List[str], values: str,
                   titulo: str = "Sunburst") -> go.Figure:
    """Cria gráfico Sunburst (rosca hierárquica)"""
    fig = px.sunburst(
        df,
        path=path,
        values=values,
        title=titulo,
        color=values,
        color_continuous_scale='RdYlGn',
        hover_data={values: ':,.0f'}
    )

    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=50, b=0)
    )

    return fig

def criar_scatter_3d(df: pd.DataFrame, x: str, y: str, z: str,
                     color: str = None, titulo: str = "Scatter 3D") -> go.Figure:
    """Cria scatter plot 3D"""
    fig = px.scatter_3d(
        df,
        x=x, y=y, z=z,
        color=color,
        title=titulo,
        height=700,
        opacity=0.7,
        size_max=15
    )

    fig.update_traces(marker=dict(size=5, line=dict(width=0.5, color='DarkSlateGrey')))
    fig.update_layout(
        scene=dict(
            xaxis_title=x,
            yaxis_title=y,
            zaxis_title=z
        ),
        margin=dict(l=0, r=0, b=0, t=50)
    )

    return fig

def criar_heatmap_correlacao(df: pd.DataFrame, colunas: List[str],
                              titulo: str = "Matriz de Correlação") -> go.Figure:
    """Cria heatmap de correlação"""
    corr_matrix = df[colunas].corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.columns,
        colorscale='RdBu',
        zmid=0,
        text=corr_matrix.values.round(2),
        texttemplate='%{text}',
        textfont={"size": 10},
        colorbar=dict(title="Correlação")
    ))

    fig.update_layout(
        title=titulo,
        height=600,
        xaxis={'side': 'bottom'},
        yaxis={'autorange': 'reversed'}
    )

    return fig

def criar_sankey(source: List, target: List, value: List, labels: List,
                 titulo: str = "Fluxo Sankey") -> go.Figure:
    """Cria diagrama Sankey"""
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color='#1f77b4'
        ),
        link=dict(
            source=source,
            target=target,
            value=value,
            color='rgba(31, 119, 180, 0.4)'
        )
    )])

    fig.update_layout(
        title=titulo,
        height=600,
        font=dict(size=12)
    )

    return fig

def criar_box_plot_comparativo(df: pd.DataFrame, x: str, y: str,
                                titulo: str = "Box Plot") -> go.Figure:
    """Cria box plot comparativo"""
    fig = px.box(
        df,
        x=x, y=y,
        title=titulo,
        color=x,
        notched=True,
        hover_data=df.columns
    )

    fig.update_layout(
        height=500,
        showlegend=False,
        xaxis_title=x,
        yaxis_title=y
    )

    return fig

def criar_violin_plot(df: pd.DataFrame, x: str, y: str,
                      titulo: str = "Violin Plot") -> go.Figure:
    """Cria violin plot para distribuição"""
    fig = px.violin(
        df,
        x=x, y=y,
        title=titulo,
        color=x,
        box=True,
        points='all',
        hover_data=df.columns
    )

    fig.update_layout(
        height=500,
        showlegend=False
    )

    return fig

def criar_waterfall(df: pd.DataFrame, medida: str, valor: str,
                    titulo: str = "Waterfall Chart") -> go.Figure:
    """Cria gráfico waterfall"""
    fig = go.Figure(go.Waterfall(
        name="", orientation="v",
        measure=df[medida],
        x=df.index,
        textposition="outside",
        text=df[valor].apply(lambda x: formatar_moeda(x)),
        y=df[valor],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
    ))

    fig.update_layout(
        title=titulo,
        height=500,
        showlegend=False
    )

    return fig

def criar_area_chart_empilhado(df: pd.DataFrame, x: str, y_cols: List[str],
                                titulo: str = "Área Empilhada") -> go.Figure:
    """Cria gráfico de área empilhado"""
    fig = go.Figure()

    for col in y_cols:
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode='lines',
            name=col,
            stackgroup='one',
            fillcolor=px.colors.qualitative.Plotly[y_cols.index(col) % len(px.colors.qualitative.Plotly)]
        ))

    fig.update_layout(
        title=titulo,
        height=500,
        hovermode='x unified',
        xaxis_title=x,
        yaxis_title="Valores"
    )

    return fig

# ============================================================================
# FUNÇÕES DE MACHINE LEARNING
# ============================================================================

def executar_clustering_kmeans(df: pd.DataFrame, features: List[str],
                                n_clusters: int = 5) -> Tuple[np.ndarray, float, pd.DataFrame]:
    """
    Executa K-Means clustering

    Returns:
        (labels, silhouette_score, df_pca)
    """
    # Preparar dados
    X = df[features].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)

    # Silhouette Score
    sil_score = silhouette_score(X_scaled, labels)

    # PCA para visualização 2D
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    df_pca = pd.DataFrame({
        'PC1': X_pca[:, 0],
        'PC2': X_pca[:, 1],
        'Cluster': labels
    })

    return labels, sil_score, df_pca

def encontrar_outliers_isolation_forest(df: pd.DataFrame, features: List[str],
                                         contamination: float = 0.1) -> np.ndarray:
    """Detecta outliers usando Isolation Forest"""
    X = df[features].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    iso_forest = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=100
    )
    outliers = iso_forest.fit_predict(X_scaled)

    return outliers  # -1 = outlier, 1 = normal

def executar_pca_analise(df: pd.DataFrame, features: List[str],
                         n_components: int = 3) -> Tuple[pd.DataFrame, PCA]:
    """Executa PCA e retorna componentes principais"""
    X = df[features].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    pca = PCA(n_components=n_components, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    df_pca = pd.DataFrame(
        X_pca,
        columns=[f'PC{i+1}' for i in range(n_components)]
    )

    return df_pca, pca

def prever_serie_temporal_linear(df: pd.DataFrame, coluna_valor: str,
                                  periodos_futuros: int = 6) -> pd.DataFrame:
    """Previsão simples com regressão linear"""
    df = df.copy().sort_index()

    # Preparar X (índice numérico), y (valores)
    X = np.arange(len(df)).reshape(-1, 1)
    y = df[coluna_valor].values

    # Treinar modelo
    model = LinearRegression()
    model.fit(X, y)

    # Prever futuros
    X_futuro = np.arange(len(df), len(df) + periodos_futuros).reshape(-1, 1)
    y_pred_futuro = model.predict(X_futuro)

    # Criar DataFrame de previsão
    df_previsao = pd.DataFrame({
        'periodo': range(len(df), len(df) + periodos_futuros),
        coluna_valor: y_pred_futuro,
        'tipo': 'Previsão'
    })

    return df_previsao

# ============================================================================
# PÁGINA PRINCIPAL - HEADER
# ============================================================================

def render_header():
    """Renderiza cabeçalho principal"""
    st.markdown('<h1 class="main-header">📊 ARGOS SETORES v5.0</h1>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([2, 3, 2])

    with col1:
        st.markdown("""
        <div class='info-card'>
            <b>👤 Usuário:</b> {}<br>
            <b>🕐 Login:</b> {}
        </div>
        """.format(
            IMPALA_USER,
            st.session_state.get('login_time', datetime.now()).strftime('%d/%m/%Y %H:%M')
        ), unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class='info-card' style='text-align: center;'>
            <b>Sistema de Inteligência Tributária Setorial</b><br>
            <small>Receita Estadual de Santa Catarina - NIAT</small>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        if 'data_load_time' in st.session_state:
            tempo_cache = datetime.now() - st.session_state['data_load_time']
            minutos = int(tempo_cache.total_seconds() / 60)
            st.markdown(f"""
            <div class='success-card'>
                <b>📊 Dados:</b> Carregados<br>
                <b>🕐 Cache:</b> {minutos} min atrás
            </div>
            """, unsafe_allow_html=True)

# ============================================================================
# PÁGINA 1: DASHBOARD EXECUTIVO
# ============================================================================

def render_dashboard_executivo(dados: Dict[str, pd.DataFrame]):
    """
    Dashboard executivo com visão consolidada e KPIs principais
    """
    st.markdown('<h2 class="sub-header">📈 Dashboard Executivo</h2>', unsafe_allow_html=True)

    # Verificar dados
    if not dados or 'benchmark_setorial' not in dados:
        st.error("❌ Dados não disponíveis")
        return

    df_bench = dados['benchmark_setorial']
    df_empresas = dados.get('empresas', pd.DataFrame())
    df_evolucao = dados.get('evolucao_setor', pd.DataFrame())
    df_alertas = dados.get('alertas', pd.DataFrame())
    df_pagamentos = dados.get('pagamentos', pd.DataFrame())

    # Filtro de período
    st.markdown("### 🔍 Filtros")
    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        periodos_disponiveis = sorted(df_bench['nu_per_ref'].unique(), reverse=True)
        periodo_selecionado = st.selectbox(
            "📅 Período de Referência",
            periodos_disponiveis,
            index=0,
            format_func=converter_periodo_str
        )

    with col_f2:
        secoes = ['Todas'] + sorted(df_bench['desc_secao'].dropna().unique().tolist())
        secao_filtro = st.selectbox("🏭 Seção CNAE", secoes)

    with col_f3:
        if not df_empresas.empty:
            portes = ['Todos'] + sorted(df_empresas['porte_empresa'].dropna().unique().tolist())
            porte_filtro = st.selectbox("📏 Porte Empresarial", portes)
        else:
            porte_filtro = 'Todos'

    # Aplicar filtros
    df_periodo = df_bench[df_bench['nu_per_ref'] == periodo_selecionado].copy()

    if secao_filtro != 'Todas':
        df_periodo = df_periodo[df_periodo['desc_secao'] == secao_filtro]

    if not df_empresas.empty:
        df_emp_periodo = df_empresas[df_empresas['nu_per_ref'] == periodo_selecionado].copy()
        if porte_filtro != 'Todos':
            df_emp_periodo = df_emp_periodo[df_emp_periodo['porte_empresa'] == porte_filtro]
    else:
        df_emp_periodo = pd.DataFrame()

    st.markdown("---")

    # ==== KPIs PRINCIPAIS ====
    st.markdown("### 📊 KPIs Principais")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        total_setores = len(df_periodo)
        st.metric(
            "🏭 Setores Monitorados",
            f"{total_setores:,}",
            help="Total de setores (CNAE Classe) com dados no período"
        )

    with col2:
        if not df_emp_periodo.empty:
            total_empresas = df_emp_periodo['nu_cnpj'].nunique()
            delta_empresas = calcular_crescimento(total_empresas, total_setores * 10)
        else:
            total_empresas = df_periodo['qtd_empresas_total'].sum()
            delta_empresas = 0

        st.metric(
            "🏢 Empresas",
            f"{int(total_empresas):,}",
            f"{delta_empresas:+.1f}%" if delta_empresas != 0 else None,
            help="Total de empresas declarantes no período"
        )

    with col3:
        faturamento_total = df_periodo['faturamento_total'].sum()
        st.metric(
            "💰 Faturamento Total",
            formatar_bilhoes(faturamento_total),
            help="Faturamento consolidado de todos os setores"
        )

    with col4:
        icms_total = df_periodo['icms_devido_total'].sum()
        aliq_efetiva_media = (icms_total / faturamento_total * 100) if faturamento_total > 0 else 0
        st.metric(
            "💵 ICMS Devido",
            formatar_bilhoes(icms_total),
            f"{aliq_efetiva_media:.2f}%",
            help="ICMS devido total e alíquota efetiva média"
        )

    with col5:
        aliq_mediana_geral = df_periodo['aliq_efetiva_mediana'].mean() * 100
        st.metric(
            "📊 Alíquota Mediana",
            f"{aliq_mediana_geral:.2f}%",
            help="Mediana geral das alíquotas efetivas dos setores"
        )

    st.markdown("---")

    # ==== GRÁFICOS PRINCIPAIS ====
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Visão Geral",
        "🏭 Análise Setorial",
        "📈 Evolução Temporal",
        "⚠️ Alertas e Riscos",
        "🎯 Insights Executivos"
    ])

    with tab1:
        st.markdown("#### Distribuição e Composição")

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            # Top 15 setores por faturamento
            df_top_fat = df_periodo.nlargest(15, 'faturamento_total')[
                ['desc_cnae_classe', 'faturamento_total']
            ].copy()
            df_top_fat['faturamento_bi'] = df_top_fat['faturamento_total'] / 1e9

            fig1 = px.bar(
                df_top_fat,
                x='faturamento_bi',
                y='desc_cnae_classe',
                orientation='h',
                title="🔝 Top 15 Setores por Faturamento",
                labels={'faturamento_bi': 'Faturamento (R$ Bi)', 'desc_cnae_classe': 'Setor'},
                color='faturamento_bi',
                color_continuous_scale='Blues'
            )
            fig1.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)

        with col_g2:
            # Distribuição de empresas por porte
            if not df_emp_periodo.empty and 'porte_empresa' in df_emp_periodo.columns:
                df_porte = df_emp_periodo.groupby('porte_empresa').agg({
                    'nu_cnpj': 'nunique',
                    'vl_faturamento': 'sum'
                }).reset_index()
                df_porte.columns = ['Porte', 'Qtd_Empresas', 'Faturamento_Total']

                fig2 = px.pie(
                    df_porte,
                    values='Qtd_Empresas',
                    names='Porte',
                    title="📊 Distribuição de Empresas por Porte",
                    hole=0.4,
                    color_discrete_sequence=px.colors.sequential.RdBu
                )
                fig2.update_traces(textposition='inside', textinfo='percent+label')
                fig2.update_layout(height=500)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Dados de porte não disponíveis para este período.")

        # TreeMap de setores
        st.markdown("#### 🌳 TreeMap - Faturamento por Seção e Classe CNAE")

        df_treemap = df_periodo[['desc_secao', 'desc_cnae_classe', 'faturamento_total']].copy()
        df_treemap = df_treemap[df_treemap['faturamento_total'] > 0]

        if len(df_treemap) > 0:
            fig_treemap = criar_treemap(
                df_treemap,
                path=['desc_secao', 'desc_cnae_classe'],
                values='faturamento_total',
                titulo="Hierarquia de Faturamento por Setor"
            )
            st.plotly_chart(fig_treemap, use_container_width=True)
        else:
            st.warning("Sem dados para TreeMap")

    with tab2:
        st.markdown("#### Análise Comparativa de Setores")

        # Scatter: Faturamento vs Alíquota
        df_scatter = df_periodo.copy()
        df_scatter['fat_bi'] = df_scatter['faturamento_total'] / 1e9
        df_scatter['aliq_pct'] = df_scatter['aliq_efetiva_mediana'] * 100

        fig_scatter = px.scatter(
            df_scatter,
            x='fat_bi',
            y='aliq_pct',
            size='qtd_empresas_total',
            color='desc_secao',
            hover_name='desc_cnae_classe',
            title="💰 Faturamento vs Alíquota Efetiva (tamanho = qtd empresas)",
            labels={'fat_bi': 'Faturamento (R$ Bi)', 'aliq_pct': 'Alíquota Efetiva (%)'},
            log_x=True
        )
        fig_scatter.update_layout(height=600)
        st.plotly_chart(fig_scatter, use_container_width=True)

        # Box Plot - Alíquotas por Seção
        col_b1, col_b2 = st.columns(2)

        with col_b1:
            df_box = df_periodo.copy()
            df_box['aliq_pct'] = df_box['aliq_efetiva_mediana'] * 100

            fig_box = criar_box_plot_comparativo(
                df_box,
                x='desc_secao',
                y='aliq_pct',
                titulo="📦 Distribuição de Alíquotas por Seção CNAE"
            )
            st.plotly_chart(fig_box, use_container_width=True)

        with col_b2:
            # Violin plot
            fig_violin = criar_violin_plot(
                df_box,
                x='desc_secao',
                y='aliq_pct',
                titulo="🎻 Densidade de Alíquotas por Seção"
            )
            st.plotly_chart(fig_violin, use_container_width=True)

    with tab3:
        st.markdown("#### Evolução Temporal dos Setores")

        if not df_evolucao.empty:
            # Top 10 setores por faturamento acumulado
            df_top_evolucao = df_evolucao.nlargest(10, 'faturamento_acumulado_8m')

            # Gráfico de área empilhada (precisaria de dados mensais por setor)
            st.info("Visualização de evolução temporal disponível na página 'Evolução Temporal'")

            # Gauge de volatilidade média
            col_g1, col_g2, col_g3 = st.columns(3)

            with col_g1:
                volatilidade_media = df_evolucao['coef_variacao_temporal'].mean() * 100
                fig_gauge1 = criar_gauge_moderno(
                    volatilidade_media,
                    "📊 Volatilidade Média",
                    minimo=0,
                    maximo=50,
                    unidade="%"
                )
                st.plotly_chart(fig_gauge1, use_container_width=True)

            with col_g2:
                setores_alta_vol = (df_evolucao['categoria_volatilidade_temporal'] == 'ALTA').sum()
                pct_alta_vol = (setores_alta_vol / len(df_evolucao) * 100) if len(df_evolucao) > 0 else 0
                fig_gauge2 = criar_gauge_moderno(
                    pct_alta_vol,
                    "⚠️ Setores Alta Volatilidade",
                    minimo=0,
                    maximo=100,
                    unidade="%"
                )
                st.plotly_chart(fig_gauge2, use_container_width=True)

            with col_g3:
                aliq_media_8m = df_evolucao['aliq_mediana_media_8m'].mean() * 100
                fig_gauge3 = criar_gauge_moderno(
                    aliq_media_8m,
                    "📈 Alíquota Média 8M",
                    minimo=0,
                    maximo=20,
                    unidade="%"
                )
                st.plotly_chart(fig_gauge3, use_container_width=True)

            # Scatter 3D: Faturamento vs Volatilidade vs Alíquota
            df_3d = df_top_evolucao.copy()
            df_3d['fat_bi'] = df_3d['faturamento_acumulado_8m'] / 1e9
            df_3d['vol_pct'] = df_3d['coef_variacao_temporal'] * 100
            df_3d['aliq_pct'] = df_3d['aliq_mediana_media_8m'] * 100

            fig_3d = criar_scatter_3d(
                df_3d,
                x='fat_bi',
                y='vol_pct',
                z='aliq_pct',
                color='categoria_volatilidade_temporal',
                titulo="🎲 Análise 3D: Faturamento x Volatilidade x Alíquota (Top 10)"
            )
            st.plotly_chart(fig_3d, use_container_width=True)
        else:
            st.warning("Dados de evolução temporal não disponíveis")

    with tab4:
        st.markdown("#### Alertas e Gestão de Riscos")

        if not df_alertas.empty:
            # KPIs de alertas
            col_a1, col_a2, col_a3, col_a4 = st.columns(4)

            with col_a1:
                total_alertas = len(df_alertas)
                st.metric("⚠️ Total Alertas", f"{total_alertas:,}")

            with col_a2:
                alertas_criticos = (df_alertas.get('severidade', pd.Series()) == 'CRITICO').sum()
                st.metric("🔴 Críticos", f"{alertas_criticos:,}")

            with col_a3:
                alertas_altos = (df_alertas.get('severidade', pd.Series()) == 'ALTO').sum()
                st.metric("🟠 Altos", f"{alertas_altos:,}")

            with col_a4:
                score_risco_medio = df_alertas.get('score_risco', pd.Series([0])).mean()
                st.metric("📊 Score Médio", f"{score_risco_medio:.1f}")

            # Distribuição de alertas
            col_d1, col_d2 = st.columns(2)

            with col_d1:
                if 'severidade' in df_alertas.columns:
                    df_sev = df_alertas['severidade'].value_counts().reset_index()
                    df_sev.columns = ['Severidade', 'Quantidade']

                    fig_sev = px.bar(
                        df_sev,
                        x='Severidade',
                        y='Quantidade',
                        title="📊 Alertas por Severidade",
                        color='Severidade',
                        color_discrete_map={
                            'CRITICO': '#dc3545',
                            'ALTO': '#fd7e14',
                            'MEDIO': '#ffc107',
                            'BAIXO': '#17a2b8'
                        }
                    )
                    fig_sev.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_sev, use_container_width=True)

            with col_d2:
                if 'tipo_alerta' in df_alertas.columns:
                    df_tipo = df_alertas['tipo_alerta'].value_counts().head(10).reset_index()
                    df_tipo.columns = ['Tipo', 'Quantidade']

                    fig_tipo = px.pie(
                        df_tipo,
                        values='Quantidade',
                        names='Tipo',
                        title="🎯 Top 10 Tipos de Alertas",
                        hole=0.3
                    )
                    fig_tipo.update_layout(height=400)
                    st.plotly_chart(fig_tipo, use_container_width=True)

            # Top empresas por risco
            if 'score_risco' in df_alertas.columns:
                st.markdown("#### 🎯 Top 20 Empresas por Score de Risco")
                df_top_risco = df_alertas.nlargest(20, 'score_risco')[
                    ['nu_cnpj', 'nm_razao_social', 'score_risco', 'severidade', 'tipo_alerta']
                ].copy()

                st.dataframe(
                    df_top_risco.style.background_gradient(subset=['score_risco'], cmap='Reds'),
                    use_container_width=True,
                    height=400
                )
        else:
            st.warning("Dados de alertas não disponíveis")

    with tab5:
        st.markdown("#### 🎯 Insights Executivos Automatizados")

        # Gerar insights automáticos
        insights = []

        # Insight 1: Setor com maior faturamento
        if not df_periodo.empty:
            setor_maior_fat = df_periodo.nlargest(1, 'faturamento_total').iloc[0]
            insights.append({
                'tipo': 'success',
                'titulo': '💰 Maior Faturamento',
                'mensagem': f"O setor **{setor_maior_fat['desc_cnae_classe']}** lidera com faturamento de **{formatar_bilhoes(setor_maior_fat['faturamento_total'])}**, representando **{(setor_maior_fat['faturamento_total']/faturamento_total*100):.1f}%** do total."
            })

        # Insight 2: Setor com menor alíquota
        if not df_periodo.empty:
            setor_menor_aliq = df_periodo.nsmallest(1, 'aliq_efetiva_mediana').iloc[0]
            insights.append({
                'tipo': 'warning',
                'titulo': '📉 Menor Alíquota',
                'mensagem': f"O setor **{setor_menor_aliq['desc_cnae_classe']}** possui alíquota efetiva mediana de apenas **{setor_menor_aliq['aliq_efetiva_mediana']*100:.2f}%**, muito abaixo da média geral de **{aliq_mediana_geral:.2f}%**."
            })

        # Insight 3: Volatilidade
        if not df_evolucao.empty:
            setores_volateis = df_evolucao[df_evolucao['categoria_volatilidade_temporal'] == 'ALTA']
            if len(setores_volateis) > 0:
                insights.append({
                    'tipo': 'danger',
                    'titulo': '⚠️ Alta Volatilidade',
                    'mensagem': f"**{len(setores_volateis)} setores ({len(setores_volateis)/len(df_evolucao)*100:.1f}%)** apresentam alta volatilidade temporal, indicando instabilidade tributária que requer monitoramento."
                })

        # Insight 4: Concentração
        if not df_periodo.empty:
            top5_fat = df_periodo.nlargest(5, 'faturamento_total')['faturamento_total'].sum()
            concentracao = (top5_fat / faturamento_total * 100) if faturamento_total > 0 else 0
            insights.append({
                'tipo': 'info',
                'titulo': '🎯 Concentração Setorial',
                'mensagem': f"Os **Top 5 setores** concentram **{concentracao:.1f}%** do faturamento total, indicando {'alta' if concentracao > 50 else 'moderada'} concentração econômica."
            })

        # Insight 5: Alertas críticos
        if not df_alertas.empty and 'severidade' in df_alertas.columns:
            criticos = (df_alertas['severidade'] == 'CRITICO').sum()
            if criticos > 0:
                insights.append({
                    'tipo': 'danger',
                    'titulo': '🚨 Alertas Críticos',
                    'mensagem': f"Existem **{criticos} alertas críticos** que demandam ação imediata da fiscalização para mitigar riscos tributários."
                })

        # Exibir insights
        for insight in insights:
            card_class = f"{insight['tipo']}-card"
            st.markdown(f"""
            <div class='{card_class}'>
                <h4>{insight['titulo']}</h4>
                <p>{insight['mensagem']}</p>
            </div>
            """, unsafe_allow_html=True)

        # Recomendações
        st.markdown("#### 💡 Recomendações Estratégicas")

        recomendacoes = [
            "🔍 **Fiscalização Prioritária**: Focar nos setores com alta volatilidade e baixa alíquota efetiva.",
            "📊 **Benchmarking**: Utilizar os setores de referência para estabelecer metas de arrecadação realistas.",
            "⚠️ **Monitoramento Contínuo**: Implementar alertas automáticos para desvios significativos do benchmark setorial.",
            "📈 **Análise Preditiva**: Utilizar modelos de ML para prever tendências e antecipar problemas tributários.",
            "🤝 **Orientação ao Contribuinte**: Oferecer suporte aos setores com alta volatilidade para regularização."
        ]

        for rec in recomendacoes:
            st.markdown(f"- {rec}")

# ============================================================================
# PÁGINA 2: ANÁLISE SETORIAL PROFUNDA
# ============================================================================

def render_analise_setorial(dados: Dict[str, pd.DataFrame]):
    """Análise detalhada por setor com benchmarks e comparações"""
    st.markdown('<h2 class="sub-header">🏭 Análise Setorial Profunda</h2>', unsafe_allow_html=True)

    if not dados or 'benchmark_setorial' not in dados:
        st.error("❌ Dados não disponíveis")
        return

    df_bench = dados['benchmark_setorial']
    df_bench_porte = dados.get('benchmark_porte', pd.DataFrame())
    df_evolucao = dados.get('evolucao_setor', pd.DataFrame())

    # Seleção de setor
    st.markdown("### 🔍 Seleção de Setor")

    col1, col2 = st.columns([3, 1])

    with col1:
        setores_disponiveis = sorted([s for s in df_bench['desc_cnae_classe'].dropna().unique() if pd.notna(s)])
        setor_selecionado = st.selectbox(
            "Selecione o setor (CNAE Classe):",
            setores_disponiveis,
            index=0 if len(setores_disponiveis) > 0 else None
        )

    with col2:
        periodos = sorted(df_bench['nu_per_ref'].unique(), reverse=True)
        periodo = st.selectbox("Período:", periodos, format_func=converter_periodo_str)

    if not setor_selecionado:
        st.warning("Selecione um setor")
        return

    # Filtrar dados do setor
    df_setor_periodo = df_bench[
        (df_bench['desc_cnae_classe'] == setor_selecionado) &
        (df_bench['nu_per_ref'] == periodo)
    ]

    if df_setor_periodo.empty:
        st.warning(f"Sem dados para o setor selecionado no período {converter_periodo_str(periodo)}")
        return

    setor_info = df_setor_periodo.iloc[0]

    st.markdown("---")

    # KPIs do Setor
    st.markdown(f"### 📊 Indicadores - {setor_selecionado}")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "🏢 Empresas",
            f"{int(setor_info.get('qtd_empresas_total', 0)):,}",
            help="Total de empresas no setor"
        )

    with col2:
        fat_total = setor_info.get('faturamento_total', 0)
        st.metric(
            "💰 Faturamento",
            formatar_milhoes(fat_total),
            help="Faturamento total do setor"
        )

    with col3:
        aliq_mediana = setor_info.get('aliq_efetiva_mediana', 0) * 100
        st.metric(
            "📊 Alíquota Mediana",
            f"{aliq_mediana:.2f}%",
            help="Alíquota efetiva mediana do setor"
        )

    with col4:
        aliq_cv = setor_info.get('aliq_coef_variacao', 0) * 100
        st.metric(
            "📉 Coef. Variação",
            f"{aliq_cv:.2f}%",
            help="Coeficiente de variação das alíquotas"
        )

    with col5:
        icms_total = setor_info.get('icms_devido_total', 0)
        st.metric(
            "💵 ICMS Devido",
            formatar_milhoes(icms_total),
            help="ICMS devido total"
        )

    st.markdown("---")

    # Tabs de análise
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Benchmark",
        "📈 Evolução Temporal",
        "🔍 Análise por Porte",
        "📉 Estatísticas Detalhadas"
    ])

    with tab1:
        st.markdown("#### Benchmark de Alíquota Efetiva")

        col_g1, col_g2 = st.columns([1, 2])

        with col_g1:
            # Gauge da alíquota
            aliq_p25 = setor_info.get('aliq_efetiva_p25', 0) * 100
            aliq_p75 = setor_info.get('aliq_efetiva_p75', 0) * 100

            fig_gauge = criar_gauge_moderno(
                aliq_mediana,
                "Alíquota Mediana",
                minimo=0,
                maximo=max(aliq_p75 * 1.5, 20),
                referencia=(aliq_p25 + aliq_p75) / 2
            )
            st.plotly_chart(fig_gauge, use_container_width=True)

        with col_g2:
            # Box plot simulado
            aliq_min = setor_info.get('aliq_efetiva_min', 0) * 100
            aliq_max = setor_info.get('aliq_efetiva_max', 0) * 100
            aliq_media = setor_info.get('aliq_efetiva_media', 0) * 100

            fig_range = go.Figure()

            fig_range.add_trace(go.Box(
                y=[aliq_min, aliq_p25, aliq_mediana, aliq_p75, aliq_max],
                name="Distribuição",
                marker_color='#1f77b4',
                boxmean='sd'
            ))

            fig_range.update_layout(
                title="Distribuição de Alíquotas no Setor",
                yaxis_title="Alíquota Efetiva (%)",
                height=400,
                showlegend=False
            )

            st.plotly_chart(fig_range, use_container_width=True)

    with tab2:
        st.markdown("#### Evolução Temporal (Últimos 8 Meses)")

        # Dados históricos do setor
        df_setor_hist = df_bench[df_bench['desc_cnae_classe'] == setor_selecionado].copy()
        df_setor_hist = df_setor_hist.sort_values('nu_per_ref')
        df_setor_hist['periodo_str'] = df_setor_hist['nu_per_ref'].apply(converter_periodo_str)
        df_setor_hist['aliq_mediana_pct'] = df_setor_hist['aliq_efetiva_mediana'] * 100
        df_setor_hist['aliq_p25_pct'] = df_setor_hist['aliq_efetiva_p25'] * 100
        df_setor_hist['aliq_p75_pct'] = df_setor_hist['aliq_efetiva_p75'] * 100

        if len(df_setor_hist) > 1:
            fig_evolucao = go.Figure()

            # Área P25-P75
            fig_evolucao.add_trace(go.Scatter(
                x=df_setor_hist['periodo_str'],
                y=df_setor_hist['aliq_p75_pct'],
                fill=None,
                mode='lines',
                line=dict(width=0),
                showlegend=False
            ))

            fig_evolucao.add_trace(go.Scatter(
                x=df_setor_hist['periodo_str'],
                y=df_setor_hist['aliq_p25_pct'],
                fill='tonexty',
                mode='lines',
                name='Intervalo P25-P75',
                fillcolor='rgba(31, 119, 180, 0.2)',
                line=dict(width=0)
            ))

            # Mediana
            fig_evolucao.add_trace(go.Scatter(
                x=df_setor_hist['periodo_str'],
                y=df_setor_hist['aliq_mediana_pct'],
                mode='lines+markers',
                name='Mediana',
                line=dict(color='#1f77b4', width=3)
            ))

            fig_evolucao.update_layout(
                title="Evolução da Alíquota Efetiva",
                xaxis_title="Período",
                yaxis_title="Alíquota (%)",
                height=400,
                hovermode='x unified'
            )

            st.plotly_chart(fig_evolucao, use_container_width=True)

            # Métricas de tendência
            if len(df_setor_hist) >= 2:
                primeiro_valor = df_setor_hist.iloc[0]['aliq_mediana_pct']
                ultimo_valor = df_setor_hist.iloc[-1]['aliq_mediana_pct']
                variacao = ultimo_valor - primeiro_valor
                variacao_pct = (variacao / primeiro_valor * 100) if primeiro_valor > 0 else 0

                col_t1, col_t2, col_t3 = st.columns(3)

                with col_t1:
                    st.metric("📊 Primeiro Período", f"{primeiro_valor:.2f}%")

                with col_t2:
                    st.metric("📊 Último Período", f"{ultimo_valor:.2f}%", f"{variacao_pct:+.1f}%")

                with col_t3:
                    tendencia = "📈 Crescente" if variacao > 0 else "📉 Decrescente" if variacao < 0 else "➡️ Estável"
                    st.metric("🎯 Tendência", tendencia)
        else:
            st.info("Dados históricos insuficientes para análise temporal")

    with tab3:
        st.markdown("#### Análise por Porte Empresarial")

        if not df_bench_porte.empty:
            df_porte_setor = df_bench_porte[
                (df_bench_porte['desc_cnae_classe'] == setor_selecionado) &
                (df_bench_porte['nu_per_ref'] == periodo)
            ].copy()

            if not df_porte_setor.empty:
                df_porte_setor['aliq_media_pct'] = df_porte_setor['aliq_efetiva_media'] * 100

                fig_porte = px.bar(
                    df_porte_setor,
                    x='porte_empresa',
                    y='aliq_media_pct',
                    title="Alíquota Média por Porte",
                    labels={'porte_empresa': 'Porte', 'aliq_media_pct': 'Alíquota Média (%)'},
                    color='aliq_media_pct',
                    color_continuous_scale='Blues',
                    text='aliq_media_pct'
                )
                fig_porte.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
                fig_porte.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_porte, use_container_width=True)

                # Tabela detalhada
                st.markdown("##### Detalhamento por Porte")
                df_porte_display = df_porte_setor[[
                    'porte_empresa', 'qtd_empresas', 'faturamento_total',
                    'aliq_media_pct', 'aliq_efetiva_desvio'
                ]].copy()
                df_porte_display.columns = ['Porte', 'Qtd Empresas', 'Faturamento', 'Alíq Média (%)', 'Desvio Padrão']
                st.dataframe(df_porte_display, use_container_width=True)
            else:
                st.info("Sem dados por porte para este setor")
        else:
            st.warning("Dados de benchmark por porte não disponíveis")

    with tab4:
        st.markdown("#### Estatísticas Detalhadas do Setor")

        col_s1, col_s2 = st.columns(2)

        with col_s1:
            st.markdown("**Métricas de Alíquota:**")
            metricas_aliq = {
                "Mínima": f"{setor_info.get('aliq_efetiva_min', 0) * 100:.2f}%",
                "P25": f"{aliq_p25:.2f}%",
                "Mediana": f"{aliq_mediana:.2f}%",
                "Média": f"{setor_info.get('aliq_efetiva_media', 0) * 100:.2f}%",
                "P75": f"{aliq_p75:.2f}%",
                "Máxima": f"{setor_info.get('aliq_efetiva_max', 0) * 100:.2f}%",
                "Desvio Padrão": f"{setor_info.get('aliq_efetiva_desvio', 0) * 100:.2f}%",
                "Coef. Variação": f"{aliq_cv:.2f}%"
            }

            for metrica, valor in metricas_aliq.items():
                st.write(f"- **{metrica}**: {valor}")

        with col_s2:
            st.markdown("**Métricas Financeiras:**")
            metricas_fin = {
                "Empresas Total": f"{int(setor_info.get('qtd_empresas_total', 0)):,}",
                "Empresas Ativas": f"{int(setor_info.get('qtd_empresas_ativas', 0)):,}",
                "Faturamento Total": formatar_milhoes(setor_info.get('faturamento_total', 0)),
                "Faturamento Médio": formatar_moeda(setor_info.get('faturamento_medio', 0)),
                "ICMS Devido": formatar_milhoes(setor_info.get('icms_devido_total', 0)),
                "ICMS a Recolher": formatar_milhoes(setor_info.get('icms_recolher_total', 0)),
                "Seção CNAE": setor_info.get('desc_secao', 'N/A'),
                "Código Seção": setor_info.get('secao', 'N/A')
            }

            for metrica, valor in metricas_fin.items():
                st.write(f"- **{metrica}**: {valor}")

# ============================================================================
# PÁGINA 3: ANÁLISE EMPRESARIAL DETALHADA
# ============================================================================

def render_analise_empresarial(dados: Dict[str, pd.DataFrame]):
    """Análise detalhada de empresas individuais"""
    st.markdown('<h2 class="sub-header">🏢 Análise Empresarial Detalhada</h2>', unsafe_allow_html=True)

    if 'empresa_vs_benchmark' not in dados:
        st.error("❌ Dados não disponíveis")
        return

    df_emp = dados['empresa_vs_benchmark']
    df_evolucao_emp = dados.get('evolucao_empresa', pd.DataFrame())

    # Busca de empresa
    st.markdown("### 🔍 Busca de Empresa")

    col1, col2 = st.columns([2, 1])

    with col1:
        cnpj_busca = st.text_input(
            "Digite o CNPJ (somente números):",
            max_chars=14,
            placeholder="00000000000000"
        )

    with col2:
        periodos = sorted(df_emp['nu_per_ref'].unique(), reverse=True)
        periodo = st.selectbox("Período:", periodos, format_func=converter_periodo_str)

    if cnpj_busca and len(cnpj_busca) >= 8:
        # Buscar empresa
        df_empresa = df_emp[
            (df_emp['nu_cnpj'].astype(str).str.contains(cnpj_busca)) &
            (df_emp['nu_per_ref'] == periodo)
        ]

        if df_empresa.empty:
            st.warning(f"❌ Nenhuma empresa encontrada com CNPJ contendo '{cnpj_busca}'")
            return

        if len(df_empresa) > 1:
            st.info(f"🔍 Encontradas {len(df_empresa)} empresas. Selecione uma:")
            empresa_selecionada = st.selectbox(
                "Selecione:",
                df_empresa.index,
                format_func=lambda x: f"{df_empresa.loc[x, 'nm_razao_social']} - CNPJ: {df_empresa.loc[x, 'nu_cnpj']}"
            )
            emp_info = df_empresa.loc[empresa_selecionada]
        else:
            emp_info = df_empresa.iloc[0]

        st.markdown("---")

        # Header da empresa
        st.markdown(f"### 🏢 {emp_info['nm_razao_social']}")

        col_h1, col_h2, col_h3, col_h4 = st.columns(4)

        with col_h1:
            st.write(f"**CNPJ:** {emp_info['nu_cnpj']}")

        with col_h2:
            st.write(f"**Setor:** {emp_info.get('desc_cnae_classe', 'N/A')}")

        with col_h3:
            st.write(f"**Porte:** {emp_info.get('porte_empresa', 'N/A')}")

        with col_h4:
            status = emp_info.get('status_vs_setor', 'NORMAL')
            st.markdown(f"**Status:** {badge_status(status)}", unsafe_allow_html=True)

        st.markdown("---")

        # KPIs da empresa
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "💰 Faturamento",
                formatar_moeda(emp_info.get('vl_faturamento', 0))
            )

        with col2:
            aliq_empresa = emp_info.get('aliq_efetiva_empresa', 0) * 100
            st.metric(
                "📊 Alíquota Efetiva",
                f"{aliq_empresa:.2f}%"
            )

        with col3:
            st.metric(
                "💵 ICMS Devido",
                formatar_moeda(emp_info.get('icms_devido', 0))
            )

        with col4:
            indice_setor = emp_info.get('indice_vs_mediana_setor', 1.0)
            diferenca = (indice_setor - 1) * 100
            st.metric(
                "📈 vs Setor",
                f"{indice_setor:.2f}x",
                f"{diferenca:+.1f}%"
            )

        with col5:
            divergencia = emp_info.get('flag_divergencia_pagamento', 0)
            divergencia_status = "❌ SIM" if divergencia == 1 else "✅ NÃO"
            st.metric(
                "⚠️ Divergência Pgto",
                divergencia_status
            )

        st.markdown("---")

        # Comparação com benchmark
        tab1, tab2, tab3 = st.tabs([
            "📊 Comparação vs Setor",
            "📈 Evolução Temporal",
            "💰 Análise de Pagamentos"
        ])

        with tab1:
            st.markdown("#### Comparação com Benchmark Setorial")

            col_c1, col_c2 = st.columns(2)

            with col_c1:
                # Gauge comparativo
                aliq_setor_mediana = emp_info.get('aliq_setor_mediana', 0) * 100

                fig_comp = go.Figure()

                fig_comp.add_trace(go.Bar(
                    x=['Empresa', 'Setor (Mediana)'],
                    y=[aliq_empresa, aliq_setor_mediana],
                    marker_color=['#1f77b4', '#ff7f0e'],
                    text=[f'{aliq_empresa:.2f}%', f'{aliq_setor_mediana:.2f}%'],
                    textposition='auto'
                ))

                fig_comp.update_layout(
                    title="Alíquota: Empresa vs Setor",
                    yaxis_title="Alíquota Efetiva (%)",
                    height=400,
                    showlegend=False
                )

                st.plotly_chart(fig_comp, use_container_width=True)

            with col_c2:
                # Estatísticas do setor
                st.markdown("**Benchmark do Setor:**")

                bench_metrics = {
                    "Alíquota Média": f"{emp_info.get('aliq_setor_media', 0) * 100:.2f}%",
                    "Alíquota Mediana": f"{aliq_setor_mediana:.2f}%",
                    "P25": f"{emp_info.get('aliq_setor_p25', 0) * 100:.2f}%",
                    "P75": f"{emp_info.get('aliq_setor_p75', 0) * 100:.2f}%",
                    "Desvio Padrão": f"{emp_info.get('aliq_setor_desvio', 0) * 100:.2f}%",
                    "Empresas no Setor": f"{int(emp_info.get('empresas_no_setor', 0)):,}"
                }

                for metrica, valor in bench_metrics.items():
                    st.write(f"- **{metrica}**: {valor}")

        with tab2:
            st.markdown("#### Evolução Temporal da Empresa (8 Meses)")

            if not df_evolucao_emp.empty:
                df_emp_hist = df_evolucao_emp[
                    df_evolucao_emp['nu_cnpj'] == emp_info['nu_cnpj']
                ]

                if not df_emp_hist.empty:
                    emp_hist_info = df_emp_hist.iloc[0]

                    col_e1, col_e2, col_e3, col_e4 = st.columns(4)

                    with col_e1:
                        st.metric(
                            "📅 Meses c/ Declaração",
                            int(emp_hist_info.get('meses_com_declaracao', 0))
                        )

                    with col_e2:
                        st.metric(
                            "📊 Alíquota Média 8M",
                            f"{emp_hist_info.get('aliq_media_8m', 0) * 100:.2f}%"
                        )

                    with col_e3:
                        volatilidade = emp_hist_info.get('categoria_volatilidade', 'N/A')
                        st.markdown(f"**Volatilidade:** {badge_status(volatilidade)}", unsafe_allow_html=True)

                    with col_e4:
                        st.metric(
                            "💰 Faturamento 8M",
                            formatar_milhoes(emp_hist_info.get('faturamento_total_8m', 0))
                        )

                    st.info("📊 Gráfico de evolução mensal disponível na página 'Evolução Temporal'")
                else:
                    st.warning("Dados históricos insuficientes para esta empresa")
            else:
                st.warning("Dados de evolução temporal não disponíveis")

        with tab3:
            st.markdown("#### Análise de Pagamentos")

            col_p1, col_p2 = st.columns(2)

            with col_p1:
                st.metric(
                    "💵 ICMS a Recolher",
                    formatar_moeda(emp_info.get('icms_recolher', 0))
                )

            with col_p2:
                valor_pago = emp_info.get('valor_total_pago', 0)
                st.metric(
                    "💳 Valor Pago",
                    formatar_moeda(valor_pago)
                )

            # Divergência
            icms_recolher = emp_info.get('icms_recolher', 0)
            if icms_recolher > 0 and valor_pago > 0:
                diferenca = icms_recolher - valor_pago
                pct_diferenca = (diferenca / icms_recolher) * 100

                if abs(pct_diferenca) > 10:
                    tipo_card = 'danger-card' if pct_diferenca > 0 else 'warning-card'
                    st.markdown(f"""
                    <div class='{tipo_card}'>
                        <h4>⚠️ Divergência Detectada</h4>
                        <p>Diferença: <strong>{formatar_moeda(abs(diferenca))}</strong> ({pct_diferenca:+.1f}%)</p>
                        <p>{'Valor pago MENOR que o devido' if diferenca > 0 else 'Valor pago MAIOR que o devido'}</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='success-card'>
                        <h4>✅ Pagamento Regular</h4>
                        <p>Diferença dentro do limite aceitável ({pct_diferenca:+.1f}%)</p>
                    </div>
                    """, unsafe_allow_html=True)

    else:
        st.info("👆 Digite um CNPJ para iniciar a busca")

# ============================================================================
# CARREGAMENTO INICIAL E MENU PRINCIPAL
# ============================================================================

def main():
    """Função principal do aplicativo"""

    # Renderizar header
    render_header()

    # Carregar dados
    with st.spinner("📊 Carregando dados do sistema..."):
        dados = carregar_dados_completos()

    if not dados:
        st.error("❌ Falha ao carregar dados do banco. Verifique a conexão.")
        st.stop()

    # Sidebar - Menu de Navegação
    st.sidebar.title("🧭 Navegação")
    st.sidebar.markdown("---")

    menu_opcoes = [
        "📈 Dashboard Executivo",
        "🏭 Análise Setorial Profunda",
        "🏢 Análise Empresarial Detalhada",
        "⏱️ Evolução Temporal e Forecasting",
        "📉 Análise de Volatilidade",
        "⚠️ Alertas e Anomalias",
        "💰 Análise de Pagamentos",
        "🤖 Machine Learning Avançado",
        "📊 Análises Estatísticas",
        "🎯 Correlações e Regressões",
        "📋 Relatórios Executivos",
        "⚙️ Configurações e Logs"
    ]

    pagina_selecionada = st.sidebar.radio(
        "Selecione a análise:",
        menu_opcoes,
        index=0
    )

    st.sidebar.markdown("---")

    # Info do sistema
    with st.sidebar.expander("ℹ️ Informações do Sistema"):
        st.write(f"**Versão:** 5.0")
        st.write(f"**Banco:** {DATABASE}")
        st.write(f"**Usuário:** {IMPALA_USER}")
        if 'data_load_time' in st.session_state:
            st.write(f"**Última carga:** {st.session_state['data_load_time'].strftime('%H:%M:%S')}")

    # Logs de carregamento
    if 'logs_carregamento' in st.session_state:
        with st.sidebar.expander("📊 Logs de Carregamento"):
            for log in st.session_state['logs_carregamento']:
                st.write(log)

    # Botão de atualização
    if st.sidebar.button("🔄 Atualizar Dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # Roteamento de páginas
    if pagina_selecionada == "📈 Dashboard Executivo":
        render_dashboard_executivo(dados)

    elif pagina_selecionada == "🏭 Análise Setorial Profunda":
        render_analise_setorial(dados)

    elif pagina_selecionada == "🏢 Análise Empresarial Detalhada":
        render_analise_empresarial(dados)

    elif pagina_selecionada == "⏱️ Evolução Temporal e Forecasting":
        st.markdown('<h2 class="sub-header">⏱️ Evolução Temporal e Forecasting</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com análises temporais e previsões - Em desenvolvimento")
        st.write("Esta página incluirá:")
        st.write("- Séries temporais de alíquotas por setor")
        st.write("- Previsões com modelos de regressão")
        st.write("- Análise de sazonalidade")
        st.write("- Tendências de longo prazo")

    elif pagina_selecionada == "📉 Análise de Volatilidade":
        st.markdown('<h2 class="sub-header">📉 Análise de Volatilidade</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com análises de volatilidade - Em desenvolvimento")

    elif pagina_selecionada == "⚠️ Alertas e Anomalias":
        st.markdown('<h2 class="sub-header">⚠️ Alertas e Anomalias</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com sistema de alertas - Em desenvolvimento")

    elif pagina_selecionada == "💰 Análise de Pagamentos":
        st.markdown('<h2 class="sub-header">💰 Análise de Pagamentos</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com análises de pagamentos - Em desenvolvimento")

    elif pagina_selecionada == "🤖 Machine Learning Avançado":
        st.markdown('<h2 class="sub-header">🤖 Machine Learning Avançado</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com modelos de ML - Em desenvolvimento")

    elif pagina_selecionada == "📊 Análises Estatísticas":
        st.markdown('<h2 class="sub-header">📊 Análises Estatísticas</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com análises estatísticas - Em desenvolvimento")

    elif pagina_selecionada == "🎯 Correlações e Regressões":
        st.markdown('<h2 class="sub-header">🎯 Correlações e Regressões</h2>', unsafe_allow_html=True)
        st.info("🚧 Página com análises de correlação - Em desenvolvimento")

    elif pagina_selecionada == "📋 Relatórios Executivos":
        st.markdown('<h2 class="sub-header">📋 Relatórios Executivos</h2>', unsafe_allow_html=True)
        st.info("🚧 Página de relatórios - Em desenvolvimento")

    elif pagina_selecionada == "⚙️ Configurações e Logs":
        st.markdown('<h2 class="sub-header">⚙️ Configurações e Logs</h2>', unsafe_allow_html=True)

        tab1, tab2, tab3 = st.tabs(["📊 Informações do Sistema", "📋 Logs", "⚙️ Configurações"])

        with tab1:
            st.markdown("### 📊 Informações do Sistema")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("""
                **Sistema:**
                - Versão: 5.0
                - Nome: ARGOS SETORES
                - Desenvolvedor: tsevero
                - Organização: NIAT - Receita SC

                **Banco de Dados:**
                - Host: bdaworkernode02.sef.sc.gov.br
                - Porta: 21050
                - Database: niat
                - Usuário: {user}
                """.format(user=IMPALA_USER))

            with col2:
                st.markdown("""
                **Sessão:**
                - Login: {login}
                - Última carga: {carga}

                **Cache:**
                - TTL: 3600 segundos (1 hora)
                - Status: Ativo
                """.format(
                    login=st.session_state.get('login_time', datetime.now()).strftime('%d/%m/%Y %H:%M:%S'),
                    carga=st.session_state.get('data_load_time', datetime.now()).strftime('%d/%m/%Y %H:%M:%S')
                ))

        with tab2:
            st.markdown("### 📋 Logs de Carregamento")

            if 'logs_carregamento' in st.session_state:
                for log in st.session_state['logs_carregamento']:
                    st.text(log)
            else:
                st.info("Nenhum log disponível")

        with tab3:
            st.markdown("### ⚙️ Configurações")

            st.warning("🔒 Área restrita para administradores")

            if st.button("🗑️ Limpar Cache"):
                st.cache_data.clear()
                st.success("✅ Cache limpo com sucesso!")
                st.info("Clique em 'Atualizar Dados' na sidebar para recarregar")

    else:
        st.warning(f"Página '{pagina_selecionada}' não implementada.")
        st.info("Use o menu lateral para navegar pelas páginas disponíveis!")

# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    main()
