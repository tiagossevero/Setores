# ============================================================
# COLE ESTE CÓDIGO NO INÍCIO DE CADA ARQUIVO .PY
# ============================================================

import streamlit as st
import hashlib

# DEFINA A SENHA AQUI
SENHA = "tsevero852"  # ← TROQUE para cada projeto

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>🔐 Acesso Restrito</h1></div>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta")
        st.stop()

check_password()

"""
Sistema SETORES - Análise Tributária Setorial v4.0
Receita Estadual de Santa Catarina
Dashboard interativo para análise de comportamento tributário por setor
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import warnings
import ssl

# =============================================================================
# 1. CONFIGURAÇÕES INICIAIS
# =============================================================================

# Hack SSL
try:
    createunverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = createunverified_https_context

warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="ARGOS Setores - Análise Tributária",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }

        /* ESTILO DOS KPIs - BORDA PRETA */
    div[data-testid="stMetric"] {
        background-color: #ffffff;        /* Fundo branco */
        border: 2px solid #2c3e50;        /* Borda: 2px de largura, sólida, cor cinza-escuro */
        border-radius: 10px;              /* Cantos arredondados (10 pixels de raio) */
        padding: 15px;                    /* Espaçamento interno (15px em todos os lados) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Sombra: horizontal=0, vertical=2px, blur=4px, cor preta 10% opacidade */
    }
    
    /* Título do métrica */
    div[data-testid="stMetric"] > label {
        font-weight: 600;                 /* Negrito médio */
        color: #2c3e50;                   /* Cor do texto */
    }
    
    /* Valor do métrica */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;                /* Tamanho da fonte do valor */
        font-weight: bold;                /* Negrito */
        color: #1f77b4;                   /* Cor azul */
    }
    
    /* Delta (variação) */
    div[data-testid="stMetricDelta"] {
        font-size: 0.9rem;                /* Tamanho menor para delta */
        
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .alert-critico {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #d32f2f;
    }
    .alert-alto {
        background-color: #fff3e0;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #f57c00;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 2. CREDENCIAIS E CONEXÃO
# =============================================================================

IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'niat'

try:
    IMPALA_USER = st.secrets["impala_credentials"]["user"]
    IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]
except:
    st.error("⚠️ Credenciais não configuradas. Configure o arquivo secrets.toml")
    st.stop()

# =============================================================================
# 3. FUNÇÕES DE CARREGAMENTO DE DADOS
# =============================================================================

@st.cache_data(show_spinner="Conectando ao Impala...", ttl=3600)
def carregar_dados_setores():
    """Carrega todos os dados do Sistema ARGOS Setores."""
    dados = {}
    
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
        connection = engine.connect()
        st.session_state['conexao_status'] = "✅ Conexão estabelecida!"
        connection.close()
    except Exception as e:
        st.session_state['conexao_status'] = f"❌ Falha na conexão: {str(e)[:100]}"
        return {}
    
    # Tabelas principais
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
    
    logs_carregamento = []
    logs_carregamento.append("📊 Carregando dados:")
    
    for key, table in tabelas.items():
        try:
            logs_carregamento.append(f"⏳ {table}...")
            query = f"SELECT * FROM {DATABASE}.{table}"
            df = pd.read_sql(query, engine)
            df.columns = [col.lower() for col in df.columns]
            
            # Converter Decimals para float
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
            
            dados[key] = df
            logs_carregamento.append(f"✅ {len(df):,} linhas")
        except Exception as e:
            logs_carregamento.append(f"❌ Erro: {str(e)[:50]}")
            dados[key] = pd.DataFrame()
    
    st.session_state['logs_carregamento'] = logs_carregamento
    return dados

# =============================================================================
# 4. FUNÇÕES AUXILIARES
# =============================================================================

def formatar_moeda(valor):
    """Formata valor em moeda brasileira."""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_percentual(valor):
    """Formata valor como percentual."""
    if pd.isna(valor):
        return "0,00%"
    return f"{valor*100:.2f}%".replace(".", ",")

def criar_grafico_evolucao(df, x_col, y_col, color_col=None, title=""):
    """Cria gráfico de linha temporal."""
    fig = px.line(
        df, x=x_col, y=y_col, color=color_col,
        title=title,
        labels={x_col: 'Período', y_col: 'Valor'}
    )
    fig.update_layout(hovermode='x unified', height=400)
    return fig

def criar_mapa_calor(df, index_col, columns_col, values_col, title=""):
    """Cria mapa de calor."""
    pivot = df.pivot_table(
        index=index_col, 
        columns=columns_col, 
        values=values_col
    )
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='RdYlGn_r',
        text=pivot.values,
        texttemplate='%{text:.2f}',
        textfont={"size": 10}
    ))
    fig.update_layout(title=title, height=600)
    return fig

def criar_gauge_aliquota(aliq_mediana, aliq_p25, aliq_p75):
    """Cria gráfico de velocímetro para alíquota."""
    aliq_mediana_pct = aliq_mediana * 100
    aliq_p25_pct = aliq_p25 * 100
    aliq_p75_pct = aliq_p75 * 100
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = aliq_mediana_pct,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Alíquota Mediana (%)"},
        delta = {'reference': (aliq_p25_pct + aliq_p75_pct) / 2},
        gauge = {
            'axis': {'range': [None, max(aliq_p75_pct * 1.2, 20)]},
            'bar': {'color': "#1f77b4"},
            'steps': [
                {'range': [0, aliq_p25_pct], 'color': "#e8f5e9"},
                {'range': [aliq_p25_pct, aliq_p75_pct], 'color': "#c8e6c9"},
                {'range': [aliq_p75_pct, max(aliq_p75_pct * 1.2, 20)], 'color': "#fff3e0"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': aliq_p75_pct
            }
        }
    ))
    
    fig.add_annotation(
        x=0.5, y=0.15,
        text=f"P25: {aliq_p25_pct:.2f}%",
        showarrow=False,
        font=dict(size=12)
    )
    
    fig.add_annotation(
        x=0.5, y=0.05,
        text=f"P75: {aliq_p75_pct:.2f}%",
        showarrow=False,
        font=dict(size=12)
    )
    
    fig.update_layout(height=300)
    return fig

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def main():
    # Header
    st.markdown('<p class="main-header">📊 ARGOS SETORES - Análise Tributária Setorial</p>', 
                unsafe_allow_html=True)
    st.markdown("**Receita Estadual de Santa Catarina** | Sistema de Análise v4.0")
    st.markdown("---")
    
    # Carregar dados
    with st.spinner("Carregando dados do sistema..."):
        dados = carregar_dados_setores()
    
    if not dados or all(df.empty for df in dados.values()):
        st.error("❌ Não foi possível carregar os dados. Verifique a conexão.")
        return
    
    # Sidebar - NavegaÃ§Ã£o
    st.sidebar.title("🔐 Navegação")
    secao = st.sidebar.radio(
        "Escolha a análise:",
        [
            "📈 Visão Geral",
            "🏭 Análise Setorial",
            "🏢 Análise Empresarial",
            "⚠️ Alertas e Anomalias",
            "⏱️ Evolução Temporal",
            "📉 Análise de Volatilidade",
            "💰 Análise de Pagamentos",
            "🤖 Machine Learning",
            "📊 Análises Avançadas",
            "📋 Relatórios"
        ]
    )
    
    # Logs de carregamento abaixo da navegação
    st.sidebar.markdown("---")
    if 'conexao_status' in st.session_state:
        st.sidebar.info(st.session_state['conexao_status'])
    
    if 'logs_carregamento' in st.session_state:
        with st.sidebar.expander("📊 Logs de Carregamento"):
            for log in st.session_state['logs_carregamento']:
                st.sidebar.write(log)
    
    # Renderizar seção selecionada
    # Obter período mais recente para usar como padrão
    periodos = sorted(dados['benchmark_setorial']['nu_per_ref'].unique()) if not dados['benchmark_setorial'].empty else []
    periodo_padrao = periodos[-1] if periodos else None
    
    if secao == "📈 Visão Geral":
        render_visao_geral(dados, periodo_padrao)
    elif secao == "🏭 Análise Setorial":
        render_analise_setorial(dados, periodo_padrao)
    elif secao == "🏢 Análise Empresarial":
        render_analise_empresarial(dados, periodo_padrao)
    elif secao == "⚠️ Alertas e Anomalias":
        render_alertas_anomalias(dados, periodo_padrao)
    elif secao == "⏱️ Evolução Temporal":
        render_evolucao_temporal(dados)
    elif secao == "📉 Análise de Volatilidade":
        render_analise_volatilidade(dados, periodo_padrao)
    elif secao == "💰 Análise de Pagamentos":
        render_analise_pagamentos(dados, periodo_padrao)
    elif secao == "🤖 Machine Learning":
        render_machine_learning(dados, periodo_padrao)
    elif secao == "📊 Análises Avançadas":
        render_analises_avancadas(dados, periodo_padrao)
    elif secao == "📋 Relatórios":
        render_relatorios(dados, periodo_padrao)

# =============================================================================
# 6. SEÇÃO: VISÃO GERAL
# =============================================================================

def render_visao_geral(dados, periodo):
    st.header("📈 Visão Geral do Sistema")
    
    # Filtro de período
    periodos = sorted(dados['benchmark_setorial']['nu_per_ref'].unique()) if not dados['benchmark_setorial'].empty else []
    periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1 if periodos else 0)
    
    # Filtrar dados do período
    df_periodo = dados['benchmark_setorial'][
        dados['benchmark_setorial']['nu_per_ref'] == periodo
    ] if not dados['benchmark_setorial'].empty else pd.DataFrame()
    
    df_empresas = dados['empresas'][
        dados['empresas']['nu_per_ref'] == periodo
    ] if not dados['empresas'].empty else pd.DataFrame()
    
    df_alertas = dados['alertas'][
        dados['alertas']['nu_per_ref'] == periodo
    ] if not dados['alertas'].empty else pd.DataFrame()
    
    # KPIs principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🏭 Setores Monitorados",
            f"{len(df_periodo):,}" if not df_periodo.empty else "0"
        )
    
    with col2:
        st.metric(
            "🏢 Empresas",
            f"{df_empresas['nu_cnpj'].nunique():,}" if not df_empresas.empty else "0"
        )
    
    with col3:
        fat_total = df_periodo['faturamento_total'].sum() / 1e9 if not df_periodo.empty else 0
        st.metric(
            "💰 Faturamento Total",
            f"R$ {fat_total:.2f}B"
        )
    
    with col4:
        aliq_media = df_periodo['aliq_efetiva_mediana'].mean() * 100 if not df_periodo.empty else 0
        st.metric(
            "📊 Alíquota Média",
            f"{aliq_media:.2f}%"
        )
    
    st.markdown("---")
    
    # Gráficos principais
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Distribuição por Porte")
        if not df_empresas.empty:
            porte_dist = df_empresas.groupby('porte_empresa').size().reset_index(name='quantidade')
            fig = px.pie(
                porte_dist, 
                values='quantidade', 
                names='porte_empresa',
                title="Empresas por Porte"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Status de Alertas")
        if not df_alertas.empty:
            alertas_dist = df_alertas.groupby('severidade').size().reset_index(name='quantidade')
            fig = px.bar(
                alertas_dist,
                x='severidade',
                y='quantidade',
                title="Alertas por Severidade",
                color='severidade',
                color_discrete_map={
                    'CRITICO': '#d32f2f',
                    'ALTO': '#f57c00',
                    'MEDIO': '#fbc02d',
                    'BAIXO': '#388e3c'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Top setores
    st.markdown("---")
    st.subheader("🏆 Top 10 Setores por Faturamento")
    if not df_periodo.empty:
        top_setores = df_periodo.nlargest(10, 'faturamento_total')[
            ['cnae_classe', 'desc_cnae_classe', 'faturamento_total', 
             'qtd_empresas_total', 'aliq_efetiva_mediana']
        ].copy()
        
        top_setores['faturamento_milhoes'] = top_setores['faturamento_total'] / 1e6
        top_setores['aliq_mediana_pct'] = top_setores['aliq_efetiva_mediana'] * 100
        
        st.dataframe(
            top_setores[['cnae_classe', 'desc_cnae_classe', 'faturamento_milhoes', 
                        'qtd_empresas_total', 'aliq_mediana_pct']],
            hide_index=True,
            column_config={
                'cnae_classe': 'CNAE',
                'desc_cnae_classe': 'Descrição',
                'faturamento_milhoes': st.column_config.NumberColumn(
                    'Faturamento (R$ Milhões)',
                    format="%.2f"
                ),
                'qtd_empresas_total': 'Empresas',
                'aliq_mediana_pct': st.column_config.NumberColumn(
                    'Alíquota Mediana (%)',
                    format="%.2f"
                )
            }
        )

# =============================================================================
# 7. SEÇÃO: ANÁLISE SETORIAL
# =============================================================================

def render_analise_setorial(dados, periodo):
    st.header("🏭 Análise Setorial Detalhada")
    
    # Filtro de período
    periodos = sorted(dados['benchmark_setorial']['nu_per_ref'].unique()) if not dados['benchmark_setorial'].empty else []
    periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1 if periodos else 0)
    
    df_setor = dados['benchmark_setorial'][
        dados['benchmark_setorial']['nu_per_ref'] == periodo
    ] if not dados['benchmark_setorial'].empty else pd.DataFrame()
    
    df_evolucao = dados['evolucao_setor'] if not dados['evolucao_setor'].empty else pd.DataFrame()
    
    if df_setor.empty:
        st.warning("⚠️ Sem dados para o período selecionado")
        return
    
    # Seletor de setor
    setores = sorted([s for s in df_setor['desc_cnae_classe'].unique() if s is not None and pd.notna(s)])
    if not setores:
        st.warning("Sem setores disponíveis para o período")
        return
    setor_selecionado = st.selectbox("🔍 Selecione um setor:", setores)
    
    # Filtrar dados do setor
    setor_data = df_setor[df_setor['desc_cnae_classe'] == setor_selecionado].iloc[0]
    cnae_classe = setor_data['cnae_classe']
    
    # KPIs do setor
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🏢 Empresas", f"{int(setor_data['qtd_empresas_total']):,}")
    
    with col2:
        fat = setor_data['faturamento_total'] / 1e6
        st.metric("💰 Faturamento", f"R$ {fat:.2f}M")
    
    with col3:
        aliq = setor_data['aliq_efetiva_mediana'] * 100
        st.metric("📊 Alíquota Mediana", f"{aliq:.2f}%")
    
    with col4:
        cv = setor_data['aliq_coef_variacao']
        st.metric("📈 Coef. Variação", f"{cv:.3f}")
    
    st.markdown("---")
    
    # Gráfico de velocímetro
    st.subheader("🎯 Gráfico de Alíquota")
    fig_gauge = criar_gauge_aliquota(
        setor_data['aliq_efetiva_mediana'],
        setor_data['aliq_efetiva_p25'],
        setor_data['aliq_efetiva_p75']
    )
    st.plotly_chart(fig_gauge, use_container_width=True)
    
    st.markdown("---")
    
    # Evolução temporal
    if not df_evolucao.empty:
        setor_evolucao = df_evolucao[df_evolucao['cnae_classe'] == cnae_classe]
        if not setor_evolucao.empty:
            st.subheader("📈 Evolução Temporal (8 meses)")
            
            # Buscar dados mensais
            df_mensal = dados['benchmark_setorial'][
                dados['benchmark_setorial']['cnae_classe'] == cnae_classe
            ].sort_values('nu_per_ref')
            
            if not df_mensal.empty:
                df_mensal['aliq_pct'] = df_mensal['aliq_efetiva_mediana'] * 100
                df_mensal['periodo_str'] = df_mensal['nu_per_ref'].astype(str)
                
                fig = px.line(
                    df_mensal,
                    x='periodo_str',
                    y='aliq_pct',
                    title="Evolução da Alíquota Mediana",
                    labels={'periodo_str': 'Período', 'aliq_pct': 'Alíquota (%)'}
                )
                fig.update_traces(mode='lines+markers')
                st.plotly_chart(fig, use_container_width=True)
            
            # Métricas de evolução
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "🎯 Categoria Volatilidade",
                    setor_evolucao.iloc[0]['categoria_volatilidade_temporal']
                )
            with col2:
                st.metric(
                    "📊 Tendência",
                    setor_evolucao.iloc[0]['tendencia_aliquota']
                )
            with col3:
                aliq_8m = setor_evolucao.iloc[0]['aliq_mediana_media_8m'] * 100
                st.metric(
                    "📈 Alíquota Média 8m",
                    f"{aliq_8m:.2f}%"
                )
    
    # Distribuição por porte
    st.markdown("---")
    st.subheader("📊 Distribuição por Porte Empresarial")
    
    df_porte = dados['benchmark_porte'][
        (dados['benchmark_porte']['cnae_classe'] == cnae_classe) &
        (dados['benchmark_porte']['nu_per_ref'] == periodo)
    ] if not dados['benchmark_porte'].empty else pd.DataFrame()
    
    if not df_porte.empty:
        df_porte['aliq_mediana_pct'] = df_porte['aliq_efetiva_mediana'] * 100
        
        fig = px.bar(
            df_porte,
            x='porte_empresa',
            y='aliq_mediana_pct',
            title="Alíquota Mediana por Porte",
            labels={'porte_empresa': 'Porte', 'aliq_mediana_pct': 'Alíquota (%)'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            df_porte[['porte_empresa', 'qtd_empresas', 'aliq_mediana_pct']],
            hide_index=True,
            column_config={
                'porte_empresa': 'Porte',
                'qtd_empresas': 'Qtd Empresas',
                'aliq_mediana_pct': st.column_config.NumberColumn(
                    'Alíquota Mediana (%)',
                    format="%.2f"
                )
            }
        )

# =============================================================================
# 8. SEÇÃO: ANÁLISE EMPRESARIAL
# =============================================================================

def render_analise_empresarial(dados, periodo):
    st.header("🏢 Análise Empresarial")
    
    # Filtro de período
    periodos = sorted(dados['empresa_vs_benchmark']['nu_per_ref'].unique()) if not dados['empresa_vs_benchmark'].empty else []
    periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1 if periodos else 0)
    
    df_empresas = dados['empresa_vs_benchmark'][
        dados['empresa_vs_benchmark']['nu_per_ref'] == periodo
    ] if not dados['empresa_vs_benchmark'].empty else pd.DataFrame()
    
    if df_empresas.empty:
        st.warning("⚠️ Sem dados empresariais para o período")
        return
    
    # Busca de empresa
    cnpj_busca = st.text_input("🔍 Buscar CNPJ (apenas números):", max_chars=14)
    
    if cnpj_busca:
        # Limpar CNPJ de formatação
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_busca))
        
        # Tentar buscar com diferentes formatos
        empresa_data = df_empresas[
            (df_empresas['nu_cnpj'].astype(str).str.replace(r'\D', '', regex=True) == cnpj_limpo) |
            (df_empresas['nu_cnpj'].astype(str) == cnpj_busca) |
            (df_empresas['nu_cnpj'] == cnpj_busca)
        ]
        
        if empresa_data.empty:
            st.warning(f"❌ CNPJ {cnpj_busca} não encontrado no período")
            st.info(f"Total de empresas no período: {df_empresas['nu_cnpj'].nunique():,}")
        else:
            emp = empresa_data.iloc[0]
            
            st.success(f"✅ Empresa encontrada: **{emp['nm_razao_social']}**")
            
            # Informações principais
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"**CNAE:** {emp['cnae_classe']}")
                st.info(f"**Setor:** {emp['desc_cnae_classe'][:50]}")
                st.info(f"**Porte:** {emp['porte_empresa']}")
            
            with col2:
                st.metric("💰 Faturamento", formatar_moeda(emp['vl_faturamento']))
                st.metric("💵 ICMS Devido", formatar_moeda(emp['icms_devido']))
            
            with col3:
                aliq_emp = emp['aliq_efetiva_empresa'] * 100 if pd.notna(emp['aliq_efetiva_empresa']) else 0
                aliq_setor = emp['aliq_setor_mediana'] * 100 if pd.notna(emp['aliq_setor_mediana']) else 0
                
                st.metric("📊 Alíquota Empresa", f"{aliq_emp:.2f}%")
                st.metric("📊 Alíquota Setor", f"{aliq_setor:.2f}%")
            
            # Status comparativo
            st.markdown("---")
            st.subheader("📊 Status Comparativo")
            
            col1, col2 = st.columns(2)
            
            with col1:
                status_color = {
                    'MUITO_ABAIXO': '🔴',
                    'ABAIXO': '🟠',
                    'NORMAL': '🟢',
                    'ACIMA': '🟡',
                    'MUITO_ACIMA': '🔴'
                }
                
                st.info(f"{status_color.get(emp['status_vs_setor'], '⚪')} Status: **{emp['status_vs_setor']}**")
                
                if pd.notna(emp['indice_vs_mediana_setor']):
                    st.metric(
                        "Índice vs Setor",
                        f"{emp['indice_vs_mediana_setor']:.2f}",
                        delta=f"{(emp['indice_vs_mediana_setor'] - 1) * 100:.1f}%"
                    )
            
            with col2:
                # Gráfico comparativo de alíquotas
                if pd.notna(emp['aliq_efetiva_empresa']) and pd.notna(emp['aliq_setor_mediana']):
                    dados_comp = pd.DataFrame({
                        'Tipo': ['Empresa', 'Setor (Mediana)', 'Setor (P25)', 'Setor (P75)'],
                        'Alíquota': [
                            emp['aliq_efetiva_empresa'] * 100,
                            emp['aliq_setor_mediana'] * 100,
                            emp['aliq_setor_p25'] * 100 if pd.notna(emp.get('aliq_setor_p25')) else 0,
                            emp['aliq_setor_p75'] * 100 if pd.notna(emp.get('aliq_setor_p75')) else 0
                        ]
                    })
                    
                    fig = px.bar(
                        dados_comp,
                        x='Tipo',
                        y='Alíquota',
                        title="Comparação de Alíquotas (%)",
                        color='Tipo',
                        color_discrete_sequence=['#d32f2f', '#1f77b4', '#2ca02c', '#ff7f0e']
                    )
                    fig.update_layout(showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Evolução temporal da empresa vs setor
            st.markdown("---")
            st.subheader("📈 Evolução Temporal: Empresa vs Setor")
            
            # Buscar dados históricos da empresa
            df_hist_empresa = dados['empresa_vs_benchmark'][
                dados['empresa_vs_benchmark']['nu_cnpj'] == emp['nu_cnpj']
            ].sort_values('nu_per_ref')
            
            # Buscar dados históricos do setor
            df_hist_setor = dados['benchmark_setorial'][
                dados['benchmark_setorial']['cnae_classe'] == emp['cnae_classe']
            ].sort_values('nu_per_ref')
            
            if not df_hist_empresa.empty and not df_hist_setor.empty:
                # Preparar dados
                df_hist_empresa['periodo_str'] = df_hist_empresa['nu_per_ref'].astype(str)
                df_hist_empresa['aliq_empresa_pct'] = df_hist_empresa['aliq_efetiva_empresa'] * 100
                
                df_hist_setor['periodo_str'] = df_hist_setor['nu_per_ref'].astype(str)
                df_hist_setor['aliq_setor_pct'] = df_hist_setor['aliq_efetiva_mediana'] * 100
                
                # Criar gráfico de linhas
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=df_hist_empresa['periodo_str'],
                    y=df_hist_empresa['aliq_empresa_pct'],
                    mode='lines+markers',
                    name='Empresa',
                    line=dict(color='#d32f2f', width=3),
                    marker=dict(size=8)
                ))
                
                fig.add_trace(go.Scatter(
                    x=df_hist_setor['periodo_str'],
                    y=df_hist_setor['aliq_setor_pct'],
                    mode='lines+markers',
                    name='Setor (Mediana)',
                    line=dict(color='#1f77b4', width=3),
                    marker=dict(size=8)
                ))
                
                fig.update_layout(
                    title="Evolução da Alíquota Efetiva: Empresa vs Setor",
                    xaxis_title="Período",
                    yaxis_title="Alíquota (%)",
                    hovermode='x unified',
                    height=400,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.3,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Dados históricos insuficientes para comparação temporal")
    
    # Top empresas por score
    st.markdown("---")
    st.subheader("🎯 Top Empresas para Fiscalização")
    
    df_alertas = dados['alertas'][
        dados['alertas']['nu_per_ref'] == periodo
    ] if not dados['alertas'].empty else pd.DataFrame()
    
    if not df_alertas.empty:
        top_empresas = df_alertas.nlargest(20, 'score_risco')[
            ['nu_cnpj', 'nm_razao_social', 'cnae_classe', 'porte_empresa', 
             'tipo_alerta', 'severidade', 'score_risco']
        ]
        
        st.dataframe(
            top_empresas,
            hide_index=True,
            column_config={
                'nu_cnpj': 'CNPJ',
                'nm_razao_social': 'Razão Social',
                'cnae_classe': 'CNAE',
                'porte_empresa': 'Porte',
                'tipo_alerta': 'Tipo Alerta',
                'severidade': 'Severidade',
                'score_risco': st.column_config.NumberColumn(
                    'Score Risco',
                    format="%.1f"
                )
            }
        )

# =============================================================================
# 9. SEÇÃO: EVOLUÇÃO TEMPORAL
# =============================================================================

def render_evolucao_temporal(dados):
    st.header("⏱️ Evolução Temporal por CNAE")
    
    df_benchmark = dados['benchmark_setorial']
    
    if df_benchmark.empty:
        st.warning("Sem dados de benchmark disponíveis")
        return
    
    # Seletor de CNAE
    cnaes_raw = [
        (cnae, desc) for cnae, desc in 
        zip(df_benchmark['cnae_classe'], df_benchmark['desc_cnae_classe'])
        if cnae is not None and pd.notna(cnae) and desc is not None and pd.notna(desc)
    ]
    
    if not cnaes_raw:
        st.warning("Sem CNAEs disponíveis")
        return
    
    # Ordenar com tratamento seguro
    try:
        cnaes = sorted(cnaes_raw, key=lambda x: str(x[1]))
    except:
        cnaes = cnaes_raw
    
    # Criar dicionário para o selectbox
    cnae_dict = {f"{cnae} - {desc}": cnae for cnae, desc in cnaes}
    cnae_dict = dict(sorted(set(cnae_dict.items())))
    
    cnae_selecionado_str = st.selectbox(
        "🔍 Selecione o CNAE:",
        list(cnae_dict.keys())
    )
    
    cnae_selecionado = cnae_dict[cnae_selecionado_str]
    
    # Filtrar dados do CNAE
    df_cnae = df_benchmark[df_benchmark['cnae_classe'] == cnae_selecionado].copy()
    
    if df_cnae.empty:
        st.warning(f"Sem dados para o CNAE {cnae_selecionado}")
        return
    
    # Ordenar por período
    df_cnae = df_cnae.sort_values('nu_per_ref')
    
    # Preparar dados para visualização
    df_cnae['periodo_str'] = df_cnae['nu_per_ref'].astype(str)
    df_cnae['aliq_media_pct'] = df_cnae['aliq_efetiva_media'] * 100
    df_cnae['aliq_mediana_pct'] = df_cnae['aliq_efetiva_mediana'] * 100
    df_cnae['aliq_p25_pct'] = df_cnae['aliq_efetiva_p25'] * 100
    df_cnae['aliq_p75_pct'] = df_cnae['aliq_efetiva_p75'] * 100
    
    # Informações gerais
    st.info(f"**Setor:** {df_cnae.iloc[0]['desc_cnae_classe']}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Períodos Analisados", len(df_cnae))
    with col2:
        st.metric("🏢 Empresas (Média)", f"{df_cnae['qtd_empresas_total'].mean():.0f}")
    with col3:
        fat_total = df_cnae['faturamento_total'].sum() / 1e9
        st.metric("💰 Faturamento Total", f"R$ {fat_total:.2f}B")
    
    # Gráfico principal - Evolução da Alíquota
    st.markdown("---")
    st.subheader("📈 Evolução da Alíquota Efetiva")
    
    fig = go.Figure()
    
    # Adicionar área entre P25 e P75
    fig.add_trace(go.Scatter(
        x=df_cnae['periodo_str'],
        y=df_cnae['aliq_p75_pct'],
        fill=None,
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.add_trace(go.Scatter(
        x=df_cnae['periodo_str'],
        y=df_cnae['aliq_p25_pct'],
        fill='tonexty',
        mode='lines',
        line=dict(width=0),
        name='Intervalo P25-P75',
        fillcolor='rgba(68, 68, 68, 0.1)',
        hoverinfo='skip'
    ))
    
    # Linha da mediana
    fig.add_trace(go.Scatter(
        x=df_cnae['periodo_str'],
        y=df_cnae['aliq_mediana_pct'],
        mode='lines+markers',
        name='Mediana',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ))
    
    # Linha da média
    fig.add_trace(go.Scatter(
        x=df_cnae['periodo_str'],
        y=df_cnae['aliq_media_pct'],
        mode='lines+markers',
        name='Média',
        line=dict(color='#ff7f0e', width=2, dash='dash'),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title=f"Evolução da Alíquota - {df_cnae.iloc[0]['desc_cnae_classe'][:60]}",
        xaxis_title="Período",
        yaxis_title="Alíquota (%)",
        hovermode='x unified',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Gráfico de faturamento
    st.markdown("---")
    st.subheader("💰 Evolução do Faturamento")
    
    df_cnae['faturamento_milhoes'] = df_cnae['faturamento_total'] / 1e6
    
    fig2 = px.bar(
        df_cnae,
        x='periodo_str',
        y='faturamento_milhoes',
        title="Faturamento Total por Período",
        labels={'periodo_str': 'Período', 'faturamento_milhoes': 'Faturamento (R$ Milhões)'}
    )
    fig2.update_layout(height=400)
    st.plotly_chart(fig2, use_container_width=True)
    
    # Tabela de dados
    st.markdown("---")
    st.subheader("📋 Dados Detalhados")
    
    df_exibir = df_cnae[[
        'nu_per_ref', 'qtd_empresas_total', 'qtd_empresas_ativas',
        'aliq_mediana_pct', 'aliq_media_pct', 'aliq_coef_variacao',
        'faturamento_milhoes'
    ]].copy()
    
    st.dataframe(
        df_exibir,
        hide_index=True,
        column_config={
            'nu_per_ref': 'Período',
            'qtd_empresas_total': 'Total Empresas',
            'qtd_empresas_ativas': 'Empresas Ativas',
            'aliq_mediana_pct': st.column_config.NumberColumn(
                'Alíq. Mediana (%)',
                format="%.2f"
            ),
            'aliq_media_pct': st.column_config.NumberColumn(
                'Alíq. Média (%)',
                format="%.2f"
            ),
            'aliq_coef_variacao': st.column_config.NumberColumn(
                'Coef. Variação',
                format="%.3f"
            ),
            'faturamento_milhoes': st.column_config.NumberColumn(
                'Faturamento (R$ Mi)',
                format="%.2f"
            )
        }
    )
    
    # Estatísticas resumidas
    st.markdown("---")
    st.subheader("📊 Estatísticas do Período")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Alíquota Média",
            f"{df_cnae['aliq_mediana_pct'].mean():.2f}%"
        )
    
    with col2:
        variacao = df_cnae['aliq_mediana_pct'].std()
        st.metric(
            "Desvio Padrão",
            f"{variacao:.2f} p.p."
        )
    
    with col3:
        aliq_min = df_cnae['aliq_mediana_pct'].min()
        aliq_max = df_cnae['aliq_mediana_pct'].max()
        st.metric(
            "Amplitude",
            f"{aliq_max - aliq_min:.2f} p.p."
        )
    
    with col4:
        # Tendência (primeiro vs último)
        if len(df_cnae) >= 2:
            primeiro = df_cnae.iloc[0]['aliq_mediana_pct']
            ultimo = df_cnae.iloc[-1]['aliq_mediana_pct']
            tendencia = ((ultimo - primeiro) / primeiro * 100) if primeiro > 0 else 0
            st.metric(
                "Tendência",
                f"{tendencia:+.1f}%",
                delta=f"{ultimo - primeiro:+.2f} p.p."
            )
    
    # Nova seção: Setores Normais e Anormais
    st.markdown("---")
    st.subheader("🎯 Análise de Normalidade dos Setores")
    
    df_evolucao_setor = dados['evolucao_setor']
    
    if not df_evolucao_setor.empty:
        # Calcular score de anormalidade baseado em volatilidade e tendência
        df_analise = df_evolucao_setor.copy()
        
        # Criar score de anormalidade
        df_analise['score_anormalidade'] = (
            df_analise['coef_variacao_temporal'] * 100 +
            (df_analise['categoria_volatilidade_temporal'].map({
                'BAIXA': 0, 'MEDIA': 50, 'ALTA': 100
            }).fillna(50))
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🟢 Setores Mais Normais (Estáveis)**")
            setores_normais = df_analise.nsmallest(10, 'score_anormalidade')[
                ['cnae_classe', 'desc_cnae_classe', 'coef_variacao_temporal', 
                 'categoria_volatilidade_temporal', 'score_anormalidade']
            ]
            
            st.dataframe(
                setores_normais,
                hide_index=True,
                column_config={
                    'cnae_classe': 'CNAE',
                    'desc_cnae_classe': 'Descrição',
                    'coef_variacao_temporal': st.column_config.NumberColumn(
                        'CV',
                        format="%.3f"
                    ),
                    'categoria_volatilidade_temporal': 'Volatilidade',
                    'score_anormalidade': st.column_config.NumberColumn(
                        'Score',
                        format="%.1f"
                    )
                },
                height=400
            )
        
        with col2:
            st.markdown("**🔴 Setores Mais Anormais (Instáveis)**")
            setores_anormais = df_analise.nlargest(10, 'score_anormalidade')[
                ['cnae_classe', 'desc_cnae_classe', 'coef_variacao_temporal', 
                 'categoria_volatilidade_temporal', 'score_anormalidade']
            ]
            
            st.dataframe(
                setores_anormais,
                hide_index=True,
                column_config={
                    'cnae_classe': 'CNAE',
                    'desc_cnae_classe': 'Descrição',
                    'coef_variacao_temporal': st.column_config.NumberColumn(
                        'CV',
                        format="%.3f"
                    ),
                    'categoria_volatilidade_temporal': 'Volatilidade',
                    'score_anormalidade': st.column_config.NumberColumn(
                        'Score',
                        format="%.1f"
                    )
                },
                height=400
            )
        
        # Seleção de setor para análise de empresas
        st.markdown("---")
        st.subheader("🔍 Empresas Anormais por Setor")
        
        setores_disponiveis = df_analise.nlargest(20, 'score_anormalidade')
        setor_dict = {f"{row['cnae_classe']} - {row['desc_cnae_classe']}": row['cnae_classe'] 
                      for _, row in setores_disponiveis.iterrows()}
        
        setor_analise = st.selectbox(
            "Selecione um setor anormal para análise:",
            list(setor_dict.keys())
        )
        
        if setor_analise:
            cnae_analise = setor_dict[setor_analise]
            
            # Buscar empresas do setor com alertas
            df_alertas = dados['alertas']
            df_empresas_setor = dados['empresa_vs_benchmark']
            
            if not df_alertas.empty and not df_empresas_setor.empty:
                # Empresas com alertas no setor
                empresas_anormais = df_alertas[
                    df_alertas['cnae_classe'] == cnae_analise
                ].nlargest(15, 'score_risco')
                
                if not empresas_anormais.empty:
                    st.warning(f"⚠️ {len(empresas_anormais)} empresas anormais identificadas para fiscalização")
                    
                    st.dataframe(
                        empresas_anormais[[
                            'nu_cnpj', 'nm_razao_social', 'porte_empresa',
                            'tipo_alerta', 'severidade', 'score_risco'
                        ]],
                        hide_index=True,
                        column_config={
                            'nu_cnpj': 'CNPJ',
                            'nm_razao_social': 'Razão Social',
                            'porte_empresa': 'Porte',
                            'tipo_alerta': 'Tipo Alerta',
                            'severidade': 'Severidade',
                            'score_risco': st.column_config.NumberColumn(
                                'Score Risco',
                                format="%.1f"
                            )
                        },
                        height=400
                    )
                    
                    # Download
                    csv = empresas_anormais.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        "📥 Download Empresas para Fiscalização",
                        csv,
                        f"empresas_anormais_{cnae_analise}.csv",
                        "text/csv"
                    )
                else:
                    st.info("✅ Nenhuma empresa anormal identificada neste setor")

# =============================================================================
# 10. SEÇÃO: ANÁLISE DE VOLATILIDADE
# =============================================================================

def render_analise_volatilidade(dados, periodo):
    st.header("📉 Análise de Volatilidade Empresarial")
    st.markdown("Identifique empresas e setores com comportamento fiscal instável ao longo do tempo.")
    
    df_evolucao = dados['evolucao_empresa'] if not dados['evolucao_empresa'].empty else pd.DataFrame()
    
    if df_evolucao.empty:
        st.warning("⚠️ Dados de evolução temporal não disponíveis")
        return
    
    # Métricas gerais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        alta_vol = len(df_evolucao[df_evolucao['categoria_volatilidade'] == 'ALTA'])
        st.metric("🔴 Alta Volatilidade", f"{alta_vol:,}")
    
    with col2:
        media_vol = len(df_evolucao[df_evolucao['categoria_volatilidade'] == 'MEDIA'])
        st.metric("🟡 Média Volatilidade", f"{media_vol:,}")
    
    with col3:
        baixa_vol = len(df_evolucao[df_evolucao['categoria_volatilidade'] == 'BAIXA'])
        st.metric("🟢 Baixa Volatilidade", f"{baixa_vol:,}")
    
    with col4:
        cv_medio = df_evolucao['aliq_coef_variacao_8m'].mean() if 'aliq_coef_variacao_8m' in df_evolucao.columns else 0
        st.metric("📊 CV Médio", f"{cv_medio:.3f}")
    
    # Distribuição por categoria
    st.markdown("---")
    st.subheader("📊 Distribuição de Volatilidade")
    
    col1, col2 = st.columns(2)
    
    with col1:
        vol_counts = df_evolucao['categoria_volatilidade'].value_counts()
        fig = px.pie(
            vol_counts,
            values=vol_counts.values,
            names=vol_counts.index,
            title="Distribuição por Categoria de Volatilidade",
            color=vol_counts.index,
            color_discrete_map={
                'ALTA': '#d32f2f',
                'MEDIA': '#fbc02d',
                'BAIXA': '#388e3c'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Volatilidade por setor
        vol_setor = df_evolucao.groupby('cnae_classe').agg({
            'categoria_volatilidade': lambda x: (x == 'ALTA').sum() / len(x) * 100
        }).nlargest(10, 'categoria_volatilidade').sort_values('categoria_volatilidade')
        
        fig = px.bar(
            vol_setor,
            x='categoria_volatilidade',
            y=vol_setor.index,
            orientation='h',
            title="Top 10 Setores com Maior % de Alta Volatilidade",
            labels={'categoria_volatilidade': '% Alta Volatilidade', 'y': 'CNAE'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Top empresas mais voláteis
    st.markdown("---")
    st.subheader("🎯 Empresas Mais Voláteis")
    
    df_alta_vol = df_evolucao[
        df_evolucao['categoria_volatilidade'] == 'ALTA'
    ].nlargest(20, 'aliq_coef_variacao_8m')
    
    if not df_alta_vol.empty:
        st.dataframe(
            df_alta_vol[[
                'nm_razao_social', 'cnae_classe', 'porte_predominante',
                'aliq_coef_variacao_8m', 'aliq_media_8m', 'meses_com_declaracao'
            ]],
            hide_index=True,
            column_config={
                'nm_razao_social': 'Razão Social',
                'cnae_classe': 'CNAE',
                'porte_predominante': 'Porte',
                'aliq_coef_variacao_8m': st.column_config.NumberColumn(
                    'Coef. Variação',
                    format="%.3f"
                ),
                'aliq_media_8m': st.column_config.NumberColumn(
                    'Alíq. Média (%)',
                    format="%.2f"
                ),
                'meses_com_declaracao': 'Meses'
            }
        )
    
    # Análise de volatilidade vs faturamento
    st.markdown("---")
    st.subheader("📈 Volatilidade vs Faturamento")
    
    if 'faturamento_total_8m' in df_evolucao.columns:
        df_scatter = df_evolucao[df_evolucao['faturamento_total_8m'] > 0].copy()
        df_scatter['fat_milhoes'] = df_scatter['faturamento_total_8m'] / 1e6
        
        fig = px.scatter(
            df_scatter,
            x='fat_milhoes',
            y='aliq_coef_variacao_8m',
            color='categoria_volatilidade',
            size='meses_com_declaracao',
            hover_data=['nm_razao_social', 'cnae_classe'],
            title="Volatilidade vs Faturamento",
            labels={
                'fat_milhoes': 'Faturamento Total (R$ Milhões)',
                'aliq_coef_variacao_8m': 'Coeficiente de Variação'
            },
            color_discrete_map={
                'ALTA': '#d32f2f',
                'MEDIA': '#fbc02d',
                'BAIXA': '#388e3c'
            }
        )
        fig.update_xaxes(type='log')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# 11. SEÇÃO: ALERTAS E ANOMALIAS
# =============================================================================

def render_alertas_anomalias(dados, periodo):
    st.header("⚠️ Alertas e Anomalias")
    
    # Filtro de período
    periodos = sorted(dados['alertas']['nu_per_ref'].unique()) if not dados['alertas'].empty else []
    periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1 if periodos else 0)
    
    df_alertas = dados['alertas'][
        dados['alertas']['nu_per_ref'] == periodo
    ] if not dados['alertas'].empty else pd.DataFrame()
    
    df_anomalias = dados['anomalias'][
        dados['anomalias']['nu_per_ref'] == periodo
    ] if not dados['anomalias'].empty else pd.DataFrame()
    
    # Resumo de alertas
    col1, col2, col3, col4 = st.columns(4)
    
    if not df_alertas.empty:
        with col1:
            total = len(df_alertas)
            st.metric("📋 Total Alertas", f"{total:,}")
        
        with col2:
            criticos = len(df_alertas[df_alertas['severidade'] == 'CRITICO'])
            st.metric("🔴 Críticos", f"{criticos:,}")
        
        with col3:
            altos = len(df_alertas[df_alertas['severidade'] == 'ALTO'])
            st.metric("🟠 Altos", f"{altos:,}")
        
        with col4:
            medios = len(df_alertas[df_alertas['severidade'] == 'MEDIO'])
            st.metric("🟡 Médios", f"{medios:,}")
    
    # Filtro de alertas
    st.markdown("---")
    st.subheader("🔍 Filtrar Alertas")
    
    if not df_alertas.empty:
        tipos_alerta = ['Todos'] + sorted(df_alertas['tipo_alerta'].unique().tolist())
        tipo_selecionado = st.selectbox("Selecione o tipo de alerta:", tipos_alerta)
        
        if tipo_selecionado != 'Todos':
            df_filtrado = df_alertas[df_alertas['tipo_alerta'] == tipo_selecionado].copy()
        else:
            df_filtrado = df_alertas.copy()
        
        if not df_filtrado.empty:
            st.info(f"📊 {len(df_filtrado):,} empresa(s) encontrada(s)")
            
            # Preparar dados para exibição
            df_exibir = df_filtrado[
                ['nu_cnpj', 'nm_razao_social', 'cnae_classe', 'desc_cnae_classe',
                 'porte_empresa', 'tipo_alerta', 'severidade', 'score_risco',
                 'vl_faturamento', 'aliq_efetiva_empresa', 'aliq_setor_mediana']
            ].copy()
            
            # Formatar colunas
            if 'aliq_efetiva_empresa' in df_exibir.columns:
                df_exibir['aliq_empresa_pct'] = df_exibir['aliq_efetiva_empresa'] * 100
            if 'aliq_setor_mediana' in df_exibir.columns:
                df_exibir['aliq_setor_pct'] = df_exibir['aliq_setor_mediana'] * 100
            
            # Ordenar por score
            df_exibir = df_exibir.sort_values('score_risco', ascending=False)
            
            st.dataframe(
                df_exibir[['nu_cnpj', 'nm_razao_social', 'cnae_classe', 'porte_empresa',
                          'tipo_alerta', 'severidade', 'score_risco', 'vl_faturamento',
                          'aliq_empresa_pct', 'aliq_setor_pct']],
                hide_index=True,
                column_config={
                    'nu_cnpj': 'CNPJ',
                    'nm_razao_social': 'Razão Social',
                    'cnae_classe': 'CNAE',
                    'porte_empresa': 'Porte',
                    'tipo_alerta': 'Tipo Alerta',
                    'severidade': 'Severidade',
                    'score_risco': st.column_config.NumberColumn(
                        'Score Risco',
                        format="%.1f"
                    ),
                    'vl_faturamento': st.column_config.NumberColumn(
                        'Faturamento',
                        format="R$ %.2f"
                    ),
                    'aliq_empresa_pct': st.column_config.NumberColumn(
                        'Alíq. Empresa (%)',
                        format="%.2f"
                    ),
                    'aliq_setor_pct': st.column_config.NumberColumn(
                        'Alíq. Setor (%)',
                        format="%.2f"
                    )
                },
                height=400
            )
            
            # Opção de download
            csv = df_exibir.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"alertas_{tipo_selecionado}_{periodo}.csv",
                mime="text/csv"
            )
    
    # Distribuição de alertas
    if not df_alertas.empty:
        st.markdown("---")
        st.subheader("📊 Distribuição de Alertas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            tipo_dist = df_alertas.groupby('tipo_alerta').size().reset_index(name='quantidade')
            fig = px.bar(
                tipo_dist,
                x='quantidade',
                y='tipo_alerta',
                orientation='h',
                title="Alertas por Tipo"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            sev_dist = df_alertas.groupby('severidade').size().reset_index(name='quantidade')
            fig = px.pie(
                sev_dist,
                values='quantidade',
                names='severidade',
                title="Alertas por Severidade",
                color='severidade',
                color_discrete_map={
                    'CRITICO': '#d32f2f',
                    'ALTO': '#f57c00',
                    'MEDIO': '#fbc02d',
                    'BAIXO': '#388e3c'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Anomalias setoriais
    if not df_anomalias.empty:
        st.markdown("---")
        st.subheader("🏭 Anomalias Setoriais")
        
        top_anomalias = df_anomalias.nlargest(15, 'score_relevancia')[
            ['cnae_classe', 'desc_cnae_classe', 'tipo_anomalia', 
             'severidade', 'score_relevancia', 'qtd_empresas_total']
        ]
        
        st.dataframe(
            top_anomalias,
            hide_index=True,
            column_config={
                'cnae_classe': 'CNAE',
                'desc_cnae_classe': 'Descrição',
                'tipo_anomalia': 'Tipo',
                'severidade': 'Severidade',
                'score_relevancia': st.column_config.NumberColumn(
                    'Score',
                    format="%.1f"
                ),
                'qtd_empresas_total': 'Empresas'
            }
        )

# =============================================================================
# 12. SEÇÃO: ANÁLISE DE PAGAMENTOS
# =============================================================================

def render_analise_pagamentos(dados, periodo):
    st.header("💰 Análise de Pagamentos")
    st.markdown("Explore os dados de pagamentos de ICMS, tendências temporais e empresas com maiores contribuições.")
    
    # Filtro de período
    periodos = sorted(dados['pagamentos']['nu_per_ref'].unique()) if not dados['pagamentos'].empty and 'nu_per_ref' in dados['pagamentos'].columns else []
    if periodos:
        periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1)
    
    df_pagamentos = dados['pagamentos'] if not dados['pagamentos'].empty else pd.DataFrame()
    df_empresas = dados['empresa_vs_benchmark'] if not dados['empresa_vs_benchmark'].empty else pd.DataFrame()
    
    if df_pagamentos.empty:
        st.warning("⚠️ Dados de pagamentos não disponíveis")
        return
    
    # Filtrar período
    df_pag_periodo = df_pagamentos[df_pagamentos['nu_per_ref'] == periodo] if 'nu_per_ref' in df_pagamentos.columns else df_pagamentos
    
    # Métricas principais
    st.subheader("📊 Indicadores Gerais")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_pago = df_pag_periodo['valor_total_pago'].sum()
        st.metric("💵 Total Pago", f"R$ {total_pago/1e9:.2f}B")
    
    with col2:
        total_pagamentos = df_pag_periodo['qtd_pagamentos'].sum()
        st.metric("📋 Qtd Pagamentos", f"{total_pagamentos:,.0f}")
    
    with col3:
        empresas_pagantes = df_pag_periodo[df_pag_periodo['valor_total_pago'] > 0]['nu_cnpj'].nunique()
        st.metric("🏢 Empresas Pagantes", f"{empresas_pagantes:,}")
    
    with col4:
        ticket_medio = total_pago / total_pagamentos if total_pagamentos > 0 else 0
        st.metric("💳 Ticket Médio", f"R$ {ticket_medio:,.2f}")
    
    # Evolução temporal
    st.markdown("---")
    st.subheader("📈 Evolução Temporal dos Pagamentos")
    
    evolucao = df_pagamentos.groupby('nu_per_ref').agg({
        'valor_total_pago': 'sum',
        'qtd_pagamentos': 'sum'
    }).reset_index()
    
    evolucao['periodo_str'] = evolucao['nu_per_ref'].astype(str)
    evolucao['valor_milhoes'] = evolucao['valor_total_pago'] / 1e6
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(
            x=evolucao['periodo_str'],
            y=evolucao['valor_milhoes'],
            name='Valor Pago (R$ Mi)',
            marker_color='#1f77b4'
        ),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=evolucao['periodo_str'],
            y=evolucao['qtd_pagamentos'],
            name='Quantidade',
            line=dict(color='#ff7f0e', width=3),
            mode='lines+markers'
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title="Evolução do Valor e Quantidade de Pagamentos",
        hovermode='x unified',
        height=400
    )
    fig.update_xaxes(title_text="Período")
    fig.update_yaxes(title_text="Valor (R$ Milhões)", secondary_y=False)
    fig.update_yaxes(title_text="Quantidade", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Top empresas
    st.markdown("---")
    st.subheader("🏆 Ranking de Empresas")
    
    col1, col2 = st.columns(2)
    
    # Merge com dados de empresas para pegar nomes
    if not df_empresas.empty:
        df_pag_com_nome = df_pag_periodo.merge(
            df_empresas[['nu_cnpj', 'nm_razao_social']].drop_duplicates(),
            on='nu_cnpj',
            how='left'
        )
        df_pag_com_nome['nm_razao_social'] = df_pag_com_nome['nm_razao_social'].fillna('Não identificado')
    else:
        df_pag_com_nome = df_pag_periodo.copy()
        df_pag_com_nome['nm_razao_social'] = 'Não identificado'
    
    with col1:
        st.markdown("**Top 10 por Valor Pago**")
        top_valor = df_pag_com_nome.nlargest(10, 'valor_total_pago')
        top_valor['valor_milhoes'] = top_valor['valor_total_pago'] / 1e6
        
        fig = px.bar(
            top_valor,
            x='valor_milhoes',
            y='nm_razao_social',
            orientation='h',
            title="Maiores Pagadores",
            labels={'valor_milhoes': 'Valor (R$ Mi)', 'nm_razao_social': ''}
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("**Top 10 por Quantidade**")
        top_qtd = df_pag_com_nome.nlargest(10, 'qtd_pagamentos')
        
        fig = px.bar(
            top_qtd,
            x='qtd_pagamentos',
            y='nm_razao_social',
            orientation='h',
            title="Maior Frequência de Pagamentos",
            labels={'qtd_pagamentos': 'Quantidade', 'nm_razao_social': ''}
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Análise de divergências
    st.markdown("---")
    st.subheader("⚠️ Divergências ICMS x Pagamentos")
    
    if not df_empresas.empty and not df_pag_periodo.empty:
        # Preparar dados de pagamentos
        if 'valor_total_pago' in df_pag_periodo.columns:
            df_pag_merge = df_pag_periodo[['nu_cnpj', 'valor_total_pago']].drop_duplicates()
            
            # Comparar com ICMS devido
            df_comp = df_empresas[df_empresas['nu_per_ref'] == periodo].merge(
                df_pag_merge,
                on='nu_cnpj',
                how='left',
                suffixes=('', '_pag')
            )
            
            # Garantir que a coluna existe
            if 'valor_total_pago' in df_comp.columns:
                df_comp['valor_total_pago'] = df_comp['valor_total_pago'].fillna(0)
                
                # Calcular divergências
                df_comp['diferenca'] = df_comp['icms_recolher'] - df_comp['valor_total_pago']
                df_comp['perc_divergencia'] = np.where(
                    df_comp['icms_recolher'] > 0,
                    (df_comp['diferenca'] / df_comp['icms_recolher'] * 100),
                    0
                )
                
                # Filtrar divergências significativas
                df_div = df_comp[
                    (np.abs(df_comp['perc_divergencia']) > 30) & 
                    (df_comp['icms_recolher'] > 1000)
                ].copy()
                
                if not df_div.empty:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric(
                            "🔴 Empresas com Divergência > 30%",
                            f"{len(df_div):,}"
                        )
                    
                    with col2:
                        dif_total = df_div['diferenca'].sum()
                        st.metric(
                            "💰 Diferença Total",
                            f"R$ {dif_total/1e6:.2f}M"
                        )
                    
                    # Tabela de divergências
                    st.markdown("**Maiores Divergências:**")
                    df_div_top = df_div.nlargest(15, 'diferenca')[
                        ['nm_razao_social', 'icms_recolher', 'valor_total_pago', 
                         'diferenca', 'perc_divergencia']
                    ]
                    
                    st.dataframe(
                        df_div_top,
                        hide_index=True,
                        column_config={
                            'nm_razao_social': 'Razão Social',
                            'icms_recolher': st.column_config.NumberColumn(
                                'ICMS a Recolher',
                                format="R$ %.2f"
                            ),
                            'valor_total_pago': st.column_config.NumberColumn(
                                'Valor Pago',
                                format="R$ %.2f"
                            ),
                            'diferenca': st.column_config.NumberColumn(
                                'Diferença',
                                format="R$ %.2f"
                            ),
                            'perc_divergencia': st.column_config.NumberColumn(
                                'Divergência (%)',
                                format="%.1f"
                            )
                        }
                    )
                else:
                    st.success("✅ Não há divergências significativas no período")
            else:
                st.info("ℹ️ Coluna de valor_total_pago não disponível após merge")
        else:
            st.info("ℹ️ Coluna valor_total_pago não encontrada nos dados de pagamentos")
    else:
        st.info("ℹ️ Dados insuficientes para análise de divergências")

# =============================================================================
# 13. SEÇÃO: MACHINE LEARNING
# =============================================================================

def render_machine_learning(dados, periodo):
    st.header("🤖 Modelos Preditivos (Machine Learning)")
    st.markdown("Utilize modelos de ML para identificar padrões e prever comportamentos de risco fiscal.")
    
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
    from sklearn.preprocessing import StandardScaler
    
    # Filtro de período
    periodos = sorted(dados['empresa_vs_benchmark']['nu_per_ref'].unique()) if not dados['empresa_vs_benchmark'].empty else []
    if periodos:
        periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1)
    
    df_empresas = dados['empresa_vs_benchmark'] if not dados['empresa_vs_benchmark'].empty else pd.DataFrame()
    df_evolucao = dados['evolucao_empresa'] if not dados['evolucao_empresa'].empty else pd.DataFrame()
    
    if df_empresas.empty:
        st.warning("⚠️ Dados insuficientes para análise preditiva")
        return
    
    # Preparar dados
    df_ml = df_empresas.copy()
    
    # Criar variável target: empresa problemática
    df_ml['empresa_problematica'] = (
        (df_ml['status_vs_setor'].isin(['MUITO_ABAIXO', 'ABAIXO'])) |
        (df_ml['flag_divergencia_pagamento'] == 1)
    ).astype(int)
    
    # Features
    features = []
    
    # Adicionar features numéricas básicas
    if 'vl_faturamento' in df_ml.columns:
        df_ml['log_faturamento'] = np.log1p(df_ml['vl_faturamento'].fillna(0))
        features.append('log_faturamento')
    
    if 'aliq_efetiva_empresa' in df_ml.columns:
        df_ml['aliq_empresa'] = df_ml['aliq_efetiva_empresa'].fillna(0)
        features.append('aliq_empresa')
    
    if 'indice_vs_mediana_setor' in df_ml.columns:
        df_ml['indice_setor'] = df_ml['indice_vs_mediana_setor'].fillna(1)
        features.append('indice_setor')
    
    # One-hot encoding para porte
    if 'porte_empresa' in df_ml.columns:
        porte_dummies = pd.get_dummies(df_ml['porte_empresa'], prefix='porte')
        df_ml = pd.concat([df_ml, porte_dummies], axis=1)
        features.extend(porte_dummies.columns.tolist())
    
    # Flags
    if 'flag_divergencia_pagamento' in df_ml.columns:
        features.append('flag_divergencia_pagamento')
    
    if 'sn_omisso' in df_ml.columns:
        df_ml['sn_omisso'] = df_ml['sn_omisso'].fillna(0)
        features.append('sn_omisso')
    
    # Verificar se temos features suficientes
    if len(features) < 3:
        st.error("❌ Features insuficientes para treinar o modelo")
        return
    
    # Preparar datasets
    X = df_ml[features].fillna(0)
    y = df_ml['empresa_problematica']
    
    if y.nunique() < 2 or len(df_ml) < 100:
        st.warning("⚠️ Dados insuficientes ou sem variação na variável alvo")
        return
    
    # Split treino/teste
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    
    # Abas para diferentes análises
    tabs = st.tabs(["🎯 Modelo Preditivo", "🔍 Empresas em Risco", "📊 Análise de Features"])
    
    with tabs[0]:
        st.subheader("🎯 Treinamento do Modelo")
        
        modelo_escolhido = st.selectbox(
            "Escolha o algoritmo:",
            ["Gradient Boosting", "Random Forest"]
        )
        
        if st.button("🚀 Treinar Modelo"):
            with st.spinner("Treinando modelo..."):
                if modelo_escolhido == "Gradient Boosting":
                    modelo = GradientBoostingClassifier(n_estimators=100, random_state=42)
                else:
                    modelo = RandomForestClassifier(n_estimators=100, random_state=42)
                
                modelo.fit(X_train, y_train)
                y_pred = modelo.predict(X_test)
                y_pred_proba = modelo.predict_proba(X_test)[:, 1]
                
                # Métricas
                st.markdown("### 📈 Performance do Modelo")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    acc = accuracy_score(y_test, y_pred)
                    st.metric("Acurácia", f"{acc:.2%}")
                
                with col2:
                    prec = precision_score(y_test, y_pred)
                    st.metric("Precisão", f"{prec:.2%}")
                
                with col3:
                    rec = recall_score(y_test, y_pred)
                    st.metric("Recall", f"{rec:.2%}")
                
                with col4:
                    f1 = f1_score(y_test, y_pred)
                    st.metric("F1-Score", f"{f1:.2%}")
                
                # Matriz de confusão
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 🎲 Matriz de Confusão")
                    cm = confusion_matrix(y_test, y_pred)
                    fig = px.imshow(
                        cm,
                        labels=dict(x="Predito", y="Real"),
                        x=['Normal', 'Problemática'],
                        y=['Normal', 'Problemática'],
                        text_auto=True,
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.markdown("### 🔥 Features Mais Importantes")
                    importancias = pd.DataFrame({
                        'feature': features,
                        'importancia': modelo.feature_importances_
                    }).sort_values('importancia', ascending=False).head(10)
                    
                    fig = px.bar(
                        importancias,
                        x='importancia',
                        y='feature',
                        orientation='h',
                        title="Top 10 Features"
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                
                # Salvar modelo em session_state
                st.session_state['modelo_ml'] = modelo
                st.session_state['features_ml'] = features
                st.success("✅ Modelo treinado com sucesso!")
    
    with tabs[1]:
        st.subheader("🔍 Empresas em Alto Risco")
        
        if 'modelo_ml' in st.session_state:
            modelo = st.session_state['modelo_ml']
            features_usadas = st.session_state['features_ml']
            
            # Prever para todas as empresas
            X_all = df_ml[features_usadas].fillna(0)
            df_ml['prob_risco'] = modelo.predict_proba(X_all)[:, 1]
            
            # Filtrar empresas em risco (não problemáticas atualmente)
            df_risco = df_ml[
                (df_ml['empresa_problematica'] == 0) &
                (df_ml['prob_risco'] > 0.5)
            ].nlargest(30, 'prob_risco')
            
            if not df_risco.empty:
                st.warning(f"⚠️ {len(df_risco)} empresas identificadas com alto risco")
                
                st.dataframe(
                    df_risco[[
                        'nm_razao_social', 'cnae_classe', 'porte_empresa',
                        'prob_risco', 'vl_faturamento', 'aliq_efetiva_empresa'
                    ]],
                    hide_index=True,
                    column_config={
                        'nm_razao_social': 'Razão Social',
                        'cnae_classe': 'CNAE',
                        'porte_empresa': 'Porte',
                        'prob_risco': st.column_config.NumberColumn(
                            'Prob. Risco',
                            format="%.2%"
                        ),
                        'vl_faturamento': st.column_config.NumberColumn(
                            'Faturamento',
                            format="R$ %.2f"
                        ),
                        'aliq_efetiva_empresa': st.column_config.NumberColumn(
                            'Alíquota',
                            format="%.2%"
                        )
                    },
                    height=500
                )
                
                # Download
                csv = df_risco.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 Download Lista de Risco",
                    csv,
                    f"empresas_alto_risco_{periodo}.csv",
                    "text/csv"
                )
            else:
                st.success("✅ Nenhuma empresa em alto risco identificada")
        else:
            st.info("👆 Treine o modelo na aba anterior para ver esta análise")
    
    with tabs[2]:
        st.subheader("📊 Análise Detalhada de Features")
        
        if 'modelo_ml' in st.session_state:
            # Distribuição das features por classe
            col1, col2 = st.columns(2)
            
            with col1:
                feature_analise = st.selectbox(
                    "Selecione uma feature para análise:",
                    [f for f in features if not f.startswith('porte_')]
                )
                
                fig = px.box(
                    df_ml,
                    x='empresa_problematica',
                    y=feature_analise,
                    color='empresa_problematica',
                    title=f"Distribuição de {feature_analise}",
                    labels={'empresa_problematica': 'Tipo', feature_analise: 'Valor'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'prob_risco' in df_ml.columns:
                    fig = px.histogram(
                        df_ml,
                        x='prob_risco',
                        color='empresa_problematica',
                        title="Distribuição de Probabilidades",
                        labels={'prob_risco': 'Probabilidade de Risco'},
                        nbins=50
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("👆 Treine o modelo primeiro")

# =============================================================================
# 14. SEÇÃO: ANÁLISES AVANÇADAS
# =============================================================================

def render_analises_avancadas(dados, periodo):
    st.header("📊 Análises Avançadas")
    
    # Filtro de período
    periodos = sorted(dados['benchmark_setorial']['nu_per_ref'].unique()) if not dados['benchmark_setorial'].empty else []
    if periodos:
        periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1)
    
    tabs = st.tabs([
        "📈 Evolução Temporal",
        "🎯 Volatilidade",
        "💰 ICMS vs Pagamentos",
        "🔍 Comparações"
    ])
    
    # Tab 1: Evolução Temporal
    with tabs[0]:
        st.subheader("📈 Evolução Temporal dos Setores")
        
        df_benchmark = dados['benchmark_setorial']
        if not df_benchmark.empty:
            # Top 10 setores
            top_setores_cnae = df_benchmark.groupby('cnae_classe')['faturamento_total'].sum().nlargest(10).index
            df_top = df_benchmark[df_benchmark['cnae_classe'].isin(top_setores_cnae)].copy()
            
            df_top['aliq_pct'] = df_top['aliq_efetiva_mediana'] * 100
            df_top['periodo_str'] = df_top['nu_per_ref'].astype(str)
            
            fig = px.line(
                df_top,
                x='periodo_str',
                y='aliq_pct',
                color='desc_cnae_classe',
                title="Evolução da Alíquota Mediana - Top 10 Setores",
                labels={'periodo_str': 'Período', 'aliq_pct': 'Alíquota (%)'}
            )
            fig.update_layout(hovermode='x unified', height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: Volatilidade
    with tabs[1]:
        st.subheader("🎯 Análise de Volatilidade")
        
        df_evolucao = dados['evolucao_setor']
        if not df_evolucao.empty:
            df_vol = df_evolucao[df_evolucao['categoria_volatilidade_temporal'].isin(['ALTA', 'MEDIA'])].copy()
            df_vol['aliq_media_pct'] = df_vol['aliq_mediana_media_8m'] * 100
            df_vol['fat_milhoes'] = df_vol['faturamento_acumulado_8m'] / 1e6
            
            fig = px.scatter(
                df_vol,
                x='coef_variacao_temporal',
                y='aliq_media_pct',
                size='fat_milhoes',
                color='categoria_volatilidade_temporal',
                hover_data=['desc_cnae_classe'],
                title="Volatilidade vs Alíquota Média",
                labels={
                    'coef_variacao_temporal': 'Coeficiente de Variação',
                    'aliq_media_pct': 'Alíquota Média (%)'
                }
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: ICMS vs Pagamentos
    with tabs[2]:
        st.subheader("💰 Divergências ICMS vs Pagamentos")
        
        df_empresas = dados['empresa_vs_benchmark'][
            dados['empresa_vs_benchmark']['nu_per_ref'] == periodo
        ] if not dados['empresa_vs_benchmark'].empty else pd.DataFrame()
        
        if not df_empresas.empty:
            df_div = df_empresas[df_empresas['flag_divergencia_pagamento'] == 1].copy()
            
            if not df_div.empty:
                st.warning(f"⚠️ {len(df_div):,} empresas com divergências detectadas")
                
                df_div['diferenca'] = df_div['icms_recolher'] - df_div['valor_total_pago']
                df_div_top = df_div.nlargest(20, 'diferenca')[
                    ['nu_cnpj', 'nm_razao_social', 'icms_recolher', 
                     'valor_total_pago', 'diferenca']
                ]
                
                st.dataframe(
                    df_div_top,
                    hide_index=True,
                    column_config={
                        'nu_cnpj': 'CNPJ',
                        'nm_razao_social': 'Razão Social',
                        'icms_recolher': st.column_config.NumberColumn(
                            'ICMS a Recolher',
                            format="R$ %.2f"
                        ),
                        'valor_total_pago': st.column_config.NumberColumn(
                            'Valor Pago',
                            format="R$ %.2f"
                        ),
                        'diferenca': st.column_config.NumberColumn(
                            'Diferença',
                            format="R$ %.2f"
                        )
                    }
                )
    
    # Tab 4: Comparações
    with tabs[3]:
        st.subheader("🔍 Comparações Setoriais")
        
        df_benchmark = dados['benchmark_setorial'][
            dados['benchmark_setorial']['nu_per_ref'] == periodo
        ] if not dados['benchmark_setorial'].empty else pd.DataFrame()
        
        if not df_benchmark.empty:
            df_comp = df_benchmark.nlargest(20, 'faturamento_total').copy()
            df_comp['aliq_pct'] = df_comp['aliq_efetiva_mediana'] * 100
            df_comp['fat_milhoes'] = df_comp['faturamento_total'] / 1e6
            
            fig = px.bar(
                df_comp,
                x='desc_cnae_classe',
                y='aliq_pct',
                title="Alíquota Mediana - Top 20 Setores",
                labels={'desc_cnae_classe': 'Setor', 'aliq_pct': 'Alíquota (%)'}
            )
            fig.update_xaxes(tickangle=-45)
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# 15. SEÇÃO: RELATÓRIOS
# =============================================================================

def render_relatorios(dados, periodo):
    st.header("📋 Relatórios Gerenciais")
    st.markdown("Gere resumos executivos e insights automáticos a partir dos dados analisados.")
    
    # Filtro de período
    periodos = sorted(dados['empresas']['nu_per_ref'].unique()) if not dados['empresas'].empty else []
    if periodos:
        periodo = st.selectbox("📅 Período de Referência", periodos, index=len(periodos)-1)
    
    # Relatório Executivo
    st.markdown('<div class="insight-box">', unsafe_allow_html=True)
    st.markdown("### 🎯 Relatório Executivo - Sistema ARGOS Setores")
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Métricas consolidadas
    df_empresas = dados.get('empresas', pd.DataFrame())
    df_alertas = dados.get('alertas', pd.DataFrame())
    df_benchmark = dados.get('benchmark_setorial', pd.DataFrame())
    df_anomalias = dados.get('anomalias', pd.DataFrame())
    
    # Resumo do período
    st.subheader(f"📊 Período de Referência: {periodo}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📈 Volumes Gerais**")
        if not df_empresas.empty:
            empresas_periodo = df_empresas[df_empresas['nu_per_ref'] == periodo]
            st.write(f"• **Empresas:** {empresas_periodo['nu_cnpj'].nunique():,}")
            st.write(f"• **Setores:** {empresas_periodo['cnae_classe'].nunique():,}")
            fat_total = empresas_periodo['vl_faturamento'].sum() / 1e12
            st.write(f"• **Faturamento:** R$ {fat_total:.2f} Tri")
    
    with col2:
        st.markdown("**⚠️ Alertas e Riscos**")
        if not df_alertas.empty:
            alertas_periodo = df_alertas[df_alertas['nu_per_ref'] == periodo]
            st.write(f"• **Total Alertas:** {len(alertas_periodo):,}")
            criticos = len(alertas_periodo[alertas_periodo['severidade'] == 'CRITICO'])
            st.write(f"• **Críticos:** {criticos:,}")
            st.write(f"• **Empresas:** {alertas_periodo['nu_cnpj'].nunique():,}")
    
    with col3:
        st.markdown("**🏭 Anomalias Setoriais**")
        if not df_anomalias.empty:
            anomalias_periodo = df_anomalias[df_anomalias['nu_per_ref'] == periodo]
            st.write(f"• **Setores:** {len(anomalias_periodo):,}")
            alta_sev = len(anomalias_periodo[anomalias_periodo['severidade'] == 'ALTA'])
            st.write(f"• **Alta Severidade:** {alta_sev:,}")
    
    # Principais achados
    st.markdown("---")
    st.subheader("🔍 Principais Achados")
    
    achados = []
    
    if not df_alertas.empty:
        alertas_periodo = df_alertas[df_alertas['nu_per_ref'] == periodo]
        tipo_mais_comum = alertas_periodo['tipo_alerta'].mode()[0] if not alertas_periodo.empty else "N/A"
        qtd_tipo = len(alertas_periodo[alertas_periodo['tipo_alerta'] == tipo_mais_comum])
        achados.append(
            f"• O tipo de alerta mais frequente é **{tipo_mais_comum}** com {qtd_tipo:,} ocorrências"
        )
    
    if not df_anomalias.empty:
        anomalias_periodo = df_anomalias[df_anomalias['nu_per_ref'] == periodo]
        if not anomalias_periodo.empty:
            setor_maior_score = anomalias_periodo.nlargest(1, 'score_relevancia').iloc[0]
            achados.append(
                f"• Setor **{setor_maior_score['desc_cnae_classe']}** apresenta maior score de relevância ({setor_maior_score['score_relevancia']:.1f})"
            )
    
    if not df_empresas.empty:
        empresas_periodo = df_empresas[df_empresas['nu_per_ref'] == periodo]
        porte_dist = empresas_periodo['porte_empresa'].value_counts()
        if not porte_dist.empty:
            porte_predominante = porte_dist.index[0]
            pct_porte = (porte_dist.iloc[0] / len(empresas_periodo)) * 100
            achados.append(
                f"• **{pct_porte:.1f}%** das empresas são de porte **{porte_predominante}**"
            )
    
    for achado in achados:
        st.markdown(achado)
    
    # Recomendações
    st.markdown("---")
    st.markdown('<div class="warning-box">', unsafe_allow_html=True)
    st.markdown("### 💡 Recomendações Estratégicas")
    st.markdown("""
    1. **Priorização de Fiscalização**
       - Focar em empresas com alertas críticos e alto score de risco
       - Priorizar setores com anomalias de alta severidade
    
    2. **Monitoramento Contínuo**
       - Acompanhar empresas com alta volatilidade fiscal
       - Monitorar divergências entre ICMS devido e pagamentos realizados
    
    3. **Ações Preventivas**
       - Desenvolver orientações específicas para setores problemáticos
       - Implementar comunicação preventiva com empresas em risco
    
    4. **Otimização de Processos**
       - Utilizar modelos preditivos para seleção de alvos
       - Automatizar identificação de padrões anômalos
    
    5. **Análise Setorial**
       - Investigar setores com alta concentração de alertas
       - Desenvolver benchmarks específicos por porte e setor
    """)
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Tabelas de suporte
    st.markdown("---")
    st.subheader("📊 Tabelas de Suporte")
    
    tab1, tab2, tab3 = st.tabs(["Top Setores", "Evolução Temporal", "Distribuições"])
    
    with tab1:
        if not df_benchmark.empty:
            benchmark_periodo = df_benchmark[df_benchmark['nu_per_ref'] == periodo]
            top_setores = benchmark_periodo.nlargest(10, 'faturamento_total')
            
            st.dataframe(
                top_setores[[
                    'cnae_classe', 'desc_cnae_classe', 'faturamento_total',
                    'qtd_empresas_total', 'aliq_efetiva_mediana'
                ]],
                hide_index=True,
                column_config={
                    'cnae_classe': 'CNAE',
                    'desc_cnae_classe': 'Descrição',
                    'faturamento_total': st.column_config.NumberColumn(
                        'Faturamento',
                        format="R$ %.2f"
                    ),
                    'qtd_empresas_total': 'Empresas',
                    'aliq_efetiva_mediana': st.column_config.NumberColumn(
                        'Alíq. Mediana',
                        format="%.2%"
                    )
                }
            )
    
    with tab2:
        if not df_empresas.empty:
            evolucao = df_empresas.groupby('nu_per_ref').agg({
                'nu_cnpj': 'nunique',
                'vl_faturamento': 'sum',
                'icms_devido': 'sum'
            }).reset_index()
            
            evolucao['periodo_str'] = evolucao['nu_per_ref'].astype(str)
            evolucao['fat_bilhoes'] = evolucao['vl_faturamento'] / 1e9
            
            fig = px.line(
                evolucao,
                x='periodo_str',
                y='fat_bilhoes',
                title="Evolução do Faturamento Total",
                labels={'periodo_str': 'Período', 'fat_bilhoes': 'Faturamento (R$ Bi)'},
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            if not df_alertas.empty:
                alertas_periodo = df_alertas[df_alertas['nu_per_ref'] == periodo]
                sev_dist = alertas_periodo['severidade'].value_counts()
                
                fig = px.pie(
                    sev_dist,
                    values=sev_dist.values,
                    names=sev_dist.index,
                    title="Distribuição de Alertas por Severidade"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if not df_empresas.empty:
                empresas_periodo = df_empresas[df_empresas['nu_per_ref'] == periodo]
                porte_dist = empresas_periodo['porte_empresa'].value_counts()
                
                fig = px.bar(
                    porte_dist,
                    x=porte_dist.index,
                    y=porte_dist.values,
                    title="Distribuição por Porte Empresarial",
                    labels={'x': 'Porte', 'y': 'Quantidade'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Botão de exportação
    st.markdown("---")
    if st.button("📥 Gerar Relatório Completo (Em desenvolvimento)"):
        st.info("Funcionalidade de exportação em PDF será implementada em breve")

# =============================================================================
# 16. EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    main()