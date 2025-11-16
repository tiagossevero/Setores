# 📊 ARGOS SETORES - Sistema de Análise Tributária Setorial v4.0

> Sistema avançado de análise tributária setorial com Machine Learning para a Receita Estadual de Santa Catarina

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red.svg)](https://streamlit.io/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-Receita%20SC-green.svg)]()

## 📋 Sumário

- [Sobre o Projeto](#-sobre-o-projeto)
- [Funcionalidades](#-funcionalidades)
- [Tecnologias](#-tecnologias)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#-configuração)
- [Como Usar](#-como-usar)
- [Análises Disponíveis](#-análises-disponíveis)
- [Machine Learning](#-machine-learning)
- [Arquitetura de Dados](#-arquitetura-de-dados)
- [Contribuindo](#-contribuindo)
- [Licença](#-licença)

## 🎯 Sobre o Projeto

O **ARGOS SETORES** é um sistema completo de análise tributária que permite à Receita Estadual de Santa Catarina identificar padrões, anomalias e tendências no comportamento tributário de empresas agrupadas por setor econômico (CNAE).

### Objetivos Principais

- **Benchmark Setorial**: Comparar comportamento tributário de empresas dentro do mesmo setor
- **Detecção de Anomalias**: Identificar empresas com padrões tributários atípicos usando Machine Learning
- **Análise Temporal**: Acompanhar evolução de alíquotas efetivas e faturamento ao longo do tempo
- **Priorização Fiscal**: Gerar scores de risco para orientar ações fiscalizatórias
- **Análise Preditiva**: Prever tendências de alíquotas e comportamento setorial

## ✨ Funcionalidades

### Dashboard Interativo

- **📈 Visão Geral**: Métricas consolidadas do sistema (setores, empresas, faturamento, ICMS)
- **🏭 Análise Setorial**: Benchmark detalhado por CNAE classe com quartis e medianas
- **🏢 Análise Empresarial**: Comparação de empresas individuais vs. benchmark do setor
- **⚠️ Alertas e Anomalias**: Sistema de alertas por severidade (CRÍTICO, ALTO, MÉDIO, BAIXO)
- **⏱️ Evolução Temporal**: Séries históricas de alíquotas e faturamento (8 meses)
- **📉 Análise de Volatilidade**: Coeficiente de variação temporal por setor e empresa
- **💰 Análise de Pagamentos**: Divergências entre ICMS devido e pagamentos realizados
- **🤖 Machine Learning**: Clustering, outliers e previsões
- **📊 Análises Avançadas**: Correlações, regressões e visualizações complexas
- **📋 Relatórios**: Exportação de análises em múltiplos formatos

### Análises com Notebooks Jupyter

- Análises exploratórias com PySpark
- Visualizações avançadas (heatmaps, scatter plots, time series)
- Modelos de Machine Learning (K-Means, Isolation Forest, Random Forest, Regressão Linear)
- Resumos executivos automatizados

## 🛠️ Tecnologias

### Backend & Processamento

- **Python 3.8+**: Linguagem principal
- **PySpark 3.5**: Processamento distribuído de grandes volumes de dados
- **Pandas**: Manipulação de dados tabulares
- **NumPy**: Operações numéricas

### Frontend & Visualização

- **Streamlit**: Dashboard web interativo
- **Plotly**: Visualizações interativas
- **Matplotlib**: Gráficos estáticos
- **Seaborn**: Visualizações estatísticas

### Machine Learning

- **Scikit-learn**: Algoritmos de ML
  - K-Means: Clustering de setores similares
  - Isolation Forest: Detecção de outliers setoriais
  - Random Forest: Score de risco composto
  - Linear Regression: Previsão de tendências
  - PCA: Redução de dimensionalidade

### Banco de Dados

- **Apache Impala**: Queries SQL em larga escala
- **Apache Hive**: Data warehouse
- **SQLAlchemy**: ORM e conexão com bancos

## 📁 Estrutura do Projeto

```
Setores/
│
├── SETORES.py                      # Dashboard principal (Streamlit - 2324 linhas)
├── SETORES (1).ipynb               # Notebook de análises completas (1.7 MB)
├── SETORES-exemplo (4).ipynb       # Notebook de exemplos (46 KB)
├── SETORES.json                    # Dados/configurações (258 KB)
├── README.md                       # Este arquivo
│
└── .git/                           # Controle de versão
```

## 📋 Pré-requisitos

### Software

- Python 3.8 ou superior
- Apache Spark 3.5+
- Jupyter Notebook/Lab (opcional, para notebooks)
- Acesso ao cluster Impala da Receita SC

### Dependências Python

```bash
streamlit>=1.28.0
pandas>=1.5.0
numpy>=1.23.0
plotly>=5.14.0
matplotlib>=3.7.0
seaborn>=0.12.0
scikit-learn>=1.3.0
sqlalchemy>=2.0.0
pyspark>=3.5.0
impyla>=0.18.0
```

## 🚀 Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/receita-sc/Setores.git
cd Setores
```

### 2. Crie um Ambiente Virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

### 3. Instale as Dependências

```bash
pip install -r requirements.txt
```

Se o arquivo `requirements.txt` não existir, instale manualmente:

```bash
pip install streamlit pandas numpy plotly matplotlib seaborn scikit-learn sqlalchemy pyspark impyla
```

## ⚙️ Configuração

### 1. Configure as Credenciais Impala

Crie o arquivo `.streamlit/secrets.toml`:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

### 2. Configure a Senha do Dashboard

Edite a linha 9 do arquivo `SETORES.py`:

```python
SENHA = "sua_senha_aqui"  # Altere esta senha
```

### 3. Configure o Ambiente Spark (para notebooks)

No notebook, ajuste os caminhos no primeiro cell:

```python
sys.path.append("/caminho/para/data-pipeline/batch/poc")
sys.path.append("/caminho/para/data-pipeline/batch/plugins")
sys.path.append("/caminho/para/data-pipeline/batch/dags")
```

## 📖 Como Usar

### Dashboard Streamlit

Execute o dashboard:

```bash
streamlit run SETORES.py
```

Acesse no navegador: `http://localhost:8501`

1. **Login**: Digite a senha configurada
2. **Navegação**: Use o menu lateral para escolher a análise
3. **Filtros**: Selecione período, setor (CNAE), porte, etc.
4. **Visualizações**: Interaja com gráficos (zoom, pan, hover)
5. **Exportação**: Baixe dados e relatórios

### Notebooks Jupyter

Execute o Jupyter:

```bash
jupyter notebook
```

Abra os notebooks:

- `SETORES (1).ipynb`: Análises completas com ML
- `SETORES-exemplo (4).ipynb`: Exemplos de uso

Execute as células sequencialmente (Shift + Enter)

## 📊 Análises Disponíveis

### 1. Benchmark Setorial

Calcula estatísticas descritivas por CNAE classe:

- **Alíquota Efetiva**: P25, Mediana, P75, Média, Desvio Padrão
- **Faturamento Total**: Soma por setor/período
- **ICMS Devido**: Total arrecadado
- **Quantidade de Empresas**: Contagem por porte

**Tabelas Utilizadas**: `argos_benchmark_setorial`, `argos_benchmark_setorial_porte`

### 2. Empresa vs Benchmark

Compara cada empresa com o benchmark do seu setor:

- **Status**: MUITO_ABAIXO, ABAIXO, NORMAL, ACIMA, MUITO_ACIMA
- **Desvio Padrão**: Distância da mediana setorial
- **Quartil**: Posicionamento (Q1, Q2, Q3, Q4)
- **Divergências de Pagamento**: ICMS devido vs pago

**Tabelas Utilizadas**: `argos_empresa_vs_benchmark`

### 3. Evolução Temporal

Séries históricas (8 meses) de:

- **Setores**: Alíquota mediana média, coeficiente de variação, faturamento acumulado
- **Empresas**: Volatilidade individual, categoria (ALTA, MÉDIA, BAIXA)

**Tabelas Utilizadas**: `argos_evolucao_temporal_setor`, `argos_evolucao_temporal_empresa`

### 4. Anomalias Setoriais

Detecta setores com padrões anômalos:

- **Tipos**: Economia Atípica, Alta Volatilidade, Divergência de Pagamentos
- **Severidade**: ALTA, MÉDIA, BAIXA
- **Score de Relevância**: 0-100 (baseado em impacto fiscal)

**Tabelas Utilizadas**: `argos_anomalias_setoriais`

### 5. Alertas Empresariais

Sistema de alertas individuais:

- **Critérios**: Alíquota muito abaixo, volatilidade alta, divergência de pagamento
- **Score de Risco**: 0-100 (calculado por múltiplos fatores)
- **Prioridade**: Para orientar fiscalização

**Tabelas Utilizadas**: `argos_alertas_empresas`

## 🤖 Machine Learning

### 1. Clustering de Setores Similares (K-Means)

**Objetivo**: Agrupar setores com comportamento tributário similar

**Features Utilizadas**:
- Alíquota mediana média (8 meses)
- Coeficiente de variação temporal
- Faturamento acumulado
- ICMS devido acumulado
- Média de empresas/mês

**Método**:
- Normalização: StandardScaler
- Algoritmo: K-Means
- Otimização: Método Elbow + Silhouette Score
- Visualização: PCA 2D

**Saída**: Clusters de setores com características similares

### 2. Detecção de Outliers (Isolation Forest)

**Objetivo**: Identificar setores com comportamento atípico

**Features Utilizadas**:
- Alíquota mediana média
- Coeficiente de variação
- Log do faturamento
- Amplitude de alíquota (max - min)

**Método**:
- Algoritmo: Isolation Forest
- Contamination: 10% (ajustável)
- Score: -1 (outlier) ou 1 (normal)

**Saída**: Lista de setores outliers para investigação prioritária

### 3. Previsão de Tendências (Regressão Linear)

**Objetivo**: Prever alíquotas efetivas dos próximos 6 meses

**Método**:
- Algoritmo: Linear Regression
- Features: Período numérico (dias desde início)
- Target: Alíquota mediana
- Validação: R² Score

**Saída**: Série temporal com valores históricos + previsões

### 4. Score de Risco Composto (Random Forest)

**Objetivo**: Calcular score de risco para priorização fiscal

**Features Utilizadas**:
- Diferença de alíquota vs setor
- Status vs setor (encoded)
- Categoria de volatilidade (encoded)
- Coeficiente de variação (8 meses)
- Flag de divergência de pagamento
- Score de alerta existente

**Método**:
- Algoritmo: Random Forest Regressor
- Target Sintético: Critérios múltiplos (alíquota muito abaixo, volatilidade alta, divergência)
- Feature Importance: Identifica variáveis mais relevantes

**Saída**: Score ML (0-100) para cada empresa

## 🗄️ Arquitetura de Dados

### Schema do Banco (niat)

#### Tabelas Base

```sql
-- Empresas (base)
niat.argos_empresas
  - nu_cnpj, nm_razao_social, cnae_classe, porte_empresa
  - nu_per_ref, vl_faturamento, icms_devido, aliq_efetiva

-- Pagamentos
niat.argos_pagamentos_empresa
  - nu_cnpj, nu_per_ref, valor_total_pago
```

#### Views Analíticas

```sql
-- Benchmark Setorial
niat.argos_benchmark_setorial
  - Estatísticas por CNAE: mediana, P25, P75, média, stddev

-- Benchmark por Porte
niat.argos_benchmark_setorial_porte
  - Estatísticas por CNAE + Porte

-- Empresa vs Benchmark
niat.argos_empresa_vs_benchmark
  - Comparação empresa individual vs setor

-- Evolução Temporal Setorial
niat.argos_evolucao_temporal_setor
  - Séries históricas (8 meses) por setor

-- Evolução Temporal Empresarial
niat.argos_evolucao_temporal_empresa
  - Séries históricas (8 meses) por empresa

-- Anomalias Setoriais
niat.argos_anomalias_setoriais
  - Setores com padrões anômalos

-- Alertas Empresariais
niat.argos_alertas_empresas
  - Alertas individuais com score de risco
```

### Conexão Impala

```python
from sqlalchemy import create_engine

engine = create_engine(
    f'impala://{HOST}:{PORT}/{DATABASE}',
    connect_args={
        'user': USER,
        'password': PASSWORD,
        'auth_mechanism': 'LDAP',
        'use_ssl': True
    }
)
```

## 📈 Métricas do Sistema

### Volumetria (referência)

- **Setores Analisados**: 342 CNAE classes
- **Empresas**: ~71.000 contribuintes
- **Períodos**: 8 meses de histórico
- **Faturamento Total**: ~R$ 100 bilhões
- **ICMS Devido**: ~R$ 12 bilhões
- **Registros Totais**: +500.000 linhas

### Performance

- **Carga de Dados**: ~10-30 segundos (cache 1h)
- **Queries Impala**: 2-15 segundos
- **Clustering ML**: ~5 segundos (341 setores)
- **Dashboard**: Renderização instantânea (após carga)

## 🔒 Segurança

### Autenticação

- **Dashboard**: Senha única configurável
- **Banco**: Credenciais LDAP via secrets.toml
- **SSL**: Conexão criptografada com Impala

### Boas Práticas

- ✅ Credenciais em arquivo separado (.gitignore)
- ✅ Conexão SSL obrigatória
- ✅ Session state para autenticação
- ✅ Cache de dados (evita queries excessivas)
- ⚠️ **IMPORTANTE**: Nunca commitar senhas no código

## 🐛 Troubleshooting

### Erro de Conexão Impala

```python
# Desabilitar verificação SSL (apenas desenvolvimento)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
```

### Conflito PySpark vs Python Built-ins

```python
# Salvar referências
import builtins
max_builtin = builtins.max
min_builtin = builtins.min
abs_builtin = builtins.abs

# Usar versões built-in
valor_max = max_builtin(lista)

# Ou usar NumPy
import numpy as np
valor_max = np.max(lista)
```

### Dashboard Não Carrega Dados

1. Verificar credenciais em `.streamlit/secrets.toml`
2. Testar conexão Impala manualmente
3. Verificar VPN/rede interna
4. Verificar logs no sidebar do dashboard

### Erro de Memória (Notebooks)

```python
# Reduzir período de análise
df = spark.sql("... WHERE nu_per_ref >= 202501")  # Últimos meses

# Limitar resultados
df.limit(10000).toPandas()

# Liberar cache
spark.catalog.clearCache()
```

## 📚 Documentação Adicional

### Jupyter Notebooks

- **SETORES (1).ipynb**: Análise completa com 9 seções (ML, visualizações, relatórios)
- **SETORES-exemplo (4).ipynb**: Tutorial básico de uso do sistema

### Relatórios Gerados

- Resumo executivo com métricas principais
- Top setores por faturamento
- Ranking de empresas por score de risco
- Lista de outliers setoriais
- Matriz de correlação entre variáveis

## 🤝 Contribuindo

### Como Contribuir

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/NovaAnalise`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova análise X'`)
4. Push para a branch (`git push origin feature/NovaAnalise`)
5. Abra um Pull Request

### Padrões de Código

- **Python**: PEP 8
- **SQL**: Indentação 4 espaços
- **Commits**: Mensagens descritivas em português
- **Documentação**: Docstrings em funções

## 📝 Changelog

### v4.0 (2025-10-02)

- ✨ Sistema completo de Machine Learning
- ✨ Dashboard Streamlit com 10 seções
- ✨ Análise de evolução temporal (8 meses)
- ✨ Score de risco composto (Random Forest)
- ✨ Detecção de anomalias setoriais
- ✨ Previsão de tendências (6 meses)
- ✨ Clustering de setores similares
- 🐛 Correções de conflitos PySpark vs built-ins

### v3.0 (anterior)

- Análises básicas de benchmark
- Comparação empresa vs setor
- Alertas simples

## 📄 Licença

Este projeto é propriedade da **Receita Estadual de Santa Catarina** e é de uso exclusivo interno para fins de análise tributária e fiscalização.

**Confidencial** - Não distribuir sem autorização.

## 👥 Autores

**Equipe NIAT - Núcleo de Inteligência e Análise Tributária**
Receita Estadual de Santa Catarina

### Contato

- **Email**: niat@sef.sc.gov.br
- **Desenvolvedor Principal**: tsevero
- **Infraestrutura**: Big Data SAT/SEF-SC

## 🙏 Agradecimentos

- Equipe de Big Data da Receita SC
- Comunidade PySpark e Streamlit
- Scikit-learn e Plotly developers

---

**Desenvolvido com ❤️ para a Receita Estadual de Santa Catarina**

*Última atualização: 2025-11-16*
