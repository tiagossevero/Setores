#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
GERADOR DE DATA-SCHEMAS PARA ARGOS SETORES
=============================================================================
Gera automaticamente os arquivos de data-schema com:
- DESCRIBE FORMATTED
- SELECT * FROM ... LIMIT 10

Para executar em Jupyter Notebook, copie o código após a célula de
configuração da sessão Spark.
=============================================================================
"""

import os
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

# Lista de tabelas a serem documentadas
TABELAS_ORIGINAIS = [
    "usr_sat_ods.vw_ods_decl_dime",
    "usr_sat_ods.vw_ods_contrib",
    "usr_sat_ods.vw_ods_pagamento"
]

TABELAS_INTERMEDIARIAS = [
    "niat.argos_benchmark_setorial",
    "niat.argos_benchmark_setorial_porte",
    "niat.argos_empresas",
    "niat.argos_pagamentos_empresa",
    "niat.argos_empresa_vs_benchmark",
    "niat.argos_evolucao_temporal_setor",
    "niat.argos_evolucao_temporal_empresa",
    "niat.argos_anomalias_setoriais",
    "niat.argos_alertas_empresas"
]

VIEWS_AUXILIARES = [
    "niat.vw_dashboard_setores",
    "niat.vw_dashboard_empresas",
    "niat.vw_analise_volatilidade",
    "niat.vw_relacao_icms_pagamentos"
]

# Diretório de saída
OUTPUT_DIR = "./data-schema"

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def criar_diretorio_output(base_dir):
    """Cria estrutura de diretórios para os data-schemas."""
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    Path(f"{base_dir}/originais").mkdir(exist_ok=True)
    Path(f"{base_dir}/intermediarias").mkdir(exist_ok=True)
    Path(f"{base_dir}/views").mkdir(exist_ok=True)
    print(f"✅ Diretórios criados em: {base_dir}")


def formatar_nome_arquivo(tabela_completa):
    """Converte nome da tabela para nome de arquivo."""
    # usr_sat_ods.vw_ods_decl_dime -> vw_ods_decl_dime
    nome_tabela = tabela_completa.split('.')[-1]
    return f"{nome_tabela}.md"


def executar_describe_formatted(spark, tabela):
    """Executa DESCRIBE FORMATTED e retorna resultado como Pandas DataFrame."""
    try:
        print(f"  📋 Executando DESCRIBE FORMATTED {tabela}...")
        df = spark.sql(f"DESCRIBE FORMATTED {tabela}")
        return df.toPandas()
    except Exception as e:
        print(f"  ❌ ERRO ao descrever {tabela}: {e}")
        return None


def executar_select_sample(spark, tabela, limit=10):
    """Executa SELECT * LIMIT e retorna resultado como Pandas DataFrame."""
    try:
        print(f"  📊 Executando SELECT * FROM {tabela} LIMIT {limit}...")
        df = spark.sql(f"SELECT * FROM {tabela} LIMIT {limit}")
        return df.toPandas()
    except Exception as e:
        print(f"  ❌ ERRO ao fazer SELECT de {tabela}: {e}")
        return None


def gerar_markdown_dataschema(tabela, df_describe, df_sample):
    """Gera conteúdo markdown do data-schema."""

    nome_tabela = tabela.split('.')[-1]
    schema_nome = tabela.split('.')[0]

    md = f"""# Data Schema: {tabela}

**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 📋 DESCRIBE FORMATTED

```sql
DESCRIBE FORMATTED {tabela};
```

### Resultado:

"""

    # Adicionar DESCRIBE FORMATTED
    if df_describe is not None and not df_describe.empty:
        md += "```\n"
        md += df_describe.to_string(index=False, max_colwidth=100)
        md += "\n```\n"
    else:
        md += "_Nenhum resultado disponível_\n"

    md += f"""

---

## 📊 SAMPLE DATA (LIMIT 10)

```sql
SELECT * FROM {tabela} LIMIT 10;
```

### Resultado:

"""

    # Adicionar SELECT SAMPLE
    if df_sample is not None and not df_sample.empty:
        md += "```\n"
        md += df_sample.to_string(index=False, max_colwidth=50)
        md += "\n```\n"

        # Adicionar informações extras
        md += f"""

### Informações Adicionais:

- **Total de colunas:** {len(df_sample.columns)}
- **Colunas:** {', '.join(df_sample.columns.tolist())}
- **Registros retornados:** {len(df_sample)}

"""
    else:
        md += "_Nenhum resultado disponível_\n"

    md += """
---

## 📝 Notas

- Este arquivo foi gerado automaticamente
- Utilize este schema como referência para análises e desenvolvimento

"""

    return md


def salvar_arquivo(conteudo, caminho):
    """Salva conteúdo em arquivo."""
    try:
        with open(caminho, 'w', encoding='utf-8') as f:
            f.write(conteudo)
        print(f"  ✅ Salvo: {caminho}")
        return True
    except Exception as e:
        print(f"  ❌ ERRO ao salvar {caminho}: {e}")
        return False


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def gerar_todos_dataschemas(spark, output_dir=OUTPUT_DIR, incluir_views=False):
    """
    Gera todos os data-schemas automaticamente.

    Parâmetros:
        spark: Sessão Spark (session.sparkSession ou spark)
        output_dir: Diretório de saída (padrão: ./data-schema)
        incluir_views: Se True, inclui views auxiliares (padrão: False)
    """

    print("=" * 80)
    print("GERADOR DE DATA-SCHEMAS - ARGOS SETORES")
    print("=" * 80)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Criar diretórios
    criar_diretorio_output(output_dir)

    # Contadores
    total = 0
    sucesso = 0
    erros = 0

    # Processar tabelas originais
    print("\n" + "=" * 80)
    print("PROCESSANDO TABELAS ORIGINAIS (ODS)")
    print("=" * 80)

    for tabela in TABELAS_ORIGINAIS:
        total += 1
        print(f"\n[{total}] Processando: {tabela}")

        df_desc = executar_describe_formatted(spark, tabela)
        df_sample = executar_select_sample(spark, tabela, limit=10)

        if df_desc is not None or df_sample is not None:
            md_content = gerar_markdown_dataschema(tabela, df_desc, df_sample)
            nome_arquivo = formatar_nome_arquivo(tabela)
            caminho = f"{output_dir}/originais/{nome_arquivo}"

            if salvar_arquivo(md_content, caminho):
                sucesso += 1
            else:
                erros += 1
        else:
            erros += 1
            print(f"  ⚠️ Pulando {tabela} (sem dados)")

    # Processar tabelas intermediárias
    print("\n" + "=" * 80)
    print("PROCESSANDO TABELAS INTERMEDIÁRIAS (NIAT)")
    print("=" * 80)

    for tabela in TABELAS_INTERMEDIARIAS:
        total += 1
        print(f"\n[{total}] Processando: {tabela}")

        df_desc = executar_describe_formatted(spark, tabela)
        df_sample = executar_select_sample(spark, tabela, limit=10)

        if df_desc is not None or df_sample is not None:
            md_content = gerar_markdown_dataschema(tabela, df_desc, df_sample)
            nome_arquivo = formatar_nome_arquivo(tabela)
            caminho = f"{output_dir}/intermediarias/{nome_arquivo}"

            if salvar_arquivo(md_content, caminho):
                sucesso += 1
            else:
                erros += 1
        else:
            erros += 1
            print(f"  ⚠️ Pulando {tabela} (sem dados)")

    # Processar views (opcional)
    if incluir_views:
        print("\n" + "=" * 80)
        print("PROCESSANDO VIEWS AUXILIARES")
        print("=" * 80)

        for tabela in VIEWS_AUXILIARES:
            total += 1
            print(f"\n[{total}] Processando: {tabela}")

            df_desc = executar_describe_formatted(spark, tabela)
            df_sample = executar_select_sample(spark, tabela, limit=10)

            if df_desc is not None or df_sample is not None:
                md_content = gerar_markdown_dataschema(tabela, df_desc, df_sample)
                nome_arquivo = formatar_nome_arquivo(tabela)
                caminho = f"{output_dir}/views/{nome_arquivo}"

                if salvar_arquivo(md_content, caminho):
                    sucesso += 1
                else:
                    erros += 1
            else:
                erros += 1
                print(f"  ⚠️ Pulando {tabela} (sem dados)")

    # Relatório final
    print("\n" + "=" * 80)
    print("RELATÓRIO FINAL")
    print("=" * 80)
    print(f"Total de tabelas processadas: {total}")
    print(f"✅ Sucesso: {sucesso}")
    print(f"❌ Erros: {erros}")
    print(f"\n📁 Arquivos salvos em: {output_dir}")
    print("\n" + "=" * 80)
    print("✅ GERAÇÃO CONCLUÍDA!")
    print("=" * 80)


# =============================================================================
# FUNÇÃO DE AJUDA PARA JUPYTER
# =============================================================================

def gerar_dataschemas_jupyter(incluir_views=False):
    """
    Versão simplificada para uso em Jupyter Notebook.

    USO:
    ----
    # Após configurar a sessão Spark, execute:
    gerar_dataschemas_jupyter(incluir_views=False)

    # Ou se quiser incluir views:
    gerar_dataschemas_jupyter(incluir_views=True)
    """

    # Tentar obter a sessão Spark do contexto
    import inspect
    frame = inspect.currentframe()

    try:
        # Buscar 'spark' ou 'session' no namespace do caller
        caller_locals = frame.f_back.f_locals
        caller_globals = frame.f_back.f_globals

        spark = None

        # Tentar diferentes formas de obter a sessão Spark
        if 'session' in caller_locals and hasattr(caller_locals['session'], 'sparkSession'):
            spark = caller_locals['session'].sparkSession
            print("✅ Usando session.sparkSession do notebook")
        elif 'session' in caller_globals and hasattr(caller_globals['session'], 'sparkSession'):
            spark = caller_globals['session'].sparkSession
            print("✅ Usando session.sparkSession do notebook")
        elif 'spark' in caller_locals:
            spark = caller_locals['spark']
            print("✅ Usando spark do notebook")
        elif 'spark' in caller_globals:
            spark = caller_globals['spark']
            print("✅ Usando spark do notebook")
        else:
            print("❌ ERRO: Sessão Spark não encontrada!")
            print("Execute primeiro a célula de configuração da sessão Spark.")
            return

        # Executar geração
        gerar_todos_dataschemas(spark, incluir_views=incluir_views)

    finally:
        del frame


# =============================================================================
# EXECUÇÃO DIRETA (STANDALONE)
# =============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("MODO STANDALONE")
    print("=" * 80)
    print("Para usar este script em Jupyter Notebook, copie as funções e execute:")
    print("\n  gerar_dataschemas_jupyter(incluir_views=False)")
    print("\nOu importe o módulo e use:")
    print("\n  from gerar_data_schemas import gerar_todos_dataschemas")
    print("  gerar_todos_dataschemas(spark, incluir_views=False)")
    print("=" * 80)
