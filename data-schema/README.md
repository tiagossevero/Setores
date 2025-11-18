# 📋 Data Schemas - ARGOS SETORES

Este diretório contém os schemas (estruturas de dados) de todas as tabelas utilizadas no projeto ARGOS SETORES.

## 📁 Estrutura

```
data-schema/
├── originais/          # Tabelas fonte (ODS)
│   ├── vw_ods_decl_dime.md
│   ├── vw_ods_contrib.md
│   └── vw_ods_pagamento.md
│
├── intermediarias/     # Tabelas processadas (NIAT)
│   ├── argos_benchmark_setorial.md
│   ├── argos_benchmark_setorial_porte.md
│   ├── argos_empresas.md
│   ├── argos_pagamentos_empresa.md
│   ├── argos_empresa_vs_benchmark.md
│   ├── argos_evolucao_temporal_setor.md
│   ├── argos_evolucao_temporal_empresa.md
│   ├── argos_anomalias_setoriais.md
│   └── argos_alertas_empresas.md
│
└── views/              # Views auxiliares (opcional)
    ├── vw_dashboard_setores.md
    ├── vw_dashboard_empresas.md
    ├── vw_analise_volatilidade.md
    └── vw_relacao_icms_pagamentos.md
```

## 📊 Conteúdo de cada arquivo

Cada arquivo `.md` contém:

1. **DESCRIBE FORMATTED** - Estrutura completa da tabela
   - Nome das colunas
   - Tipos de dados
   - Comentários
   - Propriedades da tabela
   - Localização do storage

2. **SELECT * LIMIT 10** - Exemplos de dados
   - 10 primeiros registros
   - Valores reais da tabela
   - Útil para entender o formato dos dados

3. **Informações adicionais**
   - Total de colunas
   - Lista de todas as colunas
   - Quantidade de registros retornados

## 🚀 Como gerar/atualizar

### Opção 1: Usando o Notebook (Recomendado)

1. Abra o notebook `GERAR_DATA_SCHEMAS.ipynb`
2. Execute as células em ordem:
   - Célula 1: Configurar sessão Spark
   - Célula 2: Carregar funções
   - Célula 3: Executar geração
   - Célula 4: Verificar resultados

### Opção 2: Usando o script Python

```python
# No seu notebook Jupyter, após configurar a sessão Spark:
from scripts.gerar_data_schemas import gerar_todos_dataschemas

# Gerar sem views
gerar_todos_dataschemas(spark, incluir_views=False)

# Ou gerar com views
gerar_todos_dataschemas(spark, incluir_views=True)
```

## 📝 Tabelas Documentadas

### Tabelas Originais (3)
| Tabela | Descrição |
|--------|-----------|
| `usr_sat_ods.vw_ods_decl_dime` | Declarações ICMS mensais |
| `usr_sat_ods.vw_ods_contrib` | Cadastro de contribuintes com CNAE |
| `usr_sat_ods.vw_ods_pagamento` | Histórico de pagamentos ICMS |

### Tabelas Intermediárias (9)
| Tabela | Descrição |
|--------|-----------|
| `niat.argos_benchmark_setorial` | Benchmark mensal por setor |
| `niat.argos_benchmark_setorial_porte` | Benchmark setorial por porte |
| `niat.argos_empresas` | Dados individuais de empresas |
| `niat.argos_pagamentos_empresa` | Agregação mensal de pagamentos |
| `niat.argos_empresa_vs_benchmark` | Comparação empresa vs setor |
| `niat.argos_evolucao_temporal_setor` | Tendências setoriais 8 meses |
| `niat.argos_evolucao_temporal_empresa` | Tendências empresariais 8 meses |
| `niat.argos_anomalias_setoriais` | Detecção de anomalias setoriais |
| `niat.argos_alertas_empresas` | Sistema automático de alertas |

### Views Auxiliares (4) - Opcional
| View | Descrição |
|------|-----------|
| `niat.vw_dashboard_setores` | View agregada para dashboard setorial |
| `niat.vw_dashboard_empresas` | View agregada para dashboard empresarial |
| `niat.vw_analise_volatilidade` | Análise de volatilidade temporal |
| `niat.vw_relacao_icms_pagamentos` | Relação ICMS devido vs pago |

## 🔄 Quando atualizar

Atualize os data-schemas quando:

- Adicionar novas tabelas ao projeto
- Modificar estrutura de tabelas existentes (adicionar/remover colunas)
- Mudar tipos de dados
- Preparar documentação para novos desenvolvedores
- Criar apresentações técnicas

## 💡 Dicas de Uso

1. **Consulta rápida**: Use os arquivos para consultar rapidamente a estrutura sem acessar o banco

2. **Documentação**: Inclua nos PRs quando modificar schemas

3. **Onboarding**: Material de referência para novos membros da equipe

4. **Debugging**: Compare schemas quando houver problemas de tipos de dados

5. **Análise**: Entenda os dados disponíveis antes de criar novas análises

---

**Última atualização:** Execute o notebook para ver a data/hora de geração em cada arquivo
