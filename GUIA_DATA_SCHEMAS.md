# 🚀 Guia Rápido - Geração de Data Schemas

## O que foi criado

Criei um sistema completo para gerar automaticamente os data-schemas de todas as tabelas do projeto ARGOS SETORES.

## 📦 Arquivos criados

```
Setores/
├── GERAR_DATA_SCHEMAS.ipynb          # Notebook principal (USE ESTE!)
├── GUIA_DATA_SCHEMAS.md              # Este arquivo
├── scripts/
│   └── gerar_data_schemas.py         # Script Python standalone
└── data-schema/
    ├── README.md                      # Documentação da estrutura
    ├── EXEMPLO_OUTPUT.md              # Exemplo de schema gerado
    ├── originais/                     # Schemas das tabelas ODS (será criado)
    ├── intermediarias/                # Schemas das tabelas NIAT (será criado)
    └── views/                         # Schemas das views (será criado)
```

## 🎯 Como usar

### Método Recomendado: Jupyter Notebook

1. **Abra o notebook:**
   ```bash
   jupyter notebook GERAR_DATA_SCHEMAS.ipynb
   ```

2. **Execute as células em ordem:**
   - **Célula 1:** Configura a sessão Spark
   - **Célula 2:** Carrega as funções do gerador
   - **Célula 3:** Executa a geração (escolha Opção A ou B)
   - **Célula 4:** Lista os arquivos gerados

3. **Resultado:**
   - Arquivos `.md` serão criados em `./data-schema/`
   - Cada arquivo contém DESCRIBE FORMATTED + SELECT LIMIT 10

## 📋 Tabelas que serão documentadas

### Originais (3 tabelas)
```
usr_sat_ods.vw_ods_decl_dime       # Declarações ICMS
usr_sat_ods.vw_ods_contrib         # Cadastro de contribuintes
usr_sat_ods.vw_ods_pagamento       # Pagamentos ICMS
```

### Intermediárias (9 tabelas)
```
niat.argos_benchmark_setorial           # Benchmark mensal
niat.argos_benchmark_setorial_porte     # Benchmark por porte
niat.argos_empresas                     # Dados empresas
niat.argos_pagamentos_empresa           # Agregação pagamentos
niat.argos_empresa_vs_benchmark         # Comparação empresa/setor
niat.argos_evolucao_temporal_setor      # Tendências setoriais
niat.argos_evolucao_temporal_empresa    # Tendências empresas
niat.argos_anomalias_setoriais          # Anomalias
niat.argos_alertas_empresas             # Alertas
```

### Views Auxiliares (4 views - opcional)
```
niat.vw_dashboard_setores
niat.vw_dashboard_empresas
niat.vw_analise_volatilidade
niat.vw_relacao_icms_pagamentos
```

## ⚙️ Opções de Execução

### Opção A: Sem Views (Recomendado)
- Gera apenas tabelas originais + intermediárias
- Total: 12 tabelas
- Tempo estimado: 5-10 minutos

### Opção B: Com Views
- Inclui as 4 views auxiliares
- Total: 16 tabelas/views
- Tempo estimado: 7-12 minutos

## 📊 Estrutura de cada Data Schema

Cada arquivo `.md` gerado conterá:

```markdown
# Data Schema: niat.argos_benchmark_setorial

## DESCRIBE FORMATTED
- Estrutura completa da tabela
- Tipos de dados de cada coluna
- Propriedades do Hive/Impala
- Localização no HDFS

## SAMPLE DATA (LIMIT 10)
- 10 primeiros registros
- Valores reais
- Formatação em tabela

## Informações Adicionais
- Total de colunas
- Lista de colunas
- Quantidade de registros
```

## 🔍 Comandos SQL Executados

Para cada tabela, o script executa:

```sql
-- 1. Obter estrutura
DESCRIBE FORMATTED schema.tabela;

-- 2. Obter dados de exemplo
SELECT * FROM schema.tabela LIMIT 10;
```

## 🛠️ Personalização

### Alterar quantidade de registros de exemplo

No notebook, modifique a linha:

```python
df_sample = executar_select_sample(spark, tabela, limit=10)
```

Para:

```python
df_sample = executar_select_sample(spark, tabela, limit=50)  # 50 registros
```

### Adicionar novas tabelas

No notebook, edite as listas na Célula 2:

```python
TABELAS_ORIGINAIS = [
    "usr_sat_ods.vw_ods_decl_dime",
    "usr_sat_ods.vw_ods_contrib",
    "usr_sat_ods.vw_ods_pagamento",
    "usr_sat_ods.nova_tabela"  # Adicione aqui
]
```

### Alterar diretório de saída

```python
OUTPUT_DIR = "./meu-diretorio-schemas"  # Mude aqui
```

## ✅ Checklist de Execução

- [ ] Abrir Jupyter Notebook no ambiente correto
- [ ] Executar célula 1 (Spark Session) - deve aparecer ✅
- [ ] Executar célula 2 (Carregar funções) - deve aparecer ✅
- [ ] Executar célula 3 (Gerar schemas) - aguardar conclusão
- [ ] Verificar mensagem "GERAÇÃO CONCLUÍDA"
- [ ] Executar célula 4 para listar arquivos
- [ ] Conferir pasta `./data-schema/`

## 🐛 Troubleshooting

### Erro: "Sessão Spark não encontrada"
**Solução:** Execute a célula 1 primeiro para configurar o Spark

### Erro: "Table not found"
**Solução:** Verifique se a tabela existe no banco de dados
```python
spark.sql("SHOW TABLES IN niat").show()
```

### Erro: "Permission denied"
**Solução:** Verifique permissões do diretório
```bash
chmod 755 data-schema
```

### Processo muito lento
**Solução:** Execute em horário de menor uso do cluster ou reduza o LIMIT

## 📈 Próximos Passos

Após gerar os schemas:

1. **Revisar os arquivos** gerados em `./data-schema/`
2. **Adicionar ao Git** para versionamento
3. **Compartilhar** com a equipe para documentação
4. **Atualizar** quando houver mudanças nas tabelas

## 🔄 Quando Re-executar

Execute novamente quando:
- Adicionar novas tabelas ao projeto
- Modificar estrutura de tabelas existentes
- Preparar onboarding de novos membros
- Criar apresentações técnicas
- Atualizar documentação

## 💡 Dicas

1. **Versionamento:** Commit os schemas no Git junto com mudanças nas tabelas

2. **Documentação:** Use os schemas em PRs quando modificar estruturas

3. **Performance:** Se demorar muito, gere primeiro apenas as tabelas mais importantes

4. **Colaboração:** Compartilhe a pasta `data-schema/` com desenvolvedores

5. **Backup:** Os arquivos são texto simples, fácil de fazer backup

---

## 📞 Suporte

Se tiver problemas:
1. Verifique se a sessão Spark está ativa
2. Confirme acesso às tabelas no Hive/Impala
3. Valide permissões de escrita no diretório

---

**Pronto para começar? Abra o notebook `GERAR_DATA_SCHEMAS.ipynb` e execute!** 🚀
