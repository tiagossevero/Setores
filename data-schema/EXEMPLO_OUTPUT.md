# 📄 Exemplo de Data Schema Gerado

Este é um exemplo de como ficará cada arquivo `.md` gerado pelo script.

---

# Data Schema: niat.argos_benchmark_setorial

**Gerado em:** 2025-11-17 14:30:25

---

## 📋 DESCRIBE FORMATTED

```sql
DESCRIBE FORMATTED niat.argos_benchmark_setorial;
```

### Resultado:

```
                  col_name                                           data_type  comment
                  nu_per_ref                                            bigint     None
                cnae_classe                                            string     None
          desc_cnae_classe                                            string     None
        qtd_empresas_ativas                                            bigint     None
          faturamento_total                                            double     None
             icms_devido_total                                            double     None
       aliq_efetiva_mediana                                            double     None
           aliq_efetiva_p25                                            double     None
           aliq_efetiva_p75                                            double     None
    aliq_efetiva_media                                            double     None
    aliq_efetiva_desvpad                                            double     None

# Detailed Table Information
              Database                                              niat     None
                 Table                          argos_benchmark_setorial     None
                 Owner                                           tsevero     None
          Created Time                      Wed Oct 02 15:23:45 BRT 2025     None
           Last Access                      Thu Jan 01 00:00:00 BRT 1970     None
                  Type                                           MANAGED     None
              Provider                                           parquet     None
              Location     hdfs://nameservice1/user/hive/warehouse/niat...     None
```

---

## 📊 SAMPLE DATA (LIMIT 10)

```sql
SELECT * FROM niat.argos_benchmark_setorial LIMIT 10;
```

### Resultado:

```
   nu_per_ref  cnae_classe                                desc_cnae_classe  qtd_empresas_ativas  faturamento_total  icms_devido_total  aliq_efetiva_mediana  aliq_efetiva_p25  aliq_efetiva_p75
      202501         4711  Comércio varejista de mercadorias em geral                      1245      8234567890.12       987654321.45              0.117586          0.089234          0.145678
      202501         4712       Comércio varejista de produtos alimentícios                       856      5678901234.56       678901234.56              0.119456          0.092345          0.148901
      202501         4713            Comércio varejista de combustíveis                       234      3456789012.34       456789012.34              0.112345          0.087654          0.138765
      202501         4721          Comércio varejista de produtos farmacêuticos                       432      2345678901.23       345678901.23              0.147234          0.112345          0.178901
      202501         4722       Comércio varejista de cosméticos e perfumes                       189      1234567890.12       234567890.12              0.189876          0.145678          0.234567
      202501         4731              Comércio varejista de combustíveis                       567      4567890123.45       567890123.45              0.124567          0.098765          0.156789
      202501         4741   Comércio varejista de tintas e materiais para pintura                        98       987654321.98       187654321.98              0.190234          0.156789          0.245678
      202501         4742       Comércio varejista de material elétrico                       276      1876543210.87       287654321.87              0.153456          0.123456          0.189012
      202501         4743    Comércio varejista de vidros, espelhos e vitrais                        45       456789123.45        76789123.45              0.168234          0.134567          0.198765
      202501         4744 Comércio varejista de ferragens e ferramentas                       312      2123456789.01       323456789.01              0.152123          0.119876          0.187654
```

### Informações Adicionais:

- **Total de colunas:** 9
- **Colunas:** nu_per_ref, cnae_classe, desc_cnae_classe, qtd_empresas_ativas, faturamento_total, icms_devido_total, aliq_efetiva_mediana, aliq_efetiva_p25, aliq_efetiva_p75
- **Registros retornados:** 10

---

## 📝 Notas

- Este arquivo foi gerado automaticamente
- Utilize este schema como referência para análises e desenvolvimento

---

## 💡 Como usar este schema

### 1. Consultar tipos de dados

Use a seção DESCRIBE FORMATTED para verificar os tipos de cada coluna antes de fazer joins ou cálculos.

### 2. Entender valores reais

Use a seção SAMPLE DATA para ver exemplos reais dos dados armazenados.

### 3. Validar queries

Compare os resultados das suas queries com os exemplos mostrados aqui.

### 4. Documentar código

Referencie este arquivo em comentários quando trabalhar com esta tabela:

```python
# Tabela: niat.argos_benchmark_setorial
# Schema: ver data-schema/intermediarias/argos_benchmark_setorial.md
# Campos principais: nu_per_ref, cnae_classe, aliq_efetiva_mediana
```

---

**Este é apenas um exemplo ilustrativo. Execute o notebook para gerar os schemas reais.**
