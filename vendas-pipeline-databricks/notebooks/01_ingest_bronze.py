# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingestão Bronze
# MAGIC **O que faz:** carrega as tabelas de de/para (validando vigências) e ingere as 3 fontes
# MAGIC (CSV do ERP, API instável, tabela incremental) na Bronze, em UMA escrita atômica.
# MAGIC
# MAGIC **Por que assim:** Bronze é cópia fiel e append-only; nada é corrigido aqui. Se a regra
# MAGIC de limpeza mudar, reprocessamos a Silver sem reler as fontes.
# MAGIC
# MAGIC **Lógica:** `src/vendas_pipeline/ingestion/` · **Explicação completa:** `docs/EXPLICACAO_COMPLETA.md`

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))  # o bundle sincroniza o repositório inteiro

from vendas_pipeline import stages
from vendas_pipeline.notebook_utils import contexto_notebook

settings, RUN_ID, _ = contexto_notebook(dbutils)

# COMMAND ----------
stages.stage_ingest(spark, settings, RUN_ID)
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
