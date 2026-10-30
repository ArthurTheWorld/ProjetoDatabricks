# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: normalização, de/para, deduplicação e quarentena
# MAGIC **Entrada:** linhas da Bronze deste `run_id` (ou `ALL` para reprocessar tudo).
# MAGIC **Saída:** `silver_vendas` (MERGE idempotente) e `quarentena_vendas` (MERGE só-insert).
# MAGIC
# MAGIC **Garantia:** toda linha de entrada vai para a Silver, vai para a quarentena com um motivo
# MAGIC ou é uma versão antiga de uma venda já presente. Nada some sem rastro.
# MAGIC
# MAGIC **Lógica:** `src/vendas_pipeline/transforms/silver.py`

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from vendas_pipeline import stages
from vendas_pipeline.notebook_utils import contexto_notebook

settings, RUN_ID, _ = contexto_notebook(dbutils)

# COMMAND ----------
stages.stage_silver(spark, settings, RUN_ID)
