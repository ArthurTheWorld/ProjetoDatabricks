# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold (build em staging)
# MAGIC Constrói `gold_vendas_diarias_staging` com SQL (CTE + window functions). Quem consome
# MAGIC (BI) só enxerga `gold_vendas_diarias`, que só é atualizada na publicação, DEPOIS do gate.
# MAGIC
# MAGIC **Lógica:** `src/vendas_pipeline/transforms/gold.py`

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from vendas_pipeline import stages
from vendas_pipeline.notebook_utils import contexto_notebook

settings, RUN_ID, _ = contexto_notebook(dbutils)

# COMMAND ----------
stages.stage_gold_build(spark, settings, RUN_ID)
