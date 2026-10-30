# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Publicação da Gold
# MAGIC Promove staging -> produção com overwrite transacional do Delta (consumidores nunca veem
# MAGIC tabela pela metade; há time travel para voltar). **Exige atestado de qualidade**: sem
# MAGIC `dq_results` aprovado para este `run_id`, a publicação é recusada, mesmo se este
# MAGIC notebook for executado isolado.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from vendas_pipeline import stages
from vendas_pipeline.notebook_utils import contexto_notebook

settings, RUN_ID, _ = contexto_notebook(dbutils)

# COMMAND ----------
stages.stage_gold_publish(spark, settings, RUN_ID)
