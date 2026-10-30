# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Quality gate (reutilizado para Silver e Gold)
# MAGIC Executa o contrato de `src/vendas_pipeline/quality/contracts.py`, grava cada resultado em
# MAGIC `dq_results` e **lança exceção** se algum check crítico falhar (a task falha e as
# MAGIC seguintes não rodam). `dbutils.notebook.exit()` não serviria: ele encerra com SUCESSO.
# MAGIC
# MAGIC Parâmetros: `tabela_logica` (nome no contrato) e `tabela_origem` (tabela a validar).

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from vendas_pipeline import stages
from vendas_pipeline.notebook_utils import contexto_notebook

settings, RUN_ID, extras = contexto_notebook(
    dbutils, extras={"tabela_logica": "silver_vendas", "tabela_origem": "silver_vendas"}
)

# COMMAND ----------
stages.stage_gate(spark, settings, RUN_ID, extras["tabela_logica"], extras["tabela_origem"])
