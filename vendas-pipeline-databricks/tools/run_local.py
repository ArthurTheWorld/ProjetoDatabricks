"""Roda o pipeline completo na sua máquina e imprime as consultas que você mostraria na demo.

python -m tools.run_local                 # Delta (baixa os jars do Maven na 1ª vez)
python -m tools.run_local --sem-delta     # Parquet: só para ver a lógica sem Delta
"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile

from tests.support.parquet_store import ParquetStore
from tools.gerar_seeds import gerar
from tools.spark_local import criar_spark
from vendas_pipeline import stages
from vendas_pipeline.settings import Settings
from vendas_pipeline.storage import DeltaStore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sem-delta", action="store_true")
    ap.add_argument("--vendas", type=int, default=3000)
    ap.add_argument(
        "--seeds", default=None, help="pasta de seeds (padrão: gera numa pasta temporária)"
    )
    args = ap.parse_args()

    seeds = args.seeds or tempfile.mkdtemp(prefix="seeds_")
    if not os.path.exists(f"{seeds}/erp_vendas.csv"):
        gerar(seeds, n_vendas=args.vendas)
    spark = criar_spark(delta=not args.sem_delta)
    spark.sparkContext.setLogLevel("ERROR")
    settings = Settings(
        schema="vendas_local",
        seed_path=seeds,
        store=ParquetStore() if args.sem_delta else DeltaStore(),
    )
    spark.sql("DROP SCHEMA IF EXISTS vendas_local CASCADE")

    print("\n== execução 1 ==", stages.run_all(spark, settings))
    print("== execução 2 (idempotência) ==", stages.run_all(spark, settings))

    q = settings.tabela
    print("\n-- quarentena por motivo")
    spark.sql(
        f"SELECT motivo, count(*) n FROM {q('quarentena_vendas')} GROUP BY 1 ORDER BY 2 DESC"
    ).show()
    print("-- duração e volumes por etapa (última execução)")
    spark.sql(
        f"SELECT etapa, status, linhas_entrada, linhas_saida, linhas_quarentena, duracao_s "
        f"FROM {q('pipeline_run_metrics')} ORDER BY ts DESC LIMIT 8"
    ).show(truncate=False)
    print("-- checks de qualidade que falharam (se houver)")
    spark.sql(
        f"SELECT tabela, check, severidade, falhas, valor FROM {q('dq_results')} WHERE NOT passou"
    ).show(truncate=False)
    print("-- top 5 produtos por faturamento (Gold)")
    spark.sql(
        f"SELECT id_produto, round(sum(total_vendas),2) total FROM {q('gold_vendas_diarias')} "
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
    ).show()
    if not args.seeds:
        shutil.rmtree(seeds, ignore_errors=True)


if __name__ == "__main__":
    main()
