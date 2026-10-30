"""Orquestração das etapas. Os notebooks do Databricks e o runner local chamam EXATAMENTE
estas funções: o que roda em produção é o que o CI testou.

Etapas (cada uma é uma task do Job):
  ingest_bronze -> silver -> gate(silver) -> gold_build -> gate(gold) -> gold_publish
"""

from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from vendas_pipeline.ingestion.api_client import ApiInstavel
from vendas_pipeline.ingestion.bronze import ingerir_bronze
from vendas_pipeline.ingestion.referencia import carregar_de_para
from vendas_pipeline.observability.alerts import alertar
from vendas_pipeline.observability.logs import get_logger
from vendas_pipeline.observability.metrics import Recorder
from vendas_pipeline.quality.gate import QualityGateError, executar_gate
from vendas_pipeline.settings import (
    BRONZE,
    DE_PARA_CANAL,
    DE_PARA_CLIENTES,
    DQ_RESULTS,
    GOLD,
    GOLD_STAGING,
    QUARENTENA,
    SILVER,
    Settings,
    novo_run_id,
)
from vendas_pipeline.transforms.gold import construir_gold
from vendas_pipeline.transforms.silver import construir_silver

# Guard-rails da quarentena: pouca quarentena é rotina; MUITA significa fonte quebrada.
LIMITE_QUARENTENA_ALERTA = 0.10
LIMITE_QUARENTENA_BLOQUEIO = 0.30


class QuarentenaExcessiva(RuntimeError):
    pass


def garantir_schema(spark: SparkSession, settings: Settings) -> None:
    alvo = ".".join(p for p in (settings.catalog, settings.schema) if p)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {alvo}")


def carregar_api(settings: Settings, **kwargs) -> ApiInstavel:
    with open(f"{settings.seed_path}/api_vendas.json", encoding="utf-8") as f:
        return ApiInstavel(json.load(f), **kwargs)


def stage_ingest(spark: SparkSession, settings: Settings, run_id: str, api=None, logger=None):
    logger = logger or get_logger(run_id)
    garantir_schema(spark, settings)
    rec = Recorder(spark, settings, run_id, logger)
    with rec.etapa("carregar_referencias") as info:
        info.update(carregar_de_para(spark, settings))
    with rec.etapa("ingest_bronze") as info:
        info.update(ingerir_bronze(spark, settings, run_id, api or carregar_api(settings), logger))


def stage_silver(spark: SparkSession, settings: Settings, run_id: str, logger=None):
    """`run_id="ALL"` reprocessa a Bronze inteira (correção de regra); é seguro porque o MERGE
    da Silver e o da quarentena são idempotentes."""
    logger = logger or get_logger(run_id)
    t, store = settings.tabela, settings.store
    rec = Recorder(spark, settings, run_id, logger)
    with rec.etapa("silver") as info:
        bronze = spark.table(t(BRONZE))
        if run_id != "ALL":
            bronze = bronze.filter(F.col("_run_id") == run_id)
        silver, quarentena = construir_silver(
            bronze, spark.table(t(DE_PARA_CLIENTES)), spark.table(t(DE_PARA_CANAL))
        )
        n_entrada = bronze.count()
        n_quar = quarentena.count()
        motivos = {r["motivo"]: r["count"] for r in quarentena.groupBy("motivo").count().collect()}
        info.update(linhas_entrada=n_entrada, linhas_quarentena=n_quar, motivos=motivos)

        # Quarentena primeiro: se o guard-rail abaixo bloquear, a evidência já está gravada.
        store.inserir_se_ausente(spark, quarentena, t(QUARENTENA), ["_quarantine_key"])
        razao = n_quar / n_entrada if n_entrada else 0.0
        info["razao_quarentena"] = round(razao, 4)
        if razao > LIMITE_QUARENTENA_BLOQUEIO:
            raise QuarentenaExcessiva(
                f"{razao:.0%} da entrada foi para a quarentena: fonte suspeita"
            )
        if razao > LIMITE_QUARENTENA_ALERTA:
            alertar(logger, "alta", "quarentena acima do normal", razao=round(razao, 4))

        store.upsert(spark, silver, t(SILVER), ["id_venda"], "updated_at")
        info["linhas_saida"] = silver.count()


def stage_gate(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    tabela_logica: str,
    tabela_origem: str,
    logger=None,
):
    logger = logger or get_logger(run_id)
    rec = Recorder(spark, settings, run_id, logger)
    with rec.etapa(f"gate:{tabela_logica}") as info:
        df = spark.table(settings.tabela(tabela_origem))
        gate = executar_gate(spark, settings, df, tabela_logica, run_id, logger)
        info.update(
            linhas_entrada=gate.resultados[0].total if gate.resultados else 0,
            checks=len(gate.resultados),
            alertas=len(gate.alertas),
        )
    return gate


def stage_gold_build(spark: SparkSession, settings: Settings, run_id: str, logger=None):
    """Constrói em STAGING. O consumidor (BI) só enxerga a Gold depois da publicação."""
    logger = logger or get_logger(run_id)
    rec = Recorder(spark, settings, run_id, logger)
    with rec.etapa("gold_build") as info:
        df = construir_gold(spark, settings.tabela(SILVER))
        settings.store.sobrescrever(df, settings.tabela(GOLD_STAGING))
        info["linhas_saida"] = spark.table(settings.tabela(GOLD_STAGING)).count()


def _exigir_atestado(spark: SparkSession, settings: Settings, run_id: str) -> None:
    """Defesa em profundidade: mesmo que alguém rode o notebook de publicação isolado (ou o
    Job seja reconfigurado sem a dependência), só publica se existir resultado de qualidade
    aprovado PARA ESTE run."""
    dq = spark.table(settings.tabela(DQ_RESULTS)).filter(
        (F.col("run_id") == run_id) & (F.col("tabela") == GOLD)
    )
    total = dq.count()
    reprovados = dq.filter((F.col("severidade") == "critica") & (~F.col("passou"))).count()
    if total == 0 or reprovados > 0:
        raise QualityGateError(
            f"publicação bloqueada: sem atestado de qualidade válido para run_id={run_id} "
            f"(checks={total}, críticos reprovados={reprovados})"
        )


def stage_gold_publish(spark: SparkSession, settings: Settings, run_id: str, logger=None):
    logger = logger or get_logger(run_id)
    rec = Recorder(spark, settings, run_id, logger)
    with rec.etapa("gold_publish") as info:
        _exigir_atestado(spark, settings, run_id)
        staging = spark.table(settings.tabela(GOLD_STAGING))
        settings.store.sobrescrever(staging, settings.tabela(GOLD))
        info["linhas_saida"] = spark.table(settings.tabela(GOLD)).count()


def run_all(spark: SparkSession, settings: Settings, run_id: str | None = None, api=None) -> dict:
    run_id = run_id or novo_run_id()
    logger = get_logger(run_id)
    stage_ingest(spark, settings, run_id, api, logger)
    stage_silver(spark, settings, run_id, logger)
    stage_gate(spark, settings, run_id, "silver_vendas", SILVER, logger)
    stage_gold_build(spark, settings, run_id, logger)
    stage_gate(spark, settings, run_id, GOLD, GOLD_STAGING, logger)
    stage_gold_publish(spark, settings, run_id, logger)
    return {
        "run_id": run_id,
        "bronze": spark.table(settings.tabela(BRONZE)).count(),
        "silver": spark.table(settings.tabela(SILVER)).count(),
        "quarentena": spark.table(settings.tabela(QUARENTENA)).count(),
        "gold": spark.table(settings.tabela(GOLD)).count(),
    }
