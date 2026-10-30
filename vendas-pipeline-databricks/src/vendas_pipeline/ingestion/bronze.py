"""Ingestão Bronze: copia fiel das fontes, append-only, com metadados de rastreabilidade.

Regra de ouro da Bronze: NÃO CORRIGE NADA. Se a regra de limpeza mudar amanhã, reprocessamos a
Silver a partir da Bronze sem precisar reler (nem reconseguir) as fontes originais.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vendas_pipeline.ingestion.api_client import buscar_tudo
from vendas_pipeline.ingestion.schemas import (
    COLUNAS_BRONZE,
    COLUNAS_DADOS,
    SCHEMA_VENDAS_CSV,
)
from vendas_pipeline.settings import BRONZE, Settings

FONTE_ERP = "erp_csv"
FONTE_API = "api_json"
FONTE_DB = "db_incremental"
WATERMARK_INICIAL = "1900-01-01 00:00:00"
_REGEX_TS = "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"


def _meta(df: DataFrame, fonte: str, run_id: str) -> DataFrame:
    return (
        df.withColumn("_fonte", F.lit(fonte))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*COLUNAS_BRONZE)
    )


def ler_csv(
    spark: SparkSession, caminho: str, fonte: str, run_id: str, encoding: str = "UTF-8"
) -> DataFrame:
    """Lê CSV com schema explícito. PERMISSIVE + coluna `_corrupt`: linha quebrada NÃO derruba o
    job (FAILFAST) e NÃO some (DROPMALFORMED): é guardada para a Silver mandar à quarentena."""
    df = (
        spark.read.option("header", True)
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(SCHEMA_VENDAS_CSV)
        .csv(caminho)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return _meta(df, fonte, run_id)


def ler_api(spark: SparkSession, registros: list[dict], run_id: str) -> DataFrame:
    """Converte o JSON da API para o MESMO formato (tudo string) das outras fontes."""
    linhas = [
        tuple(None if r.get(c) is None else str(r[c]) for c in COLUNAS_DADOS) + (None,)
        for r in registros
    ]
    df = spark.createDataFrame(linhas, schema=SCHEMA_VENDAS_CSV).withColumn(
        "_source_file", F.lit("api://vendas")
    )
    return _meta(df, FONTE_API, run_id)


def obter_watermark(spark: SparkSession, settings: Settings, fonte: str) -> str:
    """Maior `updated_at` já ingerido para a fonte. Só considera timestamps bem formados:
    um 'ZZZ' sujo ordena DEPOIS de qualquer data e envenenaria o watermark para sempre."""
    tabela = settings.tabela(BRONZE)
    if not settings.store.existe(spark, tabela):
        return WATERMARK_INICIAL
    maximo = (
        spark.table(tabela)
        .filter((F.col("_fonte") == fonte) & F.col("updated_at").rlike(_REGEX_TS))
        .agg(F.max("updated_at"))
        .first()[0]
    )
    return maximo or WATERMARK_INICIAL


def ingerir_bronze(spark: SparkSession, settings: Settings, run_id: str, api, logger) -> dict:
    tabela = settings.tabela(BRONZE)
    existe = settings.store.existe(spark, tabela)

    # 1) ERP: arquivo entregue em lote. Idempotência por ARQUIVO (não relê o que já entrou).
    erp = ler_csv(spark, f"{settings.seed_path}/erp_vendas.csv", FONTE_ERP, run_id, "ISO-8859-1")
    if existe:
        ja_lidos = (
            spark.table(tabela)
            .filter(F.col("_fonte") == FONTE_ERP)
            .select("_source_file")
            .distinct()
        )
        erp = erp.join(ja_lidos, "_source_file", "left_anti").select(*COLUNAS_BRONZE)

    # 2) API: incremental por watermark; `>=` (e não `>`) para nunca perder registros que
    #    compartilham o mesmo timestamp da borda. O custo é reingerir a borda: a Silver deduplica.
    wm_api = obter_watermark(spark, settings, FONTE_API)
    registros = buscar_tudo(api, since=wm_api, logger=logger)
    api_df = ler_api(spark, registros, run_id)

    # 3) Tabela transacional (simulada por CSV): mesmo padrão de watermark.
    wm_db = obter_watermark(spark, settings, FONTE_DB)
    db = ler_csv(spark, f"{settings.seed_path}/db_vendas.csv", FONTE_DB, run_id).filter(
        F.col("updated_at") >= F.lit(wm_db)
    )

    # Uma ÚNICA escrita atômica: ou as 3 fontes entram neste run, ou nenhuma. Isso torna o
    # `run_id` uma fronteira confiável para a Silver ler "o que chegou neste run".
    settings.store.anexar(erp.unionByName(api_df).unionByName(db), tabela)

    por_fonte = {
        r["_fonte"]: {"linhas": r["linhas"], "corrompidas": int(r["corrompidas"] or 0)}
        for r in spark.table(tabela)
        .filter(F.col("_run_id") == run_id)
        .groupBy("_fonte")
        .agg(
            F.count("*").alias("linhas"),
            F.sum(F.col("_corrupt").isNotNull().cast("int")).alias("corrompidas"),
        )
        .collect()
    }
    return {
        "linhas_saida": sum(v["linhas"] for v in por_fonte.values()),
        "por_fonte": por_fonte,
        "watermark_api": wm_api,
        "watermark_db": wm_db,
        "chamadas_api": getattr(api, "chamadas", None),
    }
