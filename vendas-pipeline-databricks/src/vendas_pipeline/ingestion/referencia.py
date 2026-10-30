"""Tabelas de referência (de/para) e a validação que as protege.

Por que validar a de/para ANTES de usar? Uma vigência sobreposta duplica linhas no join
(cada venda casa com 2 linhas do de/para) e o erro é silencioso: o total de vendas sobe sem
nenhum erro no log. Falhar aqui, cedo e alto, é muito mais barato que descobrir no BI.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vendas_pipeline.ingestion.schemas import SCHEMA_DE_PARA_CANAL, SCHEMA_DE_PARA_CLIENTES
from vendas_pipeline.settings import DE_PARA_CANAL, DE_PARA_CLIENTES, Settings


class DeParaInvalido(ValueError):
    pass


def validar_vigencias(df: DataFrame) -> None:
    invertidas = df.filter(F.col("vigencia_inicio") > F.col("vigencia_fim"))
    mesmo_inicio = df.groupBy("id_origem", "vigencia_inicio").count().filter(F.col("count") > 1)
    a, b = df.alias("a"), df.alias("b")
    sobrepostas = a.join(
        b,
        (F.col("a.id_origem") == F.col("b.id_origem"))
        & (F.col("a.vigencia_inicio") < F.col("b.vigencia_inicio"))
        & (F.col("a.vigencia_fim") >= F.col("b.vigencia_inicio")),
    ).select(F.col("a.id_origem").alias("id_origem"))

    problemas = {
        "vigencia_invertida": invertidas.select("id_origem"),
        "mesmo_inicio_duplicado": mesmo_inicio.select("id_origem"),
        "vigencias_sobrepostas": sobrepostas,
    }
    achados = {}
    for nome, consulta in problemas.items():
        ids = [r["id_origem"] for r in consulta.distinct().limit(5).collect()]
        if ids:
            achados[nome] = ids
    if achados:
        raise DeParaInvalido(f"de/para de clientes inválido: {achados}")


def carregar_de_para(spark: SparkSession, settings: Settings) -> dict:
    """Carrega as referências (hoje de CSV; em produção, a tabela mantida pelo negócio)."""
    clientes = (
        spark.read.option("header", True)
        .schema(SCHEMA_DE_PARA_CLIENTES)
        .csv(f"{settings.seed_path}/de_para_clientes.csv")
    )
    validar_vigencias(clientes)
    canal = (
        spark.read.option("header", True)
        .schema(SCHEMA_DE_PARA_CANAL)
        .csv(f"{settings.seed_path}/de_para_canal.csv")
    )
    duplicadas = canal.groupBy("chave").count().filter(F.col("count") > 1).count()
    if duplicadas:
        raise DeParaInvalido(f"de/para de canal com {duplicadas} chave(s) duplicada(s)")

    settings.store.sobrescrever(clientes, settings.tabela(DE_PARA_CLIENTES))
    settings.store.sobrescrever(canal, settings.tabela(DE_PARA_CANAL))
    return {"de_para_clientes": clientes.count(), "de_para_canal": canal.count()}
