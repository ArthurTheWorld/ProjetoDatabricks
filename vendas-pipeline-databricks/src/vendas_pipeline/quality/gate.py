"""Quality gate: executa o contrato, GRAVA a evidência, alerta e (se preciso) BLOQUEIA.

Dois detalhes que importam:
1. A evidência é persistida ANTES de lançar a exceção: quem investiga a falha precisa do
   resultado em dq_results, e a falha não pode impedir que ele exista.
2. O bloqueio é uma EXCEÇÃO (task falha, dependentes não rodam). `dbutils.notebook.exit("erro")`
   NÃO serve: encerra o notebook com SUCESSO e o Job seguiria para a Gold.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from vendas_pipeline.observability.alerts import alertar
from vendas_pipeline.quality.engine import CheckResult, Contexto, executar_checks
from vendas_pipeline.settings import DQ_RESULTS, Settings

SCHEMA_DQ = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("tabela", StringType(), False),
        StructField("check", StringType(), False),
        StructField("tipo", StringType(), False),
        StructField("severidade", StringType(), False),
        StructField("passou", BooleanType(), False),
        StructField("falhas", LongType(), True),
        StructField("total", LongType(), True),
        StructField("valor", DoubleType(), True),
        StructField("limite", DoubleType(), True),
        StructField("amostra", StringType(), True),
        StructField("erro", StringType(), True),
        StructField("ts", TimestampType(), False),
    ]
)


class QualityGateError(RuntimeError):
    pass


@dataclass
class GateResultado:
    resultados: list[CheckResult]

    @property
    def criticos_falhos(self) -> list[CheckResult]:
        return [r for r in self.resultados if r.severidade == "critica" and not r.passou]

    @property
    def alertas(self) -> list[CheckResult]:
        return [r for r in self.resultados if r.severidade == "alta" and not r.passou]


def persistir_resultados(
    spark: SparkSession, settings: Settings, resultados: list[CheckResult], run_id: str
) -> None:
    agora = datetime.now()
    linhas = []
    for r in resultados:
        d = asdict(r)
        linhas.append(
            (
                run_id,
                d["tabela"],
                d["check"],
                d["tipo"],
                d["severidade"],
                d["passou"],
                d["falhas"],
                d["total"],
                d["valor"],
                d["limite"],
                d["amostra"],
                d["erro"],
                agora,
            )
        )
    settings.store.anexar(
        spark.createDataFrame(linhas, schema=SCHEMA_DQ), settings.tabela(DQ_RESULTS)
    )


def executar_gate(
    spark: SparkSession,
    settings: Settings,
    df: DataFrame,
    tabela_logica: str,
    run_id: str,
    logger,
    contratos: dict | None = None,
) -> GateResultado:
    ctx = Contexto(spark=spark, resolver=settings.tabela, agora=datetime.now())
    gate = GateResultado(executar_checks(df, tabela_logica, ctx, contratos))
    persistir_resultados(spark, settings, gate.resultados, run_id)

    for r in gate.alertas:
        alertar(
            logger,
            "alta",
            f"check falhou em {tabela_logica}: {r.check}",
            falhas=r.falhas,
            valor=r.valor,
            limite=r.limite,
        )
    if gate.criticos_falhos:
        nomes = [r.check for r in gate.criticos_falhos]
        raise QualityGateError(
            f"{len(nomes)} check(s) crítico(s) falharam em {tabela_logica}: {nomes}"
        )
    return gate
