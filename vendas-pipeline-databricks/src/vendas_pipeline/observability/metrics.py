"""Métricas de execução: UMA linha por etapa por run, em uma tabela Delta consultável por SQL.

É a base do dashboard ("volume por dia, taxa de quarentena, duração por etapa") e a prova
quantitativa dos ganhos: sem métrica gravada, "melhorou" é opinião.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from vendas_pipeline.observability.logs import evento
from vendas_pipeline.settings import RUN_METRICS, Settings

SCHEMA_RUN_METRICS = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("etapa", StringType(), False),
        StructField("status", StringType(), False),
        StructField("linhas_entrada", LongType(), True),
        StructField("linhas_saida", LongType(), True),
        StructField("linhas_quarentena", LongType(), True),
        StructField("duracao_s", DoubleType(), True),
        StructField("detalhe", StringType(), True),
        StructField("ts", TimestampType(), False),
    ]
)


class Recorder:
    def __init__(self, spark: SparkSession, settings: Settings, run_id: str, logger):
        self.spark, self.settings, self.run_id, self.logger = spark, settings, run_id, logger

    @contextmanager
    def etapa(self, nome: str):
        """Mede a etapa e grava a métrica MESMO quando ela falha (status=falha) e re-lança.

        O `info` devolvido é um dict que a etapa preenche (linhas_entrada, ...).
        """
        info: dict = {}
        inicio = time.time()
        evento(self.logger, "etapa_inicio", etapa=nome)
        try:
            yield info
        except Exception as exc:
            info["erro"] = str(exc)[:500]
            self._gravar(nome, "falha", info, time.time() - inicio)
            evento(self.logger, "etapa_falha", etapa=nome, erro=info["erro"])
            raise
        else:
            self._gravar(nome, "sucesso", info, time.time() - inicio)
            evento(self.logger, "etapa_fim", etapa=nome, duracao_s=round(time.time() - inicio, 2))

    def _gravar(self, nome: str, status: str, info: dict, duracao: float) -> None:
        detalhe = {
            k: v
            for k, v in info.items()
            if k not in ("linhas_entrada", "linhas_saida", "linhas_quarentena")
        }
        linha = (
            self.run_id,
            nome,
            status,
            info.get("linhas_entrada"),
            info.get("linhas_saida"),
            info.get("linhas_quarentena"),
            round(duracao, 3),
            json.dumps(detalhe, ensure_ascii=False, default=str) if detalhe else None,
            datetime.now(),
        )
        df = self.spark.createDataFrame([linha], schema=SCHEMA_RUN_METRICS)
        self.settings.store.anexar(df, self.settings.tabela(RUN_METRICS))
