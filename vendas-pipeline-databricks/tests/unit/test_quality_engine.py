import uuid
from datetime import datetime, timedelta

import pytest

from tests.support.parquet_store import ParquetStore
from vendas_pipeline.observability.logs import get_logger
from vendas_pipeline.quality.engine import Contexto, executar_checks
from vendas_pipeline.quality.gate import QualityGateError, executar_gate
from vendas_pipeline.settings import DQ_RESULTS, Settings

AGORA = datetime(2026, 6, 1, 12, 0, 0)


@pytest.fixture
def ctx(spark):
    return Contexto(spark=spark, resolver=lambda n: f"{n}_view", agora=AGORA)


def rodar(df, spec, ctx, severidade="critica"):
    contrato = {"t": [{**spec, "severidade": severidade}]}
    return executar_checks(df, "t", ctx, contrato)[0]


def test_not_null_conta_nulos(spark, ctx):
    df = spark.createDataFrame([(1,), (None,), (None,)], "id int")
    r = rodar(df, {"tipo": "not_null", "col": "id"}, ctx)
    assert (r.passou, r.falhas, r.total) == (False, 2, 3)


def test_unique_detecta_duplicata_e_traz_amostra(spark, ctx):
    df = spark.createDataFrame([(1,), (1,), (2,)], "id int")
    r = rodar(df, {"tipo": "unique", "cols": ["id"]}, ctx)
    assert r.passou is False and r.falhas == 1 and '"id": 1' in r.amostra


def test_range_trata_nulo_como_falha(spark, ctx):
    df = spark.createDataFrame([(5.0,), (-1.0,), (None,)], "v double")
    r = rodar(df, {"tipo": "range", "col": "v", "min": 0}, ctx)
    assert r.falhas == 2


def test_freshness(spark, ctx):
    recente = spark.createDataFrame([(AGORA - timedelta(hours=2),)], "ts timestamp")
    antigo = spark.createDataFrame([(AGORA - timedelta(hours=100),)], "ts timestamp")
    spec = {"tipo": "freshness", "col": "ts", "max_horas": 48}
    assert rodar(recente, spec, ctx).passou is True
    assert rodar(antigo, spec, ctx).passou is False


def test_max_ratio_e_min_linhas(spark, ctx):
    df = spark.createDataFrame([("a",)] * 9 + [("NAO_MAPEADO",)], "canal string")
    assert rodar(df, {"tipo": "max_ratio", "expr": "canal = 'NAO_MAPEADO'", "max": 0.2}, ctx).passou
    assert not rodar(
        df, {"tipo": "max_ratio", "expr": "canal = 'NAO_MAPEADO'", "max": 0.05}, ctx
    ).passou
    vazio = spark.createDataFrame([], "canal string")
    assert rodar(vazio, {"tipo": "min_linhas", "min": 1}, ctx).passou is False


def test_referencial_e_reconciliacao(spark, ctx):
    spark.createDataFrame([("A",), ("B",)], "id string").createOrReplaceTempView("ref_view")
    spark.createDataFrame([(100.0,), (50.0,)], "valor double").createOrReplaceTempView(
        "origem_view"
    )
    df = spark.createDataFrame([("A",), ("Z",)], "id string")
    r = rodar(df, {"tipo": "referencial", "col": "id", "ref_table": "ref", "ref_col": "id"}, ctx)
    assert r.passou is False and r.falhas == 1 and "Z" in r.amostra

    ok = spark.createDataFrame([(150.0,)], "total double")
    ruim = spark.createDataFrame([(120.0,)], "total double")
    spec = {
        "tipo": "reconciliacao",
        "col": "total",
        "ref_table": "origem",
        "ref_col": "valor",
        "tolerancia_rel": 1e-6,
    }
    assert rodar(ok, spec, ctx).passou is True
    assert rodar(ruim, spec, ctx).passou is False


def test_fail_closed_check_que_quebra_reprova(spark, ctx):
    df = spark.createDataFrame([(1,)], "id int")
    r = rodar(df, {"tipo": "not_null", "col": "coluna_que_nao_existe"}, ctx)
    assert r.passou is False and r.erro is not None


def test_gate_grava_evidencia_e_bloqueia_em_critico_mas_so_alerta_em_alta(spark):
    schema = f"t_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA {schema}")
    settings = Settings(schema=schema, store=ParquetStore())
    logger = get_logger("run-teste")
    df = spark.createDataFrame([(1,), (1,)], "id int")

    alta = {"t": [{"tipo": "unique", "cols": ["id"], "severidade": "alta"}]}
    resultado = executar_gate(spark, settings, df, "t", "run-1", logger, alta)  # só alerta
    assert len(resultado.alertas) == 1 and not resultado.criticos_falhos

    critica = {"t": [{"tipo": "unique", "cols": ["id"], "severidade": "critica"}]}
    with pytest.raises(QualityGateError, match="unique:id"):
        executar_gate(spark, settings, df, "t", "run-2", logger, critica)

    gravados = spark.table(settings.tabela(DQ_RESULTS)).filter("run_id = 'run-2'").count()
    assert gravados == 1  # a evidência existe MESMO com a exceção
    spark.sql(f"DROP SCHEMA {schema} CASCADE")
