"""Ponta a ponta: seeds sujos -> pipeline completo -> comparação com o GABARITO do gerador.

Roda com `parquet` (lógica, sem Delta) e com `delta` (MERGE de verdade; pulado se indisponível).
"""

import shutil

import pytest
from pyspark.sql import functions as F

from vendas_pipeline import stages
from vendas_pipeline.ingestion.referencia import DeParaInvalido
from vendas_pipeline.quality.gate import QualityGateError
from vendas_pipeline.settings import (
    DQ_RESULTS,
    GOLD,
    QUARENTENA,
    RUN_METRICS,
    SILVER,
    novo_run_id,
)


def _t(settings, nome):
    return settings.tabela(nome)


def test_pipeline_bate_com_o_gabarito(spark, ambiente):
    settings, verdade = ambiente
    stages.garantir_schema(spark, settings)
    resumo = stages.run_all(spark, settings)

    silver = spark.table(_t(settings, SILVER))
    # 1) Contagem e soma: exatamente as vendas válidas e únicas, com a versão mais recente.
    assert silver.count() == verdade["vendas_validas_unicas"]
    soma = silver.agg(F.sum("valor")).first()[0]
    assert abs(soma - verdade["valor_total_valido"]) < 0.05

    # 2) Quarentena: cada defeito injetado caiu no motivo certo, nenhum a mais, nenhum a menos.
    obtido = {
        r["motivo"]: r["count"]
        for r in spark.table(_t(settings, QUARENTENA)).groupBy("motivo").count().collect()
    }
    assert obtido == verdade["quarentena_esperada"]

    # 3) Nada some: cada linha bruta está na silver, na quarentena ou é versão antiga/duplicata.
    brutas = sum(verdade["linhas_brutas"].values())
    descartadas = verdade["revisoes_erp"] + verdade["duplicatas_exatas_api"]
    assert brutas == silver.count() + sum(obtido.values()) + descartadas

    # 4) De/para com vigência: antes do corte usa o id antigo, depois usa o novo.
    for m in verdade["remapeamentos"]:
        linhas = silver.filter(F.col("id_cliente") == m["id_origem"])
        assert (
            linhas.filter(
                (F.col("data_venda") < m["corte"]) & (F.col("id_cliente_canonico") != m["antes"])
            ).count()
            == 0
        )
        assert (
            linhas.filter(
                (F.col("data_venda") >= m["corte"]) & (F.col("id_cliente_canonico") != m["depois"])
            ).count()
            == 0
        )

    # 5) Gold fecha com a Silver.
    gold = spark.table(_t(settings, GOLD))
    assert abs(gold.agg(F.sum("total_vendas")).first()[0] - soma) < 0.5
    assert resumo["gold"] == gold.count() > 0

    # 6) Observabilidade: todas as etapas registradas com sucesso e nenhum crítico reprovado.
    etapas = {r["etapa"]: r["status"] for r in spark.table(_t(settings, RUN_METRICS)).collect()}
    assert set(etapas.values()) == {"sucesso"}
    assert {"ingest_bronze", "silver", "gold_build", "gold_publish"} <= set(etapas)
    dq = spark.table(_t(settings, DQ_RESULTS))
    assert dq.filter((F.col("severidade") == "critica") & ~F.col("passou")).count() == 0


def test_rodar_duas_vezes_nao_duplica_nada(spark, ambiente):
    settings, _ = ambiente
    stages.garantir_schema(spark, settings)
    stages.run_all(spark, settings)

    def foto():
        s = spark.table(_t(settings, SILVER))
        return (
            s.count(),
            round(s.agg(F.sum("valor")).first()[0], 2),
            spark.table(_t(settings, QUARENTENA)).count(),
            spark.table(_t(settings, GOLD)).count(),
        )

    antes = foto()
    stages.run_all(spark, settings)  # segunda execução: ERP já lido, API/DB só reingerem a borda
    assert foto() == antes


def test_gate_bloqueia_e_a_gold_publicada_nao_muda(spark, ambiente):
    settings, _ = ambiente
    stages.garantir_schema(spark, settings)
    stages.run_all(spark, settings)
    gold_antes = spark.table(_t(settings, GOLD)).count()

    # Corrompe a Silver: injeta uma linha com id_venda duplicado.
    duplicada = spark.table(_t(settings, SILVER)).limit(1)
    settings.store.anexar(duplicada, _t(settings, SILVER))

    run_ruim = novo_run_id()
    with pytest.raises(QualityGateError, match="unique:id_venda"):
        stages.stage_gate(spark, settings, run_ruim, "silver_vendas", SILVER)

    # A evidência foi gravada e a publicação sem atestado é recusada.
    assert (
        spark.table(_t(settings, DQ_RESULTS))
        .filter((F.col("run_id") == run_ruim) & ~F.col("passou"))
        .count()
        >= 1
    )
    with pytest.raises(QualityGateError, match="sem atestado"):
        stages.stage_gold_publish(spark, settings, run_ruim)
    assert spark.table(_t(settings, GOLD)).count() == gold_antes


def test_de_para_com_vigencia_sobreposta_derruba_a_ingestao(spark, ambiente):
    settings, _ = ambiente
    caminho = f"{settings.seed_path}/de_para_clientes.csv"
    with open(caminho, "a", encoding="utf-8") as f:
        f.write("C0020,CAN-9999,erp,2024-01-01,9999-12-31\n")  # sobrepõe a vigência já existente
    with pytest.raises(DeParaInvalido, match="sobrepostas"):
        stages.stage_ingest(spark, settings, novo_run_id())


def test_quarentena_excessiva_bloqueia_a_silver(spark, ambiente, monkeypatch):
    settings, _ = ambiente
    monkeypatch.setattr(stages, "LIMITE_QUARENTENA_BLOQUEIO", 0.001)
    run_id = novo_run_id()
    stages.stage_ingest(spark, settings, run_id)
    with pytest.raises(stages.QuarentenaExcessiva):
        stages.stage_silver(spark, settings, run_id)
    # A evidência da quarentena foi gravada antes do bloqueio e a Silver não foi tocada.
    assert spark.table(_t(settings, QUARENTENA)).count() > 0
    assert not settings.store.existe(spark, _t(settings, SILVER))
    status = {r["etapa"]: r["status"] for r in spark.table(_t(settings, RUN_METRICS)).collect()}
    assert status["silver"] == "falha"
    shutil.rmtree(settings.seed_path, ignore_errors=True)
