from datetime import date

from vendas_pipeline.transforms.gold import construir_gold


def _silver(spark, linhas):
    spark.createDataFrame(
        linhas, "data_venda date, id_produto string, valor double, id_cliente_canonico string"
    ).createOrReplaceTempView("silver_tmp")


def test_agregacao_ticket_e_ranking(spark):
    _silver(
        spark,
        [
            (date(2026, 1, 1), "P1", 100.0, "A"),
            (date(2026, 1, 1), "P1", 50.0, "B"),
            (date(2026, 1, 1), "P2", 300.0, "A"),
        ],
    )
    r = {x["id_produto"]: x for x in construir_gold(spark, "silver_tmp").collect()}
    assert (r["P1"]["total_vendas"], r["P1"]["qtd_vendas"], r["P1"]["ticket_medio"]) == (
        150.0,
        2,
        75.0,
    )
    assert r["P1"]["clientes_distintos"] == 2
    assert (r["P2"]["rank_produto_dia"], r["P1"]["rank_produto_dia"]) == (1, 2)


def test_media_movel_usa_dias_corridos_e_nao_linhas(spark):
    # dias 1, 2 e 10: no dia 10 a janela de 7 DIAS só contém o próprio dia (média = 300).
    # Com ROWS BETWEEN 6 PRECEDING a média seria (100+200+300)/3 = 200, o erro silencioso.
    _silver(
        spark,
        [
            (date(2026, 1, 1), "P1", 100.0, "A"),
            (date(2026, 1, 2), "P1", 200.0, "A"),
            (date(2026, 1, 10), "P1", 300.0, "A"),
        ],
    )
    r = {
        x["data_venda"]: x["media_movel_7d"] for x in construir_gold(spark, "silver_tmp").collect()
    }
    assert r[date(2026, 1, 2)] == 150.0
    assert r[date(2026, 1, 10)] == 300.0
