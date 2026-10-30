from datetime import date, datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from vendas_pipeline.ingestion.schemas import (
    COLUNAS_BRONZE,
    SCHEMA_DE_PARA_CANAL,
    SCHEMA_DE_PARA_CLIENTES,
)
from vendas_pipeline.transforms.silver import (
    aplicar_de_para_canal,
    aplicar_de_para_cliente,
    classificar_invalidos,
    construir_silver,
    deduplicar,
    limpar_tipos,
    montar_quarentena,
    normalizar_canal,
)

SCHEMA_BRONZE = StructType(
    [
        StructField(c, TimestampType() if c == "_ingested_at" else StringType(), True)
        for c in COLUNAS_BRONZE
    ]
)
INGERIDO = datetime(2026, 1, 1, 12, 0, 0)


def bronze(spark, *linhas):
    """Cada linha: dict parcial; o resto assume valores válidos."""
    padrao = {
        "id_venda": "V1",
        "id_cliente": "C1",
        "id_produto": "P1",
        "canal": "Marketplace",
        "valor": "100.00",
        "data_venda": "2026-01-10",
        "updated_at": "2026-01-10 10:00:00",
        "_corrupt": None,
        "_source_file": "f.csv",
        "_fonte": "erp_csv",
        "_run_id": "r1",
        "_ingested_at": INGERIDO,
    }
    return spark.createDataFrame(
        [tuple({**padrao, **linha}[c] for c in COLUNAS_BRONZE) for linha in linhas], SCHEMA_BRONZE
    )


def de_para_cli(spark, *linhas):
    return spark.createDataFrame(list(linhas), SCHEMA_DE_PARA_CLIENTES)


def de_para_canal(spark):
    return spark.createDataFrame(
        [("marketplace", "MARKETPLACE"), ("lojafisica", "LOJA_FISICA")], SCHEMA_DE_PARA_CANAL
    )


def test_valor_aceita_formatos_ptbr_e_en_e_rejeita_lixo(spark):
    entradas = ["1.234,56", "1234,56", "1234.56", "R$ 99,90", "abc", "", None]
    df = limpar_tipos(
        bronze(spark, *[{"valor": v, "id_venda": f"V{i}"} for i, v in enumerate(entradas)])
    )
    obtido = {r["id_venda"]: r["valor_num"] for r in df.collect()}
    esperado = [1234.56, 1234.56, 1234.56, 99.90, None, None, None]
    for i, e in enumerate(esperado):
        assert obtido[f"V{i}"] == e


def test_data_aceita_iso_e_br_e_nunca_confunde_dia_com_mes(spark):
    casos = {
        "2024-03-05": date(2024, 3, 5),
        "05/03/2024": date(2024, 3, 5),
        "03/04/2024": date(2024, 4, 3),
        "31/02/2024": None,
        "2024-13-45": None,
        None: None,
    }
    df = limpar_tipos(
        bronze(spark, *[{"data_venda": d, "id_venda": f"V{i}"} for i, d in enumerate(casos)])
    )
    obtido = {r["id_venda"]: r["data_venda_dt"] for r in df.collect()}
    for i, esperado in enumerate(casos.values()):
        assert obtido[f"V{i}"] == esperado


def test_normalizar_canal_unifica_variacoes(spark):
    variantes = ["Loja Física", "LOJA FISICA", " loja fisica ", "Loja-Fisica"]
    df = spark.createDataFrame([(v,) for v in variantes], "canal string")
    chaves = {r["k"] for r in df.select(normalizar_canal(F.col("canal")).alias("k")).collect()}
    assert chaves == {"lojafisica"}


def test_classificacao_de_motivos(spark):
    futuro = (date.today() + timedelta(days=30)).isoformat()
    linhas = [
        {"id_venda": "ok"},
        {"id_venda": "a", "_corrupt": "linha,quebrada,"},
        {"id_venda": "", "valor": "5"},
        {"id_venda": "b", "data_venda": "31/02/2024"},
        {"id_venda": "c", "data_venda": futuro},
        {"id_venda": "d", "valor": "abc"},
        {"id_venda": "e", "valor": "-10,00"},
        {"id_venda": "f", "updated_at": "ontem"},
        {"id_venda": "g", "id_cliente": ""},
    ]
    df = classificar_invalidos(limpar_tipos(bronze(spark, *linhas)))
    motivos = {r["id_venda"]: r["motivo"] for r in df.collect()}
    assert motivos == {
        "ok": None,
        "a": "linha_corrompida",
        "": "id_venda_nulo",
        "b": "data_invalida",
        "c": "data_futura",
        "d": "valor_invalido",
        "e": "valor_negativo",
        "f": "updated_at_invalido",
        "g": "id_cliente_nulo",
    }


def test_linha_corrompida_tem_prioridade_sobre_outros_defeitos(spark):
    df = classificar_invalidos(limpar_tipos(bronze(spark, {"_corrupt": "x", "valor": "abc"})))
    assert df.first()["motivo"] == "linha_corrompida"


def test_de_para_respeita_a_vigencia_na_data_da_venda(spark):
    corte = date(2026, 1, 15)
    dp = de_para_cli(
        spark,
        ("C1", "CAN-A", "erp", date(2020, 1, 1), corte - timedelta(days=1)),
        ("C1", "CAN-B", "erp", corte, date(9999, 12, 31)),
    )
    df = limpar_tipos(
        bronze(
            spark,
            {"id_venda": "antes", "data_venda": "2026-01-14"},
            {"id_venda": "no_corte", "data_venda": "2026-01-15"},
            {"id_venda": "depois", "data_venda": "2026-02-01"},
            {"id_venda": "outro", "id_cliente": "C9"},
        )
    )
    r = {x["id_venda"]: x["id_cliente_canonico"] for x in aplicar_de_para_cliente(df, dp).collect()}
    assert r == {"antes": "CAN-A", "no_corte": "CAN-B", "depois": "CAN-B", "outro": None}


def test_canal_sem_mapeamento_vira_nao_mapeado_e_nao_derruba_a_venda(spark):
    df = limpar_tipos(
        bronze(
            spark,
            {"id_venda": "m", "canal": "Marketplace"},
            {"id_venda": "x", "canal": "Parceiro X"},
        )
    )
    r = {
        x["id_venda"]: x["canal_norm"]
        for x in aplicar_de_para_canal(df, de_para_canal(spark)).collect()
    }
    assert r == {"m": "MARKETPLACE", "x": "NAO_MAPEADO"}


def test_deduplicar_mantem_a_versao_mais_recente(spark):
    df = limpar_tipos(
        bronze(
            spark,
            {"id_venda": "V1", "valor": "100", "updated_at": "2026-01-10 10:00:00"},
            {"id_venda": "V1", "valor": "120", "updated_at": "2026-01-10 15:00:00"},
            {"id_venda": "V2", "valor": "50"},
        )
    )
    r = {x["id_venda"]: x["valor_num"] for x in deduplicar(df).collect()}
    assert r == {"V1": 120.0, "V2": 50.0}


def test_toda_linha_vai_para_silver_ou_quarentena_nada_some(spark):
    dp = de_para_cli(spark, ("C1", "CAN-1", "erp", date(2020, 1, 1), date(9999, 12, 31)))
    b = bronze(
        spark,
        {"id_venda": "V1", "valor": "100", "updated_at": "2026-01-10 10:00:00"},  # versão antiga
        {"id_venda": "V1", "valor": "130", "updated_at": "2026-01-10 18:00:00"},  # versão nova
        {"id_venda": "V2", "valor": "abc"},  # inválida
        {"id_venda": "V3", "id_cliente": "C777"},  # sem de/para
        {"id_venda": "V4"},  # válida
    )
    silver, quarentena = construir_silver(b, dp, de_para_canal(spark))
    assert {r["id_venda"] for r in silver.collect()} == {"V1", "V4"}
    assert {r["id_venda"]: r["motivo"] for r in quarentena.collect()} == {
        "V2": "valor_invalido",
        "V3": "cliente_sem_de_para",
    }
    assert silver.filter("id_venda = 'V1'").first()["valor"] == 130.0


def test_quarentena_tem_chave_deterministica_e_deduplica_entradas_identicas(spark):
    dp = de_para_cli(spark, ("C1", "CAN-1", "erp", date(2020, 1, 1), date(9999, 12, 31)))
    b = bronze(spark, {"id_venda": "V2", "valor": "abc"}, {"id_venda": "V2", "valor": "abc"})
    _, q1 = construir_silver(b, dp, de_para_canal(spark))
    _, q2 = construir_silver(b, dp, de_para_canal(spark))
    assert q1.count() == 1
    assert q1.first()["_quarantine_key"] == q2.first()["_quarantine_key"]
    assert (
        "motivo"
        in montar_quarentena(
            classificar_invalidos(limpar_tipos(b)).filter("motivo is not null")
        ).columns
    )
