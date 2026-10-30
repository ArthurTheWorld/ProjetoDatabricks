from datetime import date

import pytest

from vendas_pipeline.ingestion.referencia import DeParaInvalido, validar_vigencias
from vendas_pipeline.ingestion.schemas import SCHEMA_DE_PARA_CLIENTES

FIM = date(9999, 12, 31)


def _df(spark, *linhas):
    return spark.createDataFrame(list(linhas), SCHEMA_DE_PARA_CLIENTES)


def test_vigencias_validas_passam(spark):
    validar_vigencias(
        _df(
            spark,
            ("C1", "A", "erp", date(2020, 1, 1), date(2024, 6, 30)),
            ("C1", "B", "erp", date(2024, 7, 1), FIM),
            ("C2", "C", "erp", date(2020, 1, 1), FIM),
        )
    )


def test_vigencias_sobrepostas_sao_rejeitadas(spark):
    with pytest.raises(DeParaInvalido, match="sobrepostas"):
        validar_vigencias(
            _df(
                spark,
                ("C1", "A", "erp", date(2020, 1, 1), date(2024, 7, 15)),
                ("C1", "B", "erp", date(2024, 7, 1), FIM),
            )
        )


def test_vigencia_invertida_e_rejeitada(spark):
    with pytest.raises(DeParaInvalido, match="invertida"):
        validar_vigencias(_df(spark, ("C1", "A", "erp", date(2024, 7, 1), date(2024, 1, 1))))


def test_mesmo_inicio_duplicado_e_rejeitado(spark):
    with pytest.raises(DeParaInvalido, match="mesmo_inicio"):
        validar_vigencias(
            _df(
                spark,
                ("C1", "A", "erp", date(2020, 1, 1), FIM),
                ("C1", "B", "erp", date(2020, 1, 1), FIM),
            )
        )
