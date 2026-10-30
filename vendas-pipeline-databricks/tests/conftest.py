import os
import tempfile
import uuid

import pytest
from pyspark.sql import SparkSession

from tests.support.parquet_store import ParquetStore
from tools.gerar_seeds import gerar
from vendas_pipeline.settings import Settings
from vendas_pipeline.storage import DeltaStore


def _builder(warehouse: str):
    return (
        SparkSession.builder.master("local[1]")
        .appName("vendas-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )


@pytest.fixture(scope="session")
def _spark_e_delta():
    """Sessão única. Tenta Delta (precisa baixar jars do Maven na 1ª vez); se não der, cai para
    Spark puro e os testes marcados com `delta` são pulados (VENDAS_SEM_DELTA=1 força isso)."""
    warehouse = tempfile.mkdtemp(prefix="warehouse_")
    if not os.environ.get("VENDAS_SEM_DELTA"):
        try:
            from delta import configure_spark_with_delta_pip

            b = (
                _builder(warehouse)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
            return configure_spark_with_delta_pip(b).getOrCreate(), True
        except Exception:  # noqa: BLE001
            pass
    return _builder(warehouse).getOrCreate(), False


@pytest.fixture(scope="session")
def spark(_spark_e_delta):
    return _spark_e_delta[0]


@pytest.fixture(scope="session")
def delta_disponivel(_spark_e_delta):
    return _spark_e_delta[1]


@pytest.fixture(params=["parquet", "delta"])
def ambiente(request, spark, delta_disponivel, tmp_path):
    """(settings, gabarito) com seeds novos e schema isolado por teste."""
    if request.param == "delta" and not delta_disponivel:
        pytest.skip("Delta indisponível neste ambiente (rode no CI ou com acesso ao Maven)")
    store = DeltaStore() if request.param == "delta" else ParquetStore()
    schema = f"t_{uuid.uuid4().hex[:8]}"
    seeds = tmp_path / "seeds"
    verdade = gerar(seeds, n_vendas=600, seed=11)
    settings = Settings(schema=schema, seed_path=str(seeds), store=store)
    yield settings, verdade
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
