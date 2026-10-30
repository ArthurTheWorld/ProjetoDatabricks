"""Cria a SparkSession local (com ou sem Delta) usada pelos testes, runner e benchmark."""

from __future__ import annotations

import tempfile

from pyspark.sql import SparkSession


def criar_spark(
    delta: bool, warehouse: str | None = None, shuffle_partitions: int = 4
) -> SparkSession:
    warehouse = warehouse or tempfile.mkdtemp(prefix="warehouse_")
    b = (
        SparkSession.builder.master("local[*]")
        .appName("vendas-local")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    if not delta:
        return b.getOrCreate()
    from delta import configure_spark_with_delta_pip

    b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    return configure_spark_with_delta_pip(b).getOrCreate()
