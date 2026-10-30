"""Store de TESTE: mesma interface do DeltaStore, mas em Parquet, para rodar a lógica do
pipeline onde o Delta não está disponível (ex.: sem acesso ao Maven).

IMPORTANTE: isto NÃO substitui o teste com Delta. O MERGE de verdade só é exercitado nas
execuções parametrizadas com `delta` (CI do GitHub Actions). Aqui o MERGE é emulado com a
mesma semântica: atualiza só se a versão que chegou for estritamente mais nova.
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from vendas_pipeline.storage import DeltaStore


class ParquetStore(DeltaStore):
    formato = "parquet"

    def upsert(self, spark, df, tabela, chave, ordem):
        if not self.existe(spark, tabela):
            return self.sobrescrever(df, tabela)
        atual = spark.table(tabela).withColumn("_origem", F.lit(0))  # 0 = já gravado
        novo = df.withColumn("_origem", F.lit(1))
        janela = Window.partitionBy(*chave).orderBy(F.col(ordem).desc(), F.col("_origem").asc())
        melhor = (
            atual.unionByName(novo)
            .withColumn("_rn", F.row_number().over(janela))
            .filter("_rn = 1")
            .drop("_rn", "_origem")
        )
        tmp = f"{tabela}__tmp"
        self.sobrescrever(melhor, tmp)
        self.sobrescrever(spark.table(tmp), tabela)
        spark.sql(f"DROP TABLE {tmp}")

    def inserir_se_ausente(self, spark, df, tabela, chave):
        if not self.existe(spark, tabela):
            return self.sobrescrever(df, tabela)
        novos = df.join(spark.table(tabela).select(*chave), on=chave, how="left_anti")
        self.anexar(novos, tabela)
