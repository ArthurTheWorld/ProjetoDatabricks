"""Camada de armazenamento: o ÚNICO lugar que sabe escrever em Delta.

Por que isolar isso?
- Toda lógica de negócio (limpeza, de/para, checks) fica em funções que recebem e devolvem
  DataFrames e, portanto, testam em Spark local sem Delta.
- MERGE, append e overwrite ficam atrás de uma interface pequena. Os testes de lógica
  podem trocar a implementação; o teste de integração com Delta valida a de verdade.

Trade-off: uma camada extra de indireção. Em troca, ganhamos testabilidade e um ponto único
para trocar de formato (Delta -> Iceberg) ou de estratégia de escrita.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


class DeltaStore:
    formato = "delta"

    def existe(self, spark: SparkSession, tabela: str) -> bool:
        return spark.catalog.tableExists(tabela)

    def anexar(self, df: DataFrame, tabela: str) -> None:
        """Append puro. Usado na Bronze (histórico imutável) e nas tabelas de observabilidade."""
        df.write.format(self.formato).mode("append").saveAsTable(tabela)

    def sobrescrever(self, df: DataFrame, tabela: str) -> None:
        """Overwrite transacional (o Delta troca os arquivos atomicamente; há time travel)."""
        (
            df.write.format(self.formato)
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela)
        )

    def upsert(
        self, spark: SparkSession, df: DataFrame, tabela: str, chave: list[str], ordem: str
    ) -> None:
        """MERGE idempotente: atualiza só se a versão que chegou é MAIS NOVA que a gravada.

        A condição `s.<ordem> > t.<ordem>` impede que o reprocessamento de dados antigos
        sobrescreva dados novos (idempotência + proteção contra evento fora de ordem).
        Pré-requisito: `df` sem chaves duplicadas (o Delta rejeita MERGE ambíguo).
        """
        from delta.tables import DeltaTable

        if not self.existe(spark, tabela):
            self.sobrescrever(df, tabela)
            return
        condicao = " AND ".join(f"t.{c} = s.{c}" for c in chave)
        (
            DeltaTable.forName(spark, tabela)
            .alias("t")
            .merge(df.alias("s"), condicao)
            .whenMatchedUpdateAll(condition=f"s.{ordem} > t.{ordem}")
            .whenNotMatchedInsertAll()
            .execute()
        )

    def inserir_se_ausente(
        self, spark: SparkSession, df: DataFrame, tabela: str, chave: list[str]
    ) -> None:
        """MERGE só-insert: reprocessar a mesma entrada não duplica linhas (quarentena)."""
        from delta.tables import DeltaTable

        if not self.existe(spark, tabela):
            self.sobrescrever(df, tabela)
            return
        condicao = " AND ".join(f"t.{c} = s.{c}" for c in chave)
        (
            DeltaTable.forName(spark, tabela)
            .alias("t")
            .merge(df.alias("s"), condicao)
            .whenNotMatchedInsertAll()
            .execute()
        )
