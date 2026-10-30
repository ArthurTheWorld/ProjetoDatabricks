"""Gold em SQL (CTE + window functions): vendas diárias por produto.

Escolha deliberada de SQL em vez de DataFrame API: é a camada que analistas leem e revisam, e a
vaga pede SQL avançado. O motivo de cada construção está nos comentários do próprio SQL.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

SQL_GOLD_VENDAS_DIARIAS = """
WITH diario AS (               -- 1) agrega primeiro: tudo abaixo trabalha com 1 linha/dia/produto
    SELECT data_venda,
           id_produto,
           ROUND(SUM(valor), 2)                 AS total_vendas,
           COUNT(*)                             AS qtd_vendas,
           COUNT(DISTINCT id_cliente_canonico)  AS clientes_distintos
    FROM {silver}
    GROUP BY data_venda, id_produto
)
SELECT data_venda,
       id_produto,
       total_vendas,
       qtd_vendas,
       clientes_distintos,
       ROUND(total_vendas / qtd_vendas, 2)      AS ticket_medio,
       -- 2) média móvel de 7 DIAS (RANGE sobre o número do dia), não de 7 LINHAS: dias sem venda
       --    não existem na tabela, e ROWS encurtaria a janela em silêncio e distorceria a média.
       ROUND(AVG(total_vendas) OVER (
           PARTITION BY id_produto
           ORDER BY unix_date(data_venda)
           RANGE BETWEEN 6 PRECEDING AND CURRENT ROW), 2)  AS media_movel_7d,
       -- 3) ranking do produto dentro do dia (RANK: empate divide a posição, sem pular lógica)
       RANK() OVER (PARTITION BY data_venda ORDER BY total_vendas DESC) AS rank_produto_dia,
       current_timestamp()                      AS _gold_at
FROM diario
"""


def construir_gold(spark: SparkSession, silver_tabela: str) -> DataFrame:
    return spark.sql(SQL_GOLD_VENDAS_DIARIAS.format(silver=silver_tabela))
