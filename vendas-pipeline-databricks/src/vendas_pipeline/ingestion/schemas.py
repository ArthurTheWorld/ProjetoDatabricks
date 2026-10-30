"""Schemas EXPLÍCITOS. Nunca `inferSchema`.

Por quê?
1. inferSchema lê o arquivo duas vezes (custo) e CHUTA tipos: uma coluna com 99% de números
   e uma linha com "N/D" vira string, ou o contrário, dependendo da amostra.
2. O schema vira contrato: se a fonte mudar, o pipeline falha/avisa em vez de adaptar em silêncio.
3. Bronze guarda tudo como STRING de propósito: a conversão de tipo (e a decisão do que fazer com
   o valor ruim) é regra de negócio e vive na Silver, onde é testável e reprocessável.
"""

from pyspark.sql.types import DateType, StringType, StructField, StructType

COLUNAS_DADOS = [
    "id_venda",
    "id_cliente",
    "id_produto",
    "canal",
    "valor",
    "data_venda",
    "updated_at",
]
COLUNAS_META = ["_corrupt", "_source_file", "_fonte", "_run_id", "_ingested_at"]
COLUNAS_BRONZE = COLUNAS_DADOS + COLUNAS_META

SCHEMA_VENDAS_CSV = StructType(
    [StructField(c, StringType(), True) for c in COLUNAS_DADOS]
    # Coluna especial do modo PERMISSIVE: linha malformada vai inteira para cá em vez de sumir.
    + [StructField("_corrupt", StringType(), True)]
)

SCHEMA_DE_PARA_CLIENTES = StructType(
    [
        StructField("id_origem", StringType(), False),
        StructField("id_canonico", StringType(), False),
        StructField("fonte", StringType(), True),
        StructField("vigencia_inicio", DateType(), False),
        StructField("vigencia_fim", DateType(), False),
    ]
)

SCHEMA_DE_PARA_CANAL = StructType(
    [
        StructField("chave", StringType(), False),
        StructField("canal_canonico", StringType(), False),
    ]
)
