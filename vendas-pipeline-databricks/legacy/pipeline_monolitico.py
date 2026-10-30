"""PIPELINE "LEGADO" (de propósito). Representa os vícios mais comuns de um script que cresceu
sem teste, sem contrato e sem observabilidade. NÃO use como modelo: serve de linha de base para
o benchmark (tools/benchmark.py) e de roteiro para a conversa de refatoração.

Vícios (cada um é uma bomba-relógio com nome):
 1. inferSchema=True            -> tipos chutados; lê o arquivo duas vezes
 2. dropna() em tudo            -> descarta linhas SEM RASTRO (ninguém sabe quantas nem por quê)
 3. dropDuplicates() sem chave  -> só remove cópias idênticas; versões revisadas viram duplicata
 4. replace(',', '.') no valor  -> "1.234,56" vira "1.234.56" e some no cast
 5. to_date com UM formato      -> todas as datas dd/MM/yyyy viram NULL e somem no dropna
 6. join com o de/para por id   -> ignora a vigência (duplica linhas) e descarta quem não casa
 7. sem validação de negativo/futuro, sem métrica, sem teste
 8. overwrite do destino        -> sem histórico; execução parcial apaga o dado bom
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

COLUNAS = ["id_venda", "id_cliente", "id_produto", "canal", "valor", "data_venda", "updated_at"]


def executar(spark: SparkSession, seed_path: str, saida: str) -> int:
    erp = spark.read.csv(f"{seed_path}/erp_vendas.csv", header=True, inferSchema=True)
    db = spark.read.csv(f"{seed_path}/db_vendas.csv", header=True, inferSchema=True)
    api = spark.read.option("multiLine", True).json(f"{seed_path}/api_vendas.json")
    de_para = spark.read.csv(f"{seed_path}/de_para_clientes.csv", header=True, inferSchema=True)

    todos = None
    for df in (erp, db, api):
        df = df.select(*[F.col(c).cast("string").alias(c) for c in COLUNAS])
        todos = df if todos is None else todos.unionByName(df)

    df = todos.dropna().dropDuplicates()
    df = df.withColumn("valor", F.regexp_replace("valor", ",", ".").cast("double"))
    df = df.withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
    df = df.dropna()
    df = df.join(de_para, df.id_cliente == de_para.id_origem)
    df.select("id_venda", "id_cliente", "id_canonico", "valor", "data_venda").write.mode(
        "overwrite"
    ).parquet(saida)
    return spark.read.parquet(saida).count()
