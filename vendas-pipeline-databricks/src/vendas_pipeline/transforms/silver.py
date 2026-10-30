"""Transformações da Silver: funções PURAS (DataFrame entra, DataFrame sai), sem I/O.

Fluxo: tipar -> classificar inválidos -> de/para (cliente, canal) -> deduplicar -> quarentena.
Cada função faz uma coisa e é testada isoladamente em tests/unit/test_silver_transforms.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from vendas_pipeline.ingestion.schemas import COLUNAS_BRONZE

_COM_ACENTO = "áàâãäéèêëíìîïóòôõöúùûüç"
_SEM_ACENTO = "aaaaaeeeeiiiiooooouuuuc"


def normalizar_canal(coluna: F.Column) -> F.Column:
    """'Loja Física ' / 'LOJA FISICA' / 'loja-fisica' -> 'lojafisica'.

    Normaliza no CÓDIGO (minúsculas, sem acento, só [a-z0-9]) e deixa a tabela de de/para só
    com a chave limpa. Assim o negócio não precisa cadastrar cada variação de digitação.
    """
    sem_acento = F.translate(F.lower(F.trim(coluna)), _COM_ACENTO, _SEM_ACENTO)
    return F.regexp_replace(sem_acento, "[^a-z0-9]", "")


def limpar_tipos(bronze: DataFrame) -> DataFrame:
    """Converte string -> tipo certo SEM lançar erro: o que não converte vira NULL e será
    classificado depois. `try_cast`/`try_to_timestamp` são seguros também com ANSI mode ligado
    (padrão no serverless), onde `cast`/`to_date` LANÇAM exceção em valor inválido."""
    valor_txt = F.regexp_replace(F.trim(F.col("valor")), r"[R$\s]", "")
    # 'pt-BR' (1.234,56) vs 'en' (1234.56): se tem vírgula, ponto é milhar e vírgula é decimal.
    valor_norm = F.when(
        valor_txt.contains(","),
        F.regexp_replace(F.regexp_replace(valor_txt, r"\.", ""), ",", "."),
    ).otherwise(valor_txt)

    # O regex escolhe o formato ANTES de converter: '03/04/2024' nunca é lido como yyyy-MM-dd.
    data_expr = (
        "CASE "
        "WHEN data_venda RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
        "THEN CAST(try_to_timestamp(data_venda, 'yyyy-MM-dd') AS DATE) "
        "WHEN data_venda RLIKE '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' "
        "THEN CAST(try_to_timestamp(data_venda, 'dd/MM/yyyy') AS DATE) END"
    )
    return (
        bronze.withColumn("_valor_norm", valor_norm)
        .withColumn("valor_num", F.expr("try_cast(_valor_norm AS DOUBLE)"))
        .withColumn("data_venda_dt", F.expr(data_expr))
        .withColumn("updated_at_ts", F.expr("try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss')"))
        .withColumn("canal_chave", normalizar_canal(F.col("canal")))
        .drop("_valor_norm")
    )


def classificar_invalidos(df: DataFrame) -> DataFrame:
    """Adiciona `motivo` (NULL = válido). A ORDEM do when() é a prioridade do motivo."""
    vazio = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")  # noqa: E731
    motivo = (
        F.when(F.col("_corrupt").isNotNull(), "linha_corrompida")
        .when(vazio("id_venda"), "id_venda_nulo")
        .when(F.col("data_venda_dt").isNull(), "data_invalida")
        .when(F.col("data_venda_dt") > F.current_date(), "data_futura")
        .when(F.col("valor_num").isNull(), "valor_invalido")
        .when(F.col("valor_num") < 0, "valor_negativo")
        .when(F.col("updated_at_ts").isNull(), "updated_at_invalido")
        .when(vazio("id_cliente"), "id_cliente_nulo")
    )
    return df.withColumn("motivo", motivo)


def aplicar_de_para_cliente(df: DataFrame, de_para: DataFrame) -> DataFrame:
    """Resolve o id canônico do cliente NA DATA DA VENDA (SCD tipo 2 simplificado).

    Se o cliente C0007 virou CAN-0507 em 01/07, uma venda de junho continua apontando para o
    id antigo e uma de julho para o novo. Um lookup simples reescreveria o passado.
    Pré-requisito: vigências sem sobreposição (validar_vigencias), senão o join duplica linhas.
    """
    dp = de_para.select(
        F.col("id_origem").alias("_dp_origem"),
        F.col("id_canonico").alias("id_cliente_canonico"),
        F.col("vigencia_inicio").alias("_dp_ini"),
        F.col("vigencia_fim").alias("_dp_fim"),
    )
    condicao = (F.col("id_cliente") == F.col("_dp_origem")) & F.col("data_venda_dt").between(
        F.col("_dp_ini"), F.col("_dp_fim")
    )
    return df.join(dp, condicao, "left").drop("_dp_origem", "_dp_ini", "_dp_fim")


def aplicar_de_para_canal(df: DataFrame, de_para_canal: DataFrame) -> DataFrame:
    """Canal sem mapeamento NÃO é quarentena: a venda é válida, só a dimensão é desconhecida.
    Vira 'NAO_MAPEADO' e é monitorada por taxa (check `max_ratio`) em vez de bloquear."""
    dp = F.broadcast(
        de_para_canal.select(
            F.col("chave").alias("_dp_chave"), F.col("canal_canonico").alias("_canal_canonico")
        )
    )
    return (
        df.join(dp, F.col("canal_chave") == F.col("_dp_chave"), "left")
        .withColumn("canal_norm", F.coalesce(F.col("_canal_canonico"), F.lit("NAO_MAPEADO")))
        .drop("_dp_chave", "_canal_canonico")
    )


def deduplicar(df: DataFrame) -> DataFrame:
    """Uma linha por id_venda: a versão com `updated_at` mais recente vence.
    Desempate por `_ingested_at` (a ingerida depois). Nota: dedupe acontece DEPOIS da validação
    ('último válido vence'): se a versão mais nova for inválida, ela vai para a quarentena e a
    última versão boa continua valendo. Ver ADR-003."""
    janela = Window.partitionBy("id_venda").orderBy(
        F.col("updated_at_ts").desc(), F.col("_ingested_at").desc()
    )
    return df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")


def montar_quarentena(invalidos: DataFrame) -> DataFrame:
    """Linha crua da Bronze + motivo + chave determinística (idempotência do MERGE)."""
    chave = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c), F.lit(""))
                for c in (
                    "_fonte",
                    "id_venda",
                    "updated_at",
                    "valor",
                    "data_venda",
                    "id_cliente",
                    "_corrupt",
                )
            ],
            F.col("motivo"),
        ),
        256,
    )
    return (
        invalidos.select(*COLUNAS_BRONZE, "motivo")
        .withColumn("_quarantine_key", chave)
        .withColumn("_quarantined_at", F.current_timestamp())
        .dropDuplicates(["_quarantine_key"])
    )


def construir_silver(
    bronze: DataFrame, de_para_clientes: DataFrame, de_para_canal: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Devolve (silver, quarentena). Toda linha da entrada cai em exatamente um dos dois
    (ou é descartada como duplicata de uma versão mais antiga): nada some sem rastro."""
    classificado = classificar_invalidos(limpar_tipos(bronze))
    invalidos_regra = classificado.filter(F.col("motivo").isNotNull())
    validos = classificado.filter(F.col("motivo").isNull())

    com_cliente = aplicar_de_para_cliente(validos, de_para_clientes)
    sem_de_para = com_cliente.filter(F.col("id_cliente_canonico").isNull()).withColumn(
        "motivo", F.lit("cliente_sem_de_para")
    )
    com_match = com_cliente.filter(F.col("id_cliente_canonico").isNotNull())

    silver = deduplicar(aplicar_de_para_canal(com_match, de_para_canal)).select(
        "id_venda",
        F.col("id_cliente"),
        "id_cliente_canonico",
        "id_produto",
        F.col("canal_norm").alias("canal"),
        F.col("valor_num").alias("valor"),
        F.col("data_venda_dt").alias("data_venda"),
        F.col("updated_at_ts").alias("updated_at"),
        "_fonte",
        "_run_id",
        "_ingested_at",
        F.current_timestamp().alias("_silver_at"),
    )
    quarentena = montar_quarentena(
        invalidos_regra.unionByName(sem_de_para, allowMissingColumns=True)
    )
    return silver, quarentena
