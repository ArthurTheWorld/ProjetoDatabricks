"""Motor de checks: lê o contrato (contracts.py) e executa cada check sobre um DataFrame.

Princípios de projeto:
- FAIL-CLOSED: se o próprio check quebra (coluna inexistente, tabela de referência ausente),
  o resultado é `passou=False` com o erro registrado. Um validador que falha em silêncio e deixa
  passar é pior do que não ter validador.
- Cada check devolve evidência (falhas, total, amostra), não só um booleano: quem recebe o alerta
  precisa saber O QUE olhar.
- Trade-off conhecido: uma ação Spark por check. Para tabelas grandes, o próximo passo é
  combinar not_null/range em uma única agregação (uma passada).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vendas_pipeline.quality.contracts import CONTRATOS


@dataclass
class CheckResult:
    tabela: str
    check: str
    tipo: str
    severidade: str
    passou: bool
    falhas: int | None = None
    total: int | None = None
    valor: float | None = None
    limite: float | None = None
    amostra: str | None = None
    erro: str | None = None


@dataclass
class Contexto:
    spark: SparkSession
    resolver: Callable[[str], str]  # nome lógico -> nome completo da tabela
    agora: datetime


def _amostra(df: DataFrame, n: int = 5) -> str:
    return json.dumps([r.asDict() for r in df.limit(n).collect()], default=str, ensure_ascii=False)


def _not_null(df, spec, ctx, total):
    col = spec["col"]
    falhas = df.filter(F.col(col).isNull()).count()
    return {"check": f"not_null:{col}", "passou": falhas == 0, "falhas": falhas}


def _unique(df, spec, ctx, total):
    cols = spec["cols"]
    duplicadas = df.groupBy(*cols).count().filter(F.col("count") > 1)
    falhas = duplicadas.count()
    return {
        "check": f"unique:{'+'.join(cols)}",
        "passou": falhas == 0,
        "falhas": falhas,
        "amostra": _amostra(duplicadas) if falhas else None,
    }


def _range(df, spec, ctx, total):
    col = spec["col"]
    # NULL conta como falha: "não sei o valor" não satisfaz "valor >= 0".
    fora = F.col(col).isNull()
    if "min" in spec:
        fora = fora | (F.col(col) < spec["min"])
    if "max" in spec:
        fora = fora | (F.col(col) > spec["max"])
    invalidas = df.filter(fora)
    falhas = invalidas.count()
    return {
        "check": f"range:{col}[{spec.get('min', '-inf')},{spec.get('max', '+inf')}]",
        "passou": falhas == 0,
        "falhas": falhas,
        "amostra": _amostra(invalidas.select(col)) if falhas else None,
    }


def _freshness(df, spec, ctx, total):
    col = spec["col"]
    ultimo = df.agg(F.max(col)).first()[0]
    if ultimo is None:
        horas = 1e9
    else:
        if not isinstance(ultimo, datetime):
            ultimo = (
                datetime.combine(ultimo, datetime.min.time())
                if isinstance(ultimo, date)
                else ultimo
            )
        horas = (ctx.agora - ultimo).total_seconds() / 3600
    return {
        "check": f"freshness:{col}<={spec['max_horas']}h",
        "passou": horas <= spec["max_horas"],
        "valor": float(horas),
        "limite": float(spec["max_horas"]),
    }


def _max_ratio(df, spec, ctx, total):
    falhas = df.filter(F.expr(spec["expr"])).count()
    razao = falhas / total if total else 0.0
    return {
        "check": f"max_ratio:{spec['expr']}<={spec['max']}",
        "passou": razao <= spec["max"],
        "falhas": falhas,
        "valor": razao,
        "limite": float(spec["max"]),
    }


def _min_linhas(df, spec, ctx, total):
    # Tabela vazia passa em todos os outros checks (não há linha para falhar). Este pega o
    # pior bug silencioso: o pipeline "deu certo" e entregou zero linhas.
    return {
        "check": f"min_linhas>={spec['min']}",
        "passou": total >= spec["min"],
        "valor": float(total),
        "limite": float(spec["min"]),
    }


def _referencial(df, spec, ctx, total):
    col, ref_col = spec["col"], spec["ref_col"]
    ref = ctx.spark.table(ctx.resolver(spec["ref_table"])).select(ref_col).distinct()
    orfaos = df.join(ref, df[col] == ref[ref_col], "left_anti")
    falhas = orfaos.count()
    return {
        "check": f"referencial:{col}->{spec['ref_table']}.{ref_col}",
        "passou": falhas == 0,
        "falhas": falhas,
        "amostra": _amostra(orfaos.select(col).distinct()) if falhas else None,
    }


def _reconciliacao(df, spec, ctx, total):
    col, ref_col = spec["col"], spec["ref_col"]
    a = df.agg(F.sum(col)).first()[0] or 0.0
    b = ctx.spark.table(ctx.resolver(spec["ref_table"])).agg(F.sum(ref_col)).first()[0] or 0.0
    # Tolerância RELATIVA: arredondar por grupo (ROUND(SUM,2)) acumula erro de centavos.
    diferenca_rel = abs(a - b) / max(abs(b), 1.0)
    return {
        "check": f"reconciliacao:sum({col})~{spec['ref_table']}.sum({ref_col})",
        "passou": diferenca_rel <= spec["tolerancia_rel"],
        "valor": float(diferenca_rel),
        "limite": float(spec["tolerancia_rel"]),
        "amostra": json.dumps({"soma_tabela": a, "soma_referencia": b}),
    }


REGISTRO = {
    "not_null": _not_null,
    "unique": _unique,
    "range": _range,
    "freshness": _freshness,
    "max_ratio": _max_ratio,
    "min_linhas": _min_linhas,
    "referencial": _referencial,
    "reconciliacao": _reconciliacao,
}


def executar_checks(
    df: DataFrame, tabela: str, ctx: Contexto, contratos: dict[str, list[dict]] | None = None
) -> list[CheckResult]:
    contratos = CONTRATOS if contratos is None else contratos
    total = df.count()  # uma contagem compartilhada por todos os checks
    resultados: list[CheckResult] = []
    for spec in contratos.get(tabela, []):
        base = {
            "tabela": tabela,
            "tipo": spec["tipo"],
            "severidade": spec["severidade"],
            "total": total,
        }
        try:
            parcial = REGISTRO[spec["tipo"]](df, spec, ctx, total)
            resultados.append(CheckResult(**base, **parcial))
        except Exception as exc:  # noqa: BLE001 - fail-closed, ver docstring do módulo
            resultados.append(
                CheckResult(
                    **base,
                    check=f"{spec['tipo']}:{spec.get('col', '')}",
                    passou=False,
                    erro=f"{type(exc).__name__}: {str(exc)[:300]}",
                )
            )
    return resultados
