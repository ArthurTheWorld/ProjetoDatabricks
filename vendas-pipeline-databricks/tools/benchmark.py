"""Compara o pipeline LEGADO com o NOVO contra o MESMO gabarito e grava docs/benchmark.md.

Por que medir contra um gabarito e não só tempo? Porque o ganho real de uma refatoração de dados
raramente é velocidade: é CORREÇÃO (quantas vendas válidas chegam certas), RASTREABILIDADE (o que
foi descartado e por quê) e IDEMPOTÊNCIA. Tempo é reportado, inclusive quando o novo é mais lento.

  python -m tools.benchmark --vendas 3000 --repeticoes 3 [--sem-delta]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import tempfile
import time
import uuid
from datetime import datetime

import pyspark

from legacy.pipeline_monolitico import executar as executar_legado
from tests.support.parquet_store import ParquetStore
from tools.gerar_seeds import gerar
from tools.spark_local import criar_spark
from vendas_pipeline import stages
from vendas_pipeline.settings import SILVER, Settings
from vendas_pipeline.storage import DeltaStore


def avaliar(
    linhas: list[tuple], verdade: dict[str, float], brutas: int, valor_esperado: float
) -> dict:
    ids = [i for i, _ in linhas]
    distintos = set(ids)
    validas = set(verdade)
    valor_errado = sum(
        1 for i, v in linhas if i in validas and (v is None or abs(v - verdade[i]) > 0.01)
    )
    soma = sum(v for _, v in linhas if v is not None)
    return {
        "linhas_saida": len(linhas),
        "validas_entregues": len(distintos & validas),
        "validas_perdidas": len(validas - distintos),
        "invalidas_entregues": len(distintos - validas),
        "duplicatas": len(ids) - len(distintos),
        "linhas_com_valor_errado": valor_errado,
        "erro_soma_pct": 100 * (soma - valor_esperado) / valor_esperado,
        "linhas_brutas": brutas,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vendas", type=int, default=3000)
    ap.add_argument("--repeticoes", type=int, default=3)
    ap.add_argument("--sem-delta", action="store_true")
    ap.add_argument("--saida", default="docs/benchmark.md")
    args = ap.parse_args()

    seeds = tempfile.mkdtemp(prefix="seeds_")
    verdade = gerar(seeds, n_vendas=args.vendas)
    with open(f"{seeds}/ground_truth_ids.json", encoding="utf-8") as f:
        ids_validos = json.load(f)
    brutas = sum(verdade["linhas_brutas"].values())
    esperado = verdade["valor_total_valido"]

    spark = criar_spark(delta=not args.sem_delta)
    spark.sparkContext.setLogLevel("ERROR")
    spark.range(1000).count()  # aquecimento da JVM: não cobrar o start-up de nenhum dos dois

    t_leg, t_new = [], []
    for _ in range(args.repeticoes):
        destino = tempfile.mkdtemp(prefix="legado_") + "/saida"
        ini = time.time()
        executar_legado(spark, seeds, destino)
        t_leg.append(time.time() - ini)
        leg = [(r["id_venda"], r["valor"]) for r in spark.read.parquet(destino).collect()]

        settings = Settings(
            schema=f"bench_{uuid.uuid4().hex[:6]}",
            seed_path=seeds,
            store=ParquetStore() if args.sem_delta else DeltaStore(),
        )
        ini = time.time()
        resumo = stages.run_all(spark, settings)
        t_new.append(time.time() - ini)
        new = [(r["id_venda"], r["valor"]) for r in spark.table(settings.tabela(SILVER)).collect()]

    m_leg = avaliar(leg, ids_validos, brutas, esperado)
    m_new = avaliar(new, ids_validos, brutas, esperado)
    m_leg["rastreadas_na_quarentena"] = 0
    m_new["rastreadas_na_quarentena"] = resumo["quarentena"]

    def mediana(xs):
        return statistics.median(xs)

    linhas = [
        ("Tempo de execução (mediana)", f"{mediana(t_leg):.1f} s", f"{mediana(t_new):.1f} s"),
        ("Vendas válidas esperadas", f"{len(ids_validos)}", f"{len(ids_validos)}"),
        ("Vendas válidas entregues", m_leg["validas_entregues"], m_new["validas_entregues"]),
        ("Vendas válidas PERDIDAS", m_leg["validas_perdidas"], m_new["validas_perdidas"]),
        (
            "Linhas inválidas que passaram",
            m_leg["invalidas_entregues"],
            m_new["invalidas_entregues"],
        ),
        ("Linhas duplicadas na saída", m_leg["duplicatas"], m_new["duplicatas"]),
        (
            "Linhas com valor errado (versão antiga)",
            m_leg["linhas_com_valor_errado"],
            m_new["linhas_com_valor_errado"],
        ),
        (
            "Erro na soma de faturamento",
            f"{m_leg['erro_soma_pct']:+.2f}%",
            f"{m_new['erro_soma_pct']:+.2f}%",
        ),
        ("Linhas descartadas COM rastro (quarentena)", 0, m_new["rastreadas_na_quarentena"]),
    ]
    tabela = "| Métrica | Legado | Novo |\n|---|---:|---:|\n" + "\n".join(
        f"| {a} | {b} | {c} |" for a, b, c in linhas
    )

    texto = f"""# Benchmark: legado vs. novo (medido, não estimado)

Gerado por `python -m tools.benchmark` em {datetime.now():%Y-%m-%d %H:%M}.

**Ambiente:** {platform.platform()}, {os.cpu_count()} vCPU, Spark {pyspark.__version__},
armazenamento `{"parquet (sem Delta)" if args.sem_delta else "delta"}`, {args.vendas} vendas geradas
({brutas} linhas brutas nas 3 fontes), {args.repeticoes} repetições por pipeline.

{tabela}

## Como ler estes números (com honestidade)

- O "legado" é um script que EU escrevi com os vícios clássicos (`legacy/pipeline_monolitico.py`).
  Ele ilustra o padrão; não é o legado de nenhum cliente. Para medir um legado real, troque o
  arquivo e reuse o harness: o gabarito e as métricas continuam valendo.
- Os dados são sintéticos, com sujeira injetada e CONTADA (`tools/gerar_seeds.py`). Por isso é
  possível dizer exatamente quantas vendas válidas existem. Em dados reais você não tem gabarito:
  o equivalente é a reconciliação com a fonte e os checks de `dq_results`.
- Tempo: volume pequeno, máquina pequena. O pipeline novo faz MAIS trabalho (valida, deduplica,
  grava métricas e roda 2 gates), então não espere que seja mais rápido em dado pequeno. O ganho
  que importa aqui é de correção e rastreabilidade. Se rodar com `--vendas 200000` e Delta, os
  números de tempo mudam; reporte os seus.
- Se este arquivo foi gerado com `parquet (sem Delta)`, rode de novo na sua máquina com Delta
  antes de citar tempos: o MERGE tem custo próprio.
"""
    with open(args.saida, "w", encoding="utf-8") as f:
        f.write(texto)
    print(texto)


if __name__ == "__main__":
    main()
