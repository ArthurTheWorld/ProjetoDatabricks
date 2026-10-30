# Benchmark: legado vs. novo (medido, não estimado)

Gerado por `python -m tools.benchmark` em 2026-10-23 21:35.

**Ambiente:** Linux-6.18.44-fc-v153-x86_64-with-glibc2.39, 1 vCPU, Spark 3.5.3,
armazenamento `parquet (sem Delta)`, 3000 vendas geradas
(3094 linhas brutas nas 3 fontes), 3 repetições por pipeline.

| Métrica | Legado | Novo |
|---|---:|---:|
| Tempo de execução (mediana) | 2.5 s | 19.0 s |
| Vendas válidas esperadas | 2812 | 2812 |
| Vendas válidas entregues | 2092 | 2812 |
| Vendas válidas PERDIDAS | 720 | 0 |
| Linhas inválidas que passaram | 24 | 0 |
| Linhas duplicadas na saída | 120 | 0 |
| Linhas com valor errado (versão antiga) | 40 | 0 |
| Erro na soma de faturamento | -23.09% | +0.00% |
| Linhas descartadas COM rastro (quarentena) | 0 | 188 |

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
