# Pipeline de Vendas Multi-fonte (Databricks · PySpark · Delta)

Pipeline Bronze → Silver → Gold que ingere 3 fontes instáveis, normaliza com de/para versionado
por vigência, valida com contratos de qualidade e só publica a Gold depois de aprovada. Construído
para demonstrar, de ponta a ponta, o que uma pessoa engenheira de dados pleno faz no dia a dia.

> **Para a entrevista, leia `docs/EXPLICACAO_COMPLETA.md`**: ela explica cada arquivo, cada
> decisão, cada trade-off e traz perguntas e respostas prováveis.

## O problema
Vendas chegam de um CSV do ERP (ISO-8859-1, datas e valores em formatos misturados), de uma API
paginada que falha intermitentemente e de uma tabela transacional incremental. O processo antigo
descartava linhas com `dropna()` sem registrar, não tratava reenvios revisados e ignorava a
vigência do de/para, então ninguém confiava no número de vendas.

## Resultado medido (ver `docs/benchmark.md`)
Contra um gabarito de 2.812 vendas válidas (dados sintéticos com sujeira contada): o legado entrega
2.092 e perde 720; o novo entrega 2.812 e perde 0, com 188 linhas na quarentena, cada uma com
motivo. O novo é mais LENTO em dado pequeno (faz mais trabalho); o ganho é correção e rastreio.

## Arquitetura
```
 ERP csv ─┐                                   ┌─> quarentena_vendas (motivo + linha crua)
 API json ┼─> bronze_vendas ─> SILVER ────────┤
 DB csv  ─┘   (string, append)   │ tipa · de/para(vigência) · dedupe · MERGE
                                 v
                           gate(silver) ─> gold_build ─> gate(gold) ─> gold_publish
                                              │                │            │
                                      gold_..._staging    dq_results   gold_vendas_diarias
                                 pipeline_run_metrics (uma linha por etapa por run)
```

## Rodar localmente
```bash
pip install -e ".[local,dev]"      # Java 11/17/21 necessário para o Spark
make teste                         # lógica, sem Delta (rápido)
make teste-completo                # inclui o MERGE de verdade (baixa os jars do Delta)
make demo                          # roda 2x e mostra as consultas da demo
make benchmark                     # legado vs novo contra o gabarito
```

## Deploy no Databricks
```bash
databricks auth login --host https://<seu-workspace>
# 1) schema e volume dos seeds (uma vez)
databricks schemas create vendas_dev workspace
databricks volumes create workspace vendas_dev seeds MANAGED
# 2) enviar os dados de demonstração
databricks fs cp -r data/seeds dbfs:/Volumes/workspace/vendas_dev/seeds
# 3) validar, publicar e rodar
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run pipeline_vendas -t dev
```
Os comandos do CLI mudam entre versões: confira com `databricks <comando> --help`.

## Estrutura
| Caminho | O que é |
|---|---|
| `src/vendas_pipeline/` | Toda a lógica (testada). `stages.py` orquestra; os notebooks só o chamam |
| `notebooks/` | 5 notebooks finos, um por task do Job |
| `resources/pipeline_job.yml`, `databricks.yml` | Job e ambientes como código (bundle) |
| `tests/` | Unitários (Spark local) e integração contra o gabarito |
| `tools/` | Gerador de seeds com gabarito, runner local, benchmark |
| `legacy/` | Script "legado" de propósito, linha de base do benchmark |
| `docs/` | Explicação completa, ADRs, runbook, estimativa, benchmark |

## Limites conhecidos (resumo; detalhes na explicação completa)
Dados sintéticos; Delta/MERGE testados só no CI (não no ambiente onde o código foi escrito);
bundle não foi validado contra um workspace real; uma ação Spark por check de qualidade.
