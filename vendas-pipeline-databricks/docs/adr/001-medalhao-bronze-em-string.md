# ADR-001: Arquitetura medalhão com Bronze 100% em string

**Status:** aceita

## Contexto
Três fontes com formatos e qualidade diferentes (CSV legado em ISO-8859-1, API paginada instável,
tabela transacional). As regras de limpeza vão mudar com o tempo (um novo formato de data, uma nova
regra de de/para).

## Decisão
Bronze, Silver e Gold em Delta. A Bronze é append-only, guarda TUDO como string e acrescenta
metadados (`_fonte`, `_source_file`, `_run_id`, `_ingested_at`, `_corrupt`). Tipagem, limpeza e
de/para só acontecem na Silver.

## Consequências
- (+) Corrigir uma regra = reprocessar a Silver a partir da Bronze, sem reconseguir os dados
  da fonte (a API pode nem guardar histórico).
- (+) Auditoria: dá para provar o que a fonte entregou naquele dia.
- (+) A Bronze nunca falha por dado ruim; o dado ruim é problema da Silver, onde é tratado e medido.
- (-) Armazena dado duplicado (Bronze + Silver). Aceito: armazenamento é barato perto do custo de
  perder o dado original.
- (-) Uma camada a mais para explicar. Em troca, cada camada tem uma responsabilidade só.

## Alternativas descartadas
- Tipar já na ingestão: mais rápido, mas valor ruim vira NULL na entrada e a informação original
  se perde para sempre.
- Duas camadas (raw + curated): perde-se o ponto de agregação do negócio (Gold) separado da limpeza.
