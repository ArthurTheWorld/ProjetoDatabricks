# ADR-005: Gold construída em staging e publicada só com atestado de qualidade

**Status:** aceita

## Contexto
O requisito é "garantir a confiabilidade da base ANTES que ela seja consumida em analytics".
Validar a Gold depois de publicada é tarde: o BI já leu o dado ruim.

## Decisão
Fluxo de tasks: `ingest_bronze -> silver -> gate_silver -> gold_build -> gate_gold -> gold_publish`.
A Gold é construída em `gold_vendas_diarias_staging`; o gate valida o staging; só então
`gold_publish` faz overwrite transacional da tabela de produção. A publicação ainda exige um
"atestado": resultados em `dq_results` para o MESMO `run_id`, sem críticos reprovados.

## Consequências
- (+) O consumidor nunca vê dado reprovado: a Gold de produção só muda após aprovação.
- (+) Defesa em profundidade: o atestado vale mesmo se alguém rodar o notebook de publicação
  isolado ou reconfigurar o Job sem a dependência.
- (-) Escrita dupla da Gold (staging + produção) e mais tasks. Em volume grande, trocar por
  `CREATE OR REPLACE ... AS` sobre o staging ou `CLONE`.
- (-) A Gold é recalculada por inteiro a cada execução. Evolução: recalcular só as datas afetadas.
