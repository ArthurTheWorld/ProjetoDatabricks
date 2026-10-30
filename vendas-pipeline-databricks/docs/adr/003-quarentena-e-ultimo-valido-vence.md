# ADR-003: Quarentena em vez de descarte, e "último válido vence"

**Status:** aceita

## Contexto
Registros inválidos existem. O pipeline legado os descartava com `dropna()` e ninguém sabia
quantos, nem por quê.

## Decisão
1. Toda linha inválida vai para `quarentena_vendas` com `motivo`, a linha CRUA e uma chave
   determinística. Nada é apagado sem rastro.
2. Guard-rails por taxa: quarentena > 10% da entrada alerta; > 30% bloqueia a Silver (fonte
   provavelmente quebrada). A quarentena é gravada ANTES do bloqueio, para ter evidência.
3. A deduplicação acontece DEPOIS da validação ("último válido vence"): se a versão mais nova
   de uma venda for inválida, ela vai para a quarentena e a última versão boa continua valendo.

## Consequências
- (+) Rastreabilidade e reprocessamento: corrigido o problema na origem, reprocessa-se `ALL`.
- (+) A taxa de quarentena vira métrica de saúde da fonte.
- (-) Mais armazenamento e uma tabela a mais para operar (ver runbook).
- (-) O item 3 é uma escolha de DISPONIBILIDADE: a Silver pode mostrar, por um tempo, um valor
  que a fonte já revisou (para um valor inválido). A alternativa (deduplicar antes) priorizaria
  frescor, mas faria a venda sumir da Silver quando a versão nova é inválida. Quem decide é o
  negócio; deixamos explícito e testado.
- Canal sem mapeamento NÃO vai para quarentena: a venda é válida e só a dimensão é desconhecida.
  Vira `NAO_MAPEADO` e é monitorada por taxa (check `max_ratio`, severidade alta).
