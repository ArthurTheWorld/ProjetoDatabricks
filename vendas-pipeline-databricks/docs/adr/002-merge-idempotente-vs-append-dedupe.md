# ADR-002: Silver com MERGE idempotente (e não append + dedupe na leitura)

**Status:** aceita

## Contexto
Jobs são reexecutados (falha, repair run, reprocessamento). Fontes reenviam registros revisados.
A mesma venda pode chegar mais de uma vez, em versões diferentes e fora de ordem.

## Decisão
`MERGE` por `id_venda`, atualizando apenas quando `s.updated_at > t.updated_at`. A quarentena usa
MERGE só-insert por uma chave determinística (`sha2` dos campos crus + motivo).

## Consequências
- (+) Reexecutar o mesmo run N vezes dá o mesmo resultado (testado: `test_rodar_duas_vezes...`).
- (+) Reprocessar dado antigo não sobrescreve dado novo (a condição `>` protege).
- (+) A Silver tem uma linha por venda: quem consome não precisa deduplicar.
- (-) MERGE é mais caro que append (reescreve arquivos). Em volume alto, mitigar com
  particionamento/Liquid Clustering por data e limitar o MERGE ao intervalo afetado.
- (-) Exige fonte sem chaves duplicadas (a deduplicação por `row_number` vem antes do MERGE).

## Alternativas descartadas
- Append + `DISTINCT`/`ROW_NUMBER` na leitura: barato para escrever, caro e propenso a erro para
  CADA consumidor (basta um esquecer o filtro para o número de vendas dobrar).
