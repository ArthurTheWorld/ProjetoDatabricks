# ADR-004: Contratos de qualidade declarativos, com severidade e fail-closed

**Status:** aceita

## Contexto
Precisamos de testes automatizados de qualidade que sejam legíveis por quem não escreve Spark e
que decidam, com critério, o que bloqueia e o que apenas avisa.

## Decisão
- Checks descritos em dados (`quality/contracts.py`), executados por um motor genérico.
- Três severidades: `critica` bloqueia (exceção), `alta` alerta, `media` só registra.
- FAIL-CLOSED: se o check em si quebra, o resultado é "reprovado" com o erro registrado.
- Cada resultado vai para `dq_results` com evidência (falhas, total, amostra), mesmo quando o
  gate bloqueia.
- Implementação própria (e não Great Expectations/Soda): ~150 linhas, sem dependência extra.

## Consequências
- (+) Adicionar um check = uma linha de contrato.
- (+) Um validador que quebra não deixa dado ruim passar.
- (-) Reimplementa o que bibliotecas maduras já oferecem (relatórios HTML, perfis, integração).
  Em um time grande, avaliar Great Expectations/Soda/DQX do Databricks. Aqui a escolha torna cada
  linha explicável, o que importa para o objetivo do projeto.
- (-) Uma ação Spark por check. Débito conhecido: combinar not_null/range em uma só agregação.
