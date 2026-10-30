# Runbook de sustentação

Consultas úteis (troque `<c>.<s>` por catálogo e schema, ex.: `workspace.vendas_dev`):

```sql
-- O que aconteceu no último run, etapa por etapa
SELECT run_id, etapa, status, linhas_entrada, linhas_saida, linhas_quarentena, duracao_s, detalhe
FROM <c>.<s>.pipeline_run_metrics ORDER BY ts DESC LIMIT 20;

-- Quais checks falharam e com que evidência
SELECT run_id, tabela, check, severidade, falhas, total, valor, limite, amostra, erro
FROM <c>.<s>.dq_results WHERE NOT passou ORDER BY ts DESC;

-- Quarentena por motivo
SELECT motivo, count(*) n FROM <c>.<s>.quarentena_vendas GROUP BY 1 ORDER BY 2 DESC;
```

## Cenários

### 1. Falhou `ingest_bronze` por erro de API
- **Sintoma:** task falha com `ErroApiTransitorio` depois de 5 tentativas por página.
- **Causa provável:** API fora do ar ou limitando requisições.
- **Ação:** confirmar o status da API; fazer *Repair run* só da task que falhou. É seguro: a
  Bronze só é escrita no FIM (uma escrita atômica), então uma falha no meio não deixa dado pela
  metade.
- **Não fazer:** rodar o Job inteiro de novo achando que duplica; não duplica (MERGE e arquivo já
  lido são ignorados), mas desperdiça tempo.

### 2. `DeParaInvalido` na ingestão
- **Sintoma:** falha ao carregar referências (`vigencias_sobrepostas`, `vigencia_invertida`...).
- **Causa:** a tabela de de/para do negócio foi editada com vigências conflitantes.
- **Ação:** o erro lista até 5 `id_origem` problemáticos. Corrigir na fonte do de/para e repetir.
- **Por que falha de propósito:** vigência sobreposta duplicaria vendas e inflaria o faturamento
  sem nenhum erro visível.

### 3. `QuarentenaExcessiva` (mais de 30% da entrada em quarentena)
- **Sintoma:** task `silver` falha; a quarentena do run já foi gravada.
- **Ação:** `SELECT motivo, count(*) ... WHERE _run_id = '<run>'`. O motivo dominante diz o que
  mudou (ex.: `data_invalida` em massa = a fonte mudou o formato de data). Ajustar a regra em
  `transforms/silver.py` (com teste), publicar e reprocessar com `run_id = ALL`.

### 4. Gate reprovou (`QualityGateError`)
- **Sintoma:** `gate_silver` ou `gate_gold` falha; as tasks seguintes não rodam; a Gold de
  PRODUÇÃO continua com o dado do último run aprovado (os consumidores não são afetados).
- **Ação:** consultar `dq_results` do run (consulta acima); a coluna `amostra` mostra exemplos.
  Corrigir a causa (dado ou regra) e fazer *Repair run*.
- **Nunca:** desligar o check para "passar". Se o check está errado, corrija o contrato em
  `quality/contracts.py` em um PR, com justificativa.

### 5. Alerta de `NAO_MAPEADO` acima de 5% (severidade alta)
- **Sintoma:** alerta no log/webhook; o pipeline segue.
- **Ação:** listar os canais crus que não casaram e pedir ao negócio para cadastrá-los em
  `de_para_canal`:
```sql
SELECT canal, count(*) n FROM <c>.<s>.bronze_vendas b
WHERE _run_id = '<run>' GROUP BY 1 ORDER BY 2 DESC;   -- compare com de_para_canal
```
  Depois, reprocessar com `run_id = ALL`.

### 6. Reprocessar o histórico depois de corrigir uma regra
1. Corrigir e testar a regra (`pytest`).
2. Rodar `02_silver_normalize` com `run_id = ALL` (idempotente: não duplica Silver nem quarentena).
3. Rodar `03_quality_gate` -> `04_gold_build` -> `03_quality_gate` (gold) -> `05_gold_publish`
   com um `run_id` novo e consistente entre as tasks.

### 7. Voltar a Gold para uma versão anterior
Delta guarda histórico: `DESCRIBE HISTORY <c>.<s>.gold_vendas_diarias;` e depois
`RESTORE TABLE <c>.<s>.gold_vendas_diarias TO VERSION AS OF <n>;`.
