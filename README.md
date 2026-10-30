# Explicação completa do projeto (para você defender na entrevista)

Este documento existe para que você consiga explicar **cada linha relevante**, **por que ela está
ali** e **o que você sacrificou ao escolhê-la**. Leia na ordem da primeira vez; depois use o
índice como consulta.

1. [A história em 60 segundos](#1-a-história-em-60-segundos)
2. [Mapa: o que a vaga pede → onde está no projeto](#2-mapa-vaga--projeto)
3. [Arquitetura](#3-arquitetura)
4. [Decisões transversais e trade-offs](#4-decisões-transversais-e-trade-offs)
5. [Arquivo por arquivo](#5-arquivo-por-arquivo)
6. [A vida de uma linha suja](#6-a-vida-de-uma-linha-suja-exemplos-concretos)
7. [O que foi testado e o que NÃO foi](#7-o-que-foi-testado-e-o-que-não-foi-leia-isto)
8. [Limitações e como evoluir](#8-limitações-e-como-evoluir)
9. [Perguntas e respostas](#9-perguntas-e-respostas-prováveis)
10. [Roteiro de demo de 7 minutos](#10-roteiro-de-demo-de-7-minutos)
11. [Correções em relação às versões anteriores desta conversa](#11-correções-em-relação-ao-que-eu-tinha-sugerido-antes)

---

## 1. A história em 60 segundos

> "Montei um pipeline de vendas com três fontes problemáticas: um CSV legado do ERP com encoding
> e formatos misturados, uma API paginada que falha, e uma tabela incremental. O ponto central não
> é ingerir, é **confiar no número**. Cada linha que entra termina em um de dois lugares: na
> Silver, tipada e deduplicada, ou na quarentena, com o motivo. Nada some sem rastro. A Gold só é
> publicada depois de um gate de qualidade aprovar; se reprovar, os consumidores continuam vendo
> o dado do último run bom. Medi contra um gabarito: o script legado perdia 720 das 2.812 vendas
> válidas e o novo perde zero, mas é mais lento, porque valida mais. Esse trade-off é consciente."

Três ideias que sustentam tudo (repita-as ao longo da conversa):
1. **Nada some sem rastro** (quarentena com motivo).
2. **Reexecutar é seguro** (idempotência: MERGE, watermark, arquivo já lido é ignorado).
3. **Dado ruim não chega ao consumidor** (gate antes de publicar).

---

## 2. Mapa: vaga → projeto

| A vaga pede | Onde está | Prova |
|---|---|---|
| Pipelines de ingestão e tratamento no Databricks | `ingestion/`, `transforms/`, `notebooks/`, `resources/pipeline_job.yml` | Job de 6 tasks |
| Rotinas de de/para e validação automatizada | `transforms/silver.py` (`aplicar_de_para_*`), `ingestion/referencia.py` (`validar_vigencias`) | `test_de_para_*` |
| Confiabilidade antes do consumo em analytics | `quality/gate.py`, `stages.stage_gold_publish` (atestado) | `test_gate_bloqueia_e_a_gold_publicada_nao_muda` |
| Testes de qualidade e observabilidade | `quality/`, `observability/`, tabelas `dq_results` e `pipeline_run_metrics` | `test_quality_engine.py` |
| Dados inconsistentes e fontes instáveis | `api_client.py` (retry), `PERMISSIVE`+`_corrupt`, quarentena, watermark | `test_api_client.py`, e2e |
| Refatorar legado | `legacy/pipeline_monolitico.py` vs. o restante | `docs/benchmark.md` |
| Medir e comunicar ganhos | `tools/benchmark.py` | `docs/benchmark.md` (números medidos) |
| Versionamento, CI/CD, documentação | `.github/workflows/ci.yml`, `databricks.yml`, `docs/` | ADRs, runbook |
| Estimar prazos | `docs/estimativa.md` | modelo de três pontos |
| Databricks: notebooks, Jobs, Delta | `notebooks/`, job YAML, `storage.py` (MERGE) | ver seção 7 sobre o que foi executado |
| SQL avançado | `transforms/gold.py` (CTE + window functions com RANGE) | `test_gold.py` |

---

## 3. Arquitetura

```
 ERP csv (ISO-8859-1) ─┐
 API json (instável)  ─┼─> BRONZE ──────> SILVER ───────────────────┐
 DB csv (incremental) ─┘  tudo string      tipa · classifica         │
                          append-only      de/para (vigência)        │
                          + metadados      dedupe · MERGE            │
                                              │                      │
                                              └──> QUARENTENA        │
                                                   (motivo + crua)   │
                                                                     v
   gate(silver) ──> gold_build ──> gate(gold) ──> gold_publish ──> GOLD (produção)
       │                │              │                │
       v                v              v                v
   dq_results     gold_staging     dq_results   pipeline_run_metrics
```

**Tabelas (todas Delta):**

| Tabela | Papel | Escrita |
|---|---|---|
| `bronze_vendas` | cópia fiel das fontes + metadados | append |
| `silver_vendas` | uma linha por venda, tipada, canônica | MERGE (atualiza só se mais nova) |
| `quarentena_vendas` | linhas rejeitadas com motivo | MERGE só-insert (chave determinística) |
| `de_para_clientes`, `de_para_canal` | referências | overwrite (após validar) |
| `gold_vendas_diarias_staging` | Gold ainda não aprovada | overwrite |
| `gold_vendas_diarias` | o que o BI lê | overwrite transacional após o atestado |
| `dq_results` | resultado de cada check de cada run | append |
| `pipeline_run_metrics` | uma linha por etapa por run | append |

**Job (DAG):** `ingest_bronze → silver → gate_silver → gold_build → gate_gold → gold_publish`.
Se qualquer gate falhar, as tasks seguintes não executam e a Gold de produção não muda.

---

## 4. Decisões transversais e trade-offs

| Decisão | Por quê | O que você sacrifica | Quando mudaria |
|---|---|---|---|
| **Delta em todas as camadas** | ACID, `MERGE`, overwrite transacional, time travel (`RESTORE`), enforcement de schema | Pequeno custo vs. Parquet puro; acoplamento ao ecossistema Delta | Se o time padronizasse em Iceberg (a `storage.py` é o único ponto a trocar) |
| **Bronze 100% string** | Corrigir regra = reprocessar Silver sem reler a fonte | Armazenamento duplicado | Nunca para dado de fonte não reproduzível |
| **Schema explícito (sem `inferSchema`)** | Contrato; evita ler duas vezes; sem tipos chutados | Precisa manter o schema quando a fonte muda | — |
| **Lógica em funções puras (DataFrame→DataFrame)** | Testável em Spark local, sem Databricks | Uma camada de indireção (`stages.py`, `storage.py`) | — |
| **Notebooks finos** | O que roda em produção é o que o CI testou | Menos "exploratório" | — |
| **`storage.py` isola o Delta** | Testes de lógica rodam sem jars do Delta; ponto único de escrita | Uma interface a mais para entender | — |
| **MERGE idempotente** (ADR-002) | Reexecução e reprocessamento seguros | MERGE custa mais que append | Volume muito alto: particionar/limitar o MERGE ao intervalo afetado |
| **Quarentena em vez de descarte** (ADR-003) | Rastreabilidade, reprocessamento, métrica de saúde da fonte | Mais armazenamento e uma tabela a operar | — |
| **Contratos declarativos próprios** (ADR-004) | Legível, sem dependência, cada linha explicável | Reimplementa o que Great Expectations/Soda/DQX fazem | Time grande: adotar uma lib madura |
| **Gate + staging + atestado** (ADR-005) | Consumidor nunca vê dado reprovado | Escrita dupla da Gold | Volume grande: `CLONE`/`CREATE OR REPLACE` |
| **Serverless no Job** | YAML independente de nuvem, sem cluster ocioso, funciona no Free Edition | Menos controle fino de tuning; limitações do serverless (ex.: sem `cache()`) | Carga pesada/previsível: cluster dedicado e tuning |
| **`try_cast`/`try_to_timestamp`** | Funcionam também com ANSI mode ligado (serverless), onde `cast`/`to_date` lançam exceção | Sintaxe via `F.expr` | — |

---

## 5. Arquivo por arquivo

Formato: **O que faz** · **Por que assim** · **Trade-off** · **Pergunta provável**.

### `src/vendas_pipeline/settings.py`
- **O que faz:** define os nomes lógicos das tabelas e a classe `Settings` (`catalog`, `schema`,
  `seed_path`, `store`). `Settings.tabela("silver_vendas")` devolve `catalogo.schema.silver_vendas`
  (ou `schema.silver_vendas` quando `catalog=None`, nos testes locais).
- **Por que assim:** o código só usa nomes lógicos; só este arquivo conhece catálogo/schema. Dev e
  prod são o mesmo código com `schema` diferente (`vendas_dev` / `vendas_prod` no bundle).
- **Trade-off:** `Settings` é um objeto passado por todo lado (explícito) em vez de variável global
  (implícito). Mais verboso, muito mais testável: cada teste cria o seu schema isolado.
- **Pergunta:** "Como você separa dev de prod?" → mesmo código, `target` do bundle muda o schema (e
  o `mode: development` prefixa recursos com o usuário e pausa agendamentos).

### `src/vendas_pipeline/storage.py`
- **O que faz:** `DeltaStore` com `anexar`, `sobrescrever`, `upsert` (MERGE) e
  `inserir_se_ausente` (MERGE só-insert).
- **Por que assim:** é o ÚNICO lugar que escreve em Delta. Toda a lógica de negócio recebe e
  devolve DataFrames, então testa sem Delta; MERGE/overwrite ficam atrás de uma interface pequena.
- **Detalhe que vale citar:** `upsert` usa `whenMatchedUpdateAll(condition="s.updated_at >
  t.updated_at")`. Reprocessar dado velho não sobrescreve dado novo (proteção contra evento fora
  de ordem). `sobrescrever` usa `overwriteSchema=true` para a Gold poder evoluir de schema.
- **Trade-off:** indireção extra. **Limite honesto:** esta é a parte que meu ambiente NÃO executou
  (sem acesso ao Maven para baixar os jars do Delta); ela é exercitada pelas variantes `delta` dos
  testes de integração, que rodam no CI (seção 7).
- **Pergunta:** "Por que MERGE e não append + dedupe?" → ADR-002.

### `src/vendas_pipeline/ingestion/schemas.py`
- **O que faz:** schemas explícitos das fontes e dos de/para; listas de colunas.
- **Por que assim:** sem `inferSchema` (chuta tipo e lê duas vezes). Schema é contrato. Bronze é
  string de propósito (ADR-001). `_corrupt` é a coluna especial do modo PERMISSIVE.
- **Trade-off:** manutenção manual do schema quando a fonte muda — e é isso que você quer: mudança
  de fonte virar falha/alerta visível em vez de adaptação silenciosa.

### `src/vendas_pipeline/ingestion/api_client.py`
- **O que faz:** `ApiInstavel` (mock determinístico que falha em ~10% das chamadas e pagina),
  `com_retry` (backoff exponencial + jitter) e `buscar_tudo` (todas as páginas, retry por página).
- **Por que assim:**
  - Backoff exponencial dá tempo da API se recuperar; **jitter** evita que várias execuções
    retentem no mesmo instante (*thundering herd*).
  - Só repete `ConnectionError`/`TimeoutError`: erro de lógica (`KeyError`, 4xx) não melhora
    repetindo.
  - Retry **por página**: uma falha na página 7 não refaz as páginas 1–6.
  - `sleep` injetável: o teste roda em milissegundos.
  - `since` permite ingestão incremental pelo `updated_at`.
- **Trade-off:** é um mock; o cliente real (`requests`) manteria a mesma interface. 5 tentativas
  é um valor escolhido, não medido.
- **Pergunta:** "E se a API cair de vez?" → a task falha após as tentativas; nada foi gravado na
  Bronze (escrita atômica no fim); *Repair run* da task. Alerta pelo e-mail do Job.

### `src/vendas_pipeline/ingestion/bronze.py`
- **O que faz:** lê as 3 fontes, acrescenta metadados e faz **uma única** escrita `append`.
- **Por que assim (os pontos que mais rendem conversa):**
  - **`PERMISSIVE` + `_corrupt`**: `FAILFAST` derrubaria o job por uma linha ruim; `DROPMALFORMED`
    descartaria sem rastro; PERMISSIVE guarda a linha crua para a Silver mandar à quarentena.
  - **Encoding `ISO-8859-1`** no ERP: sem isso "Loja Física" quebra.
  - **ERP idempotente por arquivo:** faz `left_anti` contra os `_source_file` já ingeridos. Em
    produção, Auto Loader (`cloudFiles`) faz isso nativamente com checkpoint.
  - **Watermark com `>=`**, não `>`: nunca perde registros que compartilham o timestamp da borda;
    custo: reingere a borda, e a Silver deduplica (idempotência resolve).
  - **Watermark só sobre timestamps bem formados** (regex): um `updated_at` sujo tipo `"ZZZ"`
    ordena depois de qualquer data e envenenaria o watermark para sempre.
  - **Escrita atômica única** das 3 fontes: ou o run inteiro entra, ou nada. Isso faz do `run_id`
    uma fronteira confiável para a Silver ("o que chegou neste run").
- **Trade-off:** watermark guardado como "máximo da própria Bronze" (simples, sem tabela de
  controle). Em produção eu usaria uma tabela de controle explícita. A API e o DB são lidos
  inteiros em memória do driver (aceitável em volume pequeno; em grande, ler em streaming/paginado).
- **Pergunta:** "Dois jobs rodando juntos?" → `max_concurrent_runs: 1` no Job; e o MERGE do Delta
  tem controle otimista de concorrência.

### `src/vendas_pipeline/ingestion/referencia.py`
- **O que faz:** carrega `de_para_clientes` e `de_para_canal` e **valida** antes de usar:
  vigência invertida, mesmo início duplicado e vigências sobrepostas (self-join).
- **Por que assim:** vigência sobreposta faz cada venda casar com 2 linhas do de/para e **duplica
  o faturamento sem erro nenhum**. Falhar cedo e alto custa muito menos que descobrir no BI.
- **Trade-off:** o self-join é O(n²) por `id_origem`; para um de/para pequeno é irrelevante, e para
  milhões de linhas converteria em janela (`lead(vigencia_inicio)`).
- **Pergunta:** "O que acontece quando o negócio edita o de/para errado?" → `DeParaInvalido` com
  até 5 ids de exemplo; runbook cenário 2.

### `src/vendas_pipeline/transforms/silver.py` (o coração)
Funções puras, uma responsabilidade cada:

| Função | O que faz | Detalhe que importa |
|---|---|---|
| `normalizar_canal` | `'Loja Física '` / `'LOJA FISICA'` / `'loja-fisica'` → `lojafisica` | Normaliza no CÓDIGO (minúsculas, sem acento, só `[a-z0-9]`); o de/para só guarda a chave limpa, então o negócio não cadastra cada digitação |
| `limpar_tipos` | `valor`, `data_venda`, `updated_at` → tipos certos, **sem lançar erro** | Valor: se tem vírgula, ponto é milhar e vírgula é decimal (`1.234,56`→`1234.56`). Data: regex escolhe o formato ANTES de converter (`03/04/2024` é 3 de abril, nunca lido como ISO). `try_*` funciona com ANSI ligado |
| `classificar_invalidos` | adiciona `motivo` (NULL = válido) | A ordem do `when()` é a prioridade: `linha_corrompida`, `id_venda_nulo`, `data_invalida`, `data_futura`, `valor_invalido`, `valor_negativo`, `updated_at_invalido`, `id_cliente_nulo` |
| `aplicar_de_para_cliente` | resolve o id canônico **na data da venda** | Join por `data_venda BETWEEN vigencia_inicio AND vigencia_fim` (SCD tipo 2 simplificado): uma venda de junho mantém o id antigo, uma de julho usa o novo. Lookup simples reescreveria o passado |
| `aplicar_de_para_canal` | canal → canônico; sem match vira `NAO_MAPEADO` | **Não** vai para quarentena: a venda é válida, só a dimensão é desconhecida; monitorado por taxa |
| `deduplicar` | 1 linha por `id_venda`, `updated_at` mais recente; desempate `_ingested_at` | Acontece **depois** da validação: "último válido vence" (ADR-003) |
| `montar_quarentena` | linha crua + `motivo` + `_quarantine_key` (sha2) | A chave determinística torna a escrita idempotente (reprocessar não duplica) |
| `construir_silver` | compõe tudo e devolve `(silver, quarentena)` | Toda linha cai em exatamente um destino (ou é versão antiga deduplicada) |

- **Trade-offs:** `try_cast` perde o motivo fino ("abc" e "" viram o mesmo `valor_invalido`); o
  valor `"1.234"` (sem vírgula) é ambíguo e é lido como 1,234 (limitação documentada; o produtor
  deveria mandar um formato único). Dedupe-depois-de-validar prioriza disponibilidade.
- **Pergunta:** "Por que o join de de/para é `left` e depois separa?" → para não perder a linha:
  quem não casa vira `cliente_sem_de_para` na quarentena, com rastro.

### `src/vendas_pipeline/transforms/gold.py`
- **O que faz:** SQL com CTE + window functions: total, quantidade, clientes distintos, ticket
  médio, **média móvel de 7 dias** e ranking do produto no dia.
- **Por que SQL:** é a camada que analistas leem e revisam; e a vaga pede SQL avançado.
- **Detalhe de ouro:** a média móvel usa `RANGE BETWEEN 6 PRECEDING` sobre `unix_date(data_venda)`
  (dias corridos), **não** `ROWS`. Dias sem venda não existem na tabela; com `ROWS` a janela
  encurtaria em silêncio e distorceria a média (há teste: dias 1, 2 e 10).
- **Trade-off:** recalcula a Gold inteira a cada run. Para volume grande, recalcular só as datas
  afetadas e fazer MERGE por (`data_venda`, `id_produto`).

### `src/vendas_pipeline/quality/contracts.py`
- **O que faz:** os contratos por tabela, em dados. Severidades: `critica` bloqueia, `alta`
  alerta, `media` só registra.
- **Por que assim:** quem revisa não precisa ler Spark; adicionar check = uma linha.
- **Checks que valem destacar:** `min_linhas` (tabela vazia passa em todos os outros checks, é o
  bug silencioso "o pipeline deu certo e entregou zero linhas"); `reconciliacao` Gold×Silver
  (pega join duplicando, filtro esquecido, agregação errada); `referencial` (órfãos);
  `max_ratio` (canal `NAO_MAPEADO` ≤ 5%).
- **Por que a reconciliação usa tolerância RELATIVA (`1e-6`):** a Gold faz `ROUND(SUM,2)` por
  grupo; o erro de centavos se acumula em milhares de grupos. Uma tolerância absoluta pequena
  daria falso positivo (descobri isso ao projetar, e está comentado no código).

### `src/vendas_pipeline/quality/engine.py`
- **O que faz:** `REGISTRO` mapeia o `tipo` do contrato para uma função; `executar_checks` roda
  todas e devolve `CheckResult` (passou, falhas, total, valor, limite, **amostra**, erro).
- **Por que assim:**
  - **Fail-closed:** se o próprio check quebra (coluna inexistente, tabela de referência ausente)
    o resultado é "reprovado" com o erro. Validador que falha em silêncio e deixa passar é pior
    que não ter validador.
  - **Evidência, não só booleano:** `amostra` mostra exemplos do que falhou; quem recebe o alerta
    sabe o que olhar.
  - `total` é contado uma vez e compartilhado.
- **Trade-off conhecido:** uma ação Spark por check. Para tabela grande, combinar `not_null` e
  `range` em uma agregação única (uma passada). Está listado como débito, não escondido.

### `src/vendas_pipeline/quality/gate.py`
- **O que faz:** executa o contrato, **persiste** em `dq_results`, alerta os de severidade `alta`
  e **lança `QualityGateError`** se houver crítico reprovado.
- **Dois detalhes que mostram experiência:**
  1. A evidência é gravada **antes** de lançar a exceção; quem investiga precisa dela.
  2. O bloqueio é uma exceção. `dbutils.notebook.exit("erro")` **encerra o notebook com
     SUCESSO** e o Job seguiria para a Gold (corrigi isso em relação ao que eu havia sugerido
     antes, seção 11).

### `src/vendas_pipeline/observability/`
- **`logs.py`:** logs em JSON, uma linha por evento (`ts`, `nivel`, `evento`, `run_id`, `etapa`...).
  Texto livre exige regex; JSON exige um filtro por campo.
- **`metrics.py`:** `Recorder.etapa("silver")` é um *context manager* que mede a duração e grava
  uma linha em `pipeline_run_metrics` **inclusive quando a etapa falha** (`status=falha`) e
  re-lança a exceção. Base do dashboard e da prova dos ganhos.
- **`alerts.py`:** `alertar()` loga WARNING e, se existir `ALERT_WEBHOOK_URL`, posta no
  Slack/Teams. **Nunca derruba o pipeline**: falhar ao alertar não pode virar falha de dados.
  Complementa o e-mail `on_failure` do Job: o Job avisa "QUEBROU", o alerta avisa "DEGRADADO".
- **Trade-off:** sem Prometheus/OpenTelemetry; tabelas Delta consultáveis por SQL bastam para
  este escopo e viram dashboard no Databricks SQL.

### `src/vendas_pipeline/stages.py`
- **O que faz:** uma função por task do Job (`stage_ingest`, `stage_silver`, `stage_gate`,
  `stage_gold_build`, `stage_gold_publish`) e `run_all` (usado pelo runner local e pelos testes).
- **Pontos para destacar:**
  - **Guard-rails da quarentena:** > 10% alerta; > 30% bloqueia (`QuarentenaExcessiva`). A
    quarentena é gravada ANTES do bloqueio para ter evidência.
  - **`run_id="ALL"`** reprocessa a Bronze inteira (seguro porque MERGE e quarentena são
    idempotentes).
  - **`_exigir_atestado`**: a publicação consulta `dq_results` do mesmo `run_id`; sem resultado
    ou com crítico reprovado, recusa. Defesa em profundidade além da dependência entre tasks.
- **Trade-off:** as contagens (`count()`) disparam ações extras; em volume grande, derivar as
  contagens das métricas do próprio MERGE (`operationMetrics` do Delta).

### `src/vendas_pipeline/notebook_utils.py` e `notebooks/*.py`
- **O que fazem:** os notebooks têm ~5 linhas de código: declaram widgets, montam `Settings` e
  chamam uma `stage_*`. O texto (`%md`) explica o porquê dentro do próprio notebook.
- **Por que assim:** notebook com lógica própria é o caminho para o "legado" de amanhã. Aqui o
  que roda em produção é o que o CI testou.
- **`sys.path.append(os.path.abspath("../src"))`:** simples e funciona com o bundle sincronizando
  o repositório. **Trade-off:** o caminho de produção maduro é empacotar um *wheel* e instalá-lo
  na task; simplicidade primeiro, wheel quando houver mais de um consumidor.

### `databricks.yml` e `resources/pipeline_job.yml`
- **O que fazem:** definem o Job (6 tasks encadeadas por `depends_on`), variáveis
  (`catalog`, `schema`, `alert_email`) e dois ambientes (`dev`, `prod`).
- **Por que assim:** `mode: development` prefixa recursos com o usuário e pausa agendamentos;
  `max_concurrent_runs: 1` evita duas execuções escrevendo nas mesmas tabelas;
  `run_id: "{{job.run_id}}"` dá o mesmo id a todas as tasks e sobrevive a um *repair run*;
  sem cluster declarado, roda em compute serverless.
- **Limite honesto:** não validei este YAML contra um workspace real (`databricks bundle validate`
  exige credenciais). Rode a validação antes de apresentar; está no CI (job `validar-bundle`).

### `.github/workflows/ci.yml`
- **O que faz:** lint (`ruff`) → testes de lógica sem Delta (rápidos, com cobertura) → testes
  completos com Delta (o MERGE real) → validação do bundle (só se houver `DATABRICKS_HOST`).
- **Por que assim:** feedback rápido primeiro, o caro depois. Em produção o deploy seria por
  *service principal*, nunca por pessoa.

### `tools/gerar_seeds.py`
- **O que faz:** gera dados sujos de forma determinística (`seed` fixa) **e o gabarito** (quantas
  vendas são válidas, quantas vão para a quarentena e por qual motivo, o valor correto de cada
  venda válida).
- **Por que assim:** transforma o teste de integração em "o pipeline acertou o gabarito?" em vez
  de "rodou sem erro". Cada linha defeituosa tem exatamente um defeito, então a contagem por
  motivo é exata.
- **Trade-off/limite:** dado sintético não tem as surpresas do dado real. O equivalente em dado
  real é reconciliar com a fonte e olhar `dq_results`. Os dados são relativos à data de geração
  (`data_futura` = hoje + 30 dias): **rode `make seeds` antes da demo** para o `freshness` e as
  datas fazerem sentido.

### `tests/`
- **Unitários (Spark local):** conversão de valor e data (inclui a ambiguidade `03/04`),
  classificação de cada motivo, prioridade de `linha_corrompida`, de/para na fronteira da
  vigência, dedupe, "nada some", retry (com falhas simuladas), cada check do motor, fail-closed,
  Gold (incluindo a média móvel com dias faltantes).
- **Integração (`test_pipeline_e2e.py`):** pipeline completo contra o gabarito: contagem, soma,
  quarentena por motivo, "nada some", vigência do de/para, Gold fecha com Silver, métricas
  registradas; **rodar duas vezes não duplica nada**; gate bloqueia e a Gold publicada não muda;
  de/para sobreposto derruba a ingestão; quarentena excessiva bloqueia a Silver.
- **`ParquetStore` (em `tests/support/`):** emula o MERGE em Parquet para rodar a lógica onde não
  há Delta. **Não substitui** o teste com Delta (seção 7).
- **Verifiquei que os testes pegam bugs:** quebrei de propósito a regra de vigência (`AND TRUE`) e
  o teste de de/para falhou.

### `legacy/pipeline_monolitico.py` e `tools/benchmark.py`
- O legado reúne vícios clássicos numerados no próprio docstring (`inferSchema`, `dropna()`
  sem rastro, `dropDuplicates()` sem chave, vírgula→ponto que quebra `1.234,56`, `to_date` com um
  só formato, join sem vigência, sem teste/métrica, `overwrite`).
- O benchmark mede os dois contra o mesmo gabarito e reporta **inclusive o tempo desfavorável**.
- **Seja explícito na entrevista:** o "legado" é um script que eu escrevi para ilustrar o
  padrão; não é o legado de nenhum cliente.

---

## 6. A vida de uma linha suja (exemplos concretos)

Use estes exemplos na demo: concretos convencem mais que a arquitetura.

**Exemplo A: valor e data em formato brasileiro**
`ERP-000123, C0042, P007, "Loja Física", "1.234,56", "05/03/2026", "2026-03-05 09:00:00"`
1. Bronze: entra exatamente assim (tudo string), com `_fonte=erp_csv`, `_source_file`, `_run_id`.
2. `limpar_tipos`: tem vírgula, então `1.234,56` → remove o ponto → `1234,56` → `1234.56` → `1234.56`
   (double). Data casa com `dd/MM/yyyy` → `2026-03-05`. Canal → `lojafisica`.
3. Sem motivo de rejeição → de/para do cliente na data `2026-03-05` → id canônico.
4. Canal `lojafisica` → `LOJA_FISICA`. Dedupe: só existe uma versão. MERGE na Silver.

**Exemplo B: venda revisada (reenvio)**
`ERP-000123` chega de novo com valor diferente e `updated_at` 4 horas depois. As duas linhas são
válidas; `deduplicar` mantém a mais recente; o MERGE só atualiza porque `s.updated_at > t.updated_at`.
Se o mesmo arquivo for reprocessado depois, a versão velha **não** sobrescreve a nova.

**Exemplo C: valor `"abc"`**
Vira `NULL` em `limpar_tipos` → `motivo = valor_invalido` → quarentena com a linha crua e a chave
determinística. Reprocessar não duplica (MERGE só-insert).

**Exemplo D: cliente que trocou de código em 8/set** (as datas do exemplo são as do seed gerado em 23/10/2026; ao regenerar os seeds, a data de corte muda e fica em `ground_truth.json`)
`C0001` → `CAN-0001` até 7/set e `CAN-0501` a partir de 8/set. Uma venda de 20/ago fica com
`CAN-0001`; uma de 15/set, com `CAN-0501`. É isso que "de/para com vigência" significa na prática.

**Exemplo E: linha com campos a mais**
`PERMISSIVE` guarda a linha inteira em `_corrupt`; a Silver classifica como `linha_corrompida`
(prioridade máxima, antes de qualquer outra regra) e vai para a quarentena.

---

## 7. O que foi testado e o que NÃO foi (leia isto)

**Executado de verdade, no ambiente onde escrevi o código** (Spark 3.5.3, Java 21, 1 vCPU):
- 36 testes passando (unitários + integração), lint limpo, cobertura de 96% das linhas do pacote.
- O pipeline completo, de ponta a ponta, contra o gabarito (contagem, soma, quarentena por
  motivo, vigência do de/para, idempotência, gate bloqueando, guard-rail da quarentena).
- O benchmark (números em `docs/benchmark.md`).

**NÃO foi executado — você precisa fazer antes de apresentar:**
1. **Delta e MERGE.** Meu ambiente não alcança o Maven (onde o Delta baixa seus jars), então os
   testes usaram `ParquetStore`, que emula o MERGE com a mesma semântica. As 5 variantes `delta`
   dos testes foram **puladas** aqui. `storage.py` é a parte sem cobertura (50%) por isso. Rode
   `make teste-completo` na sua máquina (ou deixe o CI rodar): é o teste que prova o MERGE real.
2. **Deploy no Databricks.** Não fiz `bundle validate`/`deploy`/`run` (exigem seu workspace).
   O YAML foi lido por um parser, mas não validado pelo CLI. Rode `databricks bundle validate -t dev`.
3. **Serverless.** Escrevi o código evitando o que o serverless não suporta (`cache()`) e usando
   `try_cast` por causa do ANSI mode, mas não o executei lá.
4. **O workflow do GitHub Actions** nunca rodou (só foi parseado como YAML).
5. **Comandos do CLI do Databricks no README** variam entre versões; confira com `--help`.
6. Afirmei que o Databricks Free Edition só tem compute serverless; confirme na sua conta.

Se algo falhar ao rodar na sua máquina, o mais provável é detalhe de ambiente (Java, rede para o
Maven, permissões do Unity Catalog). Me mande o erro e eu corrijo.

**Sobre os números do benchmark:** vieram de um ambiente de 1 vCPU, com Parquet no lugar de Delta,
3.000 vendas sintéticas. Valem como demonstração do método e da direção (correção e rastreio),
**não** como medida de performance de produção. Rode `make benchmark` com Delta e cite os seus.

---

## 8. Limitações e como evoluir

Cada item abaixo é uma resposta pronta para "o que você faria diferente?".

| Limitação | Como evoluir |
|---|---|
| Uma ação Spark por check | Combinar `not_null`/`range` em uma agregação (uma passada); derivar contagens de `operationMetrics` do Delta |
| Gold recalculada por inteiro | Recalcular só as datas afetadas e fazer MERGE por (`data_venda`,`id_produto`) |
| Watermark = máximo da Bronze | Tabela de controle explícita por fonte |
| ERP lido por arquivo com `left_anti` | Auto Loader (`cloudFiles`) com checkpoint |
| API/DB lidos em memória do driver | Leitura paginada/streaming, ou ingestão por Lakeflow Connect/JDBC particionado |
| `sys.path` para importar `src/` | Empacotar um wheel no bundle e instalar na task |
| Contratos de qualidade próprios | Great Expectations/Soda/DQX do Databricks em time maior; manter o gate e o atestado |
| Alerta por webhook simples | Integrar ao sistema de alertas do cliente (PagerDuty/Opsgenie) e ao Databricks SQL Alerts sobre `dq_results` |
| Sem lineage/governança | Unity Catalog: lineage automático, tags e permissões por camada |
| `1.234` (sem vírgula) ambíguo | Combinar com o produtor do ERP um formato único; validar por fonte |
| Dado sintético | Rodar em dado real anonimizado; reconciliar com a fonte |
| Sem particionamento/otimização | `CLUSTER BY` (Liquid Clustering) por data, `OPTIMIZE`/`VACUUM` agendados |

---

## 9. Perguntas e respostas prováveis

**1. Por que Delta e não Parquet?**
Preciso de MERGE idempotente, overwrite transacional (o BI nunca vê tabela pela metade), enforcement
de schema e time travel para auditoria e `RESTORE`. Custo: pequeno overhead e acoplamento; a
`storage.py` isola isso.

**2. Como você garante idempotência se o Job rodar duas vezes?**
Em quatro lugares: a Bronze ignora arquivos já lidos e usa watermark (`>=`, a borda é reingerida);
a Silver faz MERGE por `id_venda` só atualizando se a versão é mais nova; a quarentena faz MERGE
só-insert por chave determinística; a Gold é overwrite. Há um teste que roda o pipeline duas vezes
e compara contagens e somas.

**3. O que acontece se a API cair no meio?**
Cada página tem retry com backoff e jitter. Esgotadas as tentativas, a task falha **antes** de
gravar (a Bronze é escrita uma vez, no fim), então não fica dado pela metade. *Repair run* da task.

**4. Como você garante que a Gold não publica dado ruim?**
Três camadas: gate crítico na Silver (task falha, dependentes não rodam); a Gold é construída em
staging e validada (inclusive reconciliação com a Silver); a publicação exige um atestado em
`dq_results` para o mesmo `run_id`. Se algo reprova, a Gold de produção continua com o último
dado aprovado.

**5. Um check crítico falhou às 6h. O que acontece e quem é avisado?**
A task falha, o e-mail `on_failure` do Job dispara, o BI continua lendo a Gold anterior. A
evidência está em `dq_results` (com amostra). Runbook, cenário 4.

**6. Por que quarentena e não descarte?**
Rastreabilidade e reprocessamento: corrigida a causa, reprocesso e a linha entra. A taxa de
quarentena vira métrica de saúde da fonte (>10% alerta, >30% bloqueia).

**7. Por que o de/para tem vigência?**
Porque o passado não pode mudar. Se um cliente trocou de código em 8/set, vendas anteriores
continuam com o id antigo. Lookup simples reescreveria o histórico. Como vigência sobreposta
duplica linhas em silêncio, valido as vigências antes de usar.

**8. Por que o canal sem mapeamento não vai para a quarentena?**
Porque a venda é válida; só a dimensão é desconhecida. Bloquear venda boa por causa de uma
categoria nova prejudica o negócio. Vira `NAO_MAPEADO` e é monitorado por taxa (≤5%, severidade
alta).

**9. Por que Bronze em string?**
Para poder corrigir regras sem reler a fonte (que pode nem guardar histórico) e porque a Bronze
nunca deve falhar por dado ruim. A conversão e a decisão sobre valor ruim são regra de negócio e
vivem na Silver, onde são testadas.

**10. Por que não `inferSchema`?**
Chuta tipos conforme a amostra, lê o arquivo duas vezes e esconde mudanças da fonte. Schema
explícito é contrato.

**11. Por que `try_cast` em vez de `cast`?**
Com ANSI mode ligado (padrão no serverless), `cast` lança exceção em valor inválido e derrubaria o
job. `try_cast` devolve NULL e o valor segue para a classificação/quarentena.

**12. Por que a média móvel usa `RANGE` e não `ROWS`?**
Dias sem venda não existem na tabela; com `ROWS` a janela de "7 dias" encurtaria em silêncio e
distorceria a média. `RANGE` sobre o número do dia respeita dias corridos. Tem teste.

**13. Como você mede qualidade ao longo do tempo?**
`dq_results` e `pipeline_run_metrics` são séries temporais por run: taxa de quarentena, falhas por
check, duração por etapa. Dá para montar um dashboard em Databricks SQL e alertas sobre elas.

**14. Como você escalaria para 100x o volume?**
Medir primeiro. Provavelmente: Liquid Clustering por data, MERGE limitado ao intervalo afetado,
Gold incremental por data, checks em uma passada, `broadcast` já usado nos de/para, Auto Loader no
lugar do `left_anti` por arquivo e leitura paginada em vez de coletar no driver.

**15. Qual é o maior débito técnico?**
Ser honesto: (a) uma ação Spark por check; (b) Gold recalculada por inteiro; (c) watermark derivado
da Bronze em vez de tabela de controle; (d) o MERGE foi validado no CI, não na minha máquina de
desenvolvimento original. Todos estão documentados com o caminho de evolução.

**16. Por que implementou o motor de qualidade em vez de usar Great Expectations?**
Para que cada linha seja explicável e não haver dependência extra num escopo pequeno; ADR-004
registra o trade-off. Num time maior eu adotaria Great Expectations/Soda/DQX e manteria o gate, as
severidades e o atestado, que são o que dá valor operacional.

**17. O que é fail-closed e por que importa?**
Se o próprio check quebra (coluna inexistente, tabela de referência fora do ar), conta como
reprovado. Um validador que erra em silêncio deixa dado ruim passar, pior que não ter validador.

**18. Por que o gate lança exceção em vez de `dbutils.notebook.exit`?**
`notebook.exit` encerra o notebook com SUCESSO; o Job continuaria. Exceção falha a task e bloqueia
as dependentes.

**19. Como você refatoraria um legado de 40 notebooks?**
Medir antes (volume, tempo, custo, falhas, linhas perdidas) com um gabarito ou reconciliação;
escolher os 20% que causam 80% da dor; extrair lógica para módulos testáveis mantendo o notebook
como casca; paralelizar em runs comparáveis (legado × novo) até a saída bater; só então desligar o
antigo. Este repositório é essa metodologia em miniatura.

**20. Como você estimou prazos?**
Três pontos por entrega, média ponderada (O+4M+P)/6, com os riscos de acesso e de dados no
pessimista; reporto faixa e registro o real para calibrar (`docs/estimativa.md`). Só cite números
que você mediu.

**21. O pipeline novo é mais lento que o legado. Isso é um problema?**
No benchmark, sim, 19 s contra 2,5 s em dado pequeno e máquina pequena: ele valida, deduplica,
grava evidência e roda dois gates. O legado era rápido porque jogava fora 720 vendas válidas. Se
o custo importar em volume, o caminho é otimizar (checks em uma passada, Gold incremental), não
tirar a validação.

**22. Como você separa dev e prod?**
Mesmo código; o `target` do bundle muda o schema. `mode: development` prefixa recursos com o
usuário e pausa agendamentos; produção é deployada por *service principal* via CI.

**23. Duas execuções simultâneas?**
`max_concurrent_runs: 1` no Job e o controle otimista de concorrência do Delta no MERGE.

**24. E se o de/para for editado errado pelo negócio?**
`validar_vigencias` falha a ingestão com `DeParaInvalido` e lista exemplos. Melhor falhar cedo do
que inflar o faturamento sem erro.

**25. Como você garantiria governança no Databricks?**
Unity Catalog: permissões por camada (consumidores só leem a Gold), lineage automático,
`COMMENT`/tags nas tabelas. Aqui o código já usa nomes de três níveis via `Settings.tabela`.

**26. O que você NÃO testou?**
Responda direto (seção 7): o MERGE real com Delta foi validado só no CI; o deploy no workspace e o
serverless dependem da sua conta. Dizer isso com clareza vale mais do que fingir cobertura.

---

## 10. Roteiro de demo de 7 minutos

1. **(40 s) Problema:** "ninguém confiava no número de vendas" + as 3 fontes sujas.
2. **(60 s) Arquitetura:** o diagrama da seção 3; as 3 ideias (nada some / reexecutar é seguro /
   dado ruim não chega ao consumidor).
3. **(2 min) Uma decisão difícil, à sua escolha:** o de/para com vigência (Exemplo D) **ou** o
   gate com atestado (ADR-005) **ou** quarentena vs. descarte. Mostre o trade-off, não só a escolha.
4. **(2 min) Demo ao vivo:** `make demo`. Mostre (a) a quarentena por motivo; (b) `dq_results`;
   (c) a segunda execução não alterando nenhuma contagem; (d) um teste falhando de propósito (ou o
   `test_gate_bloqueia...`).
5. **(1 min) Números e honestidade:** tabela de `docs/benchmark.md` incluindo "mais lento, e por
   quê"; e as limitações da seção 8.
6. **(30 s) Próximos passos:** Auto Loader, Gold incremental, checks em uma passada, wheel no bundle.

Antes da demo: `make seeds` (datas relativas a hoje), `make teste-completo` (prova o MERGE) e,
se for mostrar o Databricks, `databricks bundle validate -t dev`.

---

## 11. Correções em relação ao que eu tinha sugerido antes

Ao construir e testar o projeto, encontrei problemas no esboço anterior desta conversa. Estão
corrigidos aqui; cito para você não repetir o erro de memória:

- **`dbutils.notebook.exit("FALHA...")` não bloqueava nada** (encerra com sucesso). Agora o gate
  lança exceção.
- **`engine.py` usava `spark` como variável global** (só existe em notebook). Agora o motor recebe
  o contexto explicitamente.
- **`createDataFrame` de dicionários heterogêneos** quebrava; agora há schema explícito.
- **O teste de `not_null` não chamava o motor** (repetia um `filter`). Substituído por testes que
  executam o código de verdade.
- **Os números "12 min → 4 min", "8% → 0,3%" e "cobertura 72%" eram exemplos meus**, não medições.
  Foram substituídos pelos números reais do benchmark, que inclusive mostram o pipeline novo MAIS
  lento em dado pequeno.
- A reconciliação usa **tolerância relativa**; a absoluta daria falso positivo por arredondamento.
