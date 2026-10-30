# Estimativa de prazos (modelo para você preencher com o real)

> A coluna "Estimado" é uma SUGESTÃO inicial minha, de planejamento. A coluna "Real" é sua:
> preencha conforme avançar. O valor desta tabela na entrevista está na comparação e no
> aprendizado, não nos números em si. NÃO apresente horas que você não gastou.

## Técnica usada: três pontos
Para cada entrega: otimista (O), mais provável (M), pessimista (P). Estimativa = (O + 4M + P) / 6.
Pessimista deve incluir o que costuma dar errado (acesso ao workspace, Maven bloqueado, cota).

| Entrega | O | M | P | Estimado (h) | Real (h) | O que mudou / aprendizado |
|---|---:|---:|---:|---:|---:|---|
| Ambiente local (Spark + Delta) e repositório | 1 | 2 | 5 | 2,3 | | |
| Gerador de seeds com gabarito | 2 | 3 | 5 | 3,2 | | |
| Bronze (3 fontes, retry, watermark) | 3 | 5 | 8 | 5,2 | | |
| Silver (tipagem, de/para, dedupe, quarentena) | 4 | 6 | 10 | 6,3 | | |
| Motor de qualidade + gate | 3 | 5 | 8 | 5,2 | | |
| Gold + publicação com atestado | 2 | 3 | 5 | 3,2 | | |
| Testes unitários e de integração | 4 | 6 | 10 | 6,3 | | |
| Bundle + CI/CD + deploy no workspace | 3 | 5 | 10 | 5,5 | | |
| Documentação (README, ADRs, runbook) | 2 | 3 | 4 | 3,0 | | |
| **Total** | | | | **40,2** | | |

## Como responder "como você estima?" na entrevista
1. Quebro em entregas verificáveis (cada uma termina com um teste ou uma demo).
2. Estimo três cenários e uso a média ponderada; o pessimista carrega os riscos de acesso e de
   dados desconhecidos, que costumam pesar mais que o código.
3. Dou a estimativa como faixa ("entre 35 e 50 horas") e digo o que reduziria a incerteza
   (ex.: ter acesso ao workspace desde o dia 1).
4. Registro o real e comparo; a diferença calibra a próxima estimativa.
