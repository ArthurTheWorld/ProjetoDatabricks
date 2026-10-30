"""Contratos de qualidade DECLARATIVOS: o que se espera de cada tabela, em dados, não em código.

Por que declarativo? (1) Quem revisa o contrato não precisa ler Spark. (2) Adicionar um check é
uma linha, sem tocar no motor. (3) O contrato é documentação executável.

Severidades (decidem o que acontece quando o check falha):
  critica -> BLOQUEIA: o gate lança exceção, a task falha, as tasks seguintes não rodam.
  alta    -> ALERTA: segue o pipeline, mas notifica (webhook/log WARNING).
  media   -> REGISTRA: só fica em dq_results para tendência/dashboard.
"""

CONTRATOS: dict[str, list[dict]] = {
    "silver_vendas": [
        {"tipo": "min_linhas", "min": 1, "severidade": "critica"},
        {"tipo": "not_null", "col": "id_venda", "severidade": "critica"},
        {"tipo": "not_null", "col": "id_cliente_canonico", "severidade": "critica"},
        {"tipo": "not_null", "col": "data_venda", "severidade": "critica"},
        {"tipo": "unique", "cols": ["id_venda"], "severidade": "critica"},
        {"tipo": "range", "col": "valor", "min": 0, "severidade": "critica"},
        {
            "tipo": "referencial",
            "col": "id_cliente_canonico",
            "ref_table": "de_para_clientes",
            "ref_col": "id_canonico",
            "severidade": "critica",
        },
        {"tipo": "max_ratio", "expr": "canal = 'NAO_MAPEADO'", "max": 0.05, "severidade": "alta"},
        {"tipo": "freshness", "col": "updated_at", "max_horas": 48, "severidade": "media"},
    ],
    "gold_vendas_diarias": [
        {"tipo": "min_linhas", "min": 1, "severidade": "critica"},
        {"tipo": "not_null", "col": "data_venda", "severidade": "critica"},
        {"tipo": "not_null", "col": "id_produto", "severidade": "critica"},
        {"tipo": "unique", "cols": ["data_venda", "id_produto"], "severidade": "critica"},
        {"tipo": "range", "col": "total_vendas", "min": 0, "severidade": "critica"},
        {"tipo": "range", "col": "qtd_vendas", "min": 1, "severidade": "critica"},
        # Reconciliação: a Gold tem que fechar com a Silver. É o check que pega join
        # duplicando linha, filtro esquecido e agregação errada.
        {
            "tipo": "reconciliacao",
            "col": "total_vendas",
            "ref_table": "silver_vendas",
            "ref_col": "valor",
            "tolerancia_rel": 1e-6,
            "severidade": "critica",
        },
    ],
}
