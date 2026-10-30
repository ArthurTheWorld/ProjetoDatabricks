"""Cola entre o notebook do Databricks e o pacote: lê widgets e monta o `Settings`.

Existe para que os notebooks fiquem FINOS (5 linhas de código): toda a lógica mora em `src/`,
onde tem teste. Notebook com lógica própria é o caminho para o "legado" de amanhã.
"""

from __future__ import annotations

from vendas_pipeline.settings import Settings, novo_run_id


def contexto_notebook(dbutils, extras: dict[str, str] | None = None):
    """Declara os widgets, lê os valores e devolve (settings, run_id, valores_extras).

    `run_id`: o Job passa `{{job.run_id}}` para TODAS as tasks, então elas compartilham o mesmo
    id (e um *repair run* mantém o id). Em execução manual, cai num valor fixo de desenvolvimento.
    """
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "vendas_dev")
    dbutils.widgets.text("run_id", "")
    for nome, padrao in (extras or {}).items():
        dbutils.widgets.text(nome, padrao)

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    run_id = dbutils.widgets.get("run_id") or novo_run_id()
    settings = Settings(
        catalog=catalog, schema=schema, seed_path=f"/Volumes/{catalog}/{schema}/seeds"
    )
    return settings, run_id, {n: dbutils.widgets.get(n) for n in (extras or {})}
