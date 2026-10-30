"""Configuração única do pipeline: onde estão as tabelas e os arquivos de entrada."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from vendas_pipeline.storage import DeltaStore

# Nomes lógicos das tabelas. Todo o código usa estes nomes; só `Settings.tabela` conhece
# catálogo/schema. Assim, dev e prod são o mesmo código com `schema` diferente.
BRONZE = "bronze_vendas"
SILVER = "silver_vendas"
QUARENTENA = "quarentena_vendas"
GOLD = "gold_vendas_diarias"
GOLD_STAGING = "gold_vendas_diarias_staging"
DE_PARA_CLIENTES = "de_para_clientes"
DE_PARA_CANAL = "de_para_canal"
RUN_METRICS = "pipeline_run_metrics"
DQ_RESULTS = "dq_results"


@dataclass(frozen=True)
class Settings:
    schema: str
    catalog: str | None = None  # None = metastore local (testes); no Databricks = Unity Catalog
    seed_path: str = "data/seeds"
    store: DeltaStore = field(default_factory=DeltaStore)

    def tabela(self, nome: str) -> str:
        partes = [p for p in (self.catalog, self.schema, nome) if p]
        return ".".join(partes)


def novo_run_id() -> str:
    return uuid.uuid4().hex[:12]
