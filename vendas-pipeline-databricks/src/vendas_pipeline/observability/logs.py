"""Logs estruturados em JSON: uma linha = um evento, pesquisável por campo.

Por que JSON e não print()? No Databricks os logs vão para o driver/Job run e podem ser
exportados para um agregador (Datadog, CloudWatch, Log Analytics). Texto livre exige regex;
JSON exige só um filtro por campo (`run_id`, `etapa`, `evento`).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "nivel": record.levelname,
            "evento": record.getMessage(),
        }
        payload.update(getattr(record, "ctx", {}))
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(run_id: str) -> logging.LoggerAdapter:
    logger = logging.getLogger("vendas_pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logging.LoggerAdapter(logger, {"run_id": run_id})


def evento(logger: logging.LoggerAdapter, mensagem: str, nivel: int = logging.INFO, **campos):
    ctx = {**logger.extra, **campos}
    logger.logger.log(nivel, mensagem, extra={"ctx": ctx})
