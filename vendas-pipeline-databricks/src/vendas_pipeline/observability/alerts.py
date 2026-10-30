"""Alertas. Nunca derrubam o pipeline: falhar ao alertar não pode virar falha de dados.

Em produção: `ALERT_WEBHOOK_URL` aponta para Slack/Teams. Sem a variável, o alerta vira só
um log de nível WARNING (suficiente para a demo e para os testes).
Complementa, não substitui, o `email_notifications.on_failure` do Job: o Job avisa que
ALGO QUEBROU; este módulo avisa que algo está DEGRADADO mas o pipeline seguiu.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

from vendas_pipeline.observability.logs import evento


def alertar(logger, severidade: str, mensagem: str, **contexto) -> None:
    evento(logger, f"ALERTA[{severidade}] {mensagem}", nivel=logging.WARNING, **contexto)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        corpo = json.dumps({"text": f"[{severidade}] {mensagem} {contexto}"}).encode()
        req = urllib.request.Request(url, corpo, {"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)  # noqa: S310
    except Exception as exc:  # noqa: BLE001 - alertar nunca pode quebrar o pipeline
        evento(logger, "falha ao enviar alerta", nivel=logging.ERROR, erro=str(exc))
