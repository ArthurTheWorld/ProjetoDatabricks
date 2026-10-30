"""Cliente de API instável (mock) + retry com backoff exponencial e jitter.

Em produção o `ApiInstavel` seria um cliente `requests` com a mesma interface
(`get_pagina(pagina, since)`); o retry e a paginação não mudam.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

from vendas_pipeline.observability.logs import evento


class ErroApiTransitorio(ConnectionError):
    """Falha que vale a pena repetir (timeout, 429, 503)."""


class ApiInstavel:
    """Mock determinístico: pagina os registros e falha em ~`taxa_falha` das chamadas."""

    def __init__(
        self,
        registros: list[dict],
        tamanho_pagina: int = 100,
        taxa_falha: float = 0.1,
        seed: int = 42,
    ):
        # Ordenar garante paginação estável entre chamadas (cursor consistente).
        self.registros = sorted(registros, key=lambda r: r["updated_at"])
        self.tamanho_pagina = tamanho_pagina
        self.taxa_falha = taxa_falha
        self._rng = random.Random(seed)
        self.chamadas = 0

    def get_pagina(self, pagina: int, since: str | None = None) -> tuple[list[dict], bool]:
        self.chamadas += 1
        if self._rng.random() < self.taxa_falha:
            raise ErroApiTransitorio(f"timeout na página {pagina}")
        base = [r for r in self.registros if since is None or r["updated_at"] >= since]
        ini = pagina * self.tamanho_pagina
        return base[ini : ini + self.tamanho_pagina], (ini + self.tamanho_pagina) < len(base)


def com_retry(
    fn: Callable,
    tentativas: int = 5,
    base: float = 0.5,
    jitter: float = 0.3,
    sleep: Callable[[float], None] = time.sleep,
    logger=None,
):
    """Repete `fn` só para erros TRANSITÓRIOS (ConnectionError/TimeoutError).

    - Backoff exponencial (0.5s, 1s, 2s, 4s): dá tempo da API se recuperar.
    - Jitter aleatório: evita que vários jobs retentem todos no mesmo instante (thundering herd).
    - Erro de lógica (KeyError, 4xx) NÃO é repetido: repetir não conserta bug.
    - `sleep` injetável: o teste roda em milissegundos.
    """
    for i in range(tentativas):
        try:
            return fn()
        except (ConnectionError, TimeoutError) as exc:
            if i == tentativas - 1:
                raise
            espera = base * (2**i) + random.uniform(0, jitter)
            if logger:
                evento(
                    logger, "retry_api", tentativa=i + 1, espera_s=round(espera, 2), erro=str(exc)
                )
            sleep(espera)


def buscar_tudo(
    api: ApiInstavel,
    since: str | None = None,
    logger=None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict]:
    """Percorre todas as páginas; cada página tem seu próprio retry (não refaz o que já veio)."""
    registros: list[dict] = []
    pagina, ha_mais = 0, True
    while ha_mais:
        itens, ha_mais = com_retry(
            lambda p=pagina: api.get_pagina(p, since), logger=logger, sleep=sleep
        )
        registros.extend(itens)
        pagina += 1
    return registros
