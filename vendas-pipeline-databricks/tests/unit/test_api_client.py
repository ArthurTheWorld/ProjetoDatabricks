import pytest

from vendas_pipeline.ingestion.api_client import ApiInstavel, buscar_tudo, com_retry


def _registros(n):
    return [
        {"id_venda": f"A{i:04d}", "updated_at": f"2026-01-01 00:{i // 60:02d}:{i % 60:02d}"}
        for i in range(n)
    ]


def test_retry_tem_sucesso_apos_falhas_transitorias():
    chamadas, esperas = [], []

    def instavel():
        chamadas.append(1)
        if len(chamadas) < 3:
            raise ConnectionError("timeout")
        return "ok"

    assert com_retry(instavel, sleep=esperas.append) == "ok"
    assert len(chamadas) == 3
    assert len(esperas) == 2 and esperas[1] > esperas[0]  # backoff exponencial


def test_retry_desiste_apos_esgotar_tentativas():
    with pytest.raises(ConnectionError):
        com_retry(
            lambda: (_ for _ in ()).throw(ConnectionError("x")), tentativas=3, sleep=lambda s: None
        )


def test_retry_nao_repete_erro_de_logica():
    chamadas = []

    def bug():
        chamadas.append(1)
        raise KeyError("campo")

    with pytest.raises(KeyError):
        com_retry(bug, sleep=lambda s: None)
    assert len(chamadas) == 1


def test_buscar_tudo_traz_todas_as_paginas_sem_duplicar_mesmo_com_falhas():
    regs = _registros(250)
    api = ApiInstavel(regs, tamanho_pagina=100, taxa_falha=0.4, seed=1)
    resultado = buscar_tudo(api, sleep=lambda s: None)
    ids = [r["id_venda"] for r in resultado]
    assert len(ids) == 250 and len(set(ids)) == 250
    assert api.chamadas > 3  # houve falhas e novas tentativas


def test_since_filtra_incremental():
    regs = _registros(100)
    api = ApiInstavel(regs, tamanho_pagina=30, taxa_falha=0, seed=1)
    corte = regs[90]["updated_at"]
    resultado = buscar_tudo(api, since=corte, sleep=lambda s: None)
    assert len(resultado) == 10 and all(r["updated_at"] >= corte for r in resultado)
