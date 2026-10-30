"""Gera dados de demonstração SUJOS DE PROPÓSITO, de forma determinística, e o `ground_truth.json`.

Por que um gerador (e não alguns CSVs escritos à mão)?
1. A sujeira é CONTROLADA e CONTADA: sabemos exatamente quantas vendas são válidas, quantas
   devem ir para a quarentena e por qual motivo. Isso transforma o teste de integração em
   "o pipeline acertou o gabarito?", e não em "rodou sem erro".
2. É reprodutível (seed fixa): o benchmark e os testes dão o mesmo resultado em qualquer máquina.
3. Dá para escalar (`--vendas 200000`) e medir o pipeline sob volume.

Uso: python -m tools.gerar_seeds --destino data/seeds --vendas 3000
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

CANAIS = {
    "LOJA_FISICA": ["Loja Física", "LOJA FISICA", "loja fisica ", "Loja-Fisica"],
    "E_COMMERCE": ["E-commerce", "ecommerce", "E-COMMERCE", "e commerce"],
    "TELEVENDAS": ["Televendas", "TELEVENDAS", "tele-vendas"],
    "MARKETPLACE": ["Marketplace", "market place"],
}
CHAVES_CANAL = {
    "lojafisica": "LOJA_FISICA",
    "ecommerce": "E_COMMERCE",
    "televendas": "TELEVENDAS",
    "marketplace": "MARKETPLACE",
}

# Taxa de cada defeito por fonte. Cada linha com defeito tem EXATAMENTE um defeito.
TAXAS = {
    "erp": {
        "valor_invalido": 0.03,
        "valor_negativo": 0.01,
        "data_invalida": 0.01,
        "data_futura": 0.005,
        "id_venda_nulo": 0.005,
        "cliente_sem_de_para": 0.025,
        "linha_corrompida": 0.005,
        "canal_parceiro": 0.02,
    },
    "api": {
        "valor_negativo": 0.005,
        "data_invalida": 0.005,
        "cliente_sem_de_para": 0.02,
        "canal_parceiro": 0.02,
    },
    "db": {
        "data_invalida": 0.005,
        "valor_invalido": 0.005,
        "cliente_sem_de_para": 0.02,
        "canal_parceiro": 0.02,
    },
}
N_CLIENTES, N_SEM_DE_PARA, N_REMAPEADOS, JANELA_DIAS = 200, 5, 10, 90


def gerar(
    destino: str | Path,
    n_vendas: int = 3000,
    seed: int = 7,
    hoje: date | None = None,
    agora: datetime | None = None,
) -> dict:
    rng = random.Random(seed)
    destino = Path(destino)
    destino.mkdir(parents=True, exist_ok=True)
    agora = (agora or datetime.now()).replace(microsecond=0)
    hoje = hoje or agora.date()
    inicio = hoje - timedelta(days=JANELA_DIAS - 1)
    corte_remap = hoje - timedelta(days=45)

    # ---------- de/para de clientes (com vigência) ----------
    mapeados = [f"C{i:04d}" for i in range(1, N_CLIENTES - N_SEM_DE_PARA + 1)]
    sem_de_para = [f"C{i:04d}" for i in range(N_CLIENTES - N_SEM_DE_PARA + 1, N_CLIENTES + 1)]
    de_para, remaps = [], []
    for i, cid in enumerate(mapeados, start=1):
        if i <= N_REMAPEADOS:
            antes, depois = f"CAN-{i:04d}", f"CAN-{i + 500:04d}"
            de_para.append(
                (cid, antes, "erp", "2020-01-01", (corte_remap - timedelta(days=1)).isoformat())
            )
            de_para.append((cid, depois, "erp", corte_remap.isoformat(), "9999-12-31"))
            remaps.append(
                {
                    "id_origem": cid,
                    "antes": antes,
                    "depois": depois,
                    "corte": corte_remap.isoformat(),
                }
            )
        else:
            de_para.append((cid, f"CAN-{i:04d}", "erp", "2020-01-01", "9999-12-31"))
    _escrever_csv(
        destino / "de_para_clientes.csv",
        ["id_origem", "id_canonico", "fonte", "vigencia_inicio", "vigencia_fim"],
        de_para,
    )
    _escrever_csv(
        destino / "de_para_canal.csv", ["chave", "canal_canonico"], list(CHAVES_CANAL.items())
    )

    # ---------- vendas ----------
    n_erp, n_api = int(n_vendas * 0.55), int(n_vendas * 0.25)
    quantidades = {"erp": n_erp, "api": n_api, "db": n_vendas - n_erp - n_api}
    prefixos = {"erp": "ERP", "api": "API", "db": "DB"}
    motivos_esperados: Counter = Counter()
    validas: dict[str, float] = {}
    linhas = {"erp": [], "api": [], "db": []}
    revisoes = exatos = 0

    for fonte, qtd in quantidades.items():
        defeitos: dict[int, str] = {}
        livres = list(range(qtd))
        rng.shuffle(livres)
        for nome, taxa in TAXAS[fonte].items():
            for _ in range(max(1, round(qtd * taxa))):
                defeitos[livres.pop()] = nome

        for n in range(qtd):
            data_v = inicio + timedelta(days=rng.randint(0, JANELA_DIAS - 1))
            base_upd = datetime.combine(data_v, datetime.min.time()) + timedelta(
                hours=rng.randint(1, 30)
            )
            upd = min(base_upd, agora - timedelta(hours=7))
            venda = {
                "id_venda": f"{prefixos[fonte]}-{n + 1:06d}",
                "id_cliente": rng.choice(mapeados),
                "id_produto": f"P{rng.randint(1, 20):03d}",
                "canal": rng.choice(CANAIS[rng.choice(list(CANAIS))]),
                "valor": round(rng.uniform(10, 2000), 2),
                "data_venda": data_v,
                "updated_at": upd,
            }
            defeito = defeitos.get(n)
            raw = _para_texto(venda, fonte, rng)

            if defeito == "canal_parceiro":
                raw["canal"] = "Parceiro X"
                defeito = None
            elif defeito == "valor_invalido":
                raw["valor"] = rng.choice(["", "abc", "N/D"])
            elif defeito == "valor_negativo":
                raw["valor"] = "-50,00" if fonte != "api" else -50.0
            elif defeito == "data_invalida":
                raw["data_venda"] = (
                    rng.choice(["31/02/2024", "2024-13-45", "00/00/0000"])
                    if fonte == "erp"
                    else "2024-02-30"
                )
            elif defeito == "data_futura":
                raw["data_venda"] = (hoje + timedelta(days=30)).isoformat()
            elif defeito == "id_venda_nulo":
                raw["id_venda"] = ""
            elif defeito == "cliente_sem_de_para":
                raw["id_cliente"] = rng.choice(sem_de_para)
            elif defeito == "linha_corrompida":
                raw["_extra"] = ["campo_extra_1", "campo_extra_2"]

            if defeito:
                motivos_esperados[defeito] += 1
                linhas[fonte].append(raw)
                continue

            valor_final = venda["valor"]
            linhas[fonte].append(raw)
            if fonte == "erp" and rng.random() < 0.04:  # reenvio com valor revisado
                rev = dict(
                    venda,
                    valor=round(venda["valor"] * rng.uniform(0.9, 1.1), 2),
                    updated_at=venda["updated_at"] + timedelta(hours=rng.randint(1, 6)),
                )
                linhas[fonte].append(_para_texto(rev, fonte, rng))
                valor_final, revisoes = rev["valor"], revisoes + 1
            elif fonte == "api" and rng.random() < 0.03:  # duplicata exata (retry da API)
                linhas[fonte].append(dict(raw))
                exatos += 1
            validas[venda["id_venda"]] = valor_final

    _escrever_erp(destino / "erp_vendas.csv", linhas["erp"])
    _escrever_db(destino / "db_vendas.csv", linhas["db"])
    (destino / "api_vendas.json").write_text(
        json.dumps(linhas["api"], ensure_ascii=False), encoding="utf-8"
    )

    verdade = {
        "gerado_em": agora.isoformat(),
        "seed": seed,
        "n_vendas_geradas": n_vendas,
        "linhas_brutas": {k: len(v) for k, v in linhas.items()},
        "vendas_validas_unicas": len(validas),
        "valor_total_valido": round(sum(validas.values()), 2),
        "quarentena_esperada": dict(motivos_esperados),
        "revisoes_erp": revisoes,
        "duplicatas_exatas_api": exatos,
        "remapeamentos": remaps,
    }
    # id -> valor correto das vendas válidas (versão mais recente): permite medir QUALQUER pipeline
    # (inclusive o legado) contra o gabarito, venda por venda.
    (destino / "ground_truth_ids.json").write_text(json.dumps(validas), encoding="utf-8")
    (destino / "ground_truth.json").write_text(
        json.dumps(verdade, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return verdade


def _para_texto(v: dict, fonte: str, rng: random.Random) -> dict:
    """Formata como cada fonte entrega: ERP com formatos misturados, API com número, DB limpo."""
    base = {
        "id_venda": v["id_venda"],
        "id_cliente": v["id_cliente"],
        "id_produto": v["id_produto"],
        "canal": v["canal"],
        "updated_at": v["updated_at"].strftime("%Y-%m-%d %H:%M:%S"),
    }
    if fonte == "erp":
        valor = v["valor"]
        base["valor"] = rng.choice(
            [
                f"{valor:.2f}",
                f"{valor:.2f}".replace(".", ","),
                f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            ]
        )
        base["data_venda"] = (
            v["data_venda"].strftime("%d/%m/%Y")
            if rng.random() < 0.4
            else v["data_venda"].isoformat()
        )
    elif fonte == "api":
        base["valor"], base["data_venda"] = v["valor"], v["data_venda"].isoformat()
    else:
        base["valor"], base["data_venda"] = f"{v['valor']:.2f}", v["data_venda"].isoformat()
    return base


COLUNAS = ["id_venda", "id_cliente", "id_produto", "canal", "valor", "data_venda", "updated_at"]


def _escrever_erp(caminho: Path, linhas: list[dict]) -> None:
    # ISO-8859-1 de propósito: o ERP legado exporta assim e "Loja Física" quebra se lido como UTF-8.
    with open(caminho, "w", encoding="ISO-8859-1", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS)
        for r in linhas:
            w.writerow([r[c] for c in COLUNAS] + r.get("_extra", []))


def _escrever_db(caminho: Path, linhas: list[dict]) -> None:
    with open(caminho, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS)
        for r in sorted(linhas, key=lambda r: r["updated_at"]):
            w.writerow([r[c] for c in COLUNAS])


def _escrever_csv(caminho: Path, cabecalho: list[str], linhas: list) -> None:
    with open(caminho, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--destino", default="data/seeds")
    ap.add_argument("--vendas", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    print(json.dumps(gerar(args.destino, args.vendas, args.seed), indent=2, ensure_ascii=False))
