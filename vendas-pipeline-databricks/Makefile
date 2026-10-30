.PHONY: instalar lint teste teste-completo demo benchmark seeds

instalar:
	pip install -e ".[local,dev]"

lint:
	ruff check src tests tools legacy

teste:            ## lógica sem Delta (rápido)
	VENDAS_SEM_DELTA=1 pytest -q

teste-completo:   ## inclui o MERGE de verdade (baixa os jars do Delta na 1ª vez)
	pytest -q

seeds:
	python -m tools.gerar_seeds --destino data/seeds --vendas 3000

demo:             ## roda o pipeline 2x e mostra as consultas da demo
	python -m tools.run_local

benchmark:
	python -m tools.benchmark --vendas 3000 --repeticoes 3
