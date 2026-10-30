from vendas_pipeline.notebook_utils import contexto_notebook


class _Widgets:
    def __init__(self, valores):
        self.valores, self.declarados = valores, {}

    def text(self, nome, padrao):
        self.declarados[nome] = padrao

    def get(self, nome):
        return self.valores.get(nome, self.declarados.get(nome, ""))


class _Dbutils:
    def __init__(self, valores):
        self.widgets = _Widgets(valores)


def test_monta_settings_com_unity_catalog_e_volume_de_seeds():
    settings, run_id, _ = contexto_notebook(
        _Dbutils({"catalog": "main", "schema": "vendas_prod", "run_id": "123"})
    )
    assert settings.tabela("silver_vendas") == "main.vendas_prod.silver_vendas"
    assert settings.seed_path == "/Volumes/main/vendas_prod/seeds"
    assert run_id == "123"


def test_run_id_vazio_gera_um_novo_e_extras_sao_lidos():
    _, run_id, extras = contexto_notebook(_Dbutils({"tabela": "gold"}), extras={"tabela": "silver"})
    assert run_id and extras == {"tabela": "gold"}
