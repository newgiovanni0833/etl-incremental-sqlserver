import sys
from pathlib import Path

import sqlalchemy as sa

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ / "scripts"))

import etl_incremental as etl  # noqa: E402
from gerar_dados_ficticios import gerar  # noqa: E402


def contar(db_url: str) -> int:
    eng = sa.create_engine(db_url)
    with eng.connect() as c:
        n = c.execute(sa.text(f"SELECT COUNT(*) FROM {etl.TABELA_DESTINO}")).scalar()
    eng.dispose()
    return n


def preparar(tmp_path, linhas=5_000):
    csv = tmp_path / "agenda.csv"
    gerar(csv, linhas, anexar=False, semente=1)
    return csv, f"sqlite:///{tmp_path / 'teste.db'}", tmp_path / "ck.json"


def test_carga_inicial_carrega_tudo_menos_rejeitadas(tmp_path):
    csv, db, ck = preparar(tmp_path)
    r = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    assert r.linhas_lidas == 5_000
    assert r.blocos == 5
    assert r.linhas_carregadas + r.linhas_rejeitadas == 5_000
    assert contar(db) == r.linhas_carregadas
    assert etl.ler_checkpoint(ck) == 5_000


def test_reexecucao_nao_le_nem_duplica(tmp_path):
    csv, db, ck = preparar(tmp_path)
    r1 = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    r2 = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    assert r2.linhas_lidas == 0
    assert contar(db) == r1.linhas_carregadas


def test_incremental_le_apenas_linhas_novas(tmp_path):
    csv, db, ck = preparar(tmp_path)
    r1 = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    gerar(csv, 700, anexar=True, semente=2)
    r2 = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    assert r2.linhas_lidas == 700
    assert contar(db) == r1.linhas_carregadas + r2.linhas_carregadas


def test_reprocessar_do_zero_e_idempotente(tmp_path):
    """Apagar o checkpoint e rodar de novo não pode duplicar registros."""
    csv, db, ck = preparar(tmp_path)
    r1 = etl.executar(csv, db, ck, tamanho_bloco=1_000)
    ck.unlink()
    etl.executar(csv, db, ck, tamanho_bloco=1_000)
    assert contar(db) == r1.linhas_carregadas


def test_detecta_separador(tmp_path):
    for sep in (";", ",", "\t"):
        arq = tmp_path / "x.csv"
        arq.write_text(sep.join(["a", "b", "c"]) + "\n", encoding="utf-8")
        assert etl.detectar_separador(arq) == sep


def test_merge_sql_server_usa_merge():
    sql = etl.sql_merge("mssql")
    assert sql.startswith("MERGE") and "WHEN NOT MATCHED" in sql
