"""ETL incremental: CSV -> staging -> MERGE no destino, com checkpoint.

Estratégia
----------
1. Lê o CSV em blocos (chunks), a partir da última linha já processada.
2. Normaliza nomes de colunas e tipos conforme o MAPA_COLUNAS.
3. Grava cada bloco em uma tabela de staging.
4. Faz o MERGE (upsert) do staging no destino.
5. Só então avança o checkpoint. Se a execução cair no meio, ela retoma
   do último bloco confirmado, e reprocessar um bloco não duplica nada.

Banco de dados
--------------
A conexão vem da variável de ambiente ETL_DB_URL (ou do arquivo .env).
Sem ela, o script usa um SQLite local, para que a demonstração rode em
qualquer máquina. Em SQL Server é usada a instrução MERGE; em SQLite e
PostgreSQL, INSERT ... ON CONFLICT.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

log = logging.getLogger("etl")

# coluna no CSV -> (coluna normalizada, tipo lógico)
MAPA_COLUNAS: dict[str, tuple[str, str]] = {
    "ID interno": ("id_agenda", "int"),
    "Instrutor": ("instrutor", "str"),
    "Data": ("data", "date"),
    "Período": ("periodo", "str"),
    "Horas": ("horas", "float"),
    "UF": ("unidade_federativa", "str"),
    "Indisponibilidade": ("indisponibilidade", "str"),
    "Motivo": ("motivo_indisponibilidade", "str"),
}
CHAVE = "id_agenda"
TABELA_DESTINO = "agenda_instrutor"
TABELA_STAGING = "stg_agenda_instrutor"

TIPOS_SQL = {
    "int": sa.Integer,
    "str": lambda: sa.String(200),
    "date": sa.Date,
    "float": sa.Float,
}


@dataclass
class Resultado:
    linhas_lidas: int = 0
    linhas_carregadas: int = 0
    linhas_rejeitadas: int = 0
    blocos: int = 0


# --------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------
def carregar_env(caminho: Path = Path(".env")) -> None:
    """Carrega pares CHAVE=valor de um arquivo .env, sem sobrescrever o ambiente."""
    if not caminho.exists():
        return
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def url_do_banco() -> str:
    return os.environ.get("ETL_DB_URL", "sqlite:///demo.db")


# --------------------------------------------------------------------------
# Checkpoint
# --------------------------------------------------------------------------
def ler_checkpoint(caminho: Path) -> int:
    if caminho.exists():
        return int(json.loads(caminho.read_text(encoding="utf-8")).get("ultima_linha", 0))
    return 0


def salvar_checkpoint(caminho: Path, ultima_linha: int) -> None:
    caminho.write_text(
        json.dumps({"ultima_linha": ultima_linha, "atualizado_em": pd.Timestamp.now().isoformat(timespec="seconds")}),
        encoding="utf-8",
    )


# --------------------------------------------------------------------------
# Leitura e normalização
# --------------------------------------------------------------------------
def detectar_separador(caminho: Path) -> str:
    with caminho.open(encoding="utf-8-sig") as f:
        cabecalho = f.readline()
    contagem = {sep: cabecalho.count(sep) for sep in (";", ",", "\t")}
    return max(contagem, key=contagem.get)


def normalizar(bloco: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Renomeia, converte tipos e descarta linhas sem chave. Devolve (dados, rejeitadas)."""
    faltando = [c for c in MAPA_COLUNAS if c not in bloco.columns]
    if faltando:
        raise ValueError(f"Colunas ausentes no CSV: {faltando}")

    df = pd.DataFrame()
    for origem, (destino, tipo) in MAPA_COLUNAS.items():
        serie = bloco[origem].astype("string").str.strip()
        if tipo == "int":
            df[destino] = pd.to_numeric(serie, errors="coerce").astype("Int64")
        elif tipo == "float":
            df[destino] = pd.to_numeric(serie.str.replace(",", ".", regex=False), errors="coerce")
        elif tipo == "date":
            df[destino] = pd.to_datetime(serie, format="%d/%m/%Y", errors="coerce").dt.date
        else:
            df[destino] = serie.replace("", pd.NA)

    antes = len(df)
    df = df[df[CHAVE].notna()]
    # mesma chave repetida no bloco: vale a última ocorrência
    df = df.drop_duplicates(subset=CHAVE, keep="last")
    return df, antes - len(df)


def para_registros(df: pd.DataFrame) -> list[dict]:
    """Converte para tipos nativos do Python (o driver não aceita tipos do numpy/pandas)."""
    obj = df.astype(object).where(df.notna(), None)
    registros = obj.to_dict(orient="records")
    for r in registros:
        if r[CHAVE] is not None:
            r[CHAVE] = int(r[CHAVE])
        if r.get("horas") is not None:
            r["horas"] = float(r["horas"])
    return registros


# --------------------------------------------------------------------------
# Banco de dados
# --------------------------------------------------------------------------
def definir_tabelas(meta: sa.MetaData) -> tuple[sa.Table, sa.Table]:
    def colunas(com_pk: bool):
        cols = []
        for destino, tipo in MAPA_COLUNAS.values():
            cols.append(sa.Column(destino, TIPOS_SQL[tipo](), primary_key=(com_pk and destino == CHAVE)))
        return cols

    destino = sa.Table(TABELA_DESTINO, meta, *colunas(True))
    staging = sa.Table(TABELA_STAGING, meta, *colunas(False))
    return destino, staging


def sql_merge(dialeto: str) -> str:
    cols = [c for c, _ in MAPA_COLUNAS.values()]
    outras = [c for c in cols if c != CHAVE]
    lista = ", ".join(cols)
    if dialeto == "mssql":
        atualiza = ", ".join(f"d.{c} = s.{c}" for c in outras)
        valores = ", ".join(f"s.{c}" for c in cols)
        return (
            f"MERGE {TABELA_DESTINO} AS d USING {TABELA_STAGING} AS s ON d.{CHAVE} = s.{CHAVE} "
            f"WHEN MATCHED THEN UPDATE SET {atualiza} "
            f"WHEN NOT MATCHED THEN INSERT ({lista}) VALUES ({valores});"
        )
    atualiza = ", ".join(f"{c} = excluded.{c}" for c in outras)
    return (
        f"INSERT INTO {TABELA_DESTINO} ({lista}) SELECT {lista} FROM {TABELA_STAGING} WHERE 1=1 "
        f"ON CONFLICT ({CHAVE}) DO UPDATE SET {atualiza}"
    )


# --------------------------------------------------------------------------
# Orquestração
# --------------------------------------------------------------------------
def executar(csv: Path, db_url: str, checkpoint: Path, tamanho_bloco: int = 50_000) -> Resultado:
    engine = sa.create_engine(db_url)
    meta = sa.MetaData()
    destino, staging = definir_tabelas(meta)
    meta.create_all(engine)

    ultima = ler_checkpoint(checkpoint)
    sep = detectar_separador(csv)
    log.info("Separador detectado: %r | retomando após a linha %s", sep, f"{ultima:,}")

    res = Resultado()
    leitor = pd.read_csv(
        csv, sep=sep, dtype=str, encoding="utf-8-sig", keep_default_na=False,
        chunksize=tamanho_bloco, skiprows=range(1, ultima + 1),
    )
    merge = sa.text(sql_merge(engine.dialect.name))

    for bloco in leitor:
        if bloco.empty:
            continue
        dados, rejeitadas = normalizar(bloco)
        with engine.begin() as conn:  # um bloco = uma transação
            conn.execute(staging.delete())
            if len(dados):
                conn.execute(staging.insert(), para_registros(dados))
                conn.execute(merge)
            conn.execute(staging.delete())
        ultima += len(bloco)
        salvar_checkpoint(checkpoint, ultima)

        res.blocos += 1
        res.linhas_lidas += len(bloco)
        res.linhas_carregadas += len(dados)
        res.linhas_rejeitadas += rejeitadas
        log.info("Bloco %d: %s lidas, %s carregadas, %s rejeitadas",
                 res.blocos, f"{len(bloco):,}", f"{len(dados):,}", rejeitadas)

    if res.linhas_lidas == 0:
        log.info("Nenhuma linha nova desde a última execução.")
    engine.dispose()
    return res


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    carregar_env()
    p = argparse.ArgumentParser(description="ETL incremental de CSV para banco relacional")
    p.add_argument("csv", type=Path, help="arquivo CSV de origem")
    p.add_argument("--checkpoint", type=Path, default=Path("checkpoint.json"))
    p.add_argument("--bloco", type=int, default=50_000, help="linhas por bloco")
    args = p.parse_args()

    res = executar(args.csv, url_do_banco(), args.checkpoint, args.bloco)
    log.info("Concluído: %s lidas | %s carregadas | %s rejeitadas | %d bloco(s)",
             f"{res.linhas_lidas:,}", f"{res.linhas_carregadas:,}", f"{res.linhas_rejeitadas:,}", res.blocos)


if __name__ == "__main__":
    main()
