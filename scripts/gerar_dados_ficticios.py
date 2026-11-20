"""Gera um CSV fictício de agenda de instrutores.

Uso:
    python scripts/gerar_dados_ficticios.py dados/agenda.csv --linhas 200000
    python scripts/gerar_dados_ficticios.py dados/agenda.csv --linhas 5000 --anexar

Com --anexar, as linhas novas entram no fim do arquivo, com IDs que continuam
a sequência. É assim que se simula a chegada de dados novos entre duas cargas.
Cerca de 0,5% das linhas saem com defeito de propósito (ID vazio ou data
inválida), para exercitar o tratamento de qualidade do ETL.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

CABECALHO = ["ID interno", "Instrutor", "Data", "Período", "Horas", "UF", "Indisponibilidade", "Motivo"]
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Heitor", "Íris", "João",
         "Karina", "Lucas", "Marina", "Nelson", "Olívia", "Paulo", "Queila", "Rafael", "Sofia", "Tiago"]
SOBRENOMES = ["Almeida", "Barros", "Campos", "Duarte", "Esteves", "Farias", "Gomes", "Henriques",
              "Lima", "Moraes", "Nunes", "Oliveira", "Prado", "Queiroz", "Ramos", "Souza"]
PERIODOS = ["Manhã", "Tarde", "Noite", "Integral"]
MOTIVOS = ["Férias", "Licença", "Capacitação", "Compromisso pessoal"]


def contar_linhas(caminho: Path) -> int:
    if not caminho.exists():
        return 0
    with caminho.open(encoding="utf-8") as f:
        return max(sum(1 for _ in f) - 1, 0)


def gerar(caminho: Path, linhas: int, anexar: bool, semente: int) -> None:
    rnd = random.Random(semente)
    inicio = contar_linhas(caminho) if anexar else 0
    caminho.parent.mkdir(parents=True, exist_ok=True)
    modo = "a" if anexar and inicio else "w"
    instrutores = [f"{rnd.choice(NOMES)} {rnd.choice(SOBRENOMES)}" for _ in range(300)]

    with caminho.open(modo, encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        if modo == "w":
            w.writerow(CABECALHO)
        for i in range(inicio + 1, inicio + linhas + 1):
            dia = date(2026, 1, 1) + timedelta(days=rnd.randrange(365))
            indisponivel = rnd.random() < 0.08
            id_txt, data_txt = str(i), dia.strftime("%d/%m/%Y")
            sorteio = rnd.random()
            if sorteio < 0.003:
                id_txt = ""                      # defeito: sem chave
            elif sorteio < 0.005:
                data_txt = "31/02/2026"          # defeito: data inexistente
            w.writerow([
                id_txt, rnd.choice(instrutores), data_txt, rnd.choice(PERIODOS),
                f"{rnd.choice([4, 8])},0", "MT",
                "S" if indisponivel else "N",
                rnd.choice(MOTIVOS) if indisponivel else "",
            ])
    print(f"{linhas:,} linha(s) gravada(s) em {caminho} (total: {inicio + linhas:,})")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("saida", type=Path)
    p.add_argument("--linhas", type=int, default=200_000)
    p.add_argument("--anexar", action="store_true")
    p.add_argument("--semente", type=int, default=42)
    a = p.parse_args()
    gerar(a.saida, a.linhas, a.anexar, a.semente)
