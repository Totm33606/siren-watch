"""Exécute un fichier SQL DuckDB instruction par instruction et affiche chaque résultat.

Usage : uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql

La CLI DuckDB fait la même chose (`duckdb -c ".read fichier.sql"`) ; ce script évite
de l'installer, le paquet Python `duckdb` suffit (dépendance de dev du projet).
Chaque résultat est précédé du commentaire `--` qui précède la requête dans le fichier.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb


def iter_statements(sql: str) -> Iterator[tuple[str, bool, str]]:
    """(titre, est_une_requête, sql nettoyé) pour chaque instruction du fichier.

    DuckDB rattache à une instruction le `;` et les commentaires qui la suivent :
    on retire ce `;` (qui casserait `show()`) et on garde les commentaires comme
    titre de l'instruction suivante.
    """
    title = ""
    for statement in duckdb.extract_statements(sql):
        lines = statement.query.rstrip().splitlines()
        trailing: list[str] = []
        while lines and (not lines[-1].strip() or lines[-1].lstrip().startswith("--")):
            trailing.insert(0, lines.pop())
        query = "\n".join(lines).rstrip().rstrip(";")
        yield title, statement.type == duckdb.StatementType.SELECT, query
        title = _title(trailing)


def _title(comment_lines: list[str]) -> str:
    """1re ligne utile du dernier paragraphe de commentaires (lignes `-- ----` ignorées)."""
    paragraph: list[str] = []
    for line in comment_lines:
        text = line.strip().lstrip("-").strip()
        if not line.strip():
            paragraph = []
        elif text and set(text) != {"-"}:
            paragraph.append(text)
    return paragraph[0] if paragraph else ""


def run(sql: str, con: duckdb.DuckDBPyConnection) -> None:
    for title, is_query, query in iter_statements(sql):
        if not is_query:  # INSTALL, LOAD, CREATE… : on exécute sans rien afficher
            con.execute(query)
            continue
        print(f"\n=== {title or 'Requête'} ===")
        con.sql(query).show(max_rows=40, max_width=200)


def main(path: str) -> None:
    # La console Windows (cp1252) ne sait pas afficher les bordures des tableaux DuckDB.
    sys.stdout.reconfigure(encoding="utf-8")
    run(Path(path).read_text(encoding="utf-8"), duckdb.connect())


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "notebooks/explore.sql")
