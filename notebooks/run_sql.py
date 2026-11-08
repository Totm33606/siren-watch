"""Run a DuckDB SQL file statement by statement and print each result.

Usage: uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql

The DuckDB CLI does the same (`duckdb -c ".read file.sql"`); this script avoids
installing it, since the `duckdb` Python package (a dev dependency) is enough.
Each result is preceded by the `--` comment above the query in the file.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb


def iter_statements(sql: str) -> Iterator[tuple[str, bool, str]]:
    """(title, is_query, cleaned sql) for each statement of the file.

    DuckDB attaches to a statement the `;` and the comments that follow it: the `;` is
    removed (it would break `show()`) and the comments become the title of the next
    statement.
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
    """First meaningful line of the last comment paragraph (`-- ----` lines ignored)."""
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
        if not is_query:  # INSTALL, LOAD, CREATE…: executed without printing anything
            con.execute(query)
            continue
        print(f"\n=== {title or 'Requête'} ===")
        con.sql(query).show(max_rows=40, max_width=200)


def main(path: str) -> None:
    # The Windows console (cp1252) cannot display DuckDB's table borders.
    sys.stdout.reconfigure(encoding="utf-8")
    run(Path(path).read_text(encoding="utf-8"), duckdb.connect())


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "notebooks/explore.sql")
