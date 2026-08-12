import pytest

from aiochlite.client import _has_format_clause


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FORMAT Parquet",
        "SELECT 1 format tsv\n",
        "SELECT 1 FORMAT CSV;",
        "SELECT 1 FORMAT CSV SETTINGS max_threads = 1",
        "SELECT 1 FORMAT CSV SETTINGS max_threads = 1, max_block_size = 100;",
        "SELECT 1 FORMAT CSV -- trailing comment",
        "SELECT 1 FORMAT CSV /* trailing\ncomment */",
        "SELECT 1 FORMAT CSV SETTINGS max_threads = 1; -- done",
    ],
)
def test_detects_format_clause(query: str) -> None:
    assert _has_format_clause(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1",
        "SELECT * FROM t SETTINGS max_threads = 1",
        "SELECT formatDateTime(now(), '%Y-%m-%d')",
        "SELECT format('{} {}', 'a', 'b')",
        "SELECT 'FORMAT CSV' AS s",
        "SELECT * FROM t WHERE name = 'x FORMAT CSV'",
        'SELECT 1 AS "FORMAT CSV"',
        "SELECT 1 AS `FORMAT CSV`",
        "SELECT 1 -- FORMAT CSV",
        "SELECT 1 /* FORMAT CSV */",
    ],
)
def test_ignores_non_format_clause(query: str) -> None:
    assert not _has_format_clause(query)
