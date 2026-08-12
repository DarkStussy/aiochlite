import json

import pytest

from aiochlite import AsyncChClient, ExportFormat

from ._types import TableFactory

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]

PARQUET_MAGIC = b"PAR1"


async def _prepare_test_table(ch_client: AsyncChClient, make_table: TableFactory, rows: int = 5) -> str:
    table = await make_table(id="UInt64", name="String")
    await ch_client.execute(
        f"INSERT INTO {table} SELECT number AS id, concat('row_', toString(number)) AS name FROM numbers({rows})"
    )
    return table


async def test_fetch_format_parquet_returns_valid_payload(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    payload = await ch_client.fetch_format(f"SELECT id, name FROM {table} ORDER BY id", "Parquet")

    assert isinstance(payload, bytes)
    assert payload.startswith(PARQUET_MAGIC)
    assert payload.endswith(PARQUET_MAGIC)


@pytest.mark.parametrize(
    ("format_name", "expected"),
    [
        ("CSV", '0,"row_0"\n1,"row_1"\n2,"row_2"\n'),
        ("CSVWithNames", '"id","name"\n0,"row_0"\n1,"row_1"\n2,"row_2"\n'),
        ("TSV", "0\trow_0\n1\trow_1\n2\trow_2\n"),
        ("TSVWithNames", "id\tname\n0\trow_0\n1\trow_1\n2\trow_2\n"),
        ("JSONEachRow", '{"id":0,"name":"row_0"}\n{"id":1,"name":"row_1"}\n{"id":2,"name":"row_2"}\n'),
        ("JSONCompactEachRow", '[0, "row_0"]\n[1, "row_1"]\n[2, "row_2"]\n'),
        ("Values", "(0,'row_0'),(1,'row_1'),(2,'row_2')"),
    ],
)
async def test_fetch_format_text_formats(
    ch_client: AsyncChClient,
    make_table: TableFactory,
    format_name: ExportFormat,
    expected: str,
) -> None:
    table = await _prepare_test_table(ch_client, make_table, rows=3)
    payload = await ch_client.fetch_format(
        f"SELECT id, name FROM {table} ORDER BY id",
        format_name,
        settings={"output_format_json_quote_64bit_integers": 0},
    )

    assert payload.decode() == expected


async def test_fetch_format_json(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table, rows=3)
    payload = await ch_client.fetch_format(
        f"SELECT id, name FROM {table} ORDER BY id",
        "JSON",
        settings={"output_format_json_quote_64bit_integers": 0},
    )

    result = json.loads(payload)
    assert result["rows"] == 3
    assert [column["name"] for column in result["meta"]] == ["id", "name"]
    assert result["data"][0] == {"id": 0, "name": "row_0"}


@pytest.mark.parametrize("format_name", ["Parquet", "CSVWithNames", "JSONEachRow", "Native"])
async def test_stream_format_matches_fetch_format(
    ch_client: AsyncChClient,
    make_table: TableFactory,
    format_name: ExportFormat,
) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    query = f"SELECT id, name FROM {table} ORDER BY id"

    fetched = await ch_client.fetch_format(query, format_name)
    streamed = b"".join([chunk async for chunk in ch_client.stream_format(query, format_name)])

    assert streamed == fetched


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FORMAT Parquet",
        "SELECT 1 FORMAT CSV SETTINGS max_threads = 1",
        "SELECT 1 FORMAT CSV -- trailing comment",
        "SELECT 1 FORMAT CSV /* trailing comment */",
    ],
)
async def test_fetch_format_rejects_format_clause(ch_client: AsyncChClient, query: str) -> None:
    with pytest.raises(ValueError, match="FORMAT"):
        await ch_client.fetch_format(query, "Parquet")


async def test_fetch_format_allows_format_functions(ch_client: AsyncChClient) -> None:
    payload = await ch_client.fetch_format(
        "SELECT formatDateTime(toDateTime('2026-01-02 03:04:05'), '%Y-%m-%d') AS d",
        "TSV",
    )

    assert payload.decode() == "2026-01-02\n"


async def test_fetch_format_with_params(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table, rows=10)
    payload = await ch_client.fetch_format(
        f"SELECT id, name FROM {table} WHERE id < {{max_id:UInt64}} ORDER BY id",
        "CSV",
        params={"max_id": 3},
    )

    assert payload.decode() == '0,"row_0"\n1,"row_1"\n2,"row_2"\n'


async def test_fetch_parquet_is_deprecated(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    query = f"SELECT id, name FROM {table} ORDER BY id"

    with pytest.deprecated_call():
        payload = await ch_client.fetch_parquet(query)

    assert payload == await ch_client.fetch_format(query, "Parquet")


async def test_stream_parquet_is_deprecated(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    query = f"SELECT id, name FROM {table} ORDER BY id"

    with pytest.deprecated_call():
        chunks = ch_client.stream_parquet(query)

    streamed = b"".join([chunk async for chunk in chunks])
    assert streamed == await ch_client.fetch_format(query, "Parquet")
