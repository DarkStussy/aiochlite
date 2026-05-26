import pytest

from aiochlite import AsyncChClient

from ._types import TableFactory

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]

PARQUET_MAGIC = b"PAR1"


async def _prepare_test_table(ch_client: AsyncChClient, make_table: TableFactory, rows: int = 5) -> str:
    table = await make_table(id="UInt64", name="String")
    await ch_client.execute(
        f"INSERT INTO {table} SELECT number AS id, concat('row_', toString(number)) AS name FROM numbers({rows})"
    )
    return table


async def test_fetch_parquet_returns_valid_payload(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    payload = await ch_client.fetch_parquet(f"SELECT id, name FROM {table} ORDER BY id")

    assert isinstance(payload, bytes)
    assert payload.startswith(PARQUET_MAGIC)
    assert payload.endswith(PARQUET_MAGIC)


async def test_stream_parquet_matches_fetch_parquet(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    query = f"SELECT id, name FROM {table} ORDER BY id"

    fetched = await ch_client.fetch_parquet(query)
    streamed = b"".join([chunk async for chunk in ch_client.stream_parquet(query)])

    assert streamed == fetched


async def test_fetch_parquet_rejects_format_clause(ch_client: AsyncChClient) -> None:
    with pytest.raises(ValueError, match="FORMAT"):
        await ch_client.fetch_parquet("SELECT 1 FORMAT Parquet")


async def test_fetch_parquet_with_params(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table, rows=10)
    payload = await ch_client.fetch_parquet(
        f"SELECT id, name FROM {table} WHERE id < {{max_id:UInt64}} ORDER BY id",
        params={"max_id": 3},
    )

    assert payload.startswith(PARQUET_MAGIC)
    assert payload.endswith(PARQUET_MAGIC)
