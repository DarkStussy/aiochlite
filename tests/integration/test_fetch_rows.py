from datetime import datetime
from decimal import Decimal

import pytest

from aiochlite import AsyncChClient

from ._types import TableFactory

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


async def _prepare_test_table(ch_client: AsyncChClient, make_table: TableFactory, rows: int = 3) -> str:
    table = await make_table(
        id="UInt64",
        event_time="DateTime('UTC')",
        payload="Tuple(String, UInt16)",
        prices="Array(Decimal(10, 2))",
    )

    await ch_client.execute(
        f"""
        INSERT INTO {table}
        SELECT
            number as id,
            toDateTime(1734160800 + number, 'UTC') as event_time,
            tuple('evt', toUInt16(number % 65535)) as payload,
            [
                toDecimal64((number % 1000) / 100, 2),
                toDecimal64(((number + 1) % 1000) / 100, 2)
            ] as prices
        FROM numbers({rows})
        """
    )

    return table


async def test_fetch_rows_returns_tuples(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    rows = await ch_client.fetch_rows(f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id")

    assert len(rows) == 3
    first = rows[0]

    assert isinstance(first, tuple)
    assert len(first) == 4

    id_, event_time, payload, prices = first
    assert isinstance(id_, int)
    assert isinstance(event_time, datetime)
    assert event_time.tzinfo is not None
    assert isinstance(payload, tuple)
    assert payload[0] == "evt"
    assert isinstance(payload[1], int)
    assert isinstance(prices, list)
    assert isinstance(prices[0], Decimal)


async def test_stream_rows_matches_fetch_rows(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await _prepare_test_table(ch_client, make_table)
    query = f"SELECT id, event_time, payload, prices FROM {table} ORDER BY id"

    fetch_rows = await ch_client.fetch_rows(query)
    stream_rows = [row async for row in ch_client.stream_rows(query)]

    assert stream_rows == fetch_rows
