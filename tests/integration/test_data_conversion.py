from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

from ._types import TableFactory


@pytest.mark.asyncio
@pytest.mark.clickhouse
async def test_scalar_and_nullable_types(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await make_table(
        id="UInt8",
        count="Int32",
        price="Decimal(10, 2)",
        event_date="Date",
        event_time="DateTime('UTC')",
        uuid="UUID",
        optional="Nullable(String)",
    )

    row = {
        "id": 1,
        "count": 42,
        "price": Decimal("123.45"),
        "event_date": date(2025, 12, 14),
        "event_time": datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")),
        "uuid": UUID("550e8400-e29b-41d4-a716-446655440000"),
        "optional": None,
    }

    await ch_client.insert(table, [row])

    result = await ch_client.fetchone(f"SELECT id, count, price, event_date, event_time, uuid, optional FROM {table}")
    assert result is not None
    assert result["id"] == 1
    assert result["count"] == 42
    assert result["price"] == Decimal("123.45")
    assert result["event_date"] == date(2025, 12, 14)
    assert result["event_time"] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert result["uuid"] == UUID("550e8400-e29b-41d4-a716-446655440000")
    assert result["optional"] is None


@pytest.mark.asyncio
@pytest.mark.clickhouse
async def test_complex_types(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await make_table(
        id="UInt8",
        ts64="DateTime64(6, 'Europe/Moscow')",
        values="Array(UInt8)",
        names="Array(String)",
        payload="Tuple(String, Int8)",
        nested="Tuple(String, Tuple(DateTime('UTC'), UInt16))",
    )

    insert_query = f"""
        INSERT INTO {table} VALUES (
            1,
            toDateTime64('2025-12-14 13:30:45.123456', 6, 'Europe/Moscow'),
            [1, 2, 3],
            ['foo', 'bar'],
            ('meta', 7),
            ('evt', (toDateTime('2025-12-14 10:00:00', 'UTC'), 9))
        )
        """
    await ch_client.execute(insert_query)

    result = await ch_client.fetchone(f"SELECT id, ts64, values, names, payload, nested FROM {table} WHERE id = 1")

    assert result is not None
    assert result["id"] == 1
    assert result["ts64"] == datetime(2025, 12, 14, 13, 30, 45, 123456, tzinfo=ZoneInfo("Europe/Moscow"))
    assert result["values"] == [1, 2, 3]
    assert result["names"] == ["foo", "bar"]
    assert result["payload"] == ("meta", 7)
    assert result["nested"] == ("evt", (datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC")), 9))
