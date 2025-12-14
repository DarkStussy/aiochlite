from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from aiochlite import AsyncChClient

from ._types import TableFactory


@pytest.mark.asyncio
@pytest.mark.clickhouse
async def test_insert_and_fetch_roundtrip(ch_client: AsyncChClient, make_table: TableFactory) -> None:
    table = await make_table(
        id="UInt32",
        name="String",
        created_at="DateTime('UTC')",
    )

    rows = [
        {"id": 1, "name": "Alice", "created_at": datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))},
        {"id": 2, "name": "Bob", "created_at": datetime(2025, 12, 14, 12, 30, 45, tzinfo=ZoneInfo("UTC"))},
    ]

    await ch_client.insert(table, rows)

    result = await ch_client.fetch(f"SELECT id, name, created_at FROM {table} ORDER BY id")

    assert [row["id"] for row in result] == [1, 2]
    assert [row["name"] for row in result] == ["Alice", "Bob"]
    assert result[0]["created_at"] == datetime(2025, 12, 14, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert result[1]["created_at"] == datetime(2025, 12, 14, 12, 30, 45, tzinfo=ZoneInfo("UTC"))
