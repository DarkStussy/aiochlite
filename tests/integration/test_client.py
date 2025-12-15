from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest

from aiochlite import AsyncChClient

from ._types import ChConfig

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]

_CLIENT_MATRIX = [
    (True, True),
    (True, False),
    (False, True),
    (False, False),
]


@asynccontextmanager
async def _client(clickhouse_config: ChConfig, **kwargs) -> AsyncIterator[AsyncChClient]:
    client = AsyncChClient(**clickhouse_config, **kwargs)
    try:
        alive = await client.ping()
        if not alive:
            pytest.skip("ClickHouse HTTP service is not available")
        yield client
    finally:
        await client.close()


@pytest.mark.parametrize(("enable_compression", "lazy_decode"), _CLIENT_MATRIX)
async def test_enable_compression_and_lazy_decode(
    clickhouse_config: ChConfig,
    enable_compression: bool,
    lazy_decode: bool,
) -> None:
    async with _client(
        clickhouse_config,
        enable_compression=enable_compression,
        lazy_decode=lazy_decode,
    ) as ch_client:
        query = "SELECT number FROM system.numbers LIMIT 2"

        rows = await ch_client.fetch(query)
        assert [r["number"] for r in rows] == [0, 1]

        out = [row["number"] async for row in ch_client.stream(query)]
        assert out == [0, 1]


async def test_database_setting(clickhouse_config: ChConfig) -> None:
    async with _client(clickhouse_config, database="system") as ch_client:
        assert await ch_client.fetchval("SELECT currentDatabase()") == "system"


async def test_custom_session(clickhouse_config: ChConfig) -> None:
    aiohttp = pytest.importorskip("aiohttp")
    async with aiohttp.ClientSession() as session, _client(clickhouse_config, session=session) as ch_client:
        assert await ch_client.fetchval("SELECT 1") == 1
