import pytest

from aiochlite import AsyncChClient


@pytest.mark.asyncio
@pytest.mark.clickhouse
async def test_ping(ch_client: AsyncChClient) -> None:
    assert await ch_client.ping()


@pytest.mark.asyncio
@pytest.mark.clickhouse
async def test_query_params(ch_client: AsyncChClient) -> None:
    value = await ch_client.fetchval("SELECT {x:UInt8} + 2", params={"x": 5})
    assert value == 7
