import pytest

from aiochlite import AsyncChClient, ChClientError

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]


async def test_ping(ch_client: AsyncChClient):
    assert await ch_client.ping()


async def test_execute(ch_client: AsyncChClient):
    await ch_client.execute("SELECT 1")


async def test_invalid_query_raises(ch_client: AsyncChClient):
    with pytest.raises(ChClientError):
        await ch_client.fetch("SELEC 1")
