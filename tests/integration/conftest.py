import os
from typing import AsyncIterator
from uuid import uuid4

import pytest

from aiochlite import AsyncChClient

from ._types import ChConfig, TableFactory

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


@pytest.fixture(scope="session")
def clickhouse_config() -> ChConfig:
    return {
        "url": CLICKHOUSE_URL,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
    }


@pytest.fixture
async def ch_client(clickhouse_config: ChConfig) -> AsyncIterator[AsyncChClient]:
    client = AsyncChClient(**clickhouse_config)
    try:
        alive = await client.ping()
    except Exception:
        alive = False

    if not alive:
        await client.close()
        pytest.skip("ClickHouse HTTP service is not available")

    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def make_table(ch_client: AsyncChClient) -> AsyncIterator[TableFactory]:
    created: list[str] = []

    async def _create(**schema: str) -> str:
        columns = ", ".join(f"{name} {type_}" for name, type_ in schema.items())
        table_name = f"test_aiochlite_{uuid4().hex}"
        await ch_client.execute(f"CREATE TABLE {table_name} ({columns}) ENGINE = Memory")
        created.append(table_name)
        return table_name

    try:
        yield _create
    finally:
        for table_name in created:
            await ch_client.execute(f"DROP TABLE IF EXISTS {table_name}")
