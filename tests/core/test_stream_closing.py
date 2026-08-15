from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, Mapping, cast

from aiohttp import ClientSession

from aiochlite import AsyncChClient
from aiochlite.http_client import HttpClient

# RowBinaryWithNamesAndTypes: one `UInt8` column named `n`, then three rows.
_PAYLOAD = b"\x01" + b"\x01n" + b"\x05UInt8" + b"\x01\x02\x03"


class _FakeHttpClient:
    """Stands in for the transport, tracking whether the response context is still open."""

    def __init__(self):
        self.response_open = False

    @asynccontextmanager
    async def stream(
        self,
        url: str,
        params: Mapping[str, str],
        *,
        data: Any = None,
    ) -> AsyncGenerator[tuple[str | None, AsyncIterator[bytes]], None]:
        self.response_open = True
        try:
            yield None, self._chunks()
        finally:
            self.response_open = False

    async def _chunks(self) -> AsyncIterator[bytes]:
        yield _PAYLOAD


async def test_fetchone_closes_the_response_before_returning():
    """Left to the garbage collector, the generators behind fetchone() hold the response open."""
    session = ClientSession()
    client = AsyncChClient(session=session)
    transport = _FakeHttpClient()
    client._http_client = cast(HttpClient, transport)

    try:
        row = await client.fetchone("SELECT n FROM t")

        assert row is not None
        assert row["n"] == 1
        assert not transport.response_open
    finally:
        await session.close()
