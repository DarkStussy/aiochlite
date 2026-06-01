from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Any, AsyncGenerator, AsyncIterator, Mapping

from aiohttp import ClientResponse, ClientSession

from .exceptions import ChClientError

_TIMEZONE_HEADER = "X-ClickHouse-Timezone"


class HttpClient:
    """Wrapper around aiohttp ClientSession for HTTP operations."""

    def __init__(self, session: ClientSession):
        self._session = session

    async def get(self, url: str, params: Mapping[str, str]):
        async with self._session.get(url, params=params) as response:
            await _check_response(response)

    async def post(self, url: str, params: Mapping[str, str], *, data: Any = None) -> AsyncIterator[bytes] | None:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)

    async def read(self, url: str, params: Mapping[str, str], *, data: Any = None) -> tuple[bytes, str | None]:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)
            return await response.read(), response.headers.get(_TIMEZONE_HEADER)

    @asynccontextmanager
    async def stream(
        self,
        url: str,
        params: Mapping[str, str],
        *,
        data: Any = None,
    ) -> AsyncGenerator[tuple[str | None, AsyncIterator[bytes]], None]:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)

            async def _chunks() -> AsyncIterator[bytes]:
                async for chunk in response.content.iter_chunked(262_144):
                    yield chunk

            yield response.headers.get(_TIMEZONE_HEADER), _chunks()

    async def close(self):
        await self._session.close()


async def _check_response(response: ClientResponse):
    """Check HTTP response status and raise error if not OK."""
    if response.status != HTTPStatus.OK:
        raise ChClientError(await response.text(errors="replace"))
