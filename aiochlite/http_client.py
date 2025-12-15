from http import HTTPStatus
from typing import Any, AsyncIterator, Mapping

from aiohttp import ClientResponse, ClientSession

from .exceptions import ChClientError


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

    async def read(self, url: str, params: Mapping[str, str], *, data: Any = None) -> bytes:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)
            return await response.read()

    async def stream(self, url: str, params: Mapping[str, str], *, data: Any = None) -> AsyncIterator[bytes]:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)

            async for chunk in response.content.iter_chunked(262_144):
                yield chunk

    async def close(self):
        await self._session.close()


async def _check_response(response: ClientResponse):
    """Check HTTP response status and raise error if not OK."""
    if response.status != HTTPStatus.OK:
        raise ChClientError(await response.text(errors="replace"))
