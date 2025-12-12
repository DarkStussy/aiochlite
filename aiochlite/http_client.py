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

    async def stream(self, url: str, params: Mapping[str, str], *, data: Any = None) -> AsyncIterator[str]:
        async with self._session.post(url, params=params, data=data) as response:
            await _check_response(response)
            async for line in response.content:
                yield line.decode(response.get_encoding())

    async def close(self):
        await self._session.close()


async def _check_response(response: ClientResponse):
    """Check HTTP response status and raise error if not OK."""
    if response.status != HTTPStatus.OK:
        raise ChClientError(await response.text(errors="replace"))
