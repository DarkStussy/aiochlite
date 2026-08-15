import asyncio
from typing import Mapping, cast

import pytest
from aiohttp import ClientSession

from aiochlite import AsyncChClient, ChTransportError
from aiochlite.http_client import HttpClient


class _HangingHttpClient:
    """A ping that never answers, leaving the caller room to cancel mid-entry."""

    def __init__(self):
        self.closed = False
        self.reached = asyncio.Event()

    async def get(self, url: str, params: Mapping[str, str]):
        self.reached.set()
        await asyncio.Event().wait()

    async def close(self):
        self.closed = True


async def test_canceled_context_entry_runs_cleanup():
    """`CancelledError` is not an `Exception`, so catching only those would skip the cleanup."""
    session = ClientSession()
    client = AsyncChClient(session=session)
    transport = _HangingHttpClient()
    client._http_client = cast(HttpClient, transport)

    try:
        entering = asyncio.create_task(client.__aenter__())
        await transport.reached.wait()
        entering.cancel()

        with pytest.raises(asyncio.CancelledError):
            await entering

        assert transport.closed
    finally:
        await session.close()


class _BrokenHttpClient:
    """A ping that fails, and a cleanup that fails after it."""

    async def get(self, url: str, params: Mapping[str, str]):
        raise ChTransportError("ping failed")

    async def close(self):
        raise RuntimeError("close blew up")


async def test_failing_cleanup_keeps_the_original_error():
    """The caller needs the reason the entry failed, not whatever the cleanup ran into."""
    session = ClientSession()
    client = AsyncChClient(session=session)
    client._http_client = cast(HttpClient, _BrokenHttpClient())

    try:
        with pytest.raises(ChTransportError, match="ping failed"):
            async with client:
                pass
    finally:
        await session.close()
