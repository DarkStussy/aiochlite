import asyncio
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from aiohttp import ClientSession, ClientTimeout

from aiochlite import AsyncChClient, ChTransportError

# RowBinaryWithNamesAndTypes: one `UInt8` column named `n`, then two rows.
_BODY = b"\x01" + b"\x01n" + b"\x05UInt8" + b"\x01\x02"

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


async def _read_request(reader: asyncio.StreamReader):
    """Take the request headers off the socket so the client is not left writing into a full buffer."""
    await reader.readuntil(b"\r\n\r\n")


async def _cut_mid_body(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Promise more than is sent, then hang up: a response truncated on the wire."""
    await _read_request(reader)
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n" + _BODY)
    await writer.drain()
    writer.close()


async def _hang_up(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Close before answering at all."""
    await _read_request(reader)
    writer.close()


async def _never_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Read the request and go quiet, leaving the client to hit its own timeout."""
    await _read_request(reader)
    await asyncio.Event().wait()


@pytest.fixture
async def faulty_client() -> AsyncIterator[Callable[..., Awaitable[AsyncChClient]]]:
    """Hand out clients pointed at a local server that misbehaves in a chosen way."""
    servers: list[asyncio.Server] = []
    sessions: list[ClientSession] = []
    handlers: set[asyncio.Task[None]] = set()

    def _tracked(handler: Handler) -> Handler:
        """Keep hold of the handler task: waiting for one to end on its own can wait forever."""

        async def _run(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            task = asyncio.current_task()
            if task is not None:
                handlers.add(task)
            await handler(reader, writer)

        return _run

    async def _make(handler: Handler, total: float | None = None) -> AsyncChClient:
        server = await asyncio.start_server(_tracked(handler), "127.0.0.1", 0)
        servers.append(server)
        session = ClientSession(timeout=ClientTimeout(total=total))
        sessions.append(session)
        port: Any = server.sockets[0].getsockname()[1]
        return AsyncChClient(f"http://127.0.0.1:{port}", session=session)

    try:
        yield _make
    finally:
        for session in sessions:
            await session.close()
        for task in handlers:
            task.cancel()
        for server in servers:
            server.close()
            await server.wait_closed()


async def test_truncated_body_is_a_transport_error(faulty_client: Callable[..., Awaitable[AsyncChClient]]):
    """A short read must not pass for a complete result."""
    client = await faulty_client(_cut_mid_body)

    with pytest.raises(ChTransportError):
        await client.fetch("SELECT n FROM t")


async def test_truncated_stream_is_a_transport_error(faulty_client: Callable[..., Awaitable[AsyncChClient]]):
    """Rows already handed over do not turn a cut-off stream into a success."""
    client = await faulty_client(_cut_mid_body)

    async def consume() -> list[Any]:
        return [row async for row in client.stream("SELECT n FROM t")]

    with pytest.raises(ChTransportError):
        await consume()


async def test_disconnect_before_the_response_is_a_transport_error(
    faulty_client: Callable[..., Awaitable[AsyncChClient]],
):
    client = await faulty_client(_hang_up)

    with pytest.raises(ChTransportError):
        await client.fetch("SELECT n FROM t")


async def test_timeout_is_a_transport_error(faulty_client: Callable[..., Awaitable[AsyncChClient]]):
    client = await faulty_client(_never_answer, 0.2)

    with pytest.raises(ChTransportError):
        await client.fetch("SELECT n FROM t")
