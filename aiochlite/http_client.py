import re
from contextlib import asynccontextmanager, contextmanager
from http import HTTPStatus
from typing import Any, AsyncGenerator, AsyncIterator, Generator, Mapping

from aiohttp import ClientError, ClientResponse, ClientSession

from .exceptions import ChServerError, ChTransportError, _SourceError

_TIMEZONE_HEADER = "X-ClickHouse-Timezone"
_EXCEPTION_TAG_HEADER = "X-ClickHouse-Exception-Tag"
_EXCEPTION_CODE_HEADER = "X-ClickHouse-Exception-Code"
_QUERY_ID_HEADER = "X-ClickHouse-Query-Id"

_ERROR_CODE_RE = re.compile(r"^Code:\s*(\d+)")

# A query that fails after the response started still returns 200, with the error appended to the
# body in any output format. The block is the last thing on the wire and holds at most 16 KiB.
_EXCEPTION_MARKER = b"\r\n__exception__\r\n"
_MARKER_FIRST_BYTE = _EXCEPTION_MARKER[:1]
_MAX_EXCEPTION_BLOCK = 16 * 1024
_CHUNK_SIZE = 262_144


def _source_failure(error: BaseException) -> BaseException | None:
    """Find a row-source exception that aiohttp buried under a connection error."""
    seen: BaseException | None = error
    while seen is not None:
        if isinstance(seen, _SourceError):
            return seen.cause

        seen = seen.__cause__ or seen.__context__

    return None


@contextmanager
def _transport_boundary() -> Generator[None, None, None]:
    """Keep aiohttp exceptions out of the public API. ``CancelledError`` is not an ``Exception``."""
    try:
        yield
    except (ClientError, TimeoutError) as error:
        # A failing row source is not a network failure, and the caller wants their own exception.
        source = _source_failure(error)
        if source is not None:
            raise source from source.__cause__

        raise ChTransportError(str(error) or type(error).__name__) from error


def _server_error(message: str, response: ClientResponse) -> ChServerError:
    """Attach the response metadata that makes a server error traceable in `system.query_log`."""
    header_code = response.headers.get(_EXCEPTION_CODE_HEADER)
    # The code is only known upfront when the status carries the error; otherwise it is in the text.
    matched = _ERROR_CODE_RE.match(message)
    code = header_code if header_code and header_code.isdigit() else matched.group(1) if matched else None

    return ChServerError(
        message,
        status=response.status,
        code=int(code) if code else None,
        query_id=response.headers.get(_QUERY_ID_HEADER),
        exception_tag=response.headers.get(_EXCEPTION_TAG_HEADER),
    )


def _exception_sentinel(tag: str) -> bytes:
    """Marker followed by the per-response random tag, which keeps payload bytes from matching."""
    return _EXCEPTION_MARKER + tag.encode("ascii", "replace") + b"\r\n"


def _exception_message(block: bytes) -> str:
    """Take the message out of ``<message>\\n<length> <tag>\\r\\n__exception__\\r\\n``."""
    end = block.rfind(_EXCEPTION_MARKER[:-2])
    head = block[:end] if end != -1 else block
    cut = head.rfind(b"\n")
    return (head[:cut] if cut != -1 else head).decode("utf-8", "replace").strip()


class HttpClient:
    """Wrapper around aiohttp ClientSession for HTTP operations."""

    def __init__(self, session: ClientSession):
        self._session = session

    @asynccontextmanager
    async def _request(
        self,
        method: str,
        url: str,
        params: Mapping[str, str],
        data: Any = None,
    ) -> AsyncGenerator[ClientResponse, None]:
        """Single place where a request is sent, transport errors are named and the status checked."""
        with _transport_boundary():
            async with self._session.request(method, url, params=params, data=data) as response:
                await _check_response(response)
                yield response

    async def get(self, url: str, params: Mapping[str, str]):
        async with self._request("GET", url, params):
            pass

    async def post(self, url: str, params: Mapping[str, str], *, data: Any = None):
        async with self._request("POST", url, params, data) as response:
            # The body is discarded, but it still has to be walked: that is where a query failing
            # mid-response reports the error.
            async for _ in _read_chunks(response):
                pass

    async def read(self, url: str, params: Mapping[str, str], *, data: Any = None) -> tuple[bytes, str | None]:
        async with self._request("POST", url, params, data) as response:
            payload = _raise_for_body_exception(await response.read(), response)
            return payload, response.headers.get(_TIMEZONE_HEADER)

    @asynccontextmanager
    async def stream(
        self,
        url: str,
        params: Mapping[str, str],
        *,
        data: Any = None,
    ) -> AsyncGenerator[tuple[str | None, AsyncIterator[bytes]], None]:
        async with self._request("POST", url, params, data) as response:
            yield response.headers.get(_TIMEZONE_HEADER), _read_chunks(response)

    async def close(self):
        with _transport_boundary():
            await self._session.close()


def _raise_for_body_exception(payload: bytes, response: ClientResponse) -> bytes:
    """Raise if the body ends with an exception block, otherwise return it unchanged."""
    tag = response.headers.get(_EXCEPTION_TAG_HEADER)
    if not tag:
        return payload

    sentinel = _exception_sentinel(tag)
    # The block closes the response, so only its tail is worth scanning.
    index = payload.find(sentinel, max(len(payload) - _MAX_EXCEPTION_BLOCK - len(sentinel), 0))
    if index == -1:
        return payload

    raise _server_error(_exception_message(payload[index + len(sentinel) :]), response)


async def _read_chunks(response: ClientResponse) -> AsyncIterator[bytes]:
    """Yield body chunks, cutting the stream short if an exception block shows up in it."""
    with _transport_boundary():
        async for chunk in _scan_chunks(response):
            yield chunk


async def _scan_chunks(response: ClientResponse) -> AsyncIterator[bytes]:
    tag = response.headers.get(_EXCEPTION_TAG_HEADER)
    if not tag:
        async for chunk in response.content.iter_chunked(_CHUNK_SIZE):
            yield chunk
        return

    sentinel = _exception_sentinel(tag)
    keep = len(sentinel) - 1
    carry = b""

    async for chunk in response.content.iter_chunked(_CHUNK_SIZE):
        buffer = carry + chunk if carry else chunk
        carry = b""

        index = buffer.find(sentinel)
        if index != -1:
            if index:
                yield buffer[:index]

            block = buffer[index + len(sentinel) :] + await response.content.read()
            raise _server_error(_exception_message(block), response)

        # A sentinel can straddle a chunk boundary, so a tail that could still start one is held
        # back instead of going to the parser. Most chunks end on bytes that cannot, and are
        # passed on whole.
        edge = buffer.find(_MARKER_FIRST_BYTE, max(len(buffer) - keep, 0))
        if edge == -1:
            yield buffer
        else:
            if edge:
                yield buffer[:edge]
            carry = buffer[edge:]

    if carry:
        yield carry


def _error_text(payload: bytes, tag: str | None) -> str:
    """Pull the error out of a failed response, which may carry megabytes of partial results."""
    tail = payload[-_MAX_EXCEPTION_BLOCK:]
    if tag:
        sentinel = _exception_sentinel(tag)
        index = tail.find(sentinel)
        if index != -1:
            return _exception_message(tail[index + len(sentinel) :])

    if len(payload) <= _MAX_EXCEPTION_BLOCK:
        return payload.decode("utf-8", "replace").strip()

    # A query that fails after sending rows answers with the rows followed by the error, so the
    # last error line is the useful part rather than the whole body.
    start = tail.rfind(b"Code: ")
    return (tail[start:] if start != -1 else tail).decode("utf-8", "replace").strip()


async def _check_response(response: ClientResponse):
    """Check HTTP response status and raise error if not OK."""
    if response.status != HTTPStatus.OK:
        raise _server_error(_error_text(await response.read(), response.headers.get(_EXCEPTION_TAG_HEADER)), response)
