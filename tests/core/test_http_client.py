from typing import Any, AsyncIterator, cast

import pytest
from aiohttp import ClientResponse

from aiochlite.exceptions import ChClientError, ChServerError
from aiochlite.http_client import _error_text, _exception_sentinel, _read_chunks

_TAG = "abcdefghijklmnop"
_QUERY_ID = "8b95869b-2ba6-416f-a812-fe004b68a095"
_SENTINEL = _exception_sentinel(_TAG)
_MESSAGE = b"Code: 395. DB::Exception: boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)"
_BLOCK = _SENTINEL + _MESSAGE + b"\n" + f"{len(_MESSAGE) + 1} {_TAG}".encode() + b"\r\n__exception__\r\n"


class _FakeContent:
    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        while self._chunks:
            yield self._chunks.pop(0)

    async def read(self) -> bytes:
        rest = b"".join(self._chunks)
        self._chunks.clear()
        return rest


def _response(chunks: list[bytes], tag: str | None = _TAG) -> ClientResponse:
    fake: Any = type("_FakeResponse", (), {})()
    fake.content = _FakeContent(chunks)
    fake.status = 200
    fake.headers = {"X-ClickHouse-Exception-Tag": tag, "X-ClickHouse-Query-Id": _QUERY_ID} if tag else {}
    return cast(ClientResponse, fake)


async def _drain(chunks: list[bytes], tag: str | None = _TAG) -> tuple[bytes, str | None]:
    """Return what reached the parser and the error, if any."""
    out = bytearray()
    try:
        async for chunk in _read_chunks(_response(chunks, tag)):
            out += chunk
    except ChClientError as error:
        return bytes(out), str(error)

    return bytes(out), None


@pytest.mark.parametrize("split", range(1, len(_SENTINEL)))
async def test_exception_block_split_across_chunks(split: int):
    """Cutting the sentinel anywhere must still be detected, and none of it may reach the parser."""
    payload = b"DATA" + _BLOCK
    cut = len(b"DATA") + split

    data, error = await _drain([payload[:cut], payload[cut:]])

    assert data == b"DATA"
    assert error == _MESSAGE.decode()


async def test_exception_block_spread_over_single_byte_chunks():
    payload = b"DATA" + _BLOCK
    data, error = await _drain([payload[i : i + 1] for i in range(len(payload))])

    assert data == b"DATA"
    assert error == _MESSAGE.decode()


@pytest.mark.parametrize("split", [1, 2, 17, 40, 199])
async def test_payload_looking_like_a_marker_passes_through(split: int):
    """A String column may hold the marker; only the response tag makes it an exception."""
    payload = b"\r\n__exception__\r\nqqqqqqqqqqqqqqqq\r\nnot an error" + b"\r\n" * 30
    data, error = await _drain([payload[:split], payload[split:]])

    assert data == payload
    assert error is None


async def test_untagged_response_is_passed_through():
    """Servers before 25.11 send no tag, so chunks go straight to the parser."""
    payload = b"DATA" + _BLOCK
    data, error = await _drain([payload], tag=None)

    assert data == payload
    assert error is None


def test_error_text_keeps_short_bodies_whole():
    payload = b"Code: 62. DB::Exception: Syntax error"
    assert _error_text(payload, None) == payload.decode()


def test_error_text_skips_a_large_partial_payload():
    """A late failure answers with rows first; the error is what belongs in the exception."""
    error = b"Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)"
    assert _error_text(b"\x00" * 20_000 + error, None) == error.decode()


def test_error_text_reads_the_exception_block():
    assert _error_text(b"\x00" * 20_000 + _BLOCK, _TAG) == _MESSAGE.decode()


async def test_body_exception_carries_response_metadata():
    """Observability: the error must say which query it was and what code came back."""
    payload = b"DATA" + _BLOCK
    with pytest.raises(ChServerError) as error:
        async for _ in _read_chunks(_response([payload])):
            pass

    assert error.value.status == 200
    assert error.value.code == 395
    assert error.value.query_id == _QUERY_ID
    assert error.value.exception_tag == _TAG
    assert str(error.value) == _MESSAGE.decode()
