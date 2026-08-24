from itertools import chain
from typing import Any, AsyncIterable, AsyncIterator, Callable, Iterable, Iterator

from aiochlite.converters import to_json
from aiochlite.exceptions import _SourceError

# Bounds how much of an insert is held in memory at once. A single row above the limit is still
# sent whole, so it is a batching threshold rather than a hard cap.
_BATCH_BYTES = 256 * 1024

InsertRow = dict[str, Any] | tuple[Any, ...]
InsertData = Iterable[InsertRow] | AsyncIterable[InsertRow]


async def take_first_row(data: InsertData) -> tuple[InsertRow, Iterator[InsertRow] | AsyncIterator[InsertRow]] | None:
    """Pull one row to pick the format by. Returns None when there is nothing to insert."""
    if isinstance(data, AsyncIterable):
        rest = aiter(data)
        try:
            return await anext(rest), rest
        except StopAsyncIteration:
            return None

    rows = iter(data)
    for first in rows:
        return first, rows

    return None


def _to_json_row(row: InsertRow) -> str:
    """`JSONCompactEachRow` takes each row as a JSON array."""
    return to_json(list(row))


def serialize_rows(
    first: InsertRow,
    rest: Iterator[InsertRow] | AsyncIterator[InsertRow],
) -> tuple[str, Iterable[str] | AsyncIterable[str]]:
    """Pick the format from the row already taken, then put it back in front of the others."""
    format_name: str
    serialize: Callable[[InsertRow], str]
    if isinstance(first, dict):
        format_name, serialize = "JSONEachRow", to_json
    else:
        format_name, serialize = "JSONCompactEachRow", _to_json_row

    if isinstance(rest, AsyncIterator):
        return format_name, _serialize_async(serialize, first, rest)

    return format_name, map(serialize, chain((first,), rest))


async def _serialize_async(
    serialize: Callable[[InsertRow], str],
    first: InsertRow,
    rest: AsyncIterator[InsertRow],
) -> AsyncIterator[str]:
    yield serialize(first)
    async for row in rest:
        yield serialize(row)


async def build_insert_body(statement: str, rows: Iterable[str] | AsyncIterable[str]) -> AsyncIterator[bytes]:
    """Stream an insert payload in batches; rows are serialized as the request is sent."""
    yield statement.encode()

    batch: list[bytes] = []
    size = 0

    # Two loops rather than one over an async wrapper: wrapping doubles the cost of this step for
    # the common case, a list of rows already in memory.
    try:
        if isinstance(rows, AsyncIterable):
            async for row in rows:
                # Encoded upfront: the threshold is in bytes, and one character is not one byte.
                encoded = row.encode("utf-8", "surrogateescape")
                batch.append(encoded)
                size += len(encoded) + 1
                if size >= _BATCH_BYTES:
                    yield b"\n".join(batch) + b"\n"
                    batch.clear()
                    size = 0
        else:
            for row in rows:
                encoded = row.encode("utf-8", "surrogateescape")
                batch.append(encoded)
                size += len(encoded) + 1
                if size >= _BATCH_BYTES:
                    yield b"\n".join(batch) + b"\n"
                    batch.clear()
                    size = 0
    except Exception as error:
        raise _SourceError(error) from error

    if batch:
        yield b"\n".join(batch) + b"\n"
