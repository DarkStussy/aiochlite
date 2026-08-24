from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from enum import Enum

import pytest

from aiochlite.core.insert_data import _BATCH_BYTES, InsertData, build_insert_body, serialize_rows, take_first_row


async def _collect(statement: str, rows: list[str]) -> list[bytes]:
    return [chunk async for chunk in build_insert_body(statement, iter(rows))]


async def test_statement_comes_first_and_rows_follow():
    chunks = await _collect("INSERT INTO t FORMAT JSONEachRow\n", ['{"a":1}', '{"a":2}'])

    assert chunks[0] == b"INSERT INTO t FORMAT JSONEachRow\n"
    assert b"".join(chunks) == b'INSERT INTO t FORMAT JSONEachRow\n{"a":1}\n{"a":2}\n'


async def test_no_rows_sends_only_the_statement():
    assert await _collect("INSERT INTO t FORMAT JSONEachRow\n", []) == [b"INSERT INTO t FORMAT JSONEachRow\n"]


async def test_payload_is_split_into_bounded_batches():
    """The whole body must never be built at once, whatever the row count."""
    row = "x" * 1_000
    count = (_BATCH_BYTES // len(row)) * 4
    chunks = await _collect("INSERT\n", [row] * count)

    assert len(chunks) > 4
    assert max(len(chunk) for chunk in chunks) <= _BATCH_BYTES + len(row) + 1
    assert b"".join(chunks) == b"INSERT\n" + (row.encode() + b"\n") * count


async def test_batches_are_bounded_by_size_not_row_count():
    """A handful of wide rows must still be split, or the bound would depend on row width."""
    wide = "y" * (_BATCH_BYTES // 2)
    chunks = await _collect("INSERT\n", [wide] * 6)

    assert len(chunks) > 2


async def test_rows_are_serialized_while_sending():
    """Rows must be pulled as batches go out, not all of them before the first one does."""
    row = "x" * (_BATCH_BYTES // 2)
    total = 20
    pulled = 0

    def _rows():
        nonlocal pulled
        for _ in range(total):
            pulled += 1
            yield row

    body = build_insert_body("INSERT\n", _rows())
    assert await anext(body) == b"INSERT\n"
    assert pulled == 0

    await anext(body)
    assert 0 < pulled < total


@pytest.mark.parametrize("row", ["ы" * 300, "🙂" * 300], ids=["cyrillic", "emoji"])
async def test_batches_are_measured_in_bytes_not_characters(row: str):
    """A character is not a byte: counting characters would overshoot the limit several times."""
    count = (_BATCH_BYTES // len(row.encode())) * 4
    chunks = await _collect("INSERT\n", [row] * count)

    assert max(len(chunk) for chunk in chunks) <= _BATCH_BYTES + len(row.encode()) + 1
    assert b"".join(chunks) == b"INSERT\n" + (row.encode() + b"\n") * count


@pytest.mark.parametrize("rows", [[], ["a"], ["a", "b", "c"]])
async def test_body_always_ends_with_a_newline(rows: list[str]):
    chunks = await _collect("INSERT\n", rows)
    assert b"".join(chunks).endswith(b"\n")


async def test_take_first_row_from_sync_source():
    taken = await take_first_row([{"a": 1}, {"a": 2}])
    assert taken is not None

    first, rest = taken
    assert first == {"a": 1}
    assert isinstance(rest, Iterator)
    assert list(rest) == [{"a": 2}]


async def test_take_first_row_from_async_source():
    async def rows():
        yield {"a": 1}
        yield {"a": 2}

    taken = await take_first_row(rows())
    assert taken is not None

    first, rest = taken
    assert first == {"a": 1}
    assert isinstance(rest, AsyncIterator)
    assert [row async for row in rest] == [{"a": 2}]


@pytest.mark.parametrize("empty", [[], (), iter(())], ids=["list", "tuple", "iterator"])
async def test_take_first_row_of_nothing(empty: InsertData):
    assert await take_first_row(empty) is None


async def test_take_first_row_of_nothing_async():
    async def rows():
        return
        yield  # pragma: no cover

    assert await take_first_row(rows()) is None


async def test_serialize_rows_puts_the_taken_row_back_first():
    taken = await take_first_row([(1, "a"), (2, "b")])
    assert taken is not None

    format_name, rows = serialize_rows(*taken)
    assert format_name == "JSONCompactEachRow"
    assert isinstance(rows, Iterable)
    assert list(rows) == ['[1,"a"]', '[2,"b"]']


async def test_serialize_rows_picks_the_format_from_the_first_row():
    taken = await take_first_row([{"a": 1}, {"a": 2}])
    assert taken is not None

    format_name, rows = serialize_rows(*taken)
    assert format_name == "JSONEachRow"
    assert isinstance(rows, Iterable)
    assert list(rows) == ['{"a":1}', '{"a":2}']


async def test_serialize_rows_keeps_an_async_source_async():
    async def source():
        yield (1, "a")
        yield (2, "b")

    taken = await take_first_row(source())
    assert taken is not None

    format_name, rows = serialize_rows(*taken)
    assert format_name == "JSONCompactEachRow"
    assert isinstance(rows, AsyncIterable)
    assert [row async for row in rows] == ['[1,"a"]', '[2,"b"]']


async def test_body_accepts_an_async_source():
    async def rows():
        yield '{"a":1}'
        yield '{"a":2}'

    chunks = [chunk async for chunk in build_insert_body("INSERT\n", rows())]
    assert b"".join(chunks) == b'INSERT\n{"a":1}\n{"a":2}\n'


async def test_async_source_is_batched_the_same_way():
    row = "z" * (_BATCH_BYTES // 2)

    async def rows():
        for _ in range(6):
            yield row

    chunks = [chunk async for chunk in build_insert_body("INSERT\n", rows())]
    assert len(chunks) > 2
    assert b"".join(chunks) == b"INSERT\n" + (row.encode() + b"\n") * 6


class Colour(Enum):
    RED = "red"


async def test_serialize_rows_renders_a_map_key_a_row_is_keyed_by():
    """`json.dumps` takes no Enum key, and a row reaches it through here."""
    taken = await take_first_row([{"m": {Colour.RED: 1}, "s": Colour.RED}])
    assert taken is not None

    format_name, rows = serialize_rows(*taken)
    assert format_name == "JSONEachRow"
    assert isinstance(rows, Iterable)
    assert list(rows) == ['{"m":{"red":1},"s":"red"}']


async def test_serialize_rows_renders_such_a_key_in_a_tuple_row():
    taken = await take_first_row([({Colour.RED: 1}, Colour.RED)])
    assert taken is not None

    format_name, rows = serialize_rows(*taken)
    assert format_name == "JSONCompactEachRow"
    assert isinstance(rows, Iterable)
    assert list(rows) == ['[{"red":1},"red"]']
