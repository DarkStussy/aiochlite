import pytest

from aiochlite.core.insert_data import _BATCH_BYTES, build_insert_body


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
