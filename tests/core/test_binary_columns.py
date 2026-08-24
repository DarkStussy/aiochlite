import asyncio
from collections.abc import Collection
from typing import Any, AsyncIterator, Callable

import pytest

from aiochlite.converters import rowbinary
from aiochlite.converters._type_parsing import holds_text, to_binary_type
from aiochlite.converters.rowbinary import (
    RowBinaryWithNamesAndTypesStreamParser,
    parse_rowbinary_with_names_and_types,
    parse_rowbinary_with_names_and_types_lazy,
)
from aiochlite.exceptions import ChArgumentError, ChProtocolError


def _encode_varuint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _encode_varuint(len(encoded)) + encoded


def _encode_bytes(value: bytes) -> bytes:
    return _encode_varuint(len(value)) + value


def _payload(columns: list[tuple[str, str]], rows: list[bytes]) -> bytes:
    """A RowBinaryWithNamesAndTypes payload over `(name, type)` columns."""
    parts = [_encode_varuint(len(columns))]
    parts += [_encode_string(name) for name, _ in columns]
    parts += [_encode_string(ch_type) for _, ch_type in columns]
    return b"".join(parts + rows)


# Not UTF-8 in any decoding: a lone 0xFF never starts a sequence.
_BINARY = b"\x00\xff\xfe\x01"


def _eager(payload: bytes, binary_columns: Collection[str] | str | None) -> list[Any]:
    _, _, rows = parse_rowbinary_with_names_and_types(payload, binary_columns=binary_columns)
    return [list(row) for row in rows]


def _eager_tuples(payload: bytes, binary_columns: Collection[str] | str | None) -> list[Any]:
    _, _, rows = parse_rowbinary_with_names_and_types(payload, as_tuple=True, binary_columns=binary_columns)
    return [list(row) for row in rows]


def _lazy(payload: bytes, binary_columns: Collection[str] | str | None) -> list[Any]:
    _, _, rows = parse_rowbinary_with_names_and_types_lazy(payload, binary_columns=binary_columns)
    return [list(row) for row in rows]


def _streamed(payload: bytes, binary_columns: Collection[str] | str | None) -> list[Any]:
    return _stream_chunks([payload], binary_columns)


def _stream_chunks(
    chunks: list[bytes],
    binary_columns: Collection[str] | str | None,
    *,
    lazy: bool = False,
) -> list[Any]:
    async def _chunks() -> AsyncIterator[bytes]:
        for chunk in chunks:
            yield chunk

    async def _run() -> list[Any]:
        parser = RowBinaryWithNamesAndTypesStreamParser(_chunks(), lazy=lazy, binary_columns=binary_columns)
        await parser.read_header()
        return [list(row) async for row in parser.rows()]

    return asyncio.run(_run())


def _streamed_lazy(payload: bytes, binary_columns: Collection[str] | str | None) -> list[Any]:
    return _stream_chunks([payload], binary_columns, lazy=True)


Consumer = Callable[[bytes, Collection[str] | str | None], list[Any]]

_PARSERS = pytest.mark.parametrize(
    "consume",
    [_eager, _eager_tuples, _lazy, _streamed, _streamed_lazy],
    ids=["eager", "eager-tuples", "lazy", "stream", "stream-lazy"],
)


@pytest.mark.parametrize(
    ("ch_type", "expected"),
    [
        ("String", "Binary"),
        ("FixedString(16)", "FixedBinary(16)"),
        ("Nullable(String)", "Nullable(Binary)"),
        ("LowCardinality(String)", "LowCardinality(Binary)"),
        ("LowCardinality(Nullable(String))", "LowCardinality(Nullable(Binary))"),
        ("Array(String)", "Array(Binary)"),
        ("Array(Array(FixedString(2)))", "Array(Array(FixedBinary(2)))"),
        ("Map(String, Array(String))", "Map(Binary, Array(Binary))"),
        ("Tuple(String, UInt8)", "Tuple(Binary, UInt8)"),
        # A quoted enum label is not a type, however much it looks like one.
        ("Enum8('String' = 1)", "Enum8('String' = 1)"),
        ("Map(String, Enum8('FixedString(4)' = 1))", "Map(Binary, Enum8('FixedString(4)' = 1))"),
        ("UInt64", "UInt64"),
        ("Array(UInt64)", "Array(UInt64)"),
    ],
)
def test_to_binary_type_rewrites_every_nested_string(ch_type: str, expected: str):
    assert to_binary_type(ch_type) == expected
    assert holds_text(ch_type) is (expected != ch_type)


@_PARSERS
def test_a_binary_column_decodes_as_bytes(consume: Consumer):
    payload = _payload([("id", "UInt8"), ("blob", "String")], [b"\x01" + _encode_bytes(_BINARY)])

    assert consume(payload, ["blob"]) == [[1, _BINARY]]


@_PARSERS
def test_only_the_named_columns_turn_binary(consume: Consumer):
    payload = _payload(
        [("text", "String"), ("blob", "String")],
        [_encode_string("héllo") + _encode_bytes(_BINARY)],
    )

    assert consume(payload, ["blob"]) == [["héllo", _BINARY]]


@_PARSERS
def test_a_fixedstring_column_keeps_every_byte_it_holds(consume: Consumer):
    """The text path strips the null padding, which for binary data is part of the value."""
    payload = _payload([("hash", "FixedString(4)")], [b"\xff\x00\x00\x00"])

    assert consume(payload, ["hash"]) == [[b"\xff\x00\x00\x00"]]


@_PARSERS
@pytest.mark.parametrize(
    ("ch_type", "encoded", "expected"),
    [
        ("Nullable(String)", b"\x00" + _encode_bytes(_BINARY), _BINARY),
        ("Nullable(String)", b"\x01", None),
        ("LowCardinality(String)", _encode_bytes(_BINARY), _BINARY),
        ("Array(String)", _encode_varuint(2) + _encode_bytes(_BINARY) + _encode_bytes(b""), [_BINARY, b""]),
        ("Array(FixedString(2))", _encode_varuint(2) + b"\xff\x00\xfe\x00", [b"\xff\x00", b"\xfe\x00"]),
        (
            "Array(Nullable(String))",
            _encode_varuint(2) + b"\x00" + _encode_bytes(_BINARY) + b"\x01",
            [_BINARY, None],
        ),
        (
            "Map(String, String)",
            _encode_varuint(1) + _encode_bytes(_BINARY) + _encode_bytes(b"\xfe"),
            {_BINARY: b"\xfe"},
        ),
        ("Tuple(String, UInt8)", _encode_bytes(_BINARY) + b"\x07", (_BINARY, 7)),
    ],
)
def test_binary_reaches_every_level_of_nesting(consume: Consumer, ch_type: str, encoded: bytes, expected: Any):
    payload = _payload([("value", ch_type)], [encoded])

    assert consume(payload, ["value"]) == [[expected]]


def test_the_header_still_reports_the_types_the_server_named():
    payload = _payload([("blob", "String")], [_encode_bytes(_BINARY)])

    names, types, _ = parse_rowbinary_with_names_and_types(payload, binary_columns=["blob"])

    assert names == ["blob"]
    assert types == ["String"]


def test_a_fixed_width_row_of_binary_takes_the_bulk_path():
    columns = [("hash", "FixedString(2)"), ("n", "UInt8")]
    payload = _payload(columns, [b"\xff\x00\x01", b"\x00\xfe\x02"])

    assert rowbinary._fixed_row_layout(["FixedBinary(2)", "UInt8"], None) is not None
    assert _eager(payload, ["hash"]) == [[b"\xff\x00", 1], [b"\x00\xfe", 2]]


def test_the_compiled_and_reader_paths_agree(monkeypatch: pytest.MonkeyPatch):
    payload = _payload(
        [("blob", "String"), ("blobs", "Array(String)"), ("n", "UInt8")],
        [_encode_bytes(_BINARY) + _encode_varuint(1) + _encode_bytes(b"\xff") + b"\x09"],
    )
    compiled = _eager(payload, ["blob", "blobs"])

    monkeypatch.setattr(rowbinary, "_fixed_row_layout", lambda *_, **__: None)
    monkeypatch.setattr(rowbinary, "_compiled_row_decoder", lambda *_, **__: None)

    assert _eager(payload, ["blob", "blobs"]) == compiled
    assert compiled == [[_BINARY, [b"\xff"], 9]]


def test_binary_rows_survive_every_chunk_boundary():
    payload = _payload(
        [("blob", "String"), ("n", "UInt8")],
        [_encode_bytes(_BINARY) + b"\x01", _encode_bytes(b"\xfe\xff") + b"\x02"],
    )
    expected = [[_BINARY, 1], [b"\xfe\xff", 2]]

    for split in range(1, len(payload)):
        chunks = [payload[:split], payload[split:]]
        assert _stream_chunks(chunks, ["blob"]) == expected, split


@_PARSERS
def test_a_binary_column_read_as_text_names_itself(consume: Consumer):
    payload = _payload([("id", "UInt8"), ("blob", "String")], [b"\x01" + _encode_bytes(_BINARY)])

    with pytest.raises(ChProtocolError) as error:
        consume(payload, None)

    assert "not UTF-8" in str(error.value)
    assert "binary_columns" in str(error.value)


@_PARSERS
def test_a_fixedstring_holding_binary_raises_rather_than_replacing_it(consume: Consumer):
    """Replacing what will not decode would hand back a mangled value that reads as text."""
    payload = _payload([("hash", "FixedString(4)")], [_BINARY])

    with pytest.raises(ChProtocolError) as error:
        consume(payload, None)

    assert "not UTF-8" in str(error.value)


def test_the_error_lists_the_text_columns_as_candidates():
    payload = _payload(
        [("n", "UInt8"), ("blob", "String"), ("names", "Array(String)")],
        [b"\x01" + _encode_bytes(_BINARY) + _encode_varuint(0)],
    )

    with pytest.raises(ChProtocolError) as error:
        _eager(payload, None)

    assert "'blob', 'names'" in str(error.value)
    assert "'n'" not in str(error.value)


@_PARSERS
def test_an_unknown_column_is_a_bad_call(consume: Consumer):
    payload = _payload([("blob", "String")], [_encode_bytes(_BINARY)])

    with pytest.raises(ChArgumentError, match="did not select: nope"):
        consume(payload, ["nope"])


@_PARSERS
def test_a_column_with_no_text_in_it_is_a_bad_call(consume: Consumer):
    payload = _payload([("n", "UInt8")], [b"\x01"])

    with pytest.raises(ChArgumentError, match="no String/FixedString: n"):
        consume(payload, ["n"])


@_PARSERS
def test_one_column_can_be_named_without_wrapping_it(consume: Consumer):
    """A `str` is a `Collection[str]`, so iterating one would look for columns `b`, `l`, `o`."""
    payload = _payload([("blob", "String")], [_encode_bytes(_BINARY)])

    assert consume(payload, "blob") == [[_BINARY]]


def test_a_bad_call_is_catchable_as_either_error():
    payload = _payload([("blob", "String")], [_encode_bytes(_BINARY)])

    with pytest.raises(ValueError, match="did not select"):
        _eager(payload, ["nope"])
