import ipaddress
import json
import re
import struct
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Any, Callable, Iterable, Protocol
from uuid import UUID

from ._type_parsing import extract_base_type, extract_timezone, split_type_arguments, unwrap_wrappers


class _Reader(Protocol):
    def _read(self, size: int) -> memoryview: ...

    def read_int8(self) -> int: ...
    def read_int16(self) -> int: ...
    def read_int32(self) -> int: ...
    def read_int64(self) -> int: ...
    def read_uint8(self) -> int: ...
    def read_uint16(self) -> int: ...
    def read_uint32(self) -> int: ...
    def read_uint64(self) -> int: ...
    def read_float32(self) -> float: ...
    def read_float64(self) -> float: ...
    def read_varuint(self) -> int: ...
    def read_string(self) -> str: ...
    def skip(self, size: int) -> None: ...

    @property
    def pos(self) -> int: ...


class _BinaryReader:
    def __init__(self, data: bytes | memoryview):
        self._data = data if isinstance(data, memoryview) else memoryview(data)
        self._pos = 0

    def _read(self, size: int) -> memoryview:
        end = self._pos + size
        if end > len(self._data):
            raise ValueError("Unexpected end of data")
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    def read_uint8(self) -> int:
        if self._pos >= len(self._data):
            raise ValueError("Unexpected end of data")
        value = self._data[self._pos]
        self._pos += 1
        return int(value)

    def read_int8(self) -> int:
        if self._pos + 1 > len(self._data):
            raise ValueError("Unexpected end of data")
        value = struct.unpack_from("<b", self._data, self._pos)[0]
        self._pos += 1
        return value

    def read_uint16(self) -> int:
        value = struct.unpack_from("<H", self._data, self._pos)[0]
        self._pos += 2
        return value

    def read_int16(self) -> int:
        value = struct.unpack_from("<h", self._data, self._pos)[0]
        self._pos += 2
        return value

    def read_uint32(self) -> int:
        value = struct.unpack_from("<I", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_int32(self) -> int:
        value = struct.unpack_from("<i", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_uint64(self) -> int:
        value = struct.unpack_from("<Q", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_int64(self) -> int:
        value = struct.unpack_from("<q", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_int128(self) -> int:
        return int.from_bytes(self._read(16), "little", signed=True)

    def read_float32(self) -> float:
        value = struct.unpack_from("<f", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_float64(self) -> float:
        value = struct.unpack_from("<d", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_varuint(self) -> int:
        """Read LEB128 varuint."""
        shift = 0
        result = 0
        while True:
            byte = self.read_uint8()
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                break
            shift += 7
        return result

    def read_bytes(self, size: int) -> bytes:
        return self._read(size).tobytes()

    def read_string(self) -> str:
        length = self.read_varuint()
        return self._read(length).tobytes().decode("utf-8")

    @property
    def eof(self) -> bool:
        return self._pos >= len(self._data)

    @property
    def pos(self) -> int:
        return self._pos

    def skip(self, size: int) -> None:
        end = self._pos + size
        if end > len(self._data):
            raise ValueError("Unexpected end of data")
        self._pos = end


def _decimal_meta(ch_type: str) -> tuple[int, int]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    parts = [p.strip() for p in inner.split(",")]

    if ch_type.startswith("Decimal32"):
        precision = 9
        scale = int(parts[0])
    elif ch_type.startswith("Decimal64"):
        precision = 18
        scale = int(parts[0])
    elif ch_type.startswith("Decimal128"):
        precision = 38
        scale = int(parts[0])
    elif ch_type.startswith("Decimal256"):
        precision = 76
        scale = int(parts[0])
    else:
        precision = int(parts[0])
        scale = int(parts[1])

    return precision, scale


def _decimal_size(precision: int) -> int:
    if precision <= 9:
        return 4
    if precision <= 18:
        return 8
    if precision <= 38:
        return 16
    if precision <= 76:
        return 32
    raise ValueError(f"Unsupported Decimal precision: {precision}")


_EPOCH_DATE = date(1970, 1, 1)


def _datetime_reader(ch_type: str) -> Callable[[_Reader], datetime]:
    tz = extract_timezone(ch_type)

    @lru_cache(maxsize=4096)
    def _dt(ts: int) -> datetime:
        return datetime.fromtimestamp(ts, tz=tz)

    def _read_dt(reader: _Reader) -> datetime:
        return _dt(reader.read_uint32())

    return _read_dt


def _datetime64_reader(ch_type: str) -> Callable[[_Reader], datetime]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    parts = [p.strip() for p in inner.split(",")]
    scale = int(parts[0])
    tz = extract_timezone(ch_type)

    @lru_cache(maxsize=4096)
    def _dt64(ticks: int) -> datetime:
        base_seconds, remainder = divmod(ticks, 10**scale)
        dt = datetime.fromtimestamp(base_seconds, tz=tz)
        if remainder:
            micros = remainder * (10 ** (6 - scale)) if scale <= 6 else remainder / (10 ** (scale - 6))
            dt = dt + timedelta(microseconds=micros)
        return dt

    def _read_dt64(reader: _Reader) -> datetime:
        return _dt64(reader.read_int64())

    return _read_dt64


def _decimal_reader(ch_type: str) -> Callable[[_Reader], Decimal]:
    precision, scale = _decimal_meta(ch_type)
    size = _decimal_size(precision)

    @lru_cache(maxsize=4096)
    def _dec(raw: int) -> Decimal:
        return Decimal(raw).scaleb(-scale)

    def _read_dec(reader: _Reader) -> Decimal:
        return _dec(int.from_bytes(reader._read(size), "little", signed=True))

    return _read_dec


def _fixedstring_reader(ch_type: str) -> Callable[[_Reader], str]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    size = int(inner.strip())

    def _read_fixedstring(reader: _Reader) -> str:
        raw = reader._read(size).tobytes()
        return raw.decode("utf-8", errors="replace").rstrip("\x00")

    return _read_fixedstring


@lru_cache(maxsize=512)
def _enum_mapping(ch_type: str) -> dict[int, str]:
    """
    Parse ClickHouse Enum8/Enum16 type definition into {value: name}.

    Example:
      Enum8('a' = 1, 'b' = 2)
    """
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    pairs = re.findall(r"'((?:\\.|[^'])*)'\s*=\s*([+-]?\d+)", inner)
    if not pairs:
        raise ValueError(f"Invalid Enum definition: {ch_type}")

    mapping: dict[int, str] = {}
    for raw_name, raw_value in pairs:
        name = raw_name.replace("\\\\", "\\").replace("\\'", "'")
        mapping[int(raw_value)] = name

    return mapping


def _enum_reader(ch_type: str) -> Callable[[_Reader], str]:
    base = extract_base_type(ch_type)
    mapping = _enum_mapping(ch_type)

    if base == "Enum8":

        def _read_enum(reader: _Reader) -> str:
            value = reader.read_int8()
            return mapping.get(value, str(value))

        return _read_enum

    if base == "Enum16":

        def _read_enum(reader: _Reader) -> str:
            value = reader.read_int16()
            return mapping.get(value, str(value))

        return _read_enum

    raise ValueError(f"Unsupported Enum type: {ch_type}")


def _ipv4_reader(_: str) -> Callable[[_Reader], ipaddress.IPv4Address]:
    def _read_ipv4(reader: _Reader) -> ipaddress.IPv4Address:
        return ipaddress.IPv4Address(reader.read_uint32())

    return _read_ipv4


def _ipv6_reader(_: str) -> Callable[[_Reader], ipaddress.IPv6Address]:
    def _read_ipv6(reader: _Reader) -> ipaddress.IPv6Address:
        return ipaddress.IPv6Address(reader._read(16).tobytes())

    return _read_ipv6


def _array_reader(ch_type: str) -> Callable[[_Reader], list[Any]]:
    inner_type = ch_type[6:-1]
    inner = _reader_for_type(inner_type)

    def _read_array(reader: _Reader) -> list[Any]:
        return [inner(reader) for _ in range(reader.read_varuint())]

    return _read_array


def _map_reader(ch_type: str) -> Callable[[_Reader], dict[Any, Any]]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    key_type, value_type = split_type_arguments(inner)
    key_reader = _reader_for_type(key_type)
    value_reader = _reader_for_type(value_type)

    def _read_map(reader: _Reader) -> dict[Any, Any]:
        count = reader.read_varuint()
        out: dict[Any, Any] = {}
        for _ in range(count):
            key = key_reader(reader)
            value = value_reader(reader)
            out[key] = value
        return out

    return _read_map


def _tuple_reader(ch_type: str) -> Callable[[_Reader], tuple[Any, ...]]:
    inner = ch_type[6:-1]
    element_types = split_type_arguments(inner)
    readers = tuple(_reader_for_type(t) for t in element_types)

    def _read_tuple(reader: _Reader) -> tuple[Any, ...]:
        return tuple(r(reader) for r in readers)

    return _read_tuple


def _uuid_reader(reader: _Reader) -> UUID:
    raw = reader._read(16).tobytes()
    return UUID(bytes=raw[:8][::-1] + raw[8:][::-1])


_PRIMITIVE_READERS: dict[str, Callable[[_Reader], Any]] = {
    "Bool": lambda r: r.read_uint8() != 0,
    "Float32": lambda r: r.read_float32(),
    "Float64": lambda r: r.read_float64(),
    "Int8": lambda r: r.read_int8(),
    "Int16": lambda r: r.read_int16(),
    "Int32": lambda r: r.read_int32(),
    "Int64": lambda r: r.read_int64(),
    "JSON": lambda r: json.loads(r.read_string()),
    "String": lambda r: r.read_string(),
    "UInt8": lambda r: r.read_uint8(),
    "UInt16": lambda r: r.read_uint16(),
    "UInt32": lambda r: r.read_uint32(),
    "UInt64": lambda r: r.read_uint64(),
}

_COMPLEX_READERS: dict[str, Callable[[str], Callable[[_Reader], Any]]] = {
    "Array": _array_reader,
    "Date": lambda _: (lambda r: _EPOCH_DATE + timedelta(days=r.read_uint16())),
    "Date32": lambda _: (lambda r: _EPOCH_DATE + timedelta(days=r.read_int32())),
    "DateTime": _datetime_reader,
    "DateTime64": _datetime64_reader,
    "Enum16": _enum_reader,
    "Enum8": _enum_reader,
    "FixedString": _fixedstring_reader,
    "IPv4": _ipv4_reader,
    "IPv6": _ipv6_reader,
    "Map": _map_reader,
    "Tuple": _tuple_reader,
    # ClickHouse UUID RowBinary is encoded as two UInt64 (hi, lo), each in little-endian.
    "UUID": lambda _: _uuid_reader,
}


@lru_cache(maxsize=256)
def _reader_for_type(ch_type: str) -> Callable[[_Reader], Any]:
    if ch_type.startswith("LowCardinality("):
        return _reader_for_type(ch_type[15:-1])

    if ch_type.startswith("Nullable("):
        inner = _reader_for_type(ch_type[9:-1])

        def _read_nullable(reader: _Reader) -> Any:
            return None if reader.read_uint8() else inner(reader)

        return _read_nullable

    ch_type = unwrap_wrappers(ch_type)
    base = extract_base_type(ch_type)

    primitive = _PRIMITIVE_READERS.get(base)
    if primitive is not None:
        return primitive

    if base.startswith("Decimal"):
        return _decimal_reader(ch_type)

    handler = _COMPLEX_READERS.get(base)
    if handler is not None:
        return handler(ch_type)

    raise ValueError(f"Unsupported RowBinary type: {ch_type}")


def parse_rowbinary_with_names_and_types(data: bytes) -> tuple[list[str], list[str], Iterable[list[Any]]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return header and row iterator.

    Returns:
        names: list of column names
        types: list of ClickHouse types
        rows: iterable of rows (list of values)
    """
    reader = _BinaryReader(data)
    column_count = reader.read_varuint()
    names = [reader.read_string() for _ in range(column_count)]
    types = [reader.read_string() for _ in range(column_count)]
    readers = [_reader_for_type(tp) for tp in types]

    def _rows() -> Iterable[list[Any]]:
        while not reader.eof:
            yield [read(reader) for read in readers]

    return names, types, _rows()


_FIXED_SIZES: dict[str, int] = {
    "Bool": 1,
    "UInt8": 1,
    "Int8": 1,
    "UInt16": 2,
    "Int16": 2,
    "UInt32": 4,
    "Int32": 4,
    "UInt64": 8,
    "Int64": 8,
    "Float32": 4,
    "Float64": 8,
    "Date": 2,
    "Date32": 4,
    "DateTime": 4,
    "Enum8": 1,
    "Enum16": 2,
    "IPv4": 4,
    "IPv6": 16,
}


def _array_skipper(inner_type: str) -> Callable[[_Reader], None]:
    inner_skip = _skipper_for_type(inner_type)

    inner_unwrapped = unwrap_wrappers(inner_type)
    inner_base = extract_base_type(inner_unwrapped)

    inner_fixed = _FIXED_SIZES.get(inner_base)
    if inner_fixed is not None:
        return lambda reader: reader.skip(reader.read_varuint() * inner_fixed)

    if inner_base == "DateTime64":
        return lambda reader: reader.skip(reader.read_varuint() * 8)

    if inner_base.startswith("Decimal"):
        precision, _scale = _decimal_meta(inner_unwrapped)
        size = _decimal_size(precision)
        return lambda reader: reader.skip(reader.read_varuint() * size)

    if inner_base == "UUID":
        return lambda reader: reader.skip(reader.read_varuint() * 16)

    def _skip_array(reader: _Reader) -> None:
        count = reader.read_varuint()
        for _ in range(count):
            inner_skip(reader)

    return _skip_array


def _map_skipper(ch_type: str) -> Callable[[_Reader], None]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    key_type, value_type = split_type_arguments(inner)
    key_skip = _skipper_for_type(key_type)
    value_skip = _skipper_for_type(value_type)

    key_unwrapped = unwrap_wrappers(key_type)
    value_unwrapped = unwrap_wrappers(value_type)
    key_base = extract_base_type(key_unwrapped)
    value_base = extract_base_type(value_unwrapped)

    key_fixed = _FIXED_SIZES.get(key_base)
    value_fixed = _FIXED_SIZES.get(value_base)
    if key_fixed is not None and value_fixed is not None:
        pair_size = key_fixed + value_fixed
        return lambda reader: reader.skip(reader.read_varuint() * pair_size)

    if key_base == "UUID" and value_fixed is not None:
        return lambda reader: reader.skip(reader.read_varuint() * (16 + value_fixed))
    if value_base == "UUID" and key_fixed is not None:
        return lambda reader: reader.skip(reader.read_varuint() * (key_fixed + 16))

    def _skip_map(reader: _Reader) -> None:
        count = reader.read_varuint()
        for _ in range(count):
            key_skip(reader)
            value_skip(reader)

    return _skip_map


def _tuple_skipper(ch_type: str) -> Callable[[_Reader], None]:
    inner = ch_type[6:-1]
    element_types = split_type_arguments(inner)
    skippers = tuple(_skipper_for_type(t) for t in element_types)

    def _skip_tuple(reader: _Reader) -> None:
        for skip in skippers:
            skip(reader)

    return _skip_tuple


_COMPLEX_SKIPPERS: dict[str, Callable[[str], Callable[[_Reader], None]]] = {
    "Array": lambda ch_type: _array_skipper(ch_type[6:-1]),
    "DateTime64": lambda _: (lambda reader: reader.skip(8)),
    "JSON": lambda _: (lambda reader: reader.skip(reader.read_varuint())),
    "Map": _map_skipper,
    "String": lambda _: (lambda reader: reader.skip(reader.read_varuint())),
    "Tuple": _tuple_skipper,
    "UUID": lambda _: (lambda reader: reader.skip(16)),
}


@lru_cache(maxsize=256)
def _skipper_for_type(ch_type: str) -> Callable[[_Reader], None]:
    if ch_type.startswith("LowCardinality("):
        return _skipper_for_type(ch_type[15:-1])

    if ch_type.startswith("Nullable("):
        inner = _skipper_for_type(ch_type[9:-1])

        def _skip_nullable(reader: _Reader) -> None:
            if reader.read_uint8():
                return
            inner(reader)

        return _skip_nullable

    ch_type = unwrap_wrappers(ch_type)
    base = extract_base_type(ch_type)

    size = _FIXED_SIZES.get(base)
    if size is not None:
        return lambda reader: reader.skip(size)

    if base == "FixedString":
        inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
        fixed_size = int(inner.strip())
        return lambda reader: reader.skip(fixed_size)

    if base.startswith("Decimal"):
        precision, _scale = _decimal_meta(ch_type)
        size = _decimal_size(precision)
        return lambda reader: reader.skip(size)

    handler = _COMPLEX_SKIPPERS.get(base)
    if handler is not None:
        return handler(ch_type)

    raise ValueError(f"Unsupported RowBinary type: {ch_type}")


class RowBinaryLazyValues(Sequence[Any]):
    __slots__ = ("_cache", "_data", "_offsets", "_readers")
    _MISSING = object()

    def __init__(self, data: memoryview, offsets: list[tuple[int, int]], readers: list[Callable[[_Reader], Any]]):
        self._data = data
        self._offsets = offsets
        self._readers = readers
        self._cache: list[Any] = [self._MISSING] * len(offsets)

    def __len__(self) -> int:
        return len(self._offsets)

    def __getitem__(self, idx: int) -> Any:
        if idx < 0:
            idx += len(self._offsets)
        cached = self._cache[idx]
        if cached is not self._MISSING:
            return cached

        start, end = self._offsets[idx]
        reader = _BinaryReader(self._data[start:end])
        value = self._readers[idx](reader)
        self._cache[idx] = value
        return value


def parse_rowbinary_with_names_and_types_lazy(
    data: bytes,
) -> tuple[list[str], list[str], Iterable[RowBinaryLazyValues]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return rows with lazy per-cell decoding.
    """
    reader = _BinaryReader(data)
    column_count = reader.read_varuint()
    names = [reader.read_string() for _ in range(column_count)]
    types = [reader.read_string() for _ in range(column_count)]
    skippers = [_skipper_for_type(tp) for tp in types]
    readers = [_reader_for_type(tp) for tp in types]
    payload = memoryview(data)

    def _rows() -> Iterable[RowBinaryLazyValues]:
        while not reader.eof:
            offsets: list[tuple[int, int]] = []
            for skip in skippers:
                start = reader.pos
                skip(reader)
                end = reader.pos
                offsets.append((start, end))
            yield RowBinaryLazyValues(payload, offsets, readers)

    return names, types, _rows()


class _NeedMoreData(Exception):
    pass


class _StreamingReader:
    __slots__ = ("_buf", "_pos")

    def __init__(self):
        self._buf = bytearray()
        self._pos = 0

    def feed(self, data: bytes) -> None:
        if data:
            self._buf += data

    @property
    def pos(self) -> int:
        return self._pos

    @pos.setter
    def pos(self, value: int) -> None:
        self._pos = value

    @property
    def remaining(self) -> int:
        return len(self._buf) - self._pos

    @property
    def eof(self) -> bool:
        return self._pos >= len(self._buf)

    def compact(self) -> None:
        if self._pos and self._pos > 1_048_576:
            del self._buf[: self._pos]
            self._pos = 0

    def copy_slice(self, start: int, end: int) -> bytes:
        if end > len(self._buf):
            raise _NeedMoreData
        return bytes(self._buf[start:end])

    def _read(self, size: int) -> memoryview:
        end = self._pos + size
        if end > len(self._buf):
            raise _NeedMoreData
        mv = memoryview(self._buf)[self._pos : end]
        self._pos = end
        return mv

    def read_bytes(self, size: int) -> bytes:
        return self._read(size).tobytes()

    def skip(self, size: int) -> None:
        end = self._pos + size
        if end > len(self._buf):
            raise _NeedMoreData
        self._pos = end

    def read_uint8(self) -> int:
        if self._pos >= len(self._buf):
            raise _NeedMoreData
        b = self._buf[self._pos]
        self._pos += 1
        return b

    def read_int8(self) -> int:
        if self._pos + 1 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<b", self._buf, self._pos)[0]
        self._pos += 1
        return value

    def read_uint16(self) -> int:
        if self._pos + 2 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<H", self._buf, self._pos)[0]
        self._pos += 2
        return value

    def read_int16(self) -> int:
        if self._pos + 2 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<h", self._buf, self._pos)[0]
        self._pos += 2
        return value

    def read_uint32(self) -> int:
        if self._pos + 4 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<I", self._buf, self._pos)[0]
        self._pos += 4
        return value

    def read_int32(self) -> int:
        if self._pos + 4 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<i", self._buf, self._pos)[0]
        self._pos += 4
        return value

    def read_uint64(self) -> int:
        if self._pos + 8 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<Q", self._buf, self._pos)[0]
        self._pos += 8
        return value

    def read_int64(self) -> int:
        if self._pos + 8 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<q", self._buf, self._pos)[0]
        self._pos += 8
        return value

    def read_float32(self) -> float:
        if self._pos + 4 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<f", self._buf, self._pos)[0]
        self._pos += 4
        return value

    def read_float64(self) -> float:
        if self._pos + 8 > len(self._buf):
            raise _NeedMoreData
        value = struct.unpack_from("<d", self._buf, self._pos)[0]
        self._pos += 8
        return value

    def read_varuint(self) -> int:
        p = self._pos
        shift = 0
        result = 0
        while True:
            if p >= len(self._buf):
                raise _NeedMoreData
            byte = self._buf[p]
            p += 1
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                break
            shift += 7
        self._pos = p
        return result

    def read_string(self) -> str:
        p = self._pos
        length = self.read_varuint()
        if self._pos + length > len(self._buf):
            self._pos = p
            raise _NeedMoreData
        s = bytes(self._buf[self._pos : self._pos + length]).decode("utf-8")
        self._pos += length
        return s


class RowBinaryWithNamesAndTypesStreamParser:
    def __init__(self, chunks: AsyncIterator[bytes], *, lazy: bool = False):
        self._chunks = chunks.__aiter__()
        self._reader = _StreamingReader()
        self._done = False
        self._names: list[str] | None = None
        self._types: list[str] | None = None
        self._readers: list[Callable[[_Reader], Any]] | None = None
        self._skippers: list[Callable[[_Reader], None]] | None = None
        self._lazy = lazy

    async def _fill(self) -> bool:
        try:
            chunk = await anext(self._chunks)
        except StopAsyncIteration:
            self._done = True
            return False
        self._reader.feed(chunk)
        return True

    async def read_header(self) -> tuple[list[str], list[str]]:
        if self._names is not None and self._types is not None:
            return self._names, self._types

        while True:
            checkpoint = self._reader.pos
            try:
                column_count = self._reader.read_varuint()
                names = [self._reader.read_string() for _ in range(column_count)]
                types = [self._reader.read_string() for _ in range(column_count)]
                self._names = names
                self._types = types
                self._readers = [_reader_for_type(tp) for tp in types]
                if self._lazy:
                    self._skippers = [_skipper_for_type(tp) for tp in types]
                else:
                    self._skippers = None
            except _NeedMoreData:
                self._reader.pos = checkpoint
                if not await self._fill():
                    raise ValueError("Unexpected end of data") from None
            else:
                return names, types

    async def rows(self) -> AsyncIterator[list[Any] | RowBinaryLazyValues]:
        await self.read_header()
        assert self._readers is not None

        while True:
            if self._done and self._reader.remaining == 0:
                return

            checkpoint = self._reader.pos
            try:
                if self._lazy:
                    assert self._skippers is not None
                    row_start = self._reader.pos
                    offsets: list[tuple[int, int]] = []
                    for skip in self._skippers:
                        cell_start = self._reader.pos
                        skip(self._reader)
                        cell_end = self._reader.pos
                        offsets.append((cell_start - row_start, cell_end - row_start))
                    row_end = self._reader.pos
                    row_bytes = self._reader.copy_slice(row_start, row_end)
                    yield RowBinaryLazyValues(memoryview(row_bytes), offsets, self._readers)
                else:
                    values = [read(self._reader) for read in self._readers]
                    yield values
                self._reader.compact()
            except _NeedMoreData:
                self._reader.pos = checkpoint
                if not await self._fill():
                    if self._reader.remaining == 0:
                        return
                    raise ValueError("Unexpected end of data") from None
