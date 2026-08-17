import ipaddress
import json
import re
import struct
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from types import CodeType
from typing import Any, Callable, Iterable, Literal, Protocol, overload
from uuid import UUID
from zoneinfo import ZoneInfo

from aiochlite.exceptions import ChProtocolError

from ._type_parsing import extract_base_type, extract_timezone, parse_timezone, split_type_arguments, unwrap_wrappers


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
    def read_struct(self, unpack_from: Callable[..., tuple[Any, ...]], size: int) -> tuple[Any, ...]: ...
    def skip(self, size: int): ...

    @property
    def pos(self) -> int: ...


class _ShortData(ValueError):
    """A read past the end of the payload, as opposed to a payload that will not decode."""


class _BinaryReader:
    def __init__(self, data: bytes):
        # Both views of the same payload: memoryview for struct reads, bytes for string slicing,
        # which is markedly cheaper than slicing a memoryview and converting it back.
        self._raw = data
        self._data = memoryview(data)
        self._pos = 0

    def _read(self, size: int) -> memoryview:
        end = self._pos + size
        if end > len(self._data):
            raise _ShortData("Unexpected end of data")
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    def read_uint8(self) -> int:
        if self._pos >= len(self._data):
            raise _ShortData("Unexpected end of data")
        value = self._data[self._pos]
        self._pos += 1
        return int(value)

    def read_int8(self) -> int:
        if self._pos + 1 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<b", self._data, self._pos)[0]
        self._pos += 1
        return value

    def read_uint16(self) -> int:
        if self._pos + 2 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<H", self._data, self._pos)[0]
        self._pos += 2
        return value

    def read_int16(self) -> int:
        if self._pos + 2 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<h", self._data, self._pos)[0]
        self._pos += 2
        return value

    def read_uint32(self) -> int:
        if self._pos + 4 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<I", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_int32(self) -> int:
        if self._pos + 4 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<i", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_uint64(self) -> int:
        if self._pos + 8 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<Q", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_int64(self) -> int:
        if self._pos + 8 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<q", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_int128(self) -> int:
        return int.from_bytes(self._read(16), "little", signed=True)

    def read_float32(self) -> float:
        if self._pos + 4 > len(self._data):
            raise _ShortData("Unexpected end of data")
        value = struct.unpack_from("<f", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_float64(self) -> float:
        if self._pos + 8 > len(self._data):
            raise _ShortData("Unexpected end of data")
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
        end = self._pos + length
        if end > len(self._raw):
            raise _ShortData("Unexpected end of data")

        value = self._raw[self._pos : end].decode("utf-8")
        self._pos = end
        return value

    def read_struct(self, unpack_from: Callable[..., tuple[Any, ...]], size: int) -> tuple[Any, ...]:
        """Decode a run of fixed-width fields with a single struct call."""
        end = self._pos + size
        if end > len(self._data):
            raise _ShortData("Unexpected end of data")

        values = unpack_from(self._data, self._pos)
        self._pos = end
        return values

    @property
    def eof(self) -> bool:
        return self._pos >= len(self._data)

    @property
    def pos(self) -> int:
        return self._pos

    @pos.setter
    def pos(self, value: int):
        self._pos = value

    def skip(self, size: int):
        end = self._pos + size
        if end > len(self._data):
            raise _ShortData("Unexpected end of data")
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

# Bound for converters shared across queries; per-query ones use `_value_cache` and need none.
_VALUE_CACHE_SIZE = 4096


class _ValueCache(dict[Any, Any]):
    """Memoizes a converter. A hit is a plain dict lookup, with no Python frame."""

    __slots__ = ("_convert",)

    def __init__(self, convert: Callable[[Any], Any]):
        super().__init__()
        self._convert = convert

    def __missing__(self, key: Any) -> Any:
        value = self[key] = self._convert(key)
        return value


def _value_cache(convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Memoize a converter for one query, unbounded.

    Past a bound every lookup misses and every insert evicts, which costs more than no cache.
    The result already holds every distinct value, so per query there is nothing to bound.
    """
    return _ValueCache(convert).__getitem__


def _server_timezone(ch_type: str, server_tz: str | None) -> ZoneInfo:
    """The wall clock of a column without its own timezone is only knowable from the server's."""
    tz = parse_timezone(server_tz)
    if tz is None:
        raise ChProtocolError(
            f"{ch_type} has no timezone of its own and the response carried no X-ClickHouse-Timezone header."
        )

    return tz


def _datetime_converter(ch_type: str, server_tz: str | None) -> Callable[[int], datetime]:
    """Unix timestamp -> datetime."""
    explicit_tz = extract_timezone(ch_type)
    # An explicit timezone yields an aware datetime; otherwise the wall-clock time is computed
    # in the server timezone and returned naive.
    tz = explicit_tz or _server_timezone(ch_type, server_tz)
    strip_tz = explicit_tz is None

    def _convert(ts: int) -> datetime:
        dt = datetime.fromtimestamp(ts, tz=tz)
        return dt.replace(tzinfo=None) if strip_tz else dt

    return _convert


def _datetime_reader(ch_type: str, server_tz: str | None) -> Callable[[_Reader], datetime]:
    # Reached through `_reader_for_type`, so this converter outlives the query that built it.
    _dt = lru_cache(maxsize=_VALUE_CACHE_SIZE)(_datetime_converter(ch_type, server_tz))

    def _read_dt(reader: _Reader) -> datetime:
        return _dt(reader.read_uint32())

    return _read_dt


def _time64_converter(ch_type: str) -> Callable[[int], timedelta]:
    """Raw ticks -> timedelta."""
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    scale = int(inner.strip())

    if scale <= 6:
        multiplier = 10 ** (6 - scale)

        def _convert(ticks: int) -> timedelta:
            return timedelta(microseconds=ticks * multiplier)
    else:
        divisor = 10 ** (scale - 6)

        def _convert(ticks: int) -> timedelta:
            # Truncated toward zero, as the server itself narrows: `//` would push a negative
            # value one microsecond further from it.
            return timedelta(microseconds=-(-ticks // divisor) if ticks < 0 else ticks // divisor)

    return _convert


def _time64_reader(ch_type: str) -> Callable[[_Reader], timedelta]:
    # Reached through `_reader_for_type`, so this converter outlives the query that built it.
    _td = lru_cache(maxsize=_VALUE_CACHE_SIZE)(_time64_converter(ch_type))

    def _read_time64(reader: _Reader) -> timedelta:
        return _td(reader.read_int64())

    return _read_time64


def _datetime64_converter(ch_type: str, server_tz: str | None) -> Callable[[int], datetime]:
    """Raw ticks -> datetime."""
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    parts = [p.strip() for p in inner.split(",")]
    scale = int(parts[0])
    explicit_tz = extract_timezone(ch_type)
    # An explicit timezone yields an aware datetime; otherwise the wall-clock time is computed
    # in the server timezone and returned naive.
    tz = explicit_tz or _server_timezone(ch_type, server_tz)
    strip_tz = explicit_tz is None

    if scale <= 6:
        multiplier = 10 ** (6 - scale)

        def _to_micros(ticks: int) -> int:
            return ticks * multiplier
    else:
        divisor = 10 ** (scale - 6)

        def _to_micros(ticks: int) -> int:
            # Truncated toward zero, as the server itself narrows.
            return -(-ticks // divisor) if ticks < 0 else ticks // divisor

    def _convert(ticks: int) -> datetime:
        base_seconds, micros = divmod(_to_micros(ticks), 1_000_000)
        dt = datetime.fromtimestamp(base_seconds, tz=tz)
        if micros:
            dt = dt + timedelta(microseconds=micros)
        return dt.replace(tzinfo=None) if strip_tz else dt

    return _convert


def _datetime64_reader(ch_type: str, server_tz: str | None) -> Callable[[_Reader], datetime]:
    # Reached through `_reader_for_type`, so this converter outlives the query that built it.
    _dt64 = lru_cache(maxsize=_VALUE_CACHE_SIZE)(_datetime64_converter(ch_type, server_tz))

    def _read_dt64(reader: _Reader) -> datetime:
        return _dt64(reader.read_int64())

    return _read_dt64


def _decimal_converter(ch_type: str) -> Callable[[int], Decimal]:
    """Raw signed integer -> scaled Decimal."""
    _, scale = _decimal_meta(ch_type)

    def _convert(raw: int) -> Decimal:
        return Decimal(raw).scaleb(-scale)

    return _convert


def _decimal_reader(ch_type: str) -> Callable[[_Reader], Decimal]:
    precision, _ = _decimal_meta(ch_type)
    size = _decimal_size(precision)
    # Reached through `_reader_for_type`, so this converter outlives the query that built it.
    _dec = lru_cache(maxsize=_VALUE_CACHE_SIZE)(_decimal_converter(ch_type))

    def _read_dec(reader: _Reader) -> Decimal:
        return _dec(int.from_bytes(reader._read(size), "little", signed=True))

    return _read_dec


def _fixedstring_size(ch_type: str) -> int:
    return int(ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")].strip())


def _fixedstring_from_bytes(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace").rstrip("\x00")


def _fixedstring_reader(ch_type: str) -> Callable[[_Reader], str]:
    size = _fixedstring_size(ch_type)

    def _read_fixedstring(reader: _Reader) -> str:
        return _fixedstring_from_bytes(reader._read(size).tobytes())

    return _read_fixedstring


@lru_cache(maxsize=512)
def _enum_mapping(ch_type: str) -> dict[int, str]:
    """Parse an Enum8/Enum16 definition into {value: name}: Enum8('a' = 1) -> {1: "a"}."""
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    pairs = re.findall(r"'((?:\\.|[^'])*)'\s*=\s*([+-]?\d+)", inner)
    if not pairs:
        raise ValueError(f"Invalid Enum definition: {ch_type}")

    mapping: dict[int, str] = {}
    for raw_name, raw_value in pairs:
        name = raw_name.replace("\\\\", "\\").replace("\\'", "'")
        mapping[int(raw_value)] = name

    return mapping


def _enum_converter(ch_type: str) -> Callable[[int], str]:
    """Raw value -> enum name, or its decimal string when the value is not in the definition."""
    mapping = _enum_mapping(ch_type)

    def _convert(value: int) -> str:
        return mapping.get(value, str(value))

    return _convert


def _enum_reader(ch_type: str) -> Callable[[_Reader], str]:
    base = extract_base_type(ch_type)
    convert = _enum_converter(ch_type)

    if base == "Enum8":

        def _read_enum(reader: _Reader) -> str:
            return convert(reader.read_int8())

        return _read_enum

    if base == "Enum16":

        def _read_enum(reader: _Reader) -> str:
            return convert(reader.read_int16())

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


def _array_reader(ch_type: str, server_tz: str | None) -> Callable[[_Reader], list[Any]]:
    inner_type = ch_type[6:-1]
    inner = _reader_for_type(inner_type, server_tz)

    def _read_array(reader: _Reader) -> list[Any]:
        return [inner(reader) for _ in range(reader.read_varuint())]

    return _read_array


def _map_reader(ch_type: str, server_tz: str | None) -> Callable[[_Reader], dict[Any, Any]]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    key_type, value_type = split_type_arguments(inner)
    key_reader = _reader_for_type(key_type, server_tz)
    value_reader = _reader_for_type(value_type, server_tz)

    def _read_map(reader: _Reader) -> dict[Any, Any]:
        count = reader.read_varuint()
        out: dict[Any, Any] = {}
        for _ in range(count):
            key = key_reader(reader)
            value = value_reader(reader)
            out[key] = value
        return out

    return _read_map


def _tuple_reader(ch_type: str, server_tz: str | None) -> Callable[[_Reader], tuple[Any, ...]]:
    inner = ch_type[6:-1]
    element_types = split_type_arguments(inner)
    readers = tuple(_reader_for_type(t, server_tz) for t in element_types)

    # Unrolling the common sizes avoids a throwaway generator per row. A tuple display evaluates
    # left to right, as a sequential reader requires.
    if len(readers) == 2:
        first, second = readers

        def _read_tuple2(reader: _Reader) -> tuple[Any, ...]:
            return (first(reader), second(reader))

        return _read_tuple2

    if len(readers) == 3:
        first, second, third = readers

        def _read_tuple3(reader: _Reader) -> tuple[Any, ...]:
            return (first(reader), second(reader), third(reader))

        return _read_tuple3

    def _read_tuple(reader: _Reader) -> tuple[Any, ...]:
        return tuple([r(reader) for r in readers])

    return _read_tuple


def _uuid_from_bytes(raw: bytes) -> UUID:
    """ClickHouse writes a UUID as two UInt64 (hi, lo), each little-endian."""
    return UUID(bytes=raw[:8][::-1] + raw[8:][::-1])


def _uuid_reader(reader: _Reader) -> UUID:
    return _uuid_from_bytes(reader._read(16).tobytes())


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
    "Date": lambda _: lambda r: _EPOCH_DATE + timedelta(days=r.read_uint16()),
    "Date32": lambda _: lambda r: _EPOCH_DATE + timedelta(days=r.read_int32()),
    "Time": lambda _: lambda r: timedelta(seconds=r.read_int32()),
    "Time64": _time64_reader,
    "Enum16": _enum_reader,
    "Enum8": _enum_reader,
    "FixedString": _fixedstring_reader,
    "IPv4": _ipv4_reader,
    "IPv6": _ipv6_reader,
    # ClickHouse UUID RowBinary is encoded as two UInt64 (hi, lo), each in little-endian.
    "UUID": lambda _: _uuid_reader,
}

_TZ_AWARE_READERS: dict[str, Callable[[str, str | None], Callable[[_Reader], Any]]] = {
    "Array": _array_reader,
    "DateTime": _datetime_reader,
    "DateTime64": _datetime64_reader,
    "Map": _map_reader,
    "Tuple": _tuple_reader,
}


@lru_cache(maxsize=256)
def _reader_for_type(ch_type: str, server_tz: str | None = None) -> Callable[[_Reader], Any]:
    if ch_type.startswith("LowCardinality("):
        return _reader_for_type(ch_type[15:-1], server_tz)

    if ch_type.startswith("Nullable("):
        inner = _reader_for_type(ch_type[9:-1], server_tz)

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

    tz_handler = _TZ_AWARE_READERS.get(base)
    if tz_handler is not None:
        return tz_handler(ch_type, server_tz)

    handler = _COMPLEX_READERS.get(base)
    if handler is not None:
        return handler(ch_type)

    raise ValueError(f"Unsupported RowBinary type: {ch_type}")


_STRUCT_CODES: dict[str, str] = {
    "Bool": "?",
    "Float32": "f",
    "Float64": "d",
    "Int8": "b",
    "Int16": "h",
    "Int32": "i",
    "Int64": "q",
    "UInt8": "B",
    "UInt16": "H",
    "UInt32": "I",
    "UInt64": "Q",
}

# 16- and 32-byte decimals have no integer code, so they travel as raw bytes and are widened
# by their converter.
_DECIMAL_STRUCT_CODES: dict[int, str] = {4: "i", 8: "q", 16: "16s", 32: "32s"}

# Shorter runs keep the plain per-field reader. A fully fixed-width row needs no segments, so
# two fields already pay off; a mixed row costs one extra call per variable-width column and
# needs three.
_MIN_FUSED_FIELDS = 2
_MIN_SEGMENTED_FUSED_FIELDS = 3


def _days_to_date(days: int) -> date:
    return _EPOCH_DATE + timedelta(days=days)


def _seconds_to_timedelta(seconds: int) -> timedelta:
    return timedelta(seconds=seconds)


# Fixed-width types whose raw scalar still needs converting. Each entry returns the struct
# format code plus the converter.
_FIXED_CONVERTERS: dict[str, Callable[[str, str | None], tuple[str, Callable[[Any], Any]]]] = {
    "Date": lambda _ch_type, _tz: ("H", _days_to_date),
    "Date32": lambda _ch_type, _tz: ("i", _days_to_date),
    "DateTime": lambda ch_type, tz: ("I", _value_cache(_datetime_converter(ch_type, tz))),
    "DateTime64": lambda ch_type, tz: ("q", _value_cache(_datetime64_converter(ch_type, tz))),
    "Enum8": lambda ch_type, _tz: ("b", _enum_converter(ch_type)),
    "Enum16": lambda ch_type, _tz: ("h", _enum_converter(ch_type)),
    "FixedString": lambda ch_type, _tz: (f"{_fixedstring_size(ch_type)}s", _fixedstring_from_bytes),
    "IPv4": lambda _ch_type, _tz: ("I", ipaddress.IPv4Address),
    "IPv6": lambda _ch_type, _tz: ("16s", ipaddress.IPv6Address),
    "Time": lambda _ch_type, _tz: ("i", _seconds_to_timedelta),
    "Time64": lambda ch_type, _tz: ("q", _value_cache(_time64_converter(ch_type))),
    "UUID": lambda _ch_type, _tz: ("16s", _uuid_from_bytes),
}


def _wide_decimal_converter(ch_type: str) -> Callable[[bytes], Decimal]:
    """Raw little-endian bytes -> scaled Decimal, for precisions past 64 bits."""
    scale = _decimal_converter(ch_type)

    def _convert(raw: bytes) -> Decimal:
        return scale(int.from_bytes(raw, "little", signed=True))

    return _convert


def _fixed_field(ch_type: str, server_tz: str | None) -> tuple[str, Callable[[Any], Any] | None] | None:
    """Struct format code and optional converter, or None if the column is not fixed-width."""
    # A Nullable value is prefixed with a null flag, so its width varies from row to row.
    if "Nullable(" in ch_type:
        return None

    unwrapped = unwrap_wrappers(ch_type)
    base = extract_base_type(unwrapped)

    code = _STRUCT_CODES.get(base)
    if code is not None:
        return code, None

    build = _FIXED_CONVERTERS.get(base)
    if build is not None:
        return build(unwrapped, server_tz)

    if base.startswith("Decimal"):
        precision, _ = _decimal_meta(unwrapped)
        size = _decimal_size(precision)
        decimal_code = _DECIMAL_STRUCT_CODES.get(size)
        if decimal_code is None:
            return None

        widen = _wide_decimal_converter if decimal_code.endswith("s") else _decimal_converter
        return decimal_code, _value_cache(widen(unwrapped))

    return None


_ConvSlots = list[tuple[int, Callable[[Any], Any]]]


def _fixed_row_layout(types: list[str], server_tz: str | None) -> tuple[struct.Struct, _ConvSlots] | None:
    """One struct covering the whole row, or None if any column varies in width."""
    # An empty format would make a zero-width row, which `iter_unpack` rejects.
    if not types:
        return None

    codes: list[str] = []
    conv_slots: _ConvSlots = []
    for idx, ch_type in enumerate(types):
        field = _fixed_field(ch_type, server_tz)
        if field is None:
            return None

        code, convert = field
        codes.append(code)
        if convert is not None:
            conv_slots.append((idx, convert))

    return struct.Struct(f"<{''.join(codes)}"), conv_slots


def _bulk_decode(
    data: bytes,
    start: int,
    unpacker: struct.Struct,
    conv_slots: _ConvSlots,
    *,
    as_tuple: bool,
) -> list[Any]:
    """Decode every row of a fixed-width payload with one `iter_unpack` over the body."""
    try:
        # The length check is upfront, so a truncated body raises before any row is built.
        unpacked = unpacker.iter_unpack(memoryview(data)[start:])
    except struct.error as error:
        # A body that is not a whole number of rows is a bad response, not a bad call.
        raise ValueError("Unexpected end of data") from error

    if not conv_slots:
        return list(unpacked) if as_tuple else [list(values) for values in unpacked]

    rows: list[Any] = []
    append = rows.append
    # Two loops rather than a per-row branch on `as_tuple`.
    if as_tuple:
        for values in unpacked:
            row = list(values)
            for idx, convert in conv_slots:
                row[idx] = convert(row[idx])
            append(tuple(row))
    else:
        for values in unpacked:
            row = list(values)
            for idx, convert in conv_slots:
                row[idx] = convert(row[idx])
            append(row)

    return rows


_BatchDecoder = Callable[[Any, int, int], tuple[list[Any], int]]


def _bulk_batch(unpacker: struct.Struct, conv_slots: _ConvSlots) -> _BatchDecoder:
    """`_bulk_decode` as a batch over whole rows, for the streaming loop."""
    width = unpacker.size

    def batch(data: Any, pos: int, end: int) -> tuple[list[Any], int]:
        count = (end - pos) // width
        if not count:
            return [], pos

        stop = pos + count * width
        # A memoryview would block the resize that feeding the next chunk does.
        return _bulk_decode(bytes(data[pos:stop]), 0, unpacker, conv_slots, as_tuple=False), stop

    return batch


def _read_varint(data: bytes, pos: int, end: int) -> tuple[int, int]:
    """LEB128 from `pos`, or `(-1, pos)` if it runs past `end`. Called by generated code."""
    result = 0
    shift = 0
    while True:
        if pos >= end:
            return -1, pos

        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, pos

        shift += 7


# An array's length is only known per row, so a Struct is built per element count. A column of
# many distinct lengths would otherwise keep one for each.
_ARRAY_UNPACKER_CACHE = 256


class _ArrayUnpackers(dict[int, Callable[..., tuple[Any, ...]]]):
    """`unpack_from` for a run of `count` elements of one type."""

    __slots__ = ("_code",)

    def __init__(self, code: str):
        super().__init__()
        self._code = code

    def __missing__(self, count: int) -> Callable[..., tuple[Any, ...]]:
        if len(self) >= _ARRAY_UNPACKER_CACHE:
            self.clear()

        # Repeated rather than prefixed with the count: `3` before `16s` would read one 316-byte
        # string instead of three 16-byte ones.
        value = self[count] = struct.Struct(f"<{self._code * count}").unpack_from
        return value


def _read_string_array(data: bytes, pos: int, count: int, end: int) -> tuple[list[str] | None, int]:
    """`count` strings from `pos`, or `(None, pos)` if they run past `end`. Called by generated
    code: a `break` from an inner loop would leave the row loop running."""
    values: list[str] = []
    append = values.append
    for _ in range(count):
        length, pos = _read_varint(data, pos, end)
        stop = pos + length
        if length < 0 or stop > end:
            return None, pos

        append(data[pos:stop].decode())
        pos = stop

    return values, pos


def _strip_low_cardinality(ch_type: str) -> str:
    unwrapped = ch_type.strip()
    while unwrapped.startswith("LowCardinality(") and unwrapped.endswith(")"):
        unwrapped = unwrapped[15:-1].strip()

    return unwrapped


def _codegen_value_type(ch_type: str) -> str:
    """The type whose converter the column needs, with the wrappers around it taken off."""
    unwrapped = _strip_low_cardinality(ch_type)
    if unwrapped.startswith("Array(") and unwrapped.endswith(")"):
        unwrapped = _strip_low_cardinality(unwrapped[6:-1])
    if unwrapped.startswith("Nullable(") and unwrapped.endswith(")"):
        return _strip_low_cardinality(unwrapped[9:-1])

    return unwrapped


def _codegen_nullable_kind(inner_type: str, server_tz: str | None) -> tuple[str, Any]:
    inner = _codegen_kind(inner_type, server_tz)
    # Only what `_emit_nullable` can nest. ClickHouse rejects `Nullable(Array(...))` anyway, but
    # an unhandled kind here would silently emit a decoder for the wrong shape.
    return ("nullable", inner) if inner is not None and inner[0] in {"fixed", "string"} else ("reader", None)


def _codegen_array_kind(element_type: str, server_tz: str | None) -> tuple[str, Any]:
    element = _strip_low_cardinality(element_type)
    if element == "String":
        return "array_string", None

    # Only a flat array of a fixed-width type: anything deeper would need the generator to nest,
    # and the reader path handles it well enough.
    field = _fixed_field(element, server_tz)
    return ("array_fixed", field[0]) if field is not None else ("reader", None)


def _codegen_tuple_kind(element_types: str, server_tz: str | None) -> tuple[str, Any]:
    elements: list[tuple[str, str, Any]] = []
    for element in split_type_arguments(element_types):
        kind = _codegen_kind(element, server_tz)
        # Only flat elements, for the same reason as arrays.
        if kind is None or kind[0] not in {"fixed", "string"}:
            return "reader", None

        elements.append((element, kind[0], kind[1]))

    return ("tuple", elements) if elements else ("reader", None)


_CODEGEN_CONTAINERS: dict[str, Callable[[str, str | None], tuple[str, Any]]] = {
    "Nullable(": _codegen_nullable_kind,
    "Array(": _codegen_array_kind,
    "Tuple(": _codegen_tuple_kind,
}


def _codegen_kind(ch_type: str, server_tz: str | None) -> tuple[str, Any] | None:
    """How the generator emits this column: `("fixed", code)`, `("string", None)`, `("nullable",
    inner)`, `("array_fixed", code)`, `("array_string", None)`, `("tuple", elements)`, or
    `("reader", None)` for a column read through its own closure."""
    unwrapped = _strip_low_cardinality(ch_type)

    for prefix, build in _CODEGEN_CONTAINERS.items():
        if unwrapped.startswith(prefix) and unwrapped.endswith(")"):
            return build(unwrapped[len(prefix) : -1], server_tz)

    field = _fixed_field(unwrapped, server_tz)
    if field is not None:
        return "fixed", field[0]

    if unwrapped == "String":
        return "string", None

    # Everything else is read by its closure, in place, so one uncovered column no longer costs
    # the row the compiled path. `JSON` stays here on purpose: `json.loads` is over 90% of its
    # decode, so compiling the walk around it measured no faster.
    return "reader", None


def _emit_fixed_run(slots: list[str], codes: str, converted: set[str], indent: str) -> list[str]:
    unpacker = struct.Struct(f"<{codes}")
    targets = ", ".join(f"v{slot}" for slot in slots)
    lines = [
        f"{indent}_e = p + {unpacker.size}",
        f"{indent}if _e > end: break",
        f"{indent}{targets}{',' if len(slots) == 1 else ''} = _s{slots[0]}(data, p)",
        f"{indent}p = _e",
    ]
    lines += [f"{indent}v{slot} = _c{slot}(v{slot})" for slot in slots if slot in converted]
    return lines


def _emit_varuint(indent: str) -> list[str]:
    """Read a length or element count into `_l`."""
    return [
        f"{indent}if p >= end: break",
        f"{indent}_l = data[p]",
        f"{indent}p += 1",
        # One byte covers any value below 128, which is nearly every length and count.
        f"{indent}if _l > 0x7F:",
        f"{indent}    _l, p = _varint(data, p - 1, end)",
        f"{indent}    if _l < 0: break",
    ]


def _emit_string(slot: str, indent: str) -> list[str]:
    return [
        *_emit_varuint(indent),
        f"{indent}_e = p + _l",
        f"{indent}if _e > end: break",
        f"{indent}v{slot} = data[p:_e].decode()",
        f"{indent}p = _e",
    ]


def _emit_array_fixed(slot: str, code: str, converted: set[str], indent: str) -> list[str]:
    size = struct.calcsize(f"<{code}")
    elements = f"_u{slot}[_l](data, p)"
    return [
        *_emit_varuint(indent),
        f"{indent}_e = p + _l * {size}",
        f"{indent}if _e > end: break",
        # One struct call for the whole array, where the reader path costs one per element.
        f"{indent}v{slot} = " + (f"[_c{slot}(_x) for _x in {elements}]" if slot in converted else f"list({elements})"),
        f"{indent}p = _e",
    ]


def _emit_array_string(slot: str, indent: str) -> list[str]:
    return [
        *_emit_varuint(indent),
        f"{indent}v{slot}, p = _strings(data, p, _l, end)",
        f"{indent}if v{slot} is None: break",
    ]


def _emit_reader(slot: str, indent: str) -> list[str]:
    """Read one column through its closure, handing it the cursor and taking it back.

    `_pos` rather than the property: the generated code is part of this module, and the property
    would cost two calls a row.
    """
    return [
        f"{indent}_r._pos = p",
        f"{indent}try:",
        f"{indent}    v{slot} = _f{slot}(_r)",
        f"{indent}except _Short: break",
        f"{indent}p = _r._pos",
    ]


def _emit_nullable(slot: str, inner: tuple[str, Any], converted: set[str], indent: str) -> list[str]:
    # A `break` inside the else still leaves the row loop, so a short value is reported as one.
    lines = [
        f"{indent}if p >= end: break",
        f"{indent}_n = data[p]",
        f"{indent}p += 1",
        f"{indent}if _n:",
        f"{indent}    v{slot} = None",
        f"{indent}else:",
    ]
    nested = f"{indent}    "
    if inner[0] == "string":
        return lines + _emit_string(slot, nested)

    assert inner[0] == "fixed", inner[0]
    return lines + _emit_fixed_run([slot], inner[1], converted, nested)


class _Emitter:
    """Builds the body of one compiled decoder, collecting what its globals need along the way."""

    __slots__ = ("_server_tz", "converted", "converters", "namespace")

    def __init__(self, server_tz: str | None):
        self._server_tz = server_tz
        self.namespace: dict[str, Any] = {
            "_varint": _read_varint,
            "_strings": _read_string_array,
            "_Reader": _BinaryReader,
            "_Short": _ShortData,
        }
        # Slot -> the type whose converter it needs. Kept as types rather than converters: those
        # memoize per query, and the compiled decoder is cached for the life of the process.
        self.converters: dict[str, str] = {}
        self.converted: set[str] = set()

    def _register_converter(self, slot: str, ch_type: str):
        field = _fixed_field(_codegen_value_type(ch_type), self._server_tz)
        if field is not None and field[1] is not None:
            self.converters[slot] = ch_type
            self.converted.add(slot)

    def emit(self, slots: list[str], types: Sequence[str], kinds: list[tuple[str, Any]], indent: str) -> list[str]:
        """Lines decoding `types` back to back, one slot each."""
        body: list[str] = []
        index = 0
        while index < len(kinds):
            lines, index = self._emit_one(index, slots, types, kinds, indent)
            body += lines

        return body

    def _emit_one(
        self,
        index: int,
        slots: list[str],
        types: Sequence[str],
        kinds: list[tuple[str, Any]],
        indent: str,
    ) -> tuple[list[str], int]:
        if kinds[index][0] != "fixed":
            self._register_converter(slots[index], types[index])
            return self._emit_scalar(slots[index], kinds[index], types[index], indent), index + 1

        # Consecutive fixed-width columns share one struct call, as in the bulk path.
        run: list[str] = []
        codes = ""
        while index < len(kinds) and kinds[index][0] == "fixed":
            self._register_converter(slots[index], types[index])
            run.append(slots[index])
            codes += kinds[index][1]
            index += 1

        self.namespace[f"_s{run[0]}"] = struct.Struct(f"<{codes}").unpack_from
        return _emit_fixed_run(run, codes, self.converted, indent), index

    def _emit_scalar(self, slot: str, kind: tuple[str, Any], ch_type: str, indent: str) -> list[str]:
        if kind[0] == "string":
            return _emit_string(slot, indent)

        if kind[0] == "reader":
            self.namespace[f"_f{slot}"] = _reader_for_type(ch_type, self._server_tz)
            return _emit_reader(slot, indent)

        if kind[0] == "array_string":
            return _emit_array_string(slot, indent)

        if kind[0] == "array_fixed":
            self.namespace[f"_u{slot}"] = _ArrayUnpackers(kind[1])
            return _emit_array_fixed(slot, kind[1], self.converted, indent)

        if kind[0] == "nullable":
            # The null flag makes the width vary, so the column cannot join a run.
            if kind[1][0] == "fixed":
                self.namespace[f"_s{slot}"] = struct.Struct(f"<{kind[1][1]}").unpack_from
            return _emit_nullable(slot, kind[1], self.converted, indent)

        assert kind[0] == "tuple", kind[0]
        return self._emit_tuple(slot, kind[1], indent)

    def _emit_tuple(self, slot: str, elements: list[tuple[str, str, Any]], indent: str) -> list[str]:
        """A tuple carries no count, so its elements are just more columns sharing the row."""
        slots = [f"{slot}_{index}" for index in range(len(elements))]
        types = [element[0] for element in elements]
        kinds = [(element[1], element[2]) for element in elements]
        body = self.emit(slots, types, kinds, indent)
        body.append(f"{indent}v{slot} = ({', '.join(f'v{name}' for name in slots)},)")
        return body


@lru_cache(maxsize=256)
def _compiled_row_decoder(
    types: tuple[str, ...],
    server_tz: str | None,
    *,
    as_tuple: bool,
) -> tuple[CodeType, dict[str, Any], tuple[tuple[str, str], ...]] | None:
    """Code and schema-derived globals for a decoder over `types`, or None if unsupported.

    The converters are left out: they memoize per query, so caching them here would keep every
    decoded value for the life of the process.
    """
    kinds = [_codegen_kind(ch_type, server_tz) for ch_type in types]
    if not kinds or any(kind is None for kind in kinds):
        return None

    # With nothing to emit inline the compiled loop would only wrap the reader calls it already
    # makes, so the plain reader path stays.
    if all(kind is not None and kind[0] == "reader" for kind in kinds):
        return None

    emitter = _Emitter(server_tz)
    slots = [str(column) for column in range(len(types))]
    body = emitter.emit(slots, types, [kind for kind in kinds if kind is not None], "        ")

    values = ", ".join(f"v{slot}" for slot in slots)
    # Held for the whole call, so its view of the payload is released before the caller can feed
    # the buffer again and resize it.
    reader = ["    _r = _Reader(data)"] if any(name.startswith("_f") for name in emitter.namespace) else []
    source = "\n".join(
        [
            "def _decode(data, pos, end):",
            "    rows = []",
            "    append = rows.append",
            *reader,
            "    while pos < end:",
            # `p` advances through the row and is committed only once the row is whole, so a
            # partial trailing row leaves `pos` on a row boundary.
            "        p = pos",
            *body,
            f"        append({f'({values},)' if as_tuple else f'[{values}]'})",
            "        pos = p",
            "    return rows, pos",
        ]
    )

    code = compile(source, f"<rowbinary:{','.join(types)}>", "exec")
    return code, emitter.namespace, tuple(emitter.converters.items())


def _row_decoder(
    types: list[str],
    server_tz: str | None,
    *,
    as_tuple: bool,
) -> Callable[[bytes, int, int], tuple[list[Any], int]] | None:
    """Decoder specialized to this schema, or None if every column needs the reader path."""
    compiled = _compiled_row_decoder(tuple(types), server_tz, as_tuple=as_tuple)
    if compiled is None:
        return None

    code, schema_globals, converters = compiled
    namespace = dict(schema_globals)
    for slot, ch_type in converters:
        field = _fixed_field(_codegen_value_type(ch_type), server_tz)
        assert field is not None
        namespace[f"_c{slot}"] = field[1]

    exec(code, namespace)  # noqa: S102
    return namespace["_decode"]


def _batch_decoder(types: list[str], server_tz: str | None) -> _BatchDecoder | None:
    """Decoder that takes as many whole rows as a buffer holds, or None if the schema needs the
    reader path."""
    layout = _fixed_row_layout(types, server_tz)
    if layout is not None:
        return _bulk_batch(*layout)

    return _row_decoder(types, server_tz, as_tuple=False)


def _struct_row_reader(codes: list[str], conv_slots: _ConvSlots) -> Callable[[_Reader], list[Any]]:
    """Reader for a row that is fixed-width end to end: one struct call per row."""
    unpacker = struct.Struct(f"<{''.join(codes)}")
    unpack_from = unpacker.unpack_from
    size = unpacker.size

    def _read_row(reader: _Reader) -> list[Any]:
        values = list(reader.read_struct(unpack_from, size))
        for idx, convert in conv_slots:
            values[idx] = convert(values[idx])
        return values

    return _read_row


def _struct_segment(codes: list[str], conv_slots: _ConvSlots) -> Callable[[_Reader, list[Any]], None]:
    """Segment appending one fused run of fixed-width fields."""
    unpacker = struct.Struct(f"<{''.join(codes)}")
    unpack_from = unpacker.unpack_from
    size = unpacker.size

    if not conv_slots:

        def _read_run(reader: _Reader, values: list[Any]) -> None:
            values += reader.read_struct(unpack_from, size)

        return _read_run

    def _read_converted_run(reader: _Reader, values: list[Any]) -> None:
        chunk = list(reader.read_struct(unpack_from, size))
        for idx, convert in conv_slots:
            chunk[idx] = convert(chunk[idx])
        values += chunk

    return _read_converted_run


def _field_segment(read: Callable[[_Reader], Any]) -> Callable[[_Reader, list[Any]], None]:
    """Segment appending one column read the ordinary way."""

    def _read_field(reader: _Reader, values: list[Any]) -> None:
        values.append(read(reader))

    return _read_field


def _fixed_runs(fields: list[tuple[str, Callable[[Any], Any] | None] | None]) -> list[list[int]]:
    """Column indexes grouped into runs of consecutive fixed-width columns."""
    runs: list[list[int]] = []
    run: list[int] = []
    for idx, field in enumerate(fields):
        if field is None:
            if run:
                runs.append(run)
                run = []
            continue
        run.append(idx)

    if run:
        runs.append(run)

    return runs


def _make_row_reader(types: list[str], server_tz: str | None) -> Callable[[_Reader], list[Any]] | None:
    """Row decoder fusing each run of fixed-width columns into one struct call.

    Returns None when no run is long enough to be worth fusing; the caller then keeps the plain
    per-field path.
    """
    fields = [_fixed_field(tp, server_tz) for tp in types]
    runs = _fixed_runs(fields)

    def _run_layout(columns: list[int]) -> tuple[list[str], _ConvSlots]:
        codes: list[str] = []
        conv_slots: _ConvSlots = []
        for offset, column in enumerate(columns):
            field = fields[column]
            assert field is not None
            code, convert = field
            codes.append(code)
            if convert is not None:
                conv_slots.append((offset, convert))
        return codes, conv_slots

    if len(runs) == 1 and len(runs[0]) == len(types) >= _MIN_FUSED_FIELDS:
        return _struct_row_reader(*_run_layout(runs[0]))

    run_by_start = {run[0]: run for run in runs if len(run) >= _MIN_SEGMENTED_FUSED_FIELDS}
    if not run_by_start:
        return None

    segments: list[Callable[[_Reader, list[Any]], None]] = []
    idx = 0
    while idx < len(types):
        columns = run_by_start.get(idx)
        if columns is not None:
            segments.append(_struct_segment(*_run_layout(columns)))
            idx += len(columns)
            continue
        segments.append(_field_segment(_reader_for_type(types[idx], server_tz)))
        idx += 1

    def _read_row(reader: _Reader) -> list[Any]:
        values: list[Any] = []
        for segment in segments:
            segment(reader, values)
        return values

    return _read_row


@overload
def parse_rowbinary_with_names_and_types(
    data: bytes, server_tz: str | None = ..., *, as_tuple: Literal[False] = ...
) -> tuple[list[str], list[str], Iterable[list[Any]]]: ...


@overload
def parse_rowbinary_with_names_and_types(
    data: bytes, server_tz: str | None = ..., *, as_tuple: Literal[True]
) -> tuple[list[str], list[str], Iterable[tuple[Any, ...]]]: ...


def parse_rowbinary_with_names_and_types(
    data: bytes,
    server_tz: str | None = None,
    *,
    as_tuple: bool = False,
) -> tuple[list[str], list[str], Iterable[Any]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return header and row iterator.

    Args:
        data (bytes): RowBinaryWithNamesAndTypes payload.
        server_tz (str | None): Fallback timezone name for ``DateTime``/``DateTime64`` columns
            that carry no explicit timezone (the ClickHouse server timezone).
        as_tuple (bool): Yield tuples instead of lists, avoiding a second pass over every row.

    Returns:
        names: list of column names
        types: list of ClickHouse types
        rows: iterable of rows (list or tuple of values)
    """
    reader = _BinaryReader(data)
    column_count = reader.read_varuint()
    names = [reader.read_string() for _ in range(column_count)]
    types = [reader.read_string() for _ in range(column_count)]

    # A fixed-width row makes the body one repeating struct: one pass, and a list not a generator.
    layout = _fixed_row_layout(types, server_tz)
    if layout is not None:
        return names, types, _bulk_decode(data, reader.pos, *layout, as_tuple=as_tuple)

    decode = _row_decoder(types, server_tz, as_tuple=as_tuple)
    if decode is not None:
        rows, pos = decode(data, reader.pos, len(data))
        # The decoder stops on a row boundary, so anything left over is half a row.
        if pos != len(data):
            raise ValueError("Unexpected end of data")
        return names, types, rows

    return names, types, _reader_rows(reader, types, server_tz, as_tuple)


def _reader_rows(
    reader: _BinaryReader,
    types: list[str],
    server_tz: str | None,
    as_tuple: bool,
) -> Iterable[Any]:
    """Lazy rows for a payload with at least one variable-width column."""
    read_row = _make_row_reader(types, server_tz)

    # Spelled out per path instead of sharing one generator: in this loop an extra call per row
    # costs more than the duplication saves.
    if read_row is None:
        readers = [_reader_for_type(tp, server_tz) for tp in types]

        def _rows() -> Iterable[list[Any]]:
            while not reader.eof:
                yield [read(reader) for read in readers]

        def _tuple_rows() -> Iterable[tuple[Any, ...]]:
            while not reader.eof:
                yield tuple([read(reader) for read in readers])
    else:

        def _rows() -> Iterable[list[Any]]:
            while not reader.eof:
                yield read_row(reader)

        def _tuple_rows() -> Iterable[tuple[Any, ...]]:
            while not reader.eof:
                yield tuple(read_row(reader))

    return _tuple_rows() if as_tuple else _rows()


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
    "Time": 4,
    "Enum8": 1,
    "Enum16": 2,
    "IPv4": 4,
    "IPv6": 16,
}


def _array_skipper(inner_type: str) -> Callable[[_Reader], None]:
    inner_type = inner_type.strip()

    if inner_type.startswith("LowCardinality(") and inner_type.endswith(")"):
        return _array_skipper(inner_type[15:-1])

    inner_skip = _skipper_for_type(inner_type)

    # Nullable(T) elements vary in size (null flag + optional value), so they must be scanned
    # one by one instead of multiplying by a fixed width.
    if inner_type.startswith("Nullable(") and inner_type.endswith(")"):

        def _skip_array_nullable(reader: _Reader):
            count = reader.read_varuint()
            for _ in range(count):
                inner_skip(reader)

        return _skip_array_nullable

    fixed_skip = _fixed_width_array_skipper(inner_type)
    if fixed_skip is not None:
        return fixed_skip

    def _skip_array(reader: _Reader):
        count = reader.read_varuint()
        for _ in range(count):
            inner_skip(reader)

    return _skip_array


def _fixed_width_array_skipper(inner_type: str) -> Callable[[_Reader], None] | None:
    inner_base = extract_base_type(inner_type)

    inner_fixed = _FIXED_SIZES.get(inner_base)
    if inner_fixed is not None:
        return lambda reader: reader.skip(reader.read_varuint() * inner_fixed)

    if inner_base in {"DateTime64", "Time64"}:
        return lambda reader: reader.skip(reader.read_varuint() * 8)

    if inner_base.startswith("Decimal"):
        precision, _ = _decimal_meta(inner_type)
        size = _decimal_size(precision)
        return lambda reader: reader.skip(reader.read_varuint() * size)

    if inner_base == "UUID":
        return lambda reader: reader.skip(reader.read_varuint() * 16)

    return None


def _map_skipper(ch_type: str) -> Callable[[_Reader], None]:
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    key_type, value_type = split_type_arguments(inner)
    key_skip = _skipper_for_type(key_type)
    value_skip = _skipper_for_type(value_type)

    def _skip_map(reader: _Reader):
        count = reader.read_varuint()
        for _ in range(count):
            key_skip(reader)
            value_skip(reader)

    # Nullable values are not fixed-size per item, so fixed-size shortcuts are unsafe.
    if key_type.strip().startswith("Nullable(") or value_type.strip().startswith("Nullable("):
        return _skip_map

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

    return _skip_map


def _tuple_skipper(ch_type: str) -> Callable[[_Reader], None]:
    inner = ch_type[6:-1]
    element_types = split_type_arguments(inner)
    skippers = tuple(_skipper_for_type(t) for t in element_types)

    def _skip_tuple(reader: _Reader):
        for skip in skippers:
            skip(reader)

    return _skip_tuple


_COMPLEX_SKIPPERS: dict[str, Callable[[str], Callable[[_Reader], None]]] = {
    "Array": lambda ch_type: _array_skipper(ch_type[6:-1]),
    "DateTime64": lambda _: lambda reader: reader.skip(8),
    "Time64": lambda _: lambda reader: reader.skip(8),
    "JSON": lambda _: lambda reader: reader.skip(reader.read_varuint()),
    "Map": _map_skipper,
    "String": lambda _: lambda reader: reader.skip(reader.read_varuint()),
    "Tuple": _tuple_skipper,
    "UUID": lambda _: lambda reader: reader.skip(16),
}


@lru_cache(maxsize=256)
def _skipper_for_type(ch_type: str) -> Callable[[_Reader], None]:
    if ch_type.startswith("LowCardinality("):
        return _skipper_for_type(ch_type[15:-1])

    if ch_type.startswith("Nullable("):
        inner = _skipper_for_type(ch_type[9:-1])

        def _skip_nullable(reader: _Reader):
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
        fixed_size = int(ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")].strip())
        return lambda reader: reader.skip(fixed_size)

    if base.startswith("Decimal"):
        precision, _scale = _decimal_meta(ch_type)
        decimal_size = _decimal_size(precision)
        return lambda reader: reader.skip(decimal_size)

    handler = _COMPLEX_SKIPPERS.get(base)
    if handler is not None:
        return handler(ch_type)

    raise ValueError(f"Unsupported RowBinary type: {ch_type}")


# Fixed-width types missing from _FIXED_SIZES because the skippers special-case them.
_WIDE_FIXED_SIZES: dict[str, int] = {"DateTime64": 8, "Time64": 8, "UUID": 16}


def _fixed_width(ch_type: str) -> int | None:
    """Byte width of a column that RowBinary always stores at the same size, else None."""
    if "Nullable(" in ch_type:
        return None

    unwrapped = unwrap_wrappers(ch_type)
    base = extract_base_type(unwrapped)

    size = _FIXED_SIZES.get(base) or _WIDE_FIXED_SIZES.get(base)
    if size is not None:
        return size

    if base == "FixedString":
        return int(unwrapped[unwrapped.index("(") + 1 : unwrapped.rindex(")")].strip())
    if base.startswith("Decimal"):
        precision, _ = _decimal_meta(unwrapped)
        return _decimal_size(precision)

    return None


def _lazy_row_template(types: list[str]) -> tuple[list[int], int] | None:
    """Cell offsets and row width shared by every row, or None if some column varies in size."""
    offsets: list[int] = []
    width = 0
    for ch_type in types:
        size = _fixed_width(ch_type)
        if size is None:
            return None
        offsets.append(width)
        width += size

    # A zero-width row would never advance the reader, so fall back to the skipper path.
    return (offsets, width) if width else None


class RowBinaryLazyValues(Sequence[Any]):
    """Row decoding each cell on first access. Offsets are relative to ``base``, so rows of
    constant width can share one offset list."""

    __slots__ = ("_base", "_cache", "_offsets", "_reader", "_readers")
    _MISSING = object()

    def __init__(
        self,
        reader: _BinaryReader,
        offsets: list[int],
        readers: list[Callable[[_Reader], Any]],
        base: int = 0,
    ):
        self._reader = reader
        self._offsets = offsets
        self._readers = readers
        self._base = base
        self._cache: list[Any] = [self._MISSING] * len(offsets)

    def __len__(self) -> int:
        return len(self._offsets)

    def __getitem__(self, idx: int) -> Any:  # type: ignore[override]
        if idx < 0:
            idx += len(self._offsets)
        value = self._cache[idx]
        if value is not self._MISSING:
            return value

        reader = self._reader
        reader.pos = self._base + self._offsets[idx]
        # Decoding happens here rather than in the query call, so the boundary has to be here too.
        try:
            value = self._readers[idx](reader)
        except ValueError as error:
            raise ChProtocolError(str(error)) from error

        self._cache[idx] = value
        return value


def parse_rowbinary_with_names_and_types_lazy(
    data: bytes,
    server_tz: str | None = None,
) -> tuple[list[str], list[str], Iterable[RowBinaryLazyValues]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return rows with lazy per-cell decoding.

    Args:
        data (bytes): RowBinaryWithNamesAndTypes payload.
        server_tz (str | None): Fallback timezone name for ``DateTime``/``DateTime64`` columns
            that carry no explicit timezone (the ClickHouse server timezone).
    """
    reader = _BinaryReader(data)
    column_count = reader.read_varuint()
    names = [reader.read_string() for _ in range(column_count)]
    types = [reader.read_string() for _ in range(column_count)]
    readers = [_reader_for_type(tp, server_tz) for tp in types]
    # Kept independent of the reader walking the rows: a cell may be read mid-iteration.
    value_reader = _BinaryReader(data)
    template = _lazy_row_template(types)

    if template is not None:
        row_offsets, row_width = template

        def _rows() -> Iterable[RowBinaryLazyValues]:
            while not reader.eof:
                base = reader.pos
                reader.skip(row_width)
                yield RowBinaryLazyValues(value_reader, row_offsets, readers, base)
    else:
        skippers = [_skipper_for_type(tp) for tp in types]

        def _rows() -> Iterable[RowBinaryLazyValues]:
            while not reader.eof:
                offsets: list[int] = []
                for skip in skippers:
                    offsets.append(reader.pos)
                    skip(reader)
                yield RowBinaryLazyValues(value_reader, offsets, readers)

    return names, types, _rows()


class _NeedMoreData(Exception):
    pass


class _StreamingReader:
    __slots__ = ("_buf", "_pos")

    def __init__(self):
        self._buf = bytearray()
        self._pos = 0

    def feed(self, data: bytes):
        if data:
            self._buf += data

    @property
    def pos(self) -> int:
        return self._pos

    @pos.setter
    def pos(self, value: int):
        self._pos = value

    @property
    def buffer(self) -> bytearray:
        return self._buf

    @property
    def remaining(self) -> int:
        return len(self._buf) - self._pos

    @property
    def eof(self) -> bool:
        return self._pos >= len(self._buf)

    def compact(self):
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

    def skip(self, size: int):
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
        s = self._buf[self._pos : self._pos + length].decode("utf-8")
        self._pos += length
        return s

    def read_struct(self, unpack_from: Callable[..., tuple[Any, ...]], size: int) -> tuple[Any, ...]:
        """Decode a run of fixed-width fields with a single struct call."""
        end = self._pos + size
        if end > len(self._buf):
            raise _NeedMoreData
        values = unpack_from(self._buf, self._pos)
        self._pos = end
        return values


class RowBinaryWithNamesAndTypesStreamParser:
    def __init__(self, chunks: AsyncIterator[bytes], *, lazy: bool = False, server_tz: str | None = None):
        self._chunks = chunks.__aiter__()
        self._reader = _StreamingReader()
        self._done = False
        self._names: list[str] | None = None
        self._types: list[str] | None = None
        self._readers: list[Callable[[_Reader], Any]] | None = None
        self._read_row: Callable[[_Reader], list[Any]] | None = None
        self._skippers: list[Callable[[_Reader], None]] | None = None
        self._row_template: tuple[list[int], int] | None = None
        self._batch: _BatchDecoder | None = None
        self._lazy = lazy
        self._server_tz = server_tz

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
                self._readers = [_reader_for_type(tp, self._server_tz) for tp in types]
                if self._lazy:
                    self._row_template = _lazy_row_template(types)
                    self._skippers = None if self._row_template else [_skipper_for_type(tp) for tp in types]
                    self._read_row = None
                else:
                    self._skippers = None
                    self._batch = _batch_decoder(types, self._server_tz)
                    self._read_row = None if self._batch else _make_row_reader(types, self._server_tz)
            except _NeedMoreData:
                self._reader.pos = checkpoint
                if not await self._fill():
                    raise ValueError("Unexpected end of data") from None
            else:
                return names, types

    def _lazy_row(self) -> RowBinaryLazyValues:
        """One row skipped rather than decoded, leaving the reader just past it."""
        assert self._readers is not None
        reader = self._reader
        row_start = reader.pos

        if self._row_template is not None:
            offsets, row_width = self._row_template
            reader.skip(row_width)
        else:
            assert self._skippers is not None
            offsets = []
            for skip in self._skippers:
                offsets.append(reader.pos - row_start)
                skip(reader)

        # The streaming buffer keeps moving, so each row needs its own copy.
        row_bytes = reader.copy_slice(row_start, reader.pos)
        return RowBinaryLazyValues(_BinaryReader(row_bytes), offsets, self._readers)

    async def rows(self) -> AsyncIterator[list[Any] | RowBinaryLazyValues]:  # noqa: C901, PLR0912
        # Both loops stay here: delegating between async generators costs a frame per row,
        # measured at 8% on a String column and worse on the bulk path.
        await self.read_header()
        assert self._readers is not None
        read_row = self._read_row

        if self._batch is not None:
            reader = self._reader
            decode_batch = self._batch

            while True:
                # Whole rows go in one pass; a partial trailing row waits for the next chunk.
                batch, pos = decode_batch(reader.buffer, reader.pos, len(reader.buffer))
                if batch:
                    reader.pos = pos
                    reader.compact()
                    for row in batch:
                        yield row

                if not await self._fill():
                    if reader.remaining:
                        raise ValueError("Unexpected end of data")
                    return

        while True:
            if self._done and self._reader.remaining == 0:
                return

            checkpoint = self._reader.pos
            try:
                if self._lazy:
                    yield self._lazy_row()
                elif read_row is not None:
                    yield read_row(self._reader)
                else:
                    yield [read(self._reader) for read in self._readers]

                self._reader.compact()
            except _NeedMoreData:
                self._reader.pos = checkpoint
                if not await self._fill():
                    if self._reader.remaining == 0:
                        return
                    raise ValueError("Unexpected end of data") from None
