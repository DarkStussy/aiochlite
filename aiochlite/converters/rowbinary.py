import ipaddress
import json
import re
import struct
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Any, Callable, Iterable, Literal, Protocol, overload
from uuid import UUID
from zoneinfo import ZoneInfo

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
    def read_struct(self, unpack_from: Callable[..., tuple[Any, ...]], size: int) -> tuple[Any, ...]: ...
    def skip(self, size: int): ...

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

    def read_struct(self, unpack_from: Callable[..., tuple[Any, ...]], size: int) -> tuple[Any, ...]:
        """Decode a run of fixed-width fields with a single struct call."""
        end = self._pos + size
        if end > len(self._data):
            raise ValueError("Unexpected end of data")

        values = unpack_from(self._data, self._pos)
        self._pos = end
        return values

    @property
    def eof(self) -> bool:
        return self._pos >= len(self._data)

    @property
    def pos(self) -> int:
        return self._pos

    def skip(self, size: int):
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

_CACHE_MAX_SIZE = 4096
_CACHE_PROBE_CALLS = 4096
# Under a 25% hit rate the lookup costs more than recomputing the value.
_CACHE_MIN_HITS = _CACHE_PROBE_CALLS // 4
# Readers are shared across queries, so retry caching after 65,536 uncached values.
_CACHE_REPROBE_CALLS = 65_536


def _adaptive_cache(compute: Callable[[int], Any]) -> Callable[[int], Any]:
    """Cache repeated integer keys and disable caching when the hit rate is below 25%."""
    cache: dict[int, Any] = {}
    calls = 0
    hits = 0
    countdown = 0

    def cached(key: int) -> Any:
        nonlocal calls, hits, countdown

        if countdown:
            countdown -= 1
            return compute(key)

        value = cache.get(key)
        if value is None:
            if len(cache) >= _CACHE_MAX_SIZE:
                cache.clear()
            value = cache[key] = compute(key)
        else:
            hits += 1

        calls += 1
        if calls >= _CACHE_PROBE_CALLS:
            if hits < _CACHE_MIN_HITS:
                cache.clear()
                countdown = _CACHE_REPROBE_CALLS
            calls = hits = 0

        return value

    return cached


def _datetime_converter(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[int], datetime]:
    """Unix timestamp -> datetime."""
    explicit_tz = extract_timezone(ch_type)
    # An explicit timezone yields an aware datetime; otherwise the wall-clock time is computed
    # in the server timezone and returned naive.
    tz = explicit_tz or server_tz
    strip_tz = explicit_tz is None

    def _compute(ts: int) -> datetime:
        dt = datetime.fromtimestamp(ts, tz=tz)
        return dt.replace(tzinfo=None) if strip_tz else dt

    return _adaptive_cache(_compute)


def _datetime_reader(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[_Reader], datetime]:
    _dt = _datetime_converter(ch_type, server_tz)

    def _read_dt(reader: _Reader) -> datetime:
        return _dt(reader.read_uint32())

    return _read_dt


def _time64_converter(ch_type: str) -> Callable[[int], timedelta]:
    """Raw ticks -> timedelta."""
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    scale = int(inner.strip())

    if scale <= 6:
        multiplier = 10 ** (6 - scale)

        def _compute(ticks: int) -> timedelta:
            return timedelta(microseconds=ticks * multiplier)
    else:
        divisor = 10 ** (scale - 6)

        def _compute(ticks: int) -> timedelta:
            return timedelta(microseconds=ticks // divisor)

    return _adaptive_cache(_compute)


def _time64_reader(ch_type: str) -> Callable[[_Reader], timedelta]:
    _td = _time64_converter(ch_type)

    def _read_time64(reader: _Reader) -> timedelta:
        return _td(reader.read_int64())

    return _read_time64


def _datetime64_converter(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[int], datetime]:
    """Raw ticks -> datetime."""
    inner = ch_type[ch_type.index("(") + 1 : ch_type.rindex(")")]
    parts = [p.strip() for p in inner.split(",")]
    scale = int(parts[0])
    explicit_tz = extract_timezone(ch_type)
    # An explicit timezone yields an aware datetime; otherwise the wall-clock time is computed
    # in the server timezone and returned naive.
    tz = explicit_tz or server_tz
    strip_tz = explicit_tz is None

    def _compute(ticks: int) -> datetime:
        base_seconds, remainder = divmod(ticks, 10**scale)
        dt = datetime.fromtimestamp(base_seconds, tz=tz)
        if remainder:
            micros = remainder * (10 ** (6 - scale)) if scale <= 6 else remainder / (10 ** (scale - 6))
            dt = dt + timedelta(microseconds=micros)
        return dt.replace(tzinfo=None) if strip_tz else dt

    return _adaptive_cache(_compute)


def _datetime64_reader(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[_Reader], datetime]:
    _dt64 = _datetime64_converter(ch_type, server_tz)

    def _read_dt64(reader: _Reader) -> datetime:
        return _dt64(reader.read_int64())

    return _read_dt64


def _decimal_converter(ch_type: str) -> Callable[[int], Decimal]:
    """Raw signed integer -> scaled Decimal."""
    _, scale = _decimal_meta(ch_type)

    def _compute(raw: int) -> Decimal:
        return Decimal(raw).scaleb(-scale)

    return _adaptive_cache(_compute)


def _decimal_reader(ch_type: str) -> Callable[[_Reader], Decimal]:
    precision, _ = _decimal_meta(ch_type)
    size = _decimal_size(precision)
    _dec = _decimal_converter(ch_type)

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


def _array_reader(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[_Reader], list[Any]]:
    inner_type = ch_type[6:-1]
    inner = _reader_for_type(inner_type, server_tz)

    def _read_array(reader: _Reader) -> list[Any]:
        return [inner(reader) for _ in range(reader.read_varuint())]

    return _read_array


def _map_reader(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[_Reader], dict[Any, Any]]:
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


def _tuple_reader(ch_type: str, server_tz: ZoneInfo | None) -> Callable[[_Reader], tuple[Any, ...]]:
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

_TZ_AWARE_READERS: dict[str, Callable[[str, ZoneInfo | None], Callable[[_Reader], Any]]] = {
    "Array": _array_reader,
    "DateTime": _datetime_reader,
    "DateTime64": _datetime64_reader,
    "Map": _map_reader,
    "Tuple": _tuple_reader,
}


@lru_cache(maxsize=256)
def _reader_for_type(ch_type: str, server_tz: ZoneInfo | None = None) -> Callable[[_Reader], Any]:
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

_DECIMAL_STRUCT_CODES: dict[int, str] = {4: "i", 8: "q"}

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
_FIXED_CONVERTERS: dict[str, Callable[[str, ZoneInfo | None], tuple[str, Callable[[Any], Any]]]] = {
    "Date": lambda _ch_type, _tz: ("H", _days_to_date),
    "Date32": lambda _ch_type, _tz: ("i", _days_to_date),
    "DateTime": lambda ch_type, tz: ("I", _datetime_converter(ch_type, tz)),
    "DateTime64": lambda ch_type, tz: ("q", _datetime64_converter(ch_type, tz)),
    "Enum8": lambda ch_type, _tz: ("b", _enum_converter(ch_type)),
    "Enum16": lambda ch_type, _tz: ("h", _enum_converter(ch_type)),
    "IPv4": lambda _ch_type, _tz: ("I", ipaddress.IPv4Address),
    "Time": lambda _ch_type, _tz: ("i", _seconds_to_timedelta),
    "Time64": lambda ch_type, _tz: ("q", _time64_converter(ch_type)),
}


def _fixed_field(ch_type: str, server_tz: ZoneInfo | None) -> tuple[str, Callable[[Any], Any] | None] | None:
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
        decimal_code = _DECIMAL_STRUCT_CODES.get(_decimal_size(precision))
        return (decimal_code, _decimal_converter(unwrapped)) if decimal_code is not None else None

    return None


_ConvSlots = list[tuple[int, Callable[[Any], Any]]]


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


def _make_row_reader(types: list[str], server_tz: ZoneInfo | None) -> Callable[[_Reader], list[Any]] | None:
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
    data: bytes, server_tz: ZoneInfo | None = ..., *, as_tuple: Literal[False] = ...
) -> tuple[list[str], list[str], Iterable[list[Any]]]: ...


@overload
def parse_rowbinary_with_names_and_types(
    data: bytes, server_tz: ZoneInfo | None = ..., *, as_tuple: Literal[True]
) -> tuple[list[str], list[str], Iterable[tuple[Any, ...]]]: ...


def parse_rowbinary_with_names_and_types(
    data: bytes,
    server_tz: ZoneInfo | None = None,
    *,
    as_tuple: bool = False,
) -> tuple[list[str], list[str], Iterable[Any]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return header and row iterator.

    Args:
        data (bytes): RowBinaryWithNamesAndTypes payload.
        server_tz (ZoneInfo | None): Fallback timezone for ``DateTime``/``DateTime64`` columns
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

    return names, types, _tuple_rows() if as_tuple else _rows()


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

    # Nullable values are not fixed-size per item, so fixed-size shortcuts are unsafe.
    if key_type.strip().startswith("Nullable(") or value_type.strip().startswith("Nullable("):

        def _skip_map(reader: _Reader):
            count = reader.read_varuint()
            for _ in range(count):
                key_skip(reader)
                value_skip(reader)

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

    def _skip_map(reader: _Reader):
        count = reader.read_varuint()
        for _ in range(count):
            key_skip(reader)
            value_skip(reader)

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
    data: bytes, server_tz: ZoneInfo | None = None
) -> tuple[list[str], list[str], Iterable[RowBinaryLazyValues]]:
    """
    Parse RowBinaryWithNamesAndTypes payload and return rows with lazy per-cell decoding.

    Args:
        data (bytes): RowBinaryWithNamesAndTypes payload.
        server_tz (ZoneInfo | None): Fallback timezone for ``DateTime``/``DateTime64`` columns
            that carry no explicit timezone (the ClickHouse server timezone).
    """
    reader = _BinaryReader(data)
    column_count = reader.read_varuint()
    names = [reader.read_string() for _ in range(column_count)]
    types = [reader.read_string() for _ in range(column_count)]
    skippers = [_skipper_for_type(tp) for tp in types]
    readers = [_reader_for_type(tp, server_tz) for tp in types]
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
        s = bytes(self._buf[self._pos : self._pos + length]).decode("utf-8")
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
    def __init__(self, chunks: AsyncIterator[bytes], *, lazy: bool = False, server_tz: ZoneInfo | None = None):
        self._chunks = chunks.__aiter__()
        self._reader = _StreamingReader()
        self._done = False
        self._names: list[str] | None = None
        self._types: list[str] | None = None
        self._readers: list[Callable[[_Reader], Any]] | None = None
        self._read_row: Callable[[_Reader], list[Any]] | None = None
        self._skippers: list[Callable[[_Reader], None]] | None = None
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
                    self._skippers = [_skipper_for_type(tp) for tp in types]
                    self._read_row = None
                else:
                    self._skippers = None
                    self._read_row = _make_row_reader(types, self._server_tz)
            except _NeedMoreData:
                self._reader.pos = checkpoint
                if not await self._fill():
                    raise ValueError("Unexpected end of data") from None
            else:
                return names, types

    async def rows(self) -> AsyncIterator[list[Any] | RowBinaryLazyValues]:
        await self.read_header()
        assert self._readers is not None
        read_row = self._read_row

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
