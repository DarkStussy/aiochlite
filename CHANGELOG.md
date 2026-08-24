# Changelog

## 1.8.1 (2026-08-25)

### Fixed
- **A `bytes` value that is not UTF-8 could not be sent at all**, raising `UnicodeDecodeError`
  before the request was even built — the parameter type was documented, but only text ever
  survived it. A ClickHouse `String` is any byte sequence, and `binary_columns` covered only the
  reading half. A parameter now travels as `\xNN` and an insert carries the bytes themselves, so
  every value in `0..255` arrives intact, inside a container, an external table and a `Map` alike.
- **A string parameter holding a backslash arrived mangled**, and one holding a tab or a newline
  was rejected with `Code: 457`. The server reads a parameter back from the [escaped
  format](https://clickhouse.com/docs/concepts/features/interfaces/http#tabs-in-url-parameters),
  which aiochlite did not write: `C:\new\table` reached it as `C:`, a newline, `ew`, a tab and
  `able`, and `\x41` as `A`. Windows paths, regular expressions and JSON in a parameter were all
  corrupted silently. Parameters are now escaped, and `\N` for `None` stays what it already was.
  Values inside an array, tuple or map literal were never affected — those carry their own
  escaping, which the server unescapes exactly once.
- **An `enum.Enum` member sent as a parameter or inserted arrived as `Color.RED`, not `red`** — the
  only conversion that reached the server wrong rather than raising. It fell through to `str()`,
  which renders a member, not its value. A member now stands for its value everywhere: parameters,
  array, tuple and map literals, and inserts. A value that needs converting in its own right, such
  as a `datetime`, still gets it. `IntEnum` and `StrEnum` happened to work already, since `str()`
  on those is `int.__str__` and `str.__str__`.
- **A `Map` keyed by a `UUID`, `date`, `Decimal` or `Enum` could not be inserted**, raising
  `TypeError: keys must be str, int, float, bool or None`. `json.dumps` offers its `default` hook
  the values of a document, never the keys, so a key it does not accept raised instead of being
  converted. Such keys are now rendered by the same rules as values. Rendering every key upfront
  costs a row 33%-90%, so the pass runs only over a row that `dumps` has already refused.

## 1.8.0 (2026-08-25)

### Added
- `binary_columns` on every row-returning call names the columns to decode as `bytes` instead of
  `str`. A ClickHouse `String` is any sequence of bytes, and until now a column holding binary data
  could not be read at all. It covers every `String`/`FixedString` in the column's type, however
  deeply nested, and costs nothing when unused: the column type is rewritten once per query and
  the decoders stay keyed by type string.
- `ChArgumentError`, raised when a query option does not fit the query — `binary_columns` naming a
  column the query did not select, one holding no text, or any call that decodes no rows. It is
  both a `ChClientError` and a `ValueError`.
- `Int128`, `UInt128`, `Int256` and `UInt256` decode to `int`. They are wider than any `struct`
  code, so they travel as raw bytes and are widened by their converter, as the big decimals do.
- `SimpleAggregateFunction(func, T)` decodes as `T`, which is what it encodes on the wire. It is an
  ordinary column type in an `AggregatingMergeTree`, and is stripped like `LowCardinality` — ahead
  of the `Nullable` check, since `T` may itself be `Nullable`.
- `Nothing` decodes to `None`, which is what makes `SELECT NULL` work: a bare `NULL` types as
  `Nullable(Nothing)`, where the null flag is the whole value and `Nothing` occupies no bytes.
  Until now it raised `Unsupported RowBinary type: Nothing`.

### Fixed
- **A named `Tuple` raised `Unsupported RowBinary type: a UInt8`.** `Tuple(a UInt8, b String)`
  carries field names in its type string but not on the wire, so they are dropped and a named tuple
  decodes to a `tuple` like any other — nested in an `Array` or `Map` too. The type-argument scanner
  now tracks backquotes and backslash escapes as well, so a field name or enum label holding a comma
  no longer splits the type in the wrong place.
- **Lazy decoding read every column after a `Map(K, LowCardinality(Nullable(V)))` from the wrong
  offset** — silently wrong on a single row, `ChProtocolError` on several. Both halves looked
  fixed-width, so the map was skipped as one block and the per-element null flags were never counted.
  The skippers now measure width through the same check the row layout uses, which rejects a
  `Nullable` behind any wrapper.
- **A `FixedString` holding bytes that are not UTF-8 was silently corrupted**: it decoded with
  `errors="replace"`, so invalid sequences came back as `U+FFFD` with nothing to tell the value
  apart from text. It now decodes strictly, as `String` always has, and raises `ChProtocolError`.
  Read such a column through `binary_columns`.

### Changed
- A failed UTF-8 decode no longer reports a bare codec error. The message says the column is not
  UTF-8 and names the columns `binary_columns` can be passed for.
- Lazy decoding skips more containers with one multiply instead of walking them element by element:
  an `Array(FixedString(N))`, a `Map` with `FixedString`, `Decimal`, `DateTime64` or `Time64` on
  either side, and a `Map(UUID, UUID)`.

## 1.7.1 (2026-08-18)

### Changed
- `fetch()` and `stream()` build each `Row` more cheaply: the column index is passed positionally
  rather than by keyword, and the per-row dict built on first use is gone. Wrapping a decoded result
  and reading two of its columns is 23% faster. `Row(names, values, index=...)` still works.
- A hand-built `Row` whose index omits a column no longer falls back to the column names:
  `row["c"]` raises `KeyError`, `row.c` raises `AttributeError`. Rows from the client carry a
  complete index.

## 1.7.0 (2026-08-17)

### Fixed
- **On Windows, a `DateTime64` before 1970 raised `OSError: [Errno 22] Invalid argument`** instead
  of decoding. It went through `datetime.fromtimestamp`, which the Windows C runtime rejects for a
  negative timestamp; those values are now offset from the epoch by hand. Other platforms decode as
  before, and `DateTime` is unsigned, so it was never affected.
- A response truncated in the middle of a fixed-width value raised `struct.error` instead of
  `ChProtocolError`: eight `_BinaryReader` methods left the bounds check to `struct`, and
  `struct.error` is not a `ValueError`, so it slipped past the decode boundary and reached the
  caller raw.

### Changed
- A row that is fixed-width end to end now decodes in one `struct` pass instead of a Python call per
  row — in `fetch()`, `fetch_rows()` and `stream()` alike. On 200k rows: 3.5x
  on five numeric columns, 5.9x on one, 3.1x streaming. A schema with a variable-width column keeps
  the per-row reader and is unaffected.
- Converters built per query now memoize into a cache that starts over once full, instead of one
  that evicts. Past the old 4096-entry bound every lookup missed and every insert evicted, which
  cost more than having no cache at all: 300k rows over 20k distinct values took 224 ms that way,
  174 ms uncached, 51 ms now. The cache holds at most 65536 values per column and is released with
  the query, so `stream()` stays flat however long the result runs — 3M rows of distinct timestamps
  peaked at 24 MiB, against 1033 MiB unbounded. Converters shared across queries keep their own
  bound.
- A row now decodes through a loop compiled for its schema, rather than a reader call per column
  per row. On 200k rows: 3.4x on `UInt64, Float64, String`, 3.6x on
  `UInt64, Nullable(String), Nullable(DateTime)`, 2.1x-2.9x streaming. Fixed-width, `String`,
  `Nullable`, `Array`, `Tuple` and `Map` columns are emitted inline; anything else reads through its
  own closure inside the same loop, so a single uncovered column no longer costs the whole row its
  compiled path. Only a row where every column needs a closure stays on the reader path. The
  compiled code is cached per schema, the converters it uses are not — their per-query caches are
  still released with the query.
- A `Map` now decodes through a loop compiled for its pair, instead of a reader call per key and
  per value. On 100k rows of three pairs: 2.1x for `Map(String, UInt8)`, 2.3x for
  `Map(String, String)`.
- An `Array` of fixed-width elements now decodes with one `struct` call for the whole array
  instead of a reader call per element, and an `Array(String)` with one call per array. On 100k
  rows: 6.0x on `Array(UInt64)` of 20 elements, 3.8x of 3, 4.0x on
  `UInt64, Array(Decimal64(2)), String`, 1.6x on `Array(String)`.
- A container holding another container, or a `Nullable`, is now compiled too instead of going to
  the reader whole: `Array(Nullable(...))`, `Array(Array(...))`, `Array(Tuple(...))`,
  `Array(Map(...))`, and `Tuple` or `Map` with any of those inside. On 100k rows of
  `UInt64, Array(Array(UInt8)), Map(String, Array(UInt8)), Array(Nullable(UInt64))`: 2.1x end to
  end. Only nesting deeper than four levels keeps the reader path.
- A `JSON` column now decodes inside the compiled loop, calling the C scanner `json.loads` uses
  internally rather than going through the argument checks wrapped around it. `loads` scans the text
  again to see whether anything follows the value; the decoder checks the offset the scanner stopped
  at instead, so a document with a second value after the first still raises. On 100k rows of
  `UInt64, JSON` a fetch takes 173 ms against 226 ms. `JSON` inside an `Array`, `Tuple` or `Map` is
  compiled along with the container. Values and errors are unchanged.
- `UUID`, `IPv6`, `FixedString` and `Decimal` past 64 bits now count as fixed-width wherever the two
  paths above look for one, instead of being read one column at a time. They travel as raw bytes
  through an `Ns` struct code and are widened by their converter. On 200k rows of
  `Decimal128(2), UInt64`: 4.5x; `FixedString(16), UInt64`: 2.5x; `IPv6, UInt64`: 1.7x;
  `UUID, UInt64`: 1.4x, where building the `UUID` objects is most of what is left.

## 1.6.0 (2026-08-15)

### Changed
- **`tzdata` is now required on Windows**, which carries no IANA database. Without it every
  `DateTime` column would fail to decode. Other platforms are unaffected.

### Fixed
- A timezone the runtime could not load was ignored, and `DateTime` / `DateTime64` values then
  decoded in the machine's local timezone and came back naive — hours off, with nothing to show
  for it. **Such a column now raises `ChProtocolError` instead of returning a wrong value.**
- A response without the `X-ClickHouse-Timezone` header left a `DateTime` column with no wall
  clock to go by, and it decoded in the machine's local timezone. **That now raises
  `ChProtocolError` too**; a column carrying its own timezone is unaffected.
- `fetchone()` and `fetchval()` held the connection until the garbage collector reached the
  generator behind them, which on a large result meant well after the call returned.
- `async with AsyncChClient(...)` leaked the session when the opening ping was canceled rather
  than failing, since `CancelledError` is not an `Exception`.
- External table column names went into the structure unquoted, so a name such as `odd col`
  was rejected by the server even though `insert()` already accepted it.

## 1.5.0 (2026-08-15)

### Added
- `ChTransportError`, `ChServerError` and `ChProtocolError`, all deriving from `ChClientError`.
  Connection and timeout failures no longer surface as raw `aiohttp` exceptions, and decode
  failures no longer surface as `ValueError`. `ChServerError` carries `status`, `code`,
  `query_id` and `exception_tag`.
- `insert()` accepts any iterable or async iterable of rows, not only a sequence, so a dataset
  can be inserted without holding all of it in memory. An exception raised by the row source
  reaches the caller unchanged rather than as a transport failure.
- Detection of exceptions that ClickHouse writes into a `200 OK` body, so a query failing
  mid-response raises `ChServerError` instead of handing truncated data to the format parser.
  Needs ClickHouse 25.11 or newer, which introduced the `X-ClickHouse-Exception-Tag` header;
  older servers behave as before.
- `InsertData` and `InsertRow` types, exported from `aiochlite`, for annotating a row source.

### Changed
- `datetime` values keep microseconds, and aware ones are sent as a Unix timestamp, so the
  instant no longer depends on the column timezone. Applies to query parameters, inserts and
  external table data. **A `DateTime` column now rejects a value carrying microseconds instead
  of silently dropping them** — use `DateTime64` or `.replace(microsecond=0)`.
- `insert()` streams its payload in batches instead of building the whole body first. Inserting
  300,000 narrow rows now peaks at 1.9 MB above the source data instead of 33 MB, at the same
  speed.
- `insert()` no longer pastes the database, table and column names straight into the statement.
  Database and table go to the server as `Identifier` parameters, column names are quoted. Names
  needing quotes, such as `odd table` or `odd col`, now work.
- A `ClientSession` passed to the constructor is no longer treated as the client's own. Its headers
  are left untouched — credentials now travel with each request instead — and `close()` no longer
  closes it.
- `aiohttp` requirement widened from `>=3.13,<3.15` to `>=3.13,<4`.

### Fixed
- A setting named `param_*` overrode the query parameter of that name. Parameters are now applied
  last, so `settings` cannot rewrite them or redirect an insert to another table.
- External tables with no rows raised `IndexError`.
- `async with AsyncChClient(...)` left its session open when the opening ping failed.
- A query failing after the first rows were sent reported the whole partial body as the error
  text, burying the server message under megabytes of payload.
- A 64-bit integer inside a `JSON` column decoded as a string on servers older than 25.8, which
  quote them by default.
- `Time64(P)` and `DateTime64(P)` with `P > 6` were off by one microsecond for some values.
  Digits below a microsecond are now cut toward zero, as the server itself narrows them.

## 1.4.1 (2026-08-15)

### Added
- `py.typed` marker, so type checkers use the shipped annotations.
- `aiochlite[aiohttp-speedups]` extra, forwarding to `aiohttp[speedups]`.

### Fixed
- Return type of the internal `HttpClient.post()`, which declared a value it never returned.

## 1.4.0 (2026-08-14)

### Changed
- `lazy_decode` now defaults to `False`. Lazy decoding only pays off when a small share of the
  selected columns is read, and costs up to 45% when all of them are; queries that select exactly
  what they use were paying for it. Pass `lazy_decode=True` to keep the old behavior. Decoding
  errors now surface from the query call rather than from cell access.
- Faster decoding with `lazy_decode=False`, in `fetch()`, `fetch_rows()`, `stream()` and
  `stream_rows()`. Rows of fixed-width columns gain the most, followed by rows with `String`
  columns.
- Faster lazy decoding (`lazy_decode=True`), for every row shape, and most of all when a row
  consists of fixed-width columns or only some of its columns are read.

## 1.3.0 (2026-08-12)

### Added
- `AsyncChClient.fetch_format(query, format_name)` — execute a query and return the whole result
  as undecoded `bytes` in the requested ClickHouse output format (Parquet, CSV/TSV, JSON family,
  Arrow, ORC, Native, RowBinary and others).
- `AsyncChClient.stream_format(query, format_name)` — same, but yields payload chunks via
  `AsyncIterator[bytes]`.
- `ExportFormat` literal type listing the supported output formats, exported from `aiochlite`.

### Changed
- `aiohttp` requirement widened from `~=3.13.0` to `>=3.13,<3.15`.

### Deprecated
- `AsyncChClient.fetch_parquet()` — use `fetch_format(query, "Parquet")`.
- `AsyncChClient.stream_parquet()` — use `stream_format(query, "Parquet")`.
  Both emit a `DeprecationWarning` and will be removed in a future release.
  `stream_parquet()` is no longer an async generator function: it returns an `AsyncIterator[bytes]`,
  so the warning is emitted on call rather than on first iteration (`async for` usage is unchanged).

### Fixed
- The "query must not contain a FORMAT clause" check matched the substring `format` and rejected
  queries using functions such as `formatDateTime()`. It now matches a trailing `FORMAT <name>`
  clause (optionally followed by `SETTINGS ...`), ignoring comments and string/identifier literals.

## 1.2.0 (2026-06-01)

### Added
- Server timezone handling for `DateTime` / `DateTime64` via the `X-ClickHouse-Timezone`
  response header. Columns with an explicit timezone are returned as timezone-aware
  `datetime`; columns without one use the server-timezone wall-clock and are returned as
  naive `datetime`. Works inside `Array`, `Nullable`, `LowCardinality`, `Tuple`, and `Map`.

## 1.1.0 (2026-05-26)

### Added
- `AsyncChClient.fetch_parquet()` — execute a query and return the full result as
  Parquet-encoded `bytes` (uses ClickHouse's native `FORMAT Parquet`).
- `AsyncChClient.stream_parquet()` — same, but yields raw Parquet payload chunks via
  `AsyncIterator[bytes]` for large result sets.

## 1.0.2 (2026-05-16)

### Added
- `Time` and `Time64(P)` decoding from `RowBinaryWithNamesAndTypes` into `datetime.timedelta`,
  including negative values and durations beyond 24 hours. Works inside `Array`, `Nullable`,
  `LowCardinality`, `Tuple`, and `Map`. `Time64(P)` with `P > 6` is truncated to microseconds.
- `datetime.timedelta` parameter serialization to `HH:MM:SS[.ffffff]` (signed),
  suitable for `Time` / `Time64` columns.

### Changed
- Build backend migrated to `hatchling`.
- Dev workflow switched to `uv`; dev dependencies moved into `[dependency-groups.dev]`.
- Bumped `ruff` to `0.15.13` and `basedpyright` to `1.39.4`.

### Removed
- `requirements_dev.txt` (superseded by `[dependency-groups.dev]` + `uv.lock`).
