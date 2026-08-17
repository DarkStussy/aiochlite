# Changelog

## Unreleased

### Fixed
- A response truncated in the middle of a fixed-width value raised `struct.error` instead of
  `ChProtocolError`, because eight `_BinaryReader` methods left the bounds check to `struct` and
  `struct.error` is no `ValueError`. It escaped the decode boundary and reached the caller raw.

### Changed
- A row that is fixed-width end to end now decodes in a single `struct` pass instead of a Python
  call per row, in `fetch()`, `fetch_rows()` and `stream()`. On 200k rows: 3.5x on five numeric
  columns, 5.9x on one, 3.1x streaming. A schema with a variable-width column keeps the per-row
  reader and is unaffected.
- Converters built per query no longer bound their value cache. Past the old 4096-entry bound
  every lookup missed and every insert evicted, costing more than no cache at all: 200k
  `DateTime` values at 20k distinct took 51.7 ms bounded, against 39.5 ms uncached and 8.8 ms
  unbounded. The cache is released with the query, so it adds no order of memory over the result.
  Converters shared across queries keep the bound.
- A schema of fixed-width, `String` and `Nullable` columns now decodes through a loop compiled
  for that schema, rather than a reader call per column per row. On 200k rows: 3.4x on
  `UInt64, Float64, String`, 3.6x on `UInt64, Nullable(String), Nullable(DateTime)`, 2.1x-2.9x
  streaming. The compiled code is cached per schema; the converters it uses are not, so their
  per-query caches are still released with the query. A column of any other type, `Array` and
  `Map` among them, keeps the reader path.
- `UUID`, `IPv6`, `FixedString` and `Decimal` past 64 bits now count as fixed-width wherever the
  two paths above look for one, having previously been read one column at a time. They travel as
  raw bytes through a `Ns` struct code and are widened by their converter. On 200k rows of
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
