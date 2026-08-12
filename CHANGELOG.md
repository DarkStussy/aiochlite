# Changelog

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
