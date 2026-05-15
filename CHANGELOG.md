# Changelog

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
