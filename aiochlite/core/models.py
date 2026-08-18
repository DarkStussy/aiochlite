from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Iterator, NamedTuple


@dataclass(slots=True)
class ExternalTable:
    """External table data for ClickHouse queries.

    Attributes:
        structure (Sequence[tuple[str, str]]): Column definitions as (name, type) pairs.
        data (Sequence[dict[str, Any]] | Sequence[tuple[Any, ...]]): Table rows as dicts or tuples.
    """

    structure: Sequence[tuple[str, str]]
    data: Sequence[dict[str, Any]] | Sequence[tuple[Any, ...]]


class ExternalData(NamedTuple):
    """External data file representation for multipart requests."""

    name: str
    content: bytes
    filename: str
    content_type: str | None = None


class Row(Mapping[str, Any]):
    """Query result row, keyed by column name. `fetch_rows()` returns tuples for positional access."""

    __slots__ = ("_index", "_names", "_values")

    def __init__(self, names: list[str], values: Sequence[Any], index: Mapping[str, int] | None = None):
        # `index` stays positional: by keyword it costs more per row than the rest of this method.
        self._names = names
        self._values = values
        self._index = index if index is not None else {name: idx for idx, name in enumerate(names)}

    def _as_dict(self) -> dict[str, Any]:
        return dict(zip(self._names, self._values, strict=False))

    def __getattr__(self, name: str) -> Any:
        try:
            return self._values[self._index[name]]
        except KeyError:
            raise AttributeError(f"Row has no column '{name}'") from None

    def __getitem__(self, key: str) -> Any:
        return self._values[self._index[key]]

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def __repr__(self) -> str:
        return f"Row({self._as_dict()})"

    def first(self) -> Any:
        """Get value of the first column."""
        return self._values[0] if self._values else None
