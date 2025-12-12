from dataclasses import dataclass
from typing import Any, Iterator, NamedTuple, Sequence


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


class Row:
    """Query result row with column access by name or index."""

    __slots__ = ("_data", "_names")

    def __init__(self, names: list[str], values: list[Any]):
        self._data = dict(zip(names, values, strict=False))

    def __getattr__(self, name: str) -> Any:
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"Row has no column '{name}'") from None

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"Row({self._data})"

    def first(self) -> Any:
        """Get value of the first column."""
        return next(iter(self._data.values()))
