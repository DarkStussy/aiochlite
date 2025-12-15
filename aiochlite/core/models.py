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


class Row:
    """Query result row with column access by name or index."""

    __slots__ = ("_dict", "_index", "_names", "_values")

    def __init__(self, names: list[str], values: Sequence[Any], *, index: Mapping[str, int] | None = None):
        self._names = names
        self._values = values
        self._index = index
        self._dict: dict[str, Any] | None = None

    def _as_dict(self) -> dict[str, Any]:
        if self._dict is None:
            self._dict = dict(zip(self._names, self._values, strict=False))
        return self._dict

    def __getattr__(self, name: str) -> Any:
        if self._index is not None and name in self._index:
            return self._values[self._index[name]]

        mapping = self._as_dict()
        if name in mapping:
            return mapping[name]

        raise AttributeError(f"Row has no column '{name}'")

    def __getitem__(self, key: str) -> Any:
        if self._index is not None and key in self._index:
            return self._values[self._index[key]]
        return self._as_dict()[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def __repr__(self) -> str:
        return f"Row({self._as_dict()})"

    def first(self) -> Any:
        """Get value of the first column."""
        return self._values[0] if self._values else None
