import json
from typing import Any, Sequence

from .models import ExternalData, ExternalTable


def _transform_to_json_compact_each_row(columns: list[str], rows: Sequence[Any]) -> list[tuple[Any, ...]]:
    """Transform dict rows to tuple rows in column order."""
    return [tuple(row[col] for col in columns) for row in rows]


def _to_json_compact_each_row_bytes(external_table: ExternalTable) -> bytes:
    """Convert external table to JSONCompactEachRow format bytes."""
    rows = external_table.data
    if isinstance(rows[0], dict):
        rows = _transform_to_json_compact_each_row([s[0] for s in external_table.structure], rows)

    data = "\n".join(json.dumps(list(r), ensure_ascii=False, separators=(",", ":")) for r in rows) + "\n"
    return data.encode("utf-8")


def build_external_data(external_tables: dict[str, ExternalTable]) -> list[ExternalData]:
    """Build external data objects for multipart request."""
    return [
        ExternalData(
            name,
            content=_to_json_compact_each_row_bytes(external_table),
            filename=f"{name}.json",
            content_type="application/json",
        )
        for name, external_table in external_tables.items()
    ]
