"""Core ClickHouse client functionality."""

import json
from collections.abc import Mapping
from typing import Any, Callable, TypedDict, Unpack

from aiochlite.converters import converter_for_ch_type, to_clickhouse

from .models import ExternalTable, Row


class ClientCoreOptions(TypedDict, total=False):
    """Configuration options for ChClientCore."""

    user: str
    password: str
    database: str
    enable_compression: bool


class ChClientCore:
    """
    Core client logic for building requests and parsing responses.

    Args:
        user (str): ClickHouse username.
        password (str): ClickHouse password.
        database (str): Default database name.
        enable_compression (bool): Enable HTTP compression.
    """

    __slots__ = ("_database", "_enable_compression", "_password", "_user")

    def __init__(self, **kwargs: Unpack[ClientCoreOptions]):
        self._user = kwargs.get("user", "default")
        self._password = kwargs.get("password", "")
        self._database = kwargs.get("database", "default")
        self._enable_compression = kwargs.get("enable_compression", False)

    def build_headers(self) -> dict[str, str]:
        headers = {}
        if self._user:
            headers["X-ClickHouse-User"] = self._user
        if self._password:
            headers["X-ClickHouse-Key"] = self._password

        return headers

    def build_query_params(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        external_tables: dict[str, ExternalTable] | None = None,
    ) -> dict[str, Any]:
        url_params: dict[str, Any] = {"database": self._database, "output_format_json_quote_decimals": "1"}

        if self._enable_compression:
            url_params["enable_http_compression"] = "1"

        if params:
            for key, value in params.items():
                url_params[f"param_{key}"] = to_clickhouse(value)

        if settings:
            url_params.update(settings)

        if external_tables:
            for name, external_table in external_tables.items():
                url_params[f"{name}_format"] = "JSONCompactEachRow"
                url_params[f"{name}_structure"] = ", ".join(
                    f"{column} {column_type}" for column, column_type in external_table.structure
                )

        return url_params

    @staticmethod
    def build_converters(types: list[str]) -> list[Callable[[Any], Any]]:
        """Build converters for provided ClickHouse types (cached by type string)."""
        return [converter_for_ch_type(tp) for tp in types]

    def parse_row(self, names: list[str], converters: list[Callable[[Any], Any]], line: str) -> Row | None:
        """
        Parse row from ClickHouse response.

        Args:
            names (list[str]): Column names.
            converters (list[Callable[[Any], Any]]]): Pre-built converters for each column type.
            line (str): JSON line with values.

        Returns:
            Row | None: Parsed row or None if empty.
        """
        if line.strip():
            values = json.loads(line)
            converted_values = [convert(val) for val, convert in zip(values, converters, strict=True)]
            return Row(names, converted_values)

        return None
