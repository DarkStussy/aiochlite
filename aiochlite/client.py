from collections.abc import AsyncIterator
from typing import Any, Mapping, Self, Sequence, TypedDict, Unpack

from aiohttp import ClientSession, FormData

from .converters import to_json
from .converters.rowbinary import (
    RowBinaryWithNamesAndTypesStreamParser,
    parse_rowbinary_with_names_and_types,
    parse_rowbinary_with_names_and_types_lazy,
)
from .core import ChClientCore, ClientCoreOptions, ExternalTable, Row, build_external_data
from .exceptions import ChClientError
from .http_client import HttpClient


class QueryOptions(TypedDict, total=False):
    """Options for ClickHouse query execution."""

    params: Mapping[str, Any] | None
    settings: Mapping[str, Any] | None
    external_tables: dict[str, ExternalTable] | None


class AsyncChClient:
    """
    Asynchronous ClickHouse HTTP client.

    Args:
        url (str): ClickHouse server URL.
        session (ClientSession | None): Optional aiohttp session to use.
        lazy_decode (bool): If True, decode row values lazily per cell (faster if you access only a subset of columns).
        user (str): ClickHouse username.
        password (str): ClickHouse password.
        database (str): Default database name.
        enable_compression (bool): Enable HTTP compression.
    """

    __slots__ = ("_core", "_database", "_http_client", "_lazy_decode", "_url")

    def __init__(
        self,
        url: str = "http://localhost:8123",
        *,
        session: ClientSession | None = None,
        lazy_decode: bool = True,
        **kwargs: Unpack[ClientCoreOptions],
    ):
        self._url = url
        self._database = kwargs.get("database", "default")
        self._lazy_decode = lazy_decode
        self._core = ChClientCore(**kwargs)

        headers = self._core.build_headers()
        session = session or ClientSession()
        session.headers.update(headers)
        self._http_client = HttpClient(session)

    async def __aenter__(self) -> Self:
        await self.ping(raise_on_error=True)
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        """Close the underlying HTTP client session."""
        await self._http_client.close()

    async def ping(self, *, raise_on_error: bool = False) -> bool:
        """Check if ClickHouse server is reachable.

        Args:
            raise_on_error (bool): Whether to raise exception on connection failure.

        Returns:
            bool: True if server is alive, False otherwise.

        Raises:
            ChClientError: If raise_on_error is True and connection fails.
        """
        try:
            await self._http_client.get(self._url, params={**self._core.build_query_params(), "query": "SELECT 1"})
        except ChClientError:
            if raise_on_error:
                raise

            return False

        return True

    def _prepare_query(
        self,
        query: str,
        *,
        add_format: bool = True,
        **kwargs: Unpack[QueryOptions],
    ) -> tuple[dict[str, Any], str | FormData]:
        """Prepare query for execution by adding FORMAT clause (when needed) and building params."""
        if add_format:
            if "format" in query.lower():
                raise ValueError("The query must not contain a FORMAT clause.")

            query = f"{query} FORMAT RowBinaryWithNamesAndTypes"

        params = self._core.build_query_params(**kwargs)

        if external_tables := kwargs.get("external_tables"):
            data = FormData()
            for external_data in build_external_data(external_tables):
                data.add_field(
                    name=external_data.name,
                    value=external_data.content,
                    filename=external_data.filename,
                    content_type=external_data.content_type,
                )

            params["query"] = query
        else:
            data = query

        return params, data

    async def _stream(self, params: dict[str, Any], data: str | FormData) -> AsyncIterator[Row]:
        byte_chunks = self._http_client.stream(self._url, params=params, data=data)
        parser = RowBinaryWithNamesAndTypesStreamParser(byte_chunks, lazy=self._lazy_decode)
        names, _ = await parser.read_header()

        index = {name: idx for idx, name in enumerate(names)}
        async for values in parser.rows():
            yield Row(names, values, index=index)

    async def _fetch(self, params: dict[str, Any], data: str | FormData) -> list[Row]:
        payload = await self._http_client.read(self._url, params=params, data=data)
        names, _, rows = (
            parse_rowbinary_with_names_and_types_lazy(payload)
            if self._lazy_decode
            else parse_rowbinary_with_names_and_types(payload)
        )

        index = {name: idx for idx, name in enumerate(names)}
        return [Row(names, values, index=index) for values in rows]

    async def execute(self, query: str, **kwargs: Unpack[QueryOptions]):
        """Execute query without returning results.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, add_format=False, **kwargs)
        await self._http_client.post(self._url, params=params, data=data)

    async def stream(self, query: str, **kwargs: Unpack[QueryOptions]) -> AsyncIterator[Row]:
        """Execute query and iterate over results.

        Yields:
            Row: Query result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, **kwargs)
        async for row in self._stream(params, data):
            yield row

    async def fetch(self, query: str, **kwargs: Unpack[QueryOptions]) -> list[Row]:
        """Execute query and fetch all results.

        Returns:
            list[Row]: List of all result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, **kwargs)
        return await self._fetch(params, data)

    async def fetchone(self, query: str, **kwargs: Unpack[QueryOptions]) -> Row | None:
        """Execute query and fetch first result row.

        Returns:
            Row | None: First row or None if no results.

        Raises:
            ChClientError: If query execution fails.
        """
        async for row in self.stream(query, **kwargs):
            return row

        return None

    async def fetchval(self, query: str, **kwargs: Unpack[QueryOptions]) -> Any:
        """Execute query and fetch first column of first row.

        Returns:
            Any: First column value or None if no results.

        Raises:
            ChClientError: If query execution fails.
        """
        if row := await self.fetchone(query, **kwargs):
            return row.first()

        return None

    async def insert(
        self,
        table: str,
        data: Sequence[dict[str, Any]] | list[tuple[Any, ...]],
        *,
        database: str | None = None,
        column_names: Sequence[str] | None = None,
        settings: Mapping[str, Any] | None = None,
    ):
        """Insert data into a ClickHouse table.

        Args:
            table (str): Table name.
            data (Sequence[dict[str, Any]] | list[tuple[Any, ...]]): Rows to insert.
            database (str | None): Database name (uses default if None).
            column_names (Sequence[str] | None): Column names for tuple data.
            settings (Mapping[str, Any] | None): ClickHouse settings.

        Raises:
            ChClientError: If insertion fails.
        """
        if not data:
            return

        db = database or self._database

        columns_clause = f" ({', '.join(column_names)})" if column_names else ""

        if isinstance(data[0], dict):
            format_name = "JSONEachRow"
            body = "\n".join(to_json(row) for row in data)
        else:
            format_name = "JSONCompactEachRow"
            body = "\n".join(to_json(list(row)) for row in data)

        await self._http_client.post(
            self._url,
            params=self._core.build_query_params(settings=settings),
            data=f"INSERT INTO {db}.{table}{columns_clause} FORMAT {format_name}\n{body}",
        )
