import json
from typing import Any, AsyncIterator, Mapping, Self, Sequence, TypedDict, Unpack

from aiohttp import ClientSession, FormData

from .converters import to_json
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
        params (Mapping[str, Any] | None): Query parameters for substitution.
        settings (Mapping[str, Any] | None): ClickHouse settings for the query.
        external_tables (dict[str, ExternalTable] | None): External tables to attach.
    """

    __slots__ = ("_core", "_database", "_http_client", "_url")

    def __init__(
        self,
        url: str = "http://localhost:8123",
        session: ClientSession | None = None,
        **kwargs: Unpack[ClientCoreOptions],
    ):
        self._url = url
        self._database = kwargs.get("database", "default")
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

            query = f"{query} FORMAT JSONCompactEachRowWithNamesAndTypes"

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
        lines = self._http_client.stream(self._url, params=params, data=data)
        names = json.loads(await anext(lines))
        types = json.loads(await anext(lines))

        async for line in lines:
            if row := self._core.parse_row(names, types, line):
                yield row

    async def fetch(self, query: str, **kwargs: Unpack[QueryOptions]) -> list[Row]:
        """Execute query and fetch all results.

        Returns:
            list[Row]: List of all result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        return [row async for row in self.stream(query, **kwargs)]

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
