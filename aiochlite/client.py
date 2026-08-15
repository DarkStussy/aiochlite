import re
import warnings
from collections.abc import AsyncIterator
from contextlib import contextmanager
from typing import Any, Generator, Literal, Mapping, Self, Sequence, TypedDict, Unpack

from aiohttp import ClientSession, FormData, TCPConnector

from .converters import quote_identifier
from .converters._type_parsing import parse_timezone
from .converters.rowbinary import (
    RowBinaryWithNamesAndTypesStreamParser,
    parse_rowbinary_with_names_and_types,
    parse_rowbinary_with_names_and_types_lazy,
)
from .core import (
    ChClientCore,
    ClientCoreOptions,
    ExternalTable,
    InsertData,
    Row,
    build_external_data,
    build_insert_body,
    serialize_rows,
    take_first_row,
)
from .exceptions import ChClientError, ChProtocolError
from .http_client import HttpClient

_COMMENT_OR_LITERAL_RE = re.compile(
    r"'(?:\\.|[^'\\])*'"  # single-quoted string
    r'|"(?:\\.|[^"\\])*"'  # double-quoted identifier
    r"|`[^`]*`"  # backquoted identifier
    r"|--[^\n]*"  # line comment
    r"|/\*.*?\*/",  # block comment
    re.DOTALL,
)

_FORMAT_CLAUSE_RE = re.compile(
    r"\bformat\s+[a-z0-9_]+\s*(?:\bsettings\b.*)?;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

ExportFormat = Literal[
    # Columnar / binary
    "Parquet",
    "Arrow",
    "ArrowStream",
    "ORC",
    "Avro",
    "Native",
    "RowBinary",
    "RowBinaryWithNames",
    "RowBinaryWithNamesAndTypes",
    # Separated values
    "CSV",
    "CSVWithNames",
    "CSVWithNamesAndTypes",
    "TSV",
    "TSVWithNames",
    "TSVWithNamesAndTypes",
    "TabSeparated",
    "TabSeparatedWithNames",
    "TabSeparatedWithNamesAndTypes",
    "TSKV",
    "Values",
    # JSON
    "JSON",
    "JSONStrings",
    "JSONCompact",
    "JSONColumns",
    "JSONEachRow",
    "JSONStringsEachRow",
    "JSONObjectEachRow",
    "JSONCompactEachRow",
    "JSONCompactEachRowWithNames",
    "JSONCompactEachRowWithNamesAndTypes",
    # Human-readable / markup
    "XML",
    "Markdown",
    "Vertical",
    "Pretty",
    "PrettyCompact",
]
"""ClickHouse output formats for `fetch_format()` / `stream_format()`."""


def _has_format_clause(query: str) -> bool:
    """Check whether the query ends with a `FORMAT <name>` clause, optionally followed by `SETTINGS ...`.

    Comments and string/identifier literals are stripped before matching.

    Args:
        query (str): Query to inspect.

    Returns:
        bool: True if a FORMAT clause is present.
    """
    return _FORMAT_CLAUSE_RE.search(_COMMENT_OR_LITERAL_RE.sub(" ", query)) is not None


@contextmanager
def _decoding() -> Generator[None, None, None]:
    """A payload that fails to decode is a bad response, not a bad call."""
    try:
        yield
    except ValueError as error:
        raise ChProtocolError(str(error)) from error


def _warn_deprecated(old: str, new: str):
    """Warn that `old` is deprecated in favor of `new`.

    Args:
        old (str): Deprecated method name.
        new (str): Replacement call to use instead.
    """
    warnings.warn(
        f"{old}() is deprecated and will be removed in a future release, use {new} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


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
        user (str): ClickHouse username.
        password (str): ClickHouse password.
        database (str): Default database name.
        verify (bool): Verify SSL certificate. Ignored when `session` is given, which brings
            its own connector and with it its own SSL policy.
        lazy_decode (bool): If True, decode row values lazily per cell. Worth it only when a
            small share of the selected columns is read; where exactly it breaks even depends on
            how expensive the skipped columns are to decode.
        enable_compression (bool): Enable HTTP compression.
        session (ClientSession | None): Session to send requests through. A session passed here
            stays the caller's: `close()` leaves it open, and its headers are left untouched.
    """

    __slots__ = ("_core", "_database", "_http_client", "_lazy_decode", "_url")

    def __init__(
        self,
        url: str = "http://localhost:8123",
        *,
        verify: bool = True,
        session: ClientSession | None = None,
        lazy_decode: bool = False,
        **kwargs: Unpack[ClientCoreOptions],
    ):
        self._url = url
        self._database = kwargs.get("database", "default")
        self._lazy_decode = lazy_decode
        self._core = ChClientCore(**kwargs)

        self._http_client = HttpClient(
            session if session is not None else ClientSession(connector=TCPConnector(ssl=verify)),
            headers=self._core.build_headers(),
            owns_session=session is None,
        )

    async def __aenter__(self) -> Self:
        try:
            await self.ping(raise_on_error=True)
        except ChClientError:
            await self.close()
            raise

        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        """Close the session the client opened. A session passed to the constructor is left alone."""
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
        format_name: str | None = "RowBinaryWithNamesAndTypes",
        **kwargs: Unpack[QueryOptions],
    ) -> tuple[dict[str, Any], str | FormData]:
        """Prepare query for execution by adding FORMAT clause (when needed) and building params."""
        if format_name is not None:
            if _has_format_clause(query):
                raise ValueError("The query must not contain a FORMAT clause.")

            query = f"{query} FORMAT {format_name}"

        params = self._core.build_query_params(**kwargs)

        data: str | FormData
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
        async with self._http_client.stream(self._url, params=params, data=data) as (tz, byte_chunks):
            parser = RowBinaryWithNamesAndTypesStreamParser(
                byte_chunks,
                lazy=self._lazy_decode,
                server_tz=parse_timezone(tz),
            )
            with _decoding():
                names, _ = await parser.read_header()

                index = {name: idx for idx, name in enumerate(names)}
                async for values in parser.rows():
                    yield Row(names, values, index=index)

    async def _fetch(self, params: dict[str, Any], data: str | FormData) -> list[Row]:
        payload, tz = await self._http_client.read(self._url, params=params, data=data)
        server_tz = parse_timezone(tz)
        with _decoding():
            names, _, rows = (
                parse_rowbinary_with_names_and_types_lazy(payload, server_tz)
                if self._lazy_decode
                else parse_rowbinary_with_names_and_types(payload, server_tz)
            )

            index = {name: idx for idx, name in enumerate(names)}
            return [Row(names, values, index=index) for values in rows]

    async def execute(self, query: str, **kwargs: Unpack[QueryOptions]):
        """Execute query without returning results.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, format_name=None, **kwargs)
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

    async def stream_rows(self, query: str, **kwargs: Unpack[QueryOptions]) -> AsyncIterator[tuple[Any, ...]]:
        """Execute query and iterate over results as raw tuples (no `Row` wrapper).

        Yields:
            tuple: Query result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, **kwargs)
        async with self._http_client.stream(self._url, params=params, data=data) as (tz, byte_chunks):
            parser = RowBinaryWithNamesAndTypesStreamParser(byte_chunks, lazy=False, server_tz=parse_timezone(tz))
            with _decoding():
                await parser.read_header()

                async for values in parser.rows():
                    yield tuple(values)

    async def fetch(self, query: str, **kwargs: Unpack[QueryOptions]) -> list[Row]:
        """Execute query and fetch all results.

        Returns:
            list[Row]: List of all result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, **kwargs)
        return await self._fetch(params, data)

    async def fetch_rows(self, query: str, **kwargs: Unpack[QueryOptions]) -> list[tuple[Any, ...]]:
        """Execute query and fetch all results as raw tuples (no `Row` wrapper).

        Returns:
            list[tuple]: List of all result rows.

        Raises:
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, **kwargs)
        payload, tz = await self._http_client.read(self._url, params=params, data=data)
        with _decoding():
            _, _, rows = parse_rowbinary_with_names_and_types(payload, parse_timezone(tz), as_tuple=True)
            return list(rows)

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

    async def fetch_format(self, query: str, format_name: ExportFormat, **kwargs: Unpack[QueryOptions]) -> bytes:
        """Execute query and fetch the whole result encoded in the given ClickHouse output format.

        Args:
            query (str): Query without a FORMAT clause.
            format_name (ExportFormat): ClickHouse output format, e.g. `"Parquet"`, `"CSVWithNames"`, `"JSONEachRow"`.

        Returns:
            bytes: Undecoded result payload as returned by the server.

        Raises:
            ValueError: If the query contains a FORMAT clause.
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, format_name=format_name, **kwargs)
        payload, _ = await self._http_client.read(self._url, params=params, data=data)
        return payload

    async def stream_format(
        self,
        query: str,
        format_name: ExportFormat,
        **kwargs: Unpack[QueryOptions],
    ) -> AsyncIterator[bytes]:
        """Execute query and stream the result encoded in the given ClickHouse output format.

        Args:
            query (str): Query without a FORMAT clause.
            format_name (ExportFormat): ClickHouse output format, e.g. `"Parquet"`, `"CSVWithNames"`, `"JSONEachRow"`.

        Yields:
            bytes: Undecoded payload chunks as returned by the server.

        Raises:
            ValueError: If the query contains a FORMAT clause.
            ChClientError: If query execution fails.
        """
        params, data = self._prepare_query(query, format_name=format_name, **kwargs)
        async with self._http_client.stream(self._url, params=params, data=data) as (_, byte_chunks):
            async for chunk in byte_chunks:
                yield chunk

    async def fetch_parquet(self, query: str, **kwargs: Unpack[QueryOptions]) -> bytes:
        """Execute query and fetch all results as Parquet-encoded bytes.

        Deprecated:
            Use `fetch_format(query, "Parquet")` instead.

        Returns:
            bytes: Parquet-encoded result payload.

        Raises:
            ValueError: If the query contains a FORMAT clause.
            ChClientError: If query execution fails.
        """
        _warn_deprecated("fetch_parquet", 'fetch_format(query, "Parquet")')
        return await self.fetch_format(query, "Parquet", **kwargs)

    def stream_parquet(self, query: str, **kwargs: Unpack[QueryOptions]) -> AsyncIterator[bytes]:
        """Execute query and stream Parquet-encoded bytes in chunks.

        Deprecated:
            Use `stream_format(query, "Parquet")` instead.

        Returns:
            AsyncIterator[bytes]: Iterator over Parquet-encoded payload chunks.

        Raises:
            ValueError: If the query contains a FORMAT clause.
            ChClientError: If query execution fails.
        """
        _warn_deprecated("stream_parquet", 'stream_format(query, "Parquet")')
        return self.stream_format(query, "Parquet", **kwargs)

    async def insert(
        self,
        table: str,
        data: InsertData,
        *,
        database: str | None = None,
        column_names: Sequence[str] | None = None,
        settings: Mapping[str, Any] | None = None,
    ):
        """Insert data into a ClickHouse table.

        Rows are serialized and sent as the request goes out, so an iterator or async iterator
        can be inserted without holding all of it in memory. An exception from the source
        propagates unchanged, leaving the rows sent before it inserted.

        Args:
            table (str): Table name.
            data (InsertData): Rows to insert, as dicts or tuples; the first row decides how the
                rest are read.
            database (str | None): Database name (uses default if None).
            column_names (Sequence[str] | None): Column names for tuple data.
            settings (Mapping[str, Any] | None): ClickHouse settings.

        Raises:
            ChClientError: If insertion fails.
        """
        taken = await take_first_row(data)
        if taken is None:
            return

        format_name, rows = serialize_rows(*taken)

        columns_clause = f" ({', '.join(quote_identifier(name) for name in column_names)})" if column_names else ""
        statement = f"INSERT INTO {{_db:Identifier}}.{{_table:Identifier}}{columns_clause} FORMAT {format_name}\n"

        await self._http_client.post(
            self._url,
            params=self._core.build_query_params(
                params={"_db": database or self._database, "_table": table},
                settings=settings,
            ),
            data=build_insert_body(statement, rows),
        )
