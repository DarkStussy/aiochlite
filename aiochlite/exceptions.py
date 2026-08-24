class ChClientError(Exception):
    """ClickHouse query execution error."""


class _SourceError(Exception):
    """
    Carries an exception raised by a user-supplied row source.

    aiohttp turns anything raised while sending the body into a connection error, which would
    report a failed insert as a network problem. The transport boundary unwraps this instead.
    """

    __slots__ = ("cause",)

    def __init__(self, cause: BaseException):
        super().__init__(str(cause))
        self.cause = cause


class ChTransportError(ChClientError):
    """The request never got a usable answer: connection refused, timeout, truncated response."""


class ChProtocolError(ChClientError):
    """The server answered, but the payload could not be decoded in the requested format."""


class ChArgumentError(ChClientError, ValueError):
    """
    A query option that does not fit the query, such as ``binary_columns`` naming a column
    the query did not select.

    A ``ValueError`` like any other bad argument, and a ``ChClientError`` like every other
    failure of a call.
    """


class ChServerError(ChClientError):
    """
    Error reported by ClickHouse, either in the HTTP status or inside a ``200 OK`` body.

    Attributes:
        status (int | None): HTTP status of the response.
        code (int | None): ClickHouse error code, from the header or the message.
        query_id (str | None): Server-assigned query id.
        exception_tag (str | None): Per-response tag marking the exception block in the body.
    """

    __slots__ = ("code", "exception_tag", "query_id", "status")

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        code: int | None = None,
        query_id: str | None = None,
        exception_tag: str | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        self.query_id = query_id
        self.exception_tag = exception_tag
