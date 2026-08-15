class ChClientError(Exception):
    """ClickHouse query execution error."""


class ChTransportError(ChClientError):
    """The request never got a usable answer: connection refused, timeout, truncated response."""


class ChProtocolError(ChClientError):
    """The server answered, but the payload could not be decoded in the requested format."""


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
