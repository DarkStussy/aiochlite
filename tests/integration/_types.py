from typing import Awaitable, Protocol, TypedDict


class ChConfig(TypedDict):
    url: str
    user: str
    password: str


class TableFactory(Protocol):
    def __call__(self, **schema: str) -> Awaitable[str]: ...
