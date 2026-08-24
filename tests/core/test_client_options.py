import pytest

from aiochlite import AsyncChClient
from aiochlite.core import ChClientCore


async def test_lazy_decode_is_disabled_by_default():
    """Rows decode eagerly unless the caller opts in; flipping this changes when errors surface."""
    client = AsyncChClient()
    try:
        assert client._lazy_decode is False
    finally:
        await client.close()


async def test_a_misspelled_query_option_is_rejected():
    """`binary_columns` is filtered out of the options; the rest still have to be real."""
    client = AsyncChClient()
    try:
        with pytest.raises(TypeError, match="settngs"):
            await client.fetch("SELECT 1", settngs={"max_rows_to_read": 1})  # pyright: ignore[reportCallIssue]
    finally:
        await client.close()


def test_json_integers_are_not_quoted():
    """Servers before 25.8 quote 64-bit integers, which would decode a JSON number as a string."""
    assert ChClientCore().build_query_params()["output_format_json_quote_64bit_integers"] == 0


def test_settings_override_the_client_defaults():
    """The defaults the client sets for its own decoding are still the caller's to change."""
    params = ChClientCore().build_query_params(settings={"output_format_json_quote_64bit_integers": 1})

    assert params["output_format_json_quote_64bit_integers"] == 1
