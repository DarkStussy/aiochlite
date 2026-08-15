from __future__ import annotations

import pytest
from aiohttp import ClientSession

from aiochlite import AsyncChClient, ChClientError, ChProtocolError, ChServerError, ChTransportError

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]

# Fails only after rows are already on the wire, so the server answers 200 and appends the error
# to the body instead of reporting it in the status.
_LATE_FAILURE = "SELECT number, throwIf(number = 200) FROM numbers(1000) SETTINGS max_block_size = 1"


async def test_fetch_reports_exception_written_into_the_body(ch_client: AsyncChClient):
    with pytest.raises(ChClientError, match="FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"):
        await ch_client.fetch(_LATE_FAILURE)


async def test_fetch_rows_reports_exception_written_into_the_body(ch_client: AsyncChClient):
    with pytest.raises(ChClientError, match="FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"):
        await ch_client.fetch_rows(_LATE_FAILURE)


async def test_stream_reports_exception_written_into_the_body(ch_client: AsyncChClient):
    """Without the check this ends as a short, silently truncated result instead of an error."""
    with pytest.raises(ChClientError, match="FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"):
        async for _ in ch_client.stream(_LATE_FAILURE):
            pass


async def test_stream_format_reports_exception_written_into_the_body(ch_client: AsyncChClient):
    with pytest.raises(ChClientError, match="FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"):
        async for _ in ch_client.stream_format(_LATE_FAILURE, "Parquet"):
            pass


async def test_execute_reports_exception_written_into_the_body(ch_client: AsyncChClient):
    with pytest.raises(ChClientError, match="FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"):
        await ch_client.execute(_LATE_FAILURE)


async def test_late_failure_reports_the_error_not_the_payload(ch_client: AsyncChClient):
    """A query failing after megabytes of rows must not report those rows as the error text."""
    query = (
        "SELECT number, repeat('x', 500) AS s, throwIf(number = 5000) FROM numbers(10000) SETTINGS max_block_size = 100"
    )
    with pytest.raises(ChClientError) as error:
        async for _ in ch_client.stream(query):
            pass

    message = str(error.value)
    assert "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" in message
    assert message.startswith("Code: ")
    assert len(message) < 2_000


async def test_large_result_still_streams_to_the_end(ch_client: AsyncChClient):
    """A payload spanning many chunks must survive the sentinel scan untouched."""
    total = 0
    async for row in ch_client.stream("SELECT number, repeat('x', 200) AS s FROM numbers(50000)"):
        total += 1
        assert len(row["s"]) == 200

    assert total == 50000


async def test_unreachable_server_raises_transport_error():
    """aiohttp exceptions must not cross the client boundary."""
    client = AsyncChClient("http://localhost:9")
    try:
        with pytest.raises(ChTransportError):
            await client.fetch("SELECT 1")

        assert await client.ping() is False
        with pytest.raises(ChTransportError):
            await client.ping(raise_on_error=True)
    finally:
        await client.close()


async def test_server_error_carries_metadata(ch_client: AsyncChClient):
    with pytest.raises(ChServerError) as error:
        await ch_client.fetch("SELEC 1")

    assert error.value.status == 400
    assert error.value.code == 62
    assert error.value.query_id


async def test_failed_context_entry_closes_the_session():
    """__aexit__ never runs when __aenter__ raises, so the session has to be closed there."""
    session = ClientSession()
    with pytest.raises(ChTransportError):
        async with AsyncChClient("http://localhost:9", session=session):
            pass

    assert session.closed


@pytest.mark.parametrize("ch_client", [True], ids=["lazy"], indirect=True)
async def test_lazy_decoding_reports_a_protocol_error(ch_client: AsyncChClient):
    """Lazy cells decode on access, well after the query call returned."""
    rows = await ch_client.fetch("SELECT reinterpretAsString(toUInt16(65533)) AS s")
    with pytest.raises(ChProtocolError):
        rows[0]["s"]
