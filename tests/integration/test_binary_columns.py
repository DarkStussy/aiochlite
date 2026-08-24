import pytest

from aiochlite import AsyncChClient, ChArgumentError, ChProtocolError

from ._types import TableFactory

pytestmark = [pytest.mark.asyncio, pytest.mark.clickhouse]

# A ClickHouse String is any sequence of bytes; this one is valid UTF-8 in no decoding.
_BINARY = bytes.fromhex("00ff fe01".replace(" ", ""))
_QUERY = (
    "SELECT unhex('00FFFE01') AS blob,"
    " CAST(unhex('00FFFE01') AS FixedString(4)) AS fixed,"
    " [unhex('00FF'), unhex('')] AS blobs,"
    " map(unhex('00FF'), unhex('FE')) AS blob_map,"
    " 'text' AS text"
)
_BINARY_COLUMNS = ["blob", "fixed", "blobs", "blob_map"]
_EXPECTED = (_BINARY, _BINARY, [b"\x00\xff", b""], {b"\x00\xff": b"\xfe"}, "text")


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_binary_columns_decode_as_bytes(ch_client: AsyncChClient):
    row = await ch_client.fetchone(_QUERY, binary_columns=_BINARY_COLUMNS)

    assert row is not None
    assert tuple(row.values()) == _EXPECTED


async def test_binary_columns_apply_to_every_row_returning_call(ch_client: AsyncChClient):
    rows = await ch_client.fetch(_QUERY, binary_columns=_BINARY_COLUMNS)
    tuples = await ch_client.fetch_rows(_QUERY, binary_columns=_BINARY_COLUMNS)
    streamed = [row async for row in ch_client.stream(_QUERY, binary_columns=_BINARY_COLUMNS)]
    streamed_tuples = [row async for row in ch_client.stream_rows(_QUERY, binary_columns=_BINARY_COLUMNS)]

    assert tuple(rows[0].values()) == _EXPECTED
    assert tuples[0] == _EXPECTED
    assert tuple(streamed[0].values()) == _EXPECTED
    assert streamed_tuples[0] == _EXPECTED


@pytest.mark.parametrize("ch_client", [False, True], ids=["eager", "lazy"], indirect=True)
async def test_a_binary_column_read_as_text_is_a_protocol_error(ch_client: AsyncChClient):
    # `fetchval` reads the value, which is where a lazy row decodes it.
    with pytest.raises(ChProtocolError, match="not UTF-8"):
        await ch_client.fetchval("SELECT unhex('00FFFE01') AS blob")


async def test_binary_columns_round_trip_through_a_table(ch_client: AsyncChClient, make_table: TableFactory):
    """Inserts go out as JSON, so binary data reaches the server as hex and is decoded there."""
    table = await make_table(id="UInt8", blob="String")
    await ch_client.execute(
        f"INSERT INTO {table} SELECT toUInt8({{id:UInt8}}), unhex({{blob:String}})",
        params={"id": 1, "blob": _BINARY.hex()},
    )

    value = await ch_client.fetchval(f"SELECT blob FROM {table}", binary_columns=["blob"])

    assert value == _BINARY


async def test_binary_columns_is_rejected_where_nothing_is_decoded(ch_client: AsyncChClient):
    with pytest.raises(ChArgumentError, match="calls that decode rows"):
        await ch_client.fetch_format("SELECT 1 AS one", "CSV", binary_columns=["one"])


async def test_a_column_the_query_did_not_select_is_rejected(ch_client: AsyncChClient):
    with pytest.raises(ChArgumentError, match="did not select: nope"):
        await ch_client.fetch("SELECT 'x' AS text", binary_columns=["nope"])
