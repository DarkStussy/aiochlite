from aiochlite import AsyncChClient


async def test_lazy_decode_is_disabled_by_default():
    """Rows decode eagerly unless the caller opts in; flipping this changes when errors surface."""
    client = AsyncChClient()
    try:
        assert client._lazy_decode is False
    finally:
        await client.close()
