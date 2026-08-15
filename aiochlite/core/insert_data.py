from typing import AsyncIterator, Iterator

# Bounds how much of an insert is held in memory at once. A single row above the limit is still
# sent whole, so it is a batching threshold rather than a hard cap.
_BATCH_BYTES = 256 * 1024


async def build_insert_body(statement: str, rows: Iterator[str]) -> AsyncIterator[bytes]:
    """Stream an insert payload in batches; rows are serialized as the request is sent."""
    yield statement.encode()

    batch: list[bytes] = []
    size = 0
    for row in rows:
        # Encoded upfront: the threshold is in bytes, and one character is not one byte.
        encoded = row.encode()
        batch.append(encoded)
        size += len(encoded) + 1
        if size >= _BATCH_BYTES:
            yield b"\n".join(batch) + b"\n"
            batch.clear()
            size = 0

    if batch:
        yield b"\n".join(batch) + b"\n"
