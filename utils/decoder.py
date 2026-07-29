"""
Decoding and decompression utilities for F1 Live Timing SignalR payloads.

The F1 streaming protocol delivers data as JSON messages whose field values
are frequently **Base64-encoded** and **zlib-compressed** (raw DEFLATE, no
zlib header).  This module provides fast, synchronous helpers (the operations
are CPU-bound and very small) plus an async-friendly wrapper for use inside
async pipelines.

Typical usage::

    from utils.decoder import decode_payload

    raw_b64 = "eJxr ... =="
    data = decode_payload(raw_b64)   # -> dict | str | bytes
"""

from __future__ import annotations

import base64
import json
import logging
import zlib
from typing import Any, Union

logger = logging.getLogger(__name__)

# Type alias for the possible decoded results.
DecodedValue = Union[dict[str, Any], list[Any], str, bytes, None]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _base64_decode(data: str) -> bytes:
    """Decode a Base64 string, tolerating missing padding and whitespace."""
    # Strip whitespace that sometimes appears in streamed fragments.
    cleaned: str = data.strip().replace(" ", "")
    # Fix missing padding (Base64 length must be a multiple of 4).
    padding_needed: int = len(cleaned) % 4
    if padding_needed:
        cleaned += "=" * (4 - padding_needed)
    return base64.b64decode(cleaned)


def _zlib_decompress(data: bytes) -> bytes:
    """Decompress raw DEFLATE (zlib) bytes.

    The F1 protocol uses raw DEFLATE streams without a zlib wrapper, so we
    use ``wbits=-zlib.MAX_WBITS``.  We fall back to standard zlib and gzip
    decoding in case the payload includes a header.
    """
    try:
        return zlib.decompress(data, -zlib.MAX_WBITS)
    except zlib.error:
        # Try standard zlib (with header).
        try:
            return zlib.decompress(data)
        except zlib.error:
            # Try gzip.
            try:
                return zlib.decompress(data, zlib.MAX_WBITS | 16)
            except zlib.error as exc:
                logger.error("Failed to decompress payload (%d bytes): %s", len(data), exc)
                raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def decompress_bytes(data: str) -> bytes:
    """Base64-decode then zlib-decompress a payload string, returning raw bytes."""
    return _zlib_decompress(_base64_decode(data))


def decode_payload(data: str) -> DecodedValue:
    """Fully decode a Base64+zlib F1 payload into a Python object.

    Attempts JSON parsing first; if the result is plain text it is returned
    as a string, otherwise raw bytes are returned.
    """
    raw: bytes = decompress_bytes(data)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw


def decode_json(data: str) -> dict[str, Any] | list[Any] | None:
    """Decode a Base64+zlib payload that is expected to be JSON."""
    raw: bytes = decompress_bytes(data)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("Payload is not valid JSON: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Async wrapper (for use in async pipelines)
# ---------------------------------------------------------------------------

async def async_decode_payload(data: str) -> DecodedValue:
    """Async-friendly wrapper around :func:`decode_payload`.

    The actual work is CPU-bound and very fast, so we offload to a thread
    executor to avoid blocking the event loop when many payloads arrive.
    """
    import asyncio

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, decode_payload, data)


async def async_decode_json(data: str) -> dict[str, Any] | list[Any] | None:
    """Async-friendly wrapper around :func:`decode_json`."""
    import asyncio

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, decode_json, data)