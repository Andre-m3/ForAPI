"""
SignalR client skeleton for ingesting live F1 timing/telemetry data.

This module provides a placeholder async client that will eventually connect to
the official F1 SignalR stream via aiohttp. For now it implements the connection
and listening lifecycle with a dummy loop so the rest of the backend can be
developed and tested independently.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Final

import aiohttp

logger = logging.getLogger(__name__)

# Placeholder endpoint — the real F1 SignalR negotiation URL will be wired in later.
DEFAULT_HUB_URL: Final[str] = "https://livetiming.formula1.com/signalr"


class SignalRClient:
    """Async SignalR client using aiohttp.

    Attributes:
        hub_url: The SignalR hub endpoint to negotiate/connect against.
        session: The underlying aiohttp ClientSession, created on connect().
        running: Flag controlling the listen loop lifecycle.
    """

    def __init__(self, hub_url: str = DEFAULT_HUB_URL) -> None:
        self.hub_url: str = hub_url
        self.session: aiohttp.ClientSession | None = None
        self.running: bool = False

    async def connect(self) -> None:
        """Open an aiohttp session and (placeholder) negotiate the SignalR connection.

        Raises:
            RuntimeError: If connect() is called while a session is already open.
        """
        if self.session is not None and not self.session.closed:
            raise RuntimeError("SignalRClient is already connected.")

        logger.info("Connecting to SignalR hub: %s", self.hub_url)
        self.session = aiohttp.ClientSession()
        # TODO: Implement SignalR negotiation handshake here.
        logger.info("SignalR connection established (placeholder).")

    async def listen(self) -> None:
        """Continuously listen to the incoming F1 data stream.

        Runs a dummy asyncio loop for now. In production this will read
        decompressed SignalR payloads and dispatch them to the in-memory cache
        and SQLite persistence layers.
        """
        if self.session is None:
            raise RuntimeError("Cannot listen before connecting. Call connect() first.")

        self.running = True
        logger.info("Listening to stream...")
        try:
            while self.running:
                # TODO: Replace with real SignalR message handling.
                print("Listening to stream...")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            logger.info("Listen loop cancelled.")
            raise
        finally:
            self.running = False

    async def disconnect(self) -> None:
        """Gracefully close the aiohttp session and stop the listen loop."""
        self.running = False
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("SignalR session closed.")
        self.session = None