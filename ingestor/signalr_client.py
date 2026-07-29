"""
Fully functional async SignalR client for the F1 Live Timing stream.

Implements the complete connection lifecycle:
  1. HTTP negotiation to obtain a connection token.
  2. WebSocket upgrade with the correct headers (User-Agent, Origin, etc.).
  3. SignalR handshake (protocol version 1.5 / JSON).
  4. Subscription to the ``Streaming`` hub for standard F1 channels.
  5. Continuous message loop with decompression and in-memory state updates.

The F1 Live Timing SignalR endpoint uses the older ASP.NET SignalR protocol
(not the newer ASP.NET Core SignalR), so the negotiation and message framing
follow that specification.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Final

import aiohttp

from utils.decoder import decode_json
from utils.state_manager import state

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HUB_URL: Final[str] = "https://livetiming.formula1.com/signalr"

# Standard channels subscribed to on the "Streaming" hub.
DEFAULT_CHANNELS: Final[list[str]] = [
    "Heartbeat",
    "TimingData",
    "TimingStats",
    "CarData.z",
    "Position.z",
    "ExtrapolatedClock",
    "TopThree",
    "RaceControlMessages",
    "TimingAppData",
    "DriverList",
    "LapCount",
    "SessionInfo",
    "SessionStatus",
    "TeamRadio",
    "WeatherData",
]

# Headers required to avoid 403 Forbidden from the F1 servers.
DEFAULT_HEADERS: Final[dict[str, str]] = {
    "User-Agent": "BestHTTP",
    "Accept": "application/json",
    "Origin": "https://livetiming.formula1.com",
    "Host": "livetiming.formula1.com",
}

# SignalR protocol version used by the F1 endpoint.
PROTOCOL_VERSION: Final[str] = "1.5"


# ---------------------------------------------------------------------------
# SignalR Client
# ---------------------------------------------------------------------------

class SignalRClient:
    """Async SignalR client for the F1 Live Timing stream.

    Attributes:
        hub_url: The base SignalR hub URL.
        connection_token: Token obtained during negotiation.
        session: The aiohttp ClientSession used for HTTP + WebSocket.
        ws: The active WebSocket connection.
        running: Flag controlling the listen loop.
        channels: List of channel names to subscribe to.
    """

    def __init__(
        self,
        hub_url: str = HUB_URL,
        channels: list[str] | None = None,
    ) -> None:
        self.hub_url: str = hub_url.rstrip("/")
        self.connection_token: str | None = None
        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.running: bool = False
        self.channels: list[str] = channels if channels is not None else list(DEFAULT_CHANNELS)

    # ------------------------------------------------------------------
    # 1. Negotiation
    # ------------------------------------------------------------------

    async def _negotiate(self) -> str:
        """Perform the HTTP negotiation phase and return the connection token.

        Sends a POST to ``/negotiate`` with the required query parameters and
        headers.  The F1 endpoint responds with a JSON body containing the
        ``ConnectionToken``.
        """
        negotiate_url: str = f"{self.hub_url}/negotiate"
        params: dict[str, str] = {
            "connectionData": json.dumps([{"Name": "Streaming"}]),
            "clientProtocol": PROTOCOL_VERSION,
        }

        assert self.session is not None, "Session must be created before negotiating."

        logger.info("Negotiating SignalR connection at %s", negotiate_url)
        async with self.session.post(
            negotiate_url,
            params=params,
            headers=DEFAULT_HEADERS,
        ) as resp:
            if resp.status != 200:
                body: str = await resp.text()
                raise ConnectionError(
                    f"Negotiation failed (HTTP {resp.status}): {body[:500]}"
                )

            data: dict[str, Any] = await resp.json()
            token: str | None = data.get("ConnectionToken")
            if not token:
                raise ConnectionError("No ConnectionToken in negotiation response.")

            logger.info("Negotiation successful. ConnectionToken received.")
            return token

    # ------------------------------------------------------------------
    # 2. WebSocket connection
    # ------------------------------------------------------------------

    async def _connect_websocket(self) -> None:
        """Open the WebSocket connection to the SignalR hub."""
        assert self.connection_token is not None, "Connection token required."
        assert self.session is not None, "Session must be created first."

        ws_url: str = self.hub_url.replace("https://", "wss://")
        params: dict[str, str] = {
            "connectionToken": self.connection_token,
            "connectionData": json.dumps([{"Name": "Streaming"}]),
            "clientProtocol": PROTOCOL_VERSION,
            "transport": "webSockets",
        }

        logger.info("Opening WebSocket connection to %s", ws_url)
        self.ws = await self.session.ws_connect(
            ws_url,
            params=params,
            headers=DEFAULT_HEADERS,
            heartbeat=30.0,
        )
        logger.info("WebSocket connection established.")

    # ------------------------------------------------------------------
    # 3. SignalR handshake
    # ------------------------------------------------------------------

    async def _send_handshake(self) -> None:
        """Send the initial SignalR handshake message."""
        assert self.ws is not None, "WebSocket must be connected first."

        handshake_msg: dict[str, Any] = {
            "H": "Streaming",
            "M": "Subscribe",
            "A": [self.channels],
            "I": 0,
        }
        await self.ws.send_str(json.dumps(handshake_msg))
        logger.info("Handshake sent. Subscribing to %d channels.", len(self.channels))

    # ------------------------------------------------------------------
    # 4. Message handling
    # ------------------------------------------------------------------

    async def _handle_message(self, raw_data: str) -> None:
        """Parse and process a single SignalR message.

        F1 SignalR messages are JSON arrays of message objects.  Each object
        has ``H`` (hub), ``M`` (method), and ``A`` (arguments) fields.  The
        first argument is typically a dictionary with ``R`` (reference) and
        ``M`` (messages) keys, where each message has an ``A`` (channel name)
        and ``I`` (data payload) field that may be Base64+zlib compressed.
        """
        try:
            messages: list[dict[str, Any]] = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.debug("Non-JSON message received: %s", raw_data[:200])
            return

        if not isinstance(messages, list):
            messages = [messages]

        for msg in messages:
            method: str | None = msg.get("M")
            if method is None:
                # Could be a handshake response or error.
                if "S" in msg:
                    logger.info("SignalR handshake acknowledged (status: %s).", msg["S"])
                elif "E" in msg:
                    logger.error("SignalR error: %s", msg["E"])
                continue

            args: list[Any] = msg.get("A", [])
            if not args:
                continue

            # The first argument is a dict with "R" and "M" keys.
            payload: dict[str, Any] = args[0] if isinstance(args[0], dict) else {}

            # Process individual channel updates within the payload.
            # Each update has "A" (channel name) and "I" (data payload).
            updates: list[dict[str, Any]] = payload.get("M", [])
            for update in updates:
                channel: str = update.get("A", "")
                data_field: Any = update.get("I")

                if not channel:
                    continue

                # Decompress and store the data.
                await self._process_channel_update(channel, data_field)

    async def _process_channel_update(self, channel: str, data: Any) -> None:
        """Decode (if needed) and store a channel update in the state manager."""
        try:
            if isinstance(data, str) and data:
                # Channel data is Base64 + zlib compressed.
                if channel.endswith(".z"):
                    # Compressed channels (CarData.z, Position.z).
                    decoded: dict[str, Any] | list[Any] | None = decode_json(data)
                    base_channel: str = channel[:-2]  # Remove ".z" suffix.
                    if decoded is not None:
                        await state.set(base_channel, decoded)
                        logger.debug("Updated state: %s (%d bytes decoded)", base_channel, len(data))
                else:
                    # Some channels send plain JSON strings, others send compressed.
                    try:
                        decoded = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        decoded = decode_json(data)

                    if decoded is not None:
                        if isinstance(decoded, dict):
                            await state.update(channel, decoded)
                        else:
                            await state.set(channel, decoded)
                        logger.debug("Updated state: %s", channel)
            elif isinstance(data, dict):
                await state.update(channel, data)
                logger.debug("Updated state: %s (raw dict)", channel)
        except Exception as exc:
            logger.warning("Failed to process channel '%s': %s", channel, exc)

    # ------------------------------------------------------------------
    # 5. Public API
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Full connection sequence: negotiate, open WebSocket, send handshake."""
        if self.session is not None and not self.session.closed:
            raise RuntimeError("SignalRClient is already connected.")

        logger.info("Connecting to SignalR hub: %s", self.hub_url)
        self.session = aiohttp.ClientSession()

        try:
            self.connection_token = await self._negotiate()
            await self._connect_websocket()
            await self._send_handshake()
        except Exception:
            await self._cleanup()
            raise

    async def listen(self) -> None:
        """Continuously read and process messages from the WebSocket."""
        if self.ws is None or self.ws.closed:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")

        self.running = True
        logger.info("Listening to stream...")

        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    # Binary messages are less common but handle them.
                    try:
                        text: str = msg.data.decode("utf-8")
                        await self._handle_message(text)
                    except UnicodeDecodeError:
                        logger.debug("Received binary message (%d bytes)", len(msg.data))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket error: %s", self.ws.exception())
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSE):
                    logger.warning("WebSocket closed by server.")
                    break
        except asyncio.CancelledError:
            logger.info("Listen loop cancelled.")
            raise
        finally:
            self.running = False

    async def disconnect(self) -> None:
        """Gracefully close the WebSocket and session."""
        self.running = False
        await self._cleanup()

    async def _cleanup(self) -> None:
        """Close WebSocket and session resources."""
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()
            logger.info("WebSocket closed.")
        self.ws = None

        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("SignalR session closed.")
        self.session = None
        self.connection_token = None


# ---------------------------------------------------------------------------
# Standalone runner (for testing the ingestion pipeline)
# ---------------------------------------------------------------------------

async def main() -> None:
    """Run the SignalR client standalone for testing purposes."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    client: SignalRClient = SignalRClient()
    try:
        await client.connect()
        await client.listen()
    except KeyboardInterrupt:
        pass
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())