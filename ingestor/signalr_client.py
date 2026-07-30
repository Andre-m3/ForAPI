"""
Fully functional async SignalR client for the F1 Live Timing stream.

Implements the complete connection lifecycle:
  1. HTTP negotiation to obtain a connection token.
  2. WebSocket upgrade with the correct headers (User-Agent, Origin, etc.).
  3. SignalR handshake (protocol version 1.5 / JSON).
  4. Subscription to the ``Streaming`` hub for standard F1 channels.
  5. Continuous message loop with decompression, in-memory state updates,
     and **full Data Lake persistence** to SQLite.

All channel data is stored — high-frequency telemetry (CarData, Position) goes
through the async BatchWriter, while low-frequency data is written directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Final

import aiohttp

from db import database
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
        """Perform the HTTP negotiation phase and return the connection token."""
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

                # Decompress, store in memory, and persist to DB.
                await self._process_channel_update(channel, data_field)

    async def _process_channel_update(self, channel: str, data: Any) -> None:
        """Decode, store in memory, and persist to DB a channel update."""
        try:
            decoded: Any = None

            # --- Decompress if needed ---
            if isinstance(data, str) and data:
                if channel.endswith(".z"):
                    decoded = decode_json(data)
                    base_channel: str = channel[:-2]
                else:
                    try:
                        decoded = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        decoded = decode_json(data)
                    base_channel = channel
            elif isinstance(data, dict):
                decoded = data
                base_channel = channel
            else:
                return

            if decoded is None:
                return

            # --- Update in-memory state ---
            if isinstance(decoded, dict):
                if channel.endswith(".z"):
                    await state.set(base_channel, decoded)
                else:
                    await state.update(base_channel, decoded)
            else:
                await state.set(base_channel, decoded)

            # --- Persist to Data Lake (SQLite) ---
            await self._persist_to_db(base_channel, decoded)

        except Exception as exc:
            logger.warning("Failed to process channel '%s': %s", channel, exc)

    # ------------------------------------------------------------------
    # 5. Data Lake persistence
    # ------------------------------------------------------------------

    async def _persist_to_db(self, channel: str, data: Any) -> None:
        """Route decoded data to the appropriate DB persistence method.

        High-frequency channels (CarData, Position) are enqueued to the
        BatchWriter. All other channels are written directly via async
        aiosqlite calls.
        """
        try:
            if channel == "CarData":
                self._persist_car_data(data)
            elif channel == "Position":
                self._persist_position_data(data)
            elif channel == "TimingData":
                await self._persist_timing_data(data)
            elif channel == "TimingStats":
                await self._persist_timing_stats(data)
            elif channel == "TimingAppData":
                await self._persist_timing_app_data(data)
            elif channel == "DriverList":
                await self._persist_driver_list(data)
            elif channel == "SessionInfo":
                await database.upsert_session_info(data)
            elif channel == "SessionStatus":
                await self._persist_session_status(data)
            elif channel == "WeatherData":
                await database.insert_weather(data)
            elif channel == "RaceControlMessages":
                await self._persist_race_control(data)
            elif channel == "TeamRadio":
                await self._persist_team_radio(data)
            elif channel == "TopThree":
                await database.insert_top_three(data)
            elif channel == "LapCount":
                await self._persist_lap_count(data)
            elif channel == "ExtrapolatedClock":
                await database.insert_extrapolated_clock(data)
            elif channel == "Heartbeat":
                # Heartbeat is just a keepalive; no DB persistence needed.
                pass
            else:
                logger.debug("Unknown channel '%s' — not persisted to DB.", channel)
        except Exception as exc:
            logger.warning("DB persistence failed for channel '%s': %s", channel, exc)

    def _persist_car_data(self, data: Any) -> None:
        """Enqueue CarData entries to the batch writer (non-blocking)."""
        if not isinstance(data, dict):
            return
        entries: dict[str, Any] = data.get("Entries", {})
        ts: float = time.time()
        for driver_number, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            channels: dict[str, Any] = entry.get("Channels", {})
            database.batch_writer.enqueue_car_data(ts, str(driver_number), channels)

    def _persist_position_data(self, data: Any) -> None:
        """Enqueue Position entries to the batch writer (non-blocking)."""
        if not isinstance(data, dict):
            return
        entries: dict[str, Any] = data.get("Entries", {})
        for ts_str, drivers in entries.items():
            if not isinstance(drivers, dict):
                continue
            try:
                ts: float = float(ts_str)
            except (ValueError, TypeError):
                ts = time.time()
            for driver_number, pos in drivers.items():
                if not isinstance(pos, dict):
                    continue
                database.batch_writer.enqueue_position(ts, str(driver_number), pos)

    async def _persist_timing_data(self, data: Any) -> None:
        """Persist TimingData — extract lap times and upsert per driver/lap."""
        if not isinstance(data, dict):
            return
        lines: dict[str, Any] = data.get("Lines", {})
        for driver_number, line in lines.items():
            if not isinstance(line, dict):
                continue
            lap_number: int | None = line.get("NumberOfLaps")
            if lap_number is None:
                continue
            # Extract sector times.
            sectors: dict[str, Any] = line.get("Sectors", {})
            s1: float | None = None
            s2: float | None = None
            s3: float | None = None
            if isinstance(sectors, dict):
                s1_data: Any = sectors.get("0", {})
                s2_data: Any = sectors.get("1", {})
                s3_data: Any = sectors.get("2", {})
                s1 = self._extract_time(s1_data)
                s2 = self._extract_time(s2_data)
                s3 = self._extract_time(s3_data)
            # Extract total lap time.
            last_lap: Any = line.get("LastLapTime", {})
            total_time: float | None = self._extract_time(last_lap)
            position: int | None = line.get("Position")
            await database.upsert_lap_time(
                driver_id=str(driver_number),
                lap_number=lap_number,
                sector_1=s1,
                sector_2=s2,
                sector_3=s3,
                total_time=total_time,
                position=position,
            )

    @staticmethod
    def _extract_time(sector_data: Any) -> float | None:
        """Extract a numeric time value from a sector/lap time dict."""
        if not isinstance(sector_data, dict):
            return None
        value: Any = sector_data.get("Value")
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _persist_timing_stats(self, data: Any) -> None:
        """Persist TimingStats — store raw JSON per driver."""
        if not isinstance(data, dict):
            return
        lines: dict[str, Any] = data.get("Lines", {})
        for driver_number, line in lines.items():
            await database.upsert_timing_stats(str(driver_number), json.dumps(line))

    async def _persist_timing_app_data(self, data: Any) -> None:
        """Persist TimingAppData — store raw JSON per driver."""
        if not isinstance(data, dict):
            return
        lines: dict[str, Any] = data.get("Lines", {})
        for driver_number, line in lines.items():
            await database.upsert_timing_app_data(str(driver_number), json.dumps(line))

    async def _persist_driver_list(self, data: Any) -> None:
        """Persist DriverList — upsert each driver."""
        if not isinstance(data, dict):
            return
        drivers: dict[str, Any] = data.get("Drivers", data)
        for driver_number, driver_info in drivers.items():
            if not isinstance(driver_info, dict):
                continue
            driver_info.setdefault("RacingNumber", driver_number)
            await database.upsert_driver(driver_info)

    async def _persist_session_status(self, data: Any) -> None:
        """Persist SessionStatus."""
        if not isinstance(data, dict):
            return
        status: str | None = data.get("Status")
        session_part: int | None = data.get("SessionPart")
        if status is not None:
            await database.insert_session_status(status, session_part)

    async def _persist_race_control(self, data: Any) -> None:
        """Persist RaceControlMessages — upsert each message."""
        if not isinstance(data, dict):
            return
        messages: dict[str, Any] = data.get("Messages", {})
        for key, msg in messages.items():
            if not isinstance(msg, dict):
                continue
            await database.upsert_race_control_message(str(key), msg)

    async def _persist_team_radio(self, data: Any) -> None:
        """Persist TeamRadio — upsert each capture."""
        if not isinstance(data, dict):
            return
        captures: dict[str, Any] = data.get("Captures", {})
        for key, entry in captures.items():
            if not isinstance(entry, dict):
                continue
            await database.upsert_team_radio(str(key), entry)

    async def _persist_lap_count(self, data: Any) -> None:
        """Persist LapCount."""
        if not isinstance(data, dict):
            return
        current_lap: int | None = data.get("CurrentLap")
        total_laps: int | None = data.get("TotalLaps")
        if current_lap is not None and total_laps is not None:
            await database.insert_lap_count(current_lap, total_laps)

    # ------------------------------------------------------------------
    # 6. Public API
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

    async def run_with_reconnect(
        self,
        max_retries: int = 5,
        retry_delay: float = 5.0,
    ) -> None:
        """Run the full ingestion lifecycle with automatic reconnection.

        On connection drops, stream errors, or unexpected disconnects, the
        client cleans up and retries up to ``max_retries`` times, waiting
        ``retry_delay`` seconds between attempts.  All events are logged.

        Raises:
            asyncio.CancelledError: If the task is cancelled (graceful shutdown).
        """
        retry_count: int = 0
        while True:
            try:
                await self.connect()
                await self.listen()
                # listen() returned gracefully — stream ended, reset retries.
                logger.info(
                    "SignalR stream ended. Reconnecting in %.1fs...",
                    retry_delay,
                )
                retry_count = 0
                await asyncio.sleep(retry_delay)
            except asyncio.CancelledError:
                logger.info("Ingestion loop cancelled — shutting down.")
                raise
            except Exception as exc:
                retry_count += 1
                logger.warning(
                    "SignalR connection lost: %s. "
                    "Reconnect attempt %d/%d in %.1fs",
                    exc, retry_count, max_retries, retry_delay,
                )
                if retry_count >= max_retries:
                    logger.error(
                        "Max reconnection attempts (%d) reached. "
                        "Stopping ingestion.",
                        max_retries,
                    )
                    return
                await asyncio.sleep(retry_delay)
            finally:
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

    # Initialize DB and batch writer for standalone mode.
    await database.init_db()
    await database.batch_writer.start()

    client: SignalRClient = SignalRClient()
    try:
        await client.run_with_reconnect()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await client.disconnect()
        await database.batch_writer.stop()


if __name__ == "__main__":
    asyncio.run(main())