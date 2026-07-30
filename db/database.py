"""
Database initialization and async batch-writing CRUD for the F1 Data Lake.

Uses aiosqlite for non-blocking SQLite access. The database stores EVERYTHING
from the F1 Live Timing stream using a hybrid approach:

  - **Structured tables** for low-frequency, relational data (lap times,
    driver info, session info, race control messages, weather, team radio).
  - **JSON-blob tables** for high-frequency telemetry (CarData, Position)
    with an asynchronous batch-writing mechanism that flushes accumulated
    rows on a configurable interval, preventing SQLite lock contention
    while preserving every single data point.

WAL (Write-Ahead Logging) mode is enabled for maximum concurrent read/write
throughput.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_NAME: str = "f1_season.db"

# The database directory can be overridden via the DATA_DIR environment
# variable. This is essential for Docker deployments where the SQLite file
# must live inside a persistent volume (e.g. /app/data). When running locally
# without the env var, it defaults to a `data/` directory at the project root.
_DEFAULT_DATA_DIR: Path = Path(__file__).resolve().parent.parent / "data"
DATA_DIR: Path = Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR))
DB_PATH: Path = DATA_DIR / DB_NAME

# Batch writer flush interval (seconds) and max batch size.
FLUSH_INTERVAL_SEC: float = 2.0
MAX_BATCH_SIZE: int = 500

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

_PRAGMA_WAL: str = "PRAGMA journal_mode=WAL;"
_PRAGMA_SYNC: str = "PRAGMA synchronous=NORMAL;"

_SCHEMA: str = """
-- Low-frequency structured tables ----------------------------------------

CREATE TABLE IF NOT EXISTS lap_times (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id   TEXT NOT NULL,
    lap_number  INTEGER NOT NULL,
    sector_1    REAL,
    sector_2    REAL,
    sector_3    REAL,
    total_time  REAL,
    position    INTEGER,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (driver_id, lap_number)
);

CREATE TABLE IF NOT EXISTS drivers (
    racing_number   TEXT PRIMARY KEY,
    broadcast_name  TEXT,
    full_name       TEXT,
    abbreviation    TEXT,
    team_name       TEXT,
    team_colour     TEXT,
    first_name      TEXT,
    last_name       TEXT,
    country_a2      TEXT,
    country_a3      TEXT,
    reference       TEXT,
    headshot_url    TEXT,
    country_code    TEXT,
    team_id         TEXT,
    status          INTEGER,
    line            INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS session_info (
    id                      INTEGER PRIMARY KEY CHECK (id = 1),
    meeting_key             TEXT,
    session_key             TEXT,
    location                TEXT,
    country_code            TEXT,
    country_name            TEXT,
    circuit_key             TEXT,
    circuit_short_name      TEXT,
    session_type            TEXT,
    session_name            TEXT,
    meeting_official_name   TEXT,
    meeting_name            TEXT,
    year                    INTEGER,
    gmt_offset              TEXT,
    start_date              TEXT,
    end_date                TEXT,
    season                  INTEGER,
    raw_json                TEXT,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS session_status (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    status      TEXT,
    session_part INTEGER,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS race_control_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_key     TEXT,
    utc             TEXT,
    lap             INTEGER,
    category        TEXT,
    message         TEXT,
    racing_number   TEXT,
    flag            TEXT,
    scope           TEXT,
    sector          INTEGER,
    mode            TEXT,
    status          TEXT,
    driver          TEXT,
    reason          TEXT,
    penalty_type    TEXT,
    penalty_code    TEXT,
    time            TEXT,
    post_session    INTEGER,
    raw_json        TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (message_key)
);

CREATE TABLE IF NOT EXISTS weather_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    air_temp        REAL,
    track_temp      REAL,
    humidity        REAL,
    pressure        REAL,
    wind_speed       REAL,
    wind_direction  REAL,
    rainfall        REAL,
    raw_json        TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_radio (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_key     TEXT,
    utc             TEXT,
    racing_number   TEXT,
    audio_url       TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (capture_key)
);

CREATE TABLE IF NOT EXISTS top_three (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    racing_number   TEXT,
    tla             TEXT,
    first_name      TEXT,
    last_name       TEXT,
    team_colour     TEXT,
    position        INTEGER,
    gap_to_leader   TEXT,
    status          INTEGER,
    withheld        INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lap_count (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    current_lap INTEGER,
    total_laps  INTEGER,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extrapolated_clock (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    extrapolating       INTEGER,
    remaining           TEXT,
    stopped             INTEGER,
    base                TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- High-frequency JSON-blob tables -----------------------------------------

CREATE TABLE IF NOT EXISTS car_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       REAL NOT NULL,
    driver_number   TEXT NOT NULL,
    rpm             INTEGER,
    speed           REAL,
    gear            INTEGER,
    throttle        REAL,
    brake           REAL,
    drs             INTEGER,
    raw_json        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS position_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       REAL NOT NULL,
    driver_number   TEXT NOT NULL,
    status          INTEGER,
    x               REAL,
    y               REAL,
    z               REAL,
    raw_json        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS timing_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_number   TEXT NOT NULL,
    raw_json        TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timing_app_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_number   TEXT NOT NULL,
    raw_json        TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_car_data_driver ON car_data(driver_number);
CREATE INDEX IF NOT EXISTS idx_car_data_ts ON car_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_position_driver ON position_data(driver_number);
CREATE INDEX IF NOT EXISTS idx_position_ts ON position_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_lap_times_driver ON lap_times(driver_id);
CREATE INDEX IF NOT EXISTS idx_rcm_category ON race_control_messages(category);
CREATE INDEX IF NOT EXISTS idx_weather_created ON weather_data(created_at);
"""


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Initialize the SQLite database with WAL mode and all Data Lake tables."""
    # Ensure the data directory exists (needed for Docker volume mounts).
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_PRAGMA_WAL)
        await db.execute(_PRAGMA_SYNC)
        await db.executescript(_SCHEMA)
        await db.commit()
    logger.info("Database initialized (WAL mode, Data Lake schema).")


# ---------------------------------------------------------------------------
# Structured insert helpers (low-frequency data)
# ---------------------------------------------------------------------------

async def upsert_driver(driver: dict[str, Any]) -> None:
    """Insert or update a driver record."""
    racing_number: str = str(driver.get("RacingNumber", ""))
    if not racing_number:
        return
    query: str = """
        INSERT INTO drivers (racing_number, broadcast_name, full_name, abbreviation,
            team_name, team_colour, first_name, last_name, country_a2, country_a3,
            reference, headshot_url, country_code, team_id, status, line, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(racing_number) DO UPDATE SET
            broadcast_name=excluded.broadcast_name,
            full_name=excluded.full_name,
            abbreviation=excluded.abbreviation,
            team_name=excluded.team_name,
            team_colour=excluded.team_colour,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            country_a2=excluded.country_a2,
            country_a3=excluded.country_a3,
            reference=excluded.reference,
            headshot_url=excluded.headshot_url,
            country_code=excluded.country_code,
            team_id=excluded.team_id,
            status=excluded.status,
            line=excluded.line,
            updated_at=CURRENT_TIMESTAMP
    """
    params: tuple[Any, ...] = (
        racing_number,
        driver.get("BroadcastName"),
        driver.get("FullName"),
        driver.get("Tla"),
        driver.get("TeamName"),
        driver.get("TeamColour"),
        driver.get("FirstName"),
        driver.get("LastName"),
        driver.get("CountryA2"),
        driver.get("CountryA3"),
        driver.get("Reference"),
        driver.get("HeadshotUrl"),
        driver.get("CountryCode"),
        driver.get("TeamId"),
        driver.get("Status"),
        driver.get("Line"),
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def upsert_session_info(data: dict[str, Any]) -> None:
    """Insert or update the single session_info row (id=1)."""
    query: str = """
        INSERT INTO session_info (id, meeting_key, session_key, location, country_code,
            country_name, circuit_key, circuit_short_name, session_type, session_name,
            meeting_official_name, meeting_name, year, gmt_offset, start_date, end_date,
            season, raw_json, updated_at)
        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            meeting_key=excluded.meeting_key,
            session_key=excluded.session_key,
            location=excluded.location,
            country_code=excluded.country_code,
            country_name=excluded.country_name,
            circuit_key=excluded.circuit_key,
            circuit_short_name=excluded.circuit_short_name,
            session_type=excluded.session_type,
            session_name=excluded.session_name,
            meeting_official_name=excluded.meeting_official_name,
            meeting_name=excluded.meeting_name,
            year=excluded.year,
            gmt_offset=excluded.gmt_offset,
            start_date=excluded.start_date,
            end_date=excluded.end_date,
            season=excluded.season,
            raw_json=excluded.raw_json,
            updated_at=CURRENT_TIMESTAMP
    """
    params: tuple[Any, ...] = (
        data.get("MeetingKey"),
        data.get("SessionKey"),
        data.get("Location"),
        data.get("CountryCode"),
        data.get("CountryName"),
        data.get("CircuitKey"),
        data.get("CircuitShortName"),
        data.get("SessionType"),
        data.get("SessionName"),
        data.get("MeetingOfficialName"),
        data.get("MeetingName"),
        data.get("Year"),
        data.get("GmtOffset"),
        data.get("StartDate"),
        data.get("EndDate"),
        data.get("Season"),
        json.dumps(data),
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def insert_session_status(status: str, session_part: int | None = None) -> None:
    """Insert a session status record."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO session_status (status, session_part) VALUES (?, ?)",
            (status, session_part),
        )
        await db.commit()


async def upsert_race_control_message(key: str, msg: dict[str, Any]) -> None:
    """Insert or ignore a race control message."""
    query: str = """
        INSERT OR IGNORE INTO race_control_messages
        (message_key, utc, lap, category, message, racing_number, flag, scope,
         sector, mode, status, driver, reason, penalty_type, penalty_code,
         time, post_session, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    post_session_raw: Any = msg.get("PostSession")
    post_session: int | None = int(post_session_raw) if post_session_raw is not None else None
    params: tuple[Any, ...] = (
        key,
        msg.get("Utc"),
        msg.get("Lap"),
        msg.get("Category"),
        msg.get("Message"),
        msg.get("RacingNumber"),
        msg.get("Flag"),
        msg.get("Scope"),
        msg.get("Sector"),
        msg.get("Mode"),
        msg.get("Status"),
        msg.get("Driver"),
        msg.get("Reason"),
        msg.get("PenaltyType"),
        msg.get("PenaltyCode"),
        msg.get("Time"),
        post_session,
        json.dumps(msg),
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def insert_weather(data: dict[str, Any]) -> None:
    """Insert a weather data snapshot."""
    query: str = """
        INSERT INTO weather_data
        (air_temp, track_temp, humidity, pressure, wind_speed, wind_direction,
         rainfall, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params: tuple[Any, ...] = (
        data.get("AirTemp"),
        data.get("TrackTemp"),
        data.get("Humidity"),
        data.get("Pressure"),
        data.get("WindSpeed"),
        data.get("WindDirection"),
        data.get("Rainfall"),
        json.dumps(data),
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def upsert_team_radio(key: str, entry: dict[str, Any]) -> None:
    """Insert or ignore a team radio capture."""
    query: str = """
        INSERT OR IGNORE INTO team_radio (capture_key, utc, racing_number, audio_url)
        VALUES (?, ?, ?, ?)
    """
    params: tuple[Any, ...] = (
        key,
        entry.get("Utc"),
        entry.get("RacingNumber"),
        entry.get("Path"),
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def insert_top_three(data: dict[str, Any]) -> None:
    """Insert a top-three leaderboard snapshot."""
    withheld: int = 1 if data.get("Withheld") else 0
    lines: list[dict[str, Any]] = data.get("Lines", [])
    async with aiosqlite.connect(DB_PATH) as db:
        for line in lines:
            await db.execute(
                """INSERT INTO top_three
                (racing_number, tla, first_name, last_name, team_colour, position,
                 gap_to_leader, status, withheld)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    line.get("RacingNumber"),
                    line.get("Tla"),
                    line.get("FirstName"),
                    line.get("LastName"),
                    line.get("TeamColour"),
                    line.get("Position"),
                    line.get("GapToLeader"),
                    line.get("Status"),
                    withheld,
                ),
            )
        await db.commit()


async def insert_lap_count(current_lap: int, total_laps: int) -> None:
    """Insert a lap count snapshot."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO lap_count (current_lap, total_laps) VALUES (?, ?)",
            (current_lap, total_laps),
        )
        await db.commit()


async def insert_extrapolated_clock(data: dict[str, Any]) -> None:
    """Insert an extrapolated clock snapshot."""
    extrapolating_raw: Any = data.get("ExtrapolatingClock")
    stopped_raw: Any = data.get("Stopped")
    extrapolating: int | None = int(extrapolating_raw) if extrapolating_raw is not None else None
    stopped: int | None = int(stopped_raw) if stopped_raw is not None else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO extrapolated_clock
            (extrapolating, remaining, stopped, base)
            VALUES (?, ?, ?, ?)""",
            (
                extrapolating,
                data.get("Remaining"),
                stopped,
                data.get("Base"),
            ),
        )
        await db.commit()


async def upsert_lap_time(
    driver_id: str,
    lap_number: int,
    sector_1: float | None = None,
    sector_2: float | None = None,
    sector_3: float | None = None,
    total_time: float | None = None,
    position: int | None = None,
) -> None:
    """Insert or update a lap time record."""
    query: str = """
        INSERT INTO lap_times (driver_id, lap_number, sector_1, sector_2, sector_3,
            total_time, position)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(driver_id, lap_number) DO UPDATE SET
            sector_1=excluded.sector_1,
            sector_2=excluded.sector_2,
            sector_3=excluded.sector_3,
            total_time=excluded.total_time,
            position=excluded.position
    """
    params: tuple[Any, ...] = (driver_id, lap_number, sector_1, sector_2, sector_3, total_time, position)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def upsert_timing_stats(driver_number: str, raw_json: str) -> None:
    """Insert a timing stats snapshot for a driver."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO timing_stats (driver_number, raw_json) VALUES (?, ?)",
            (driver_number, raw_json),
        )
        await db.commit()


async def upsert_timing_app_data(driver_number: str, raw_json: str) -> None:
    """Insert a timing app data snapshot for a driver."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO timing_app_data (driver_number, raw_json) VALUES (?, ?)",
            (driver_number, raw_json),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def get_lap_times_by_driver(driver_id: str) -> list[dict[str, Any]]:
    """Retrieve all recorded lap times for a given driver."""
    query: str = (
        "SELECT driver_id, lap_number, sector_1, sector_2, sector_3, total_time, "
        "position, created_at FROM lap_times WHERE driver_id = ? ORDER BY lap_number ASC"
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (driver_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_all_drivers() -> list[dict[str, Any]]:
    """Retrieve all driver records."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM drivers ORDER BY racing_number") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_session_info() -> dict[str, Any] | None:
    """Retrieve the current session info."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM session_info WHERE id = 1") as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_race_control_messages(limit: int = 100) -> list[dict[str, Any]]:
    """Retrieve recent race control messages."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM race_control_messages ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_weather_data(limit: int = 100) -> list[dict[str, Any]]:
    """Retrieve recent weather data snapshots."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM weather_data ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_car_data_by_driver(driver_number: str, limit: int = 1000) -> list[dict[str, Any]]:
    """Retrieve car telemetry for a driver."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM car_data WHERE driver_number = ? ORDER BY timestamp DESC LIMIT ?",
            (driver_number, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_position_data_by_driver(driver_number: str, limit: int = 1000) -> list[dict[str, Any]]:
    """Retrieve position data for a driver."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM position_data WHERE driver_number = ? ORDER BY timestamp DESC LIMIT ?",
            (driver_number, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Async Batch Writer for high-frequency telemetry
# ---------------------------------------------------------------------------

class BatchWriter:
    """Asynchronous batch writer for high-frequency F1 telemetry data.

    Accumulates rows in memory and flushes them to SQLite in bulk on a
    configurable interval or when the batch reaches MAX_BATCH_SIZE. This
    prevents per-row INSERT overhead and SQLite lock contention while
    preserving every data point.

    Usage::

        writer = BatchWriter()
        await writer.start()
        writer.enqueue_car_data(timestamp, driver_num, channels_dict)
        writer.enqueue_position(timestamp, driver_num, pos_dict)
        # ...
        await writer.stop()
    """

    def __init__(
        self,
        flush_interval: float = FLUSH_INTERVAL_SEC,
        max_batch: int = MAX_BATCH_SIZE,
    ) -> None:
        self.flush_interval: float = flush_interval
        self.max_batch: int = max_batch
        self._car_queue: list[tuple[Any, ...]] = []
        self._pos_queue: list[tuple[Any, ...]] = []
        self._lock: asyncio.Lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._running: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background flush task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._flush_loop())
        logger.info("BatchWriter started (flush every %.1fs, max batch %d).",
                     self.flush_interval, self.max_batch)

    async def stop(self) -> None:
        """Stop the background task and flush remaining rows."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._flush_all()
        logger.info("BatchWriter stopped.")

    # ------------------------------------------------------------------
    # Enqueue helpers
    # ------------------------------------------------------------------

    def enqueue_car_data(
        self,
        timestamp: float,
        driver_number: str,
        channels: dict[str, Any],
    ) -> None:
        """Add a CarData row to the batch queue (non-blocking, thread-safe via lock)."""
        # Extract known channels for indexed columns.
        ch: dict[str, Any] = channels
        rpm: int | None = ch.get("0", {}).get("Value") if isinstance(ch.get("0"), dict) else None
        speed: float | None = ch.get("2", {}).get("Value") if isinstance(ch.get("2"), dict) else None
        gear: int | None = ch.get("3", {}).get("Value") if isinstance(ch.get("3"), dict) else None
        throttle: float | None = ch.get("4", {}).get("Value") if isinstance(ch.get("4"), dict) else None
        brake: float | None = ch.get("5", {}).get("Value") if isinstance(ch.get("5"), dict) else None
        drs: int | None = ch.get("45", {}).get("Value") if isinstance(ch.get("45"), dict) else None

        row: tuple[Any, ...] = (
            timestamp,
            driver_number,
            rpm,
            speed,
            gear,
            throttle,
            brake,
            drs,
            json.dumps(channels),
        )
        # Non-async append; the lock is only needed for flush coordination.
        self._car_queue.append(row)

    def enqueue_position(
        self,
        timestamp: float,
        driver_number: str,
        pos: dict[str, Any],
    ) -> None:
        """Add a Position row to the batch queue."""
        row: tuple[Any, ...] = (
            timestamp,
            driver_number,
            pos.get("Status"),
            pos.get("X"),
            pos.get("Y"),
            pos.get("Z"),
            json.dumps(pos),
        )
        self._pos_queue.append(row)

    # ------------------------------------------------------------------
    # Internal flush logic
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        """Background loop that flushes batches on interval."""
        while self._running:
            await asyncio.sleep(self.flush_interval)
            try:
                await self._flush_all()
            except Exception as exc:
                logger.error("BatchWriter flush error: %s", exc)

    async def _flush_all(self) -> None:
        """Flush all queued rows to SQLite."""
        async with self._lock:
            car_batch: list[tuple[Any, ...]] = self._car_queue[:self.max_batch]
            pos_batch: list[tuple[Any, ...]] = self._pos_queue[:self.max_batch]
            self._car_queue = self._car_queue[len(car_batch):]
            self._pos_queue = self._pos_queue[len(pos_batch):]

        if not car_batch and not pos_batch:
            return

        car_query: str = (
            "INSERT INTO car_data (timestamp, driver_number, rpm, speed, gear, "
            "throttle, brake, drs, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        pos_query: str = (
            "INSERT INTO position_data (timestamp, driver_number, status, x, y, z, "
            "raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(_PRAGMA_WAL)
            await db.execute(_PRAGMA_SYNC)
            if car_batch:
                await db.executemany(car_query, car_batch)
                logger.debug("Flushed %d car_data rows.", len(car_batch))
            if pos_batch:
                await db.executemany(pos_query, pos_batch)
                logger.debug("Flushed %d position_data rows.", len(pos_batch))
            await db.commit()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

batch_writer: BatchWriter = BatchWriter()