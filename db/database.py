"""
Database initialization and async CRUD operations for the F1 Real-Time API.

Uses aiosqlite for non-blocking SQLite access. The database file (f1_season.db)
is created in the project root and stores completed session data such as lap times.
"""

from __future__ import annotations

import aiosqlite
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_NAME: str = "f1_season.db"
DB_PATH: Path = Path(__file__).resolve().parent.parent / DB_NAME

# SQL schema for the lap_times table.
_LAP_TIMES_SCHEMA: str = """
CREATE TABLE IF NOT EXISTS lap_times (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id   INTEGER NOT NULL,
    lap_number  INTEGER NOT NULL,
    sector_1    REAL,
    sector_2    REAL,
    sector_3    REAL,
    total_time  REAL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (driver_id, lap_number)
);
"""


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Initialize the SQLite database and create required tables if missing.

    This function is idempotent and safe to call on every application startup.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_LAP_TIMES_SCHEMA)
        await db.commit()


# ---------------------------------------------------------------------------
# Async CRUD helpers
# ---------------------------------------------------------------------------

async def insert_lap_time(
    driver_id: int,
    lap_number: int,
    sector_1: float | None = None,
    sector_2: float | None = None,
    sector_3: float | None = None,
    total_time: float | None = None,
) -> int | None:
    """Insert a completed lap time record. Returns the new row id, or None on conflict."""
    query = (
        "INSERT OR IGNORE INTO lap_times "
        "(driver_id, lap_number, sector_1, sector_2, sector_3, total_time) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    params: tuple[Any, ...] = (driver_id, lap_number, sector_1, sector_2, sector_3, total_time)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor: aiosqlite.Cursor = await db.execute(query, params)
        await db.commit()
        return cursor.lastrowid


async def get_lap_times_by_driver(driver_id: int) -> list[dict[str, Any]]:
    """Retrieve all recorded lap times for a given driver, ordered by lap number."""
    query = (
        "SELECT driver_id, lap_number, sector_1, sector_2, sector_3, total_time, created_at "
        "FROM lap_times WHERE driver_id = ? ORDER BY lap_number ASC"
    )

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (driver_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]