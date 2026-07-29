"""
FastAPI router for session endpoints — drivers, session info, weather,
race control messages, team radio, and session status.

Serves the complete, unfiltered session metadata from both the in-memory
state manager (live) and the SQLite database (historical).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from db import database
from utils.state_manager import state

router: APIRouter = APIRouter(prefix="/session", tags=["session"])


# ---------------------------------------------------------------------------
# Live in-memory endpoints
# ---------------------------------------------------------------------------

@router.get("/live/drivers")
async def get_live_drivers() -> dict[str, Any]:
    """Return the complete in-memory DriverList for all drivers."""
    return state.get("DriverList") or {}


@router.get("/live/drivers/{driver_number}")
async def get_live_driver(driver_number: str) -> dict[str, Any]:
    """Return the current in-memory driver info for a specific driver."""
    driver_list: dict[str, Any] = state.get("DriverList") or {}
    drivers: dict[str, Any] = driver_list.get("Drivers", driver_list)
    return drivers.get(driver_number, {})


@router.get("/live/session-info")
async def get_live_session_info() -> dict[str, Any]:
    """Return the current SessionInfo from in-memory state."""
    return state.get("SessionInfo") or {}


@router.get("/live/session-status")
async def get_live_session_status() -> dict[str, Any]:
    """Return the current SessionStatus from in-memory state."""
    return state.get("SessionStatus") or {}


@router.get("/live/weather")
async def get_live_weather() -> dict[str, Any]:
    """Return the current WeatherData from in-memory state."""
    return state.get("WeatherData") or {}


@router.get("/live/race-control")
async def get_live_race_control() -> dict[str, Any]:
    """Return the complete in-memory RaceControlMessages."""
    return state.get("RaceControlMessages") or {}


@router.get("/live/team-radio")
async def get_live_team_radio() -> dict[str, Any]:
    """Return the complete in-memory TeamRadio captures."""
    return state.get("TeamRadio") or {}


@router.get("/live/heartbeat")
async def get_live_heartbeat() -> dict[str, Any]:
    """Return the latest Heartbeat from in-memory state."""
    return state.get("Heartbeat") or {}


# ---------------------------------------------------------------------------
# Historical DB endpoints
# ---------------------------------------------------------------------------

@router.get("/history/drivers")
async def get_history_drivers() -> list[dict[str, Any]]:
    """Retrieve all driver records from the database."""
    return await database.get_all_drivers()


@router.get("/history/session-info")
async def get_history_session_info() -> dict[str, Any] | None:
    """Retrieve the stored session info from the database."""
    return await database.get_session_info()


@router.get("/history/race-control")
async def get_history_race_control(
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict[str, Any]]:
    """Retrieve recent race control messages from the database."""
    return await database.get_race_control_messages(limit)


@router.get("/history/weather")
async def get_history_weather(
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict[str, Any]]:
    """Retrieve recent weather data snapshots from the database."""
    return await database.get_weather_data(limit)