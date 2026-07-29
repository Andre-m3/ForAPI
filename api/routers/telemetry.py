"""
FastAPI router for telemetry endpoints — CarData, Position, and full dumps.

Serves the complete, unfiltered state of high-frequency telemetry data from
both the in-memory state manager (live) and the SQLite database (historical).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from db import database
from utils.state_manager import state

router: APIRouter = APIRouter(prefix="/telemetry", tags=["telemetry"])


# ---------------------------------------------------------------------------
# Live in-memory endpoints
# ---------------------------------------------------------------------------

@router.get("/live/all")
async def get_live_all() -> dict[str, Any]:
    """Return the complete in-memory state for ALL channels (full telemetry dump)."""
    return state.get_all()


@router.get("/live/car-data")
async def get_live_car_data() -> dict[str, Any]:
    """Return the current in-memory CarData for all drivers."""
    return state.get("CarData") or {}


@router.get("/live/position")
async def get_live_position() -> dict[str, Any]:
    """Return the current in-memory Position data for all drivers."""
    return state.get("Position") or {}


@router.get("/live/car-data/{driver_number}")
async def get_live_car_data_by_driver(driver_number: str) -> dict[str, Any]:
    """Return the current in-memory CarData for a specific driver."""
    car_data: dict[str, Any] = state.get("CarData") or {}
    entries: dict[str, Any] = car_data.get("Entries", {})
    return entries.get(driver_number, {})


@router.get("/live/position/{driver_number}")
async def get_live_position_by_driver(driver_number: str) -> dict[str, Any]:
    """Return the current in-memory Position data for a specific driver."""
    pos_data: dict[str, Any] = state.get("Position") or {}
    entries: dict[str, Any] = pos_data.get("Entries", {})
    # Position entries are keyed by timestamp -> driver_number -> coords
    result: dict[str, Any] = {}
    for ts, drivers in entries.items():
        if driver_number in drivers:
            result[ts] = drivers[driver_number]
    return result


# ---------------------------------------------------------------------------
# Historical DB endpoints
# ---------------------------------------------------------------------------

@router.get("/history/car-data/{driver_number}")
async def get_history_car_data(
    driver_number: str,
    limit: int = Query(default=1000, ge=1, le=10000),
) -> list[dict[str, Any]]:
    """Retrieve historical car telemetry for a driver from the database."""
    return await database.get_car_data_by_driver(driver_number, limit)


@router.get("/history/position/{driver_number}")
async def get_history_position(
    driver_number: str,
    limit: int = Query(default=1000, ge=1, le=10000),
) -> list[dict[str, Any]]:
    """Retrieve historical position data for a driver from the database."""
    return await database.get_position_data_by_driver(driver_number, limit)