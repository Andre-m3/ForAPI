"""
FastAPI router for timing endpoints — TimingData, TimingStats, TimingAppData,
TopThree, LapCount, ExtrapolatedClock, and lap times.

Serves the complete, unfiltered timing leaderboard and per-driver timing data
from both the in-memory state manager (live) and the SQLite database (historical).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from db import database
from utils.state_manager import state

router: APIRouter = APIRouter(prefix="/timing", tags=["timing"])


# ---------------------------------------------------------------------------
# Live in-memory endpoints
# ---------------------------------------------------------------------------

@router.get("/live/timing-data")
async def get_live_timing_data() -> dict[str, Any]:
    """Return the complete in-memory TimingData for all drivers."""
    return state.get("TimingData") or {}


@router.get("/live/timing-data/{driver_number}")
async def get_live_timing_data_by_driver(driver_number: str) -> dict[str, Any]:
    """Return the current in-memory TimingData for a specific driver."""
    timing_data: dict[str, Any] = state.get("TimingData") or {}
    lines: dict[str, Any] = timing_data.get("Lines", {})
    return lines.get(driver_number, {})


@router.get("/live/timing-stats")
async def get_live_timing_stats() -> dict[str, Any]:
    """Return the complete in-memory TimingStats for all drivers."""
    return state.get("TimingStats") or {}


@router.get("/live/timing-app-data")
async def get_live_timing_app_data() -> dict[str, Any]:
    """Return the complete in-memory TimingAppData (stints, pit stops)."""
    return state.get("TimingAppData") or {}


@router.get("/live/top-three")
async def get_live_top_three() -> dict[str, Any]:
    """Return the current TopThree leaderboard from in-memory state."""
    return state.get("TopThree") or {}


@router.get("/live/lap-count")
async def get_live_lap_count() -> dict[str, Any]:
    """Return the current lap count from in-memory state."""
    return state.get("LapCount") or {}


@router.get("/live/clock")
async def get_live_extrapolated_clock() -> dict[str, Any]:
    """Return the extrapolated session clock from in-memory state."""
    return state.get("ExtrapolatedClock") or {}


# ---------------------------------------------------------------------------
# Historical DB endpoints
# ---------------------------------------------------------------------------

@router.get("/history/lap-times/{driver_id}")
async def get_history_lap_times(driver_id: str) -> list[dict[str, Any]]:
    """Retrieve all recorded lap times for a driver from the database."""
    return await database.get_lap_times_by_driver(driver_id)