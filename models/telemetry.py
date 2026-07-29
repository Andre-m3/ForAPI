"""
Pydantic models for F1 Live Timing — Telemetry channels.

Mirrors the raw structure of:
  - CarData.z (per-driver car telemetry: RPM, Speed, Gear, Throttle, Brake, DRS)
  - Position.z (per-driver XYZ coordinates on track)
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# CarData
# ---------------------------------------------------------------------------

class CarChannelData(BaseModel):
    """A single telemetry channel reading for a car."""
    value: int | float | None = Field(None, alias="Value")


class CarDataEntry(BaseModel):
    """Per-driver car telemetry data.

    Channels typically include: 0 (RPM), 2 (Speed km/h), 3 (Gear),
    4 (Throttle 0-100), 5 (Brake 0-100), 45 (DRS).
    """
    channels: dict[str, CarChannelData] = Field(default_factory=dict, alias="Channels")


class CarData(BaseModel):
    """Full CarData channel payload (decompressed from CarData.z)."""
    entries: dict[str, CarDataEntry] = Field(default_factory=dict, alias="Entries")


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------

class PositionEntry(BaseModel):
    """Per-driver position data (X, Y, Z coordinates on the track map)."""
    status: int | None = Field(None, alias="Status")
    x: float | None = Field(None, alias="X")
    y: float | None = Field(None, alias="Y")
    z: float | None = Field(None, alias="Z")


class PositionData(BaseModel):
    """Full Position channel payload (decompressed from Position.z).

    The top-level key is a timestamp; each timestamp maps driver numbers
    to PositionEntry objects.
    """
    entries: dict[str, dict[str, PositionEntry]] = Field(
        default_factory=dict, alias="Entries"
    )