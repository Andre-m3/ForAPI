"""
Pydantic models for F1 Live Timing — Timing channels.

Mirrors the raw structure of:
  - TimingData (lap times, sectors, gaps, positions)
  - TimingStats (best sectors, best speeds, fastest lap)
  - TimingAppData (stints, pit stops, tyre data)
  - TopThree (top 3 leaderboard)
  - LapCount (current lap / total laps)
  - ExtrapolatedClock (session clock)
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# TimingData
# ---------------------------------------------------------------------------

class LapTimeValue(BaseModel):
    """A lap or sector time value with positional metadata."""
    value: str | None = Field(None, alias="Value")
    position: int | None = Field(None, alias="Position")
    overall_fastest: bool | None = Field(None, alias="OverallFastest")
    personal_fastest: bool | None = Field(None, alias="PersonalFastest")


class SegmentStatus(BaseModel):
    """Mini-sector segment status (0=unknown, 1=green, 2=yellow, 3=red, 2048=pit)."""
    status: int | None = Field(None, alias="Status")


class SectorData(BaseModel):
    """Individual sector timing data with segments."""
    value: str | None = Field(None, alias="Value")
    previous_value: str | None = Field(None, alias="PreviousValue")
    position: int | None = Field(None, alias="Position")
    segments: dict[str, SegmentStatus] | None = Field(None, alias="Segments")


class SpeedTrapData(BaseModel):
    """Speed trap reading (I1, I2, FL, ST)."""
    value: str | None = Field(None, alias="Value")
    position: int | None = Field(None, alias="Position")
    overall_fastest: bool | None = Field(None, alias="OverallFastest")
    personal_fastest: bool | None = Field(None, alias="PersonalFastest")


class TimingDataLine(BaseModel):
    """Per-driver timing data within TimingData."""
    pit_out: bool | None = Field(None, alias="PitOut")
    number_of_pit_stops: int | None = Field(None, alias="NumberOfPitStops")
    number_of_laps: int | None = Field(None, alias="NumberOfLaps")
    best_lap_time: LapTimeValue | None = Field(None, alias="BestLapTime")
    last_lap_time: LapTimeValue | None = Field(None, alias="LastLapTime")
    sectors: dict[str, SectorData] | None = Field(None, alias="Sectors")
    speeds: dict[str, SpeedTrapData] | None = Field(None, alias="Speeds")
    gap_to_leader: str | None = Field(None, alias="GapToLeader")
    gap_to_position_ahead: str | None = Field(None, alias="GapToPositionAhead")
    position: int | None = Field(None, alias="Position")
    show_position_prefix: bool | None = Field(None, alias="ShowPositionPrefix")
    status: int | None = Field(None, alias="Status")
    retired: bool | None = Field(None, alias="Retired")
    in_pit: bool | None = Field(None, alias="InPit")
    pit_out_rejoined: bool | None = Field(None, alias="PitOutRejoined")
    stopped: bool | None = Field(None, alias="Stopped")
    starting_position: int | None = Field(None, alias="StartingPosition")


class TimingData(BaseModel):
    """Full TimingData channel payload."""
    withheld: bool | None = Field(None, alias="Withheld")
    lines: dict[str, TimingDataLine] = Field(default_factory=dict, alias="Lines")
    session_part: int | None = Field(None, alias="SessionPart")


# ---------------------------------------------------------------------------
# TimingStats
# ---------------------------------------------------------------------------

class TimingStatsLine(BaseModel):
    """Per-driver statistics."""
    best_sector_times: dict[str, LapTimeValue] | None = Field(None, alias="BestSectors")
    best_speeds: dict[str, SpeedTrapData] | None = Field(None, alias="BestSpeeds")
    best_lap_time: LapTimeValue | None = Field(None, alias="BestLapTime")
    fastest_lap: LapTimeValue | None = Field(None, alias="FastestLap")


class TimingStats(BaseModel):
    """Full TimingStats channel payload."""
    lines: dict[str, TimingStatsLine] = Field(default_factory=dict, alias="Lines")
    session_part: int | None = Field(None, alias="SessionPart")


# ---------------------------------------------------------------------------
# TimingAppData
# ---------------------------------------------------------------------------

class PitStopRecord(BaseModel):
    """A single pit stop record."""
    lap: int | None = Field(None, alias="Lap")
    tyre: str | None = Field(None, alias="Tyre")
    tyre_noted: bool | None = Field(None, alias="TyreNoted")
    start_laps: int | None = Field(None, alias="StartLaps")
    laps_to_start: int | None = Field(None, alias="LapsToStart")


class StintRecord(BaseModel):
    """A single stint record."""
    total_laps: int | None = Field(None, alias="TotalLaps")
    start_lap: int | None = Field(None, alias="StartLap")
    end_lap: int | None = Field(None, alias="EndLap")
    tyre: str | None = Field(None, alias="Tyre")
    unknown: bool | None = Field(None, alias="Unknown")


class TimingAppDataLine(BaseModel):
    """Per-driver application data (stints, pit stops)."""
    pit_stops: list[PitStopRecord] | None = Field(None, alias="PitStops")
    stints: dict[str, StintRecord] | None = Field(None, alias="Stints")


class TimingAppData(BaseModel):
    """Full TimingAppData channel payload."""
    lines: dict[str, TimingAppDataLine] = Field(default_factory=dict, alias="Lines")


# ---------------------------------------------------------------------------
# TopThree
# ---------------------------------------------------------------------------

class TopThreeLine(BaseModel):
    """A single entry in the TopThree leaderboard."""
    racing_number: str | None = Field(None, alias="RacingNumber")
    tla: str | None = Field(None, alias="Tla")
    first_name: str | None = Field(None, alias="FirstName")
    last_name: str | None = Field(None, alias="LastName")
    team_colour: str | None = Field(None, alias="TeamColour")
    position: int | None = Field(None, alias="Position")
    show_position_prefix: bool | None = Field(None, alias="ShowPositionPrefix")
    gap_to_leader: str | None = Field(None, alias="GapToLeader")
    status: int | None = Field(None, alias="Status")


class TopThree(BaseModel):
    """Full TopThree channel payload."""
    withheld: bool | None = Field(None, alias="Withheld")
    lines: list[TopThreeLine] = Field(default_factory=list, alias="Lines")


# ---------------------------------------------------------------------------
# LapCount
# ---------------------------------------------------------------------------

class LapCount(BaseModel):
    """Current lap and total laps in the session."""
    current_lap: int | None = Field(None, alias="CurrentLap")
    total_laps: int | None = Field(None, alias="TotalLaps")


# ---------------------------------------------------------------------------
# ExtrapolatedClock
# ---------------------------------------------------------------------------

class ExtrapolatedClock(BaseModel):
    """Extrapolated session clock."""
    extrapolating_clock: bool | None = Field(None, alias="ExtrapolatingClock")
    remaining: str | None = Field(None, alias="Remaining")
    stopped: bool | None = Field(None, alias="Stopped")
    base: str | None = Field(None, alias="Base")