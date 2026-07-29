"""Models package: Pydantic schemas for all F1 Live Timing channels.

Re-exports all model classes for convenient importing::

    from models import TimingData, CarData, WeatherData, ...
"""

from models.timing import (
    ExtrapolatedClock,
    LapCount,
    LapTimeValue,
    PitStopRecord,
    SectorData,
    SegmentStatus,
    SpeedTrapData,
    StintRecord,
    TimingAppData,
    TimingAppDataLine,
    TimingData,
    TimingDataLine,
    TimingStats,
    TimingStatsLine,
    TopThree,
    TopThreeLine,
)
from models.telemetry import (
    CarChannelData,
    CarData,
    CarDataEntry,
    PositionData,
    PositionEntry,
)
from models.session import (
    DriverInfo,
    DriverList,
    Heartbeat,
    RaceControlMessage,
    RaceControlMessages,
    SessionInfo,
    SessionStatus,
    TeamRadio,
    TeamRadioEntry,
    WeatherData,
)

__all__ = [
    # Timing
    "ExtrapolatedClock",
    "LapCount",
    "LapTimeValue",
    "PitStopRecord",
    "SectorData",
    "SegmentStatus",
    "SpeedTrapData",
    "StintRecord",
    "TimingAppData",
    "TimingAppDataLine",
    "TimingData",
    "TimingDataLine",
    "TimingStats",
    "TimingStatsLine",
    "TopThree",
    "TopThreeLine",
    # Telemetry
    "CarChannelData",
    "CarData",
    "CarDataEntry",
    "PositionData",
    "PositionEntry",
    # Session
    "DriverInfo",
    "DriverList",
    "Heartbeat",
    "RaceControlMessage",
    "RaceControlMessages",
    "SessionInfo",
    "SessionStatus",
    "TeamRadio",
    "TeamRadioEntry",
    "WeatherData",
]