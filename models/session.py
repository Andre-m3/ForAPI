"""
Pydantic models for F1 Live Timing — Session, weather, and metadata channels.

Mirrors the raw structure of:
  - WeatherData (track temp, air temp, wind, rain, humidity, pressure)
  - RaceControlMessages (flags, penalties, investigations, incidents)
  - DriverList (driver info: name, team, number, TLA, country)
  - SessionInfo (session type, circuit, country, meeting details)
  - SessionStatus (session state: started, stopped, finished, etc.)
  - TeamRadio (captured driver radio audio URLs)
  - Heartbeat (stream keepalive timestamp)
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# WeatherData
# ---------------------------------------------------------------------------

class WeatherData(BaseModel):
    """Full WeatherData channel payload."""
    air_temp: float | None = Field(None, alias="AirTemp")
    track_temp: float | None = Field(None, alias="TrackTemp")
    humidity: float | None = Field(None, alias="Humidity")
    pressure: float | None = Field(None, alias="Pressure")
    wind_speed: float | None = Field(None, alias="WindSpeed")
    wind_direction: float | None = Field(None, alias="WindDirection")
    rainfall: float | None = Field(None, alias="Rainfall")


# ---------------------------------------------------------------------------
# RaceControlMessages
# ---------------------------------------------------------------------------

class RaceControlMessage(BaseModel):
    """A single race control message (flag, penalty, investigation, etc.)."""
    utc: str | None = Field(None, alias="Utc")
    lap: int | None = Field(None, alias="Lap")
    category: str | None = Field(None, alias="Category")
    message: str | None = Field(None, alias="Message")
    racing_number: str | None = Field(None, alias="RacingNumber")
    flag: str | None = Field(None, alias="Flag")
    scope: str | None = Field(None, alias="Scope")
    sector: int | None = Field(None, alias="Sector")
    mode: str | None = Field(None, alias="Mode")
    status: str | None = Field(None, alias="Status")
    driver: str | None = Field(None, alias="Driver")
    reason: str | None = Field(None, alias="Reason")
    penalty_type: str | None = Field(None, alias="PenaltyType")
    penalty_code: str | None = Field(None, alias="PenaltyCode")
    time: str | None = Field(None, alias="Time")
    post_session: bool | None = Field(None, alias="PostSession")


class RaceControlMessages(BaseModel):
    """Full RaceControlMessages channel payload."""
    messages: dict[str, RaceControlMessage] = Field(default_factory=dict, alias="Messages")


# ---------------------------------------------------------------------------
# DriverList
# ---------------------------------------------------------------------------

class DriverInfo(BaseModel):
    """Per-driver metadata."""
    racing_number: str | None = Field(None, alias="RacingNumber")
    broadcast_name: str | None = Field(None, alias="BroadcastName")
    full_name: str | None = Field(None, alias="FullName")
    abbreviation: str | None = Field(None, alias="Tla")
    team_name: str | None = Field(None, alias="TeamName")
    team_colour: str | None = Field(None, alias="TeamColour")
    first_name: str | None = Field(None, alias="FirstName")
    last_name: str | None = Field(None, alias="LastName")
    country_a2: str | None = Field(None, alias="CountryA2")
    country_a3: str | None = Field(None, alias="CountryA3")
    reference: str | None = Field(None, alias="Reference")
    headshot_url: str | None = Field(None, alias="HeadshotUrl")
    country_code: str | None = Field(None, alias="CountryCode")
    team_id: str | None = Field(None, alias="TeamId")
    status: int | None = Field(None, alias="Status")
    line: int | None = Field(None, alias="Line")


class DriverList(BaseModel):
    """Full DriverList channel payload."""
    drivers: dict[str, DriverInfo] = Field(default_factory=dict, alias="Drivers")


# ---------------------------------------------------------------------------
# SessionInfo
# ---------------------------------------------------------------------------

class SessionInfo(BaseModel):
    """Full SessionInfo channel payload."""
    meeting_key: str | None = Field(None, alias="MeetingKey")
    session_key: str | None = Field(None, alias="SessionKey")
    location: str | None = Field(None, alias="Location")
    country_key: str | None = Field(None, alias="CountryKey")
    country_code: str | None = Field(None, alias="CountryCode")
    country_name: str | None = Field(None, alias="CountryName")
    circuit_key: str | None = Field(None, alias="CircuitKey")
    circuit_short_name: str | None = Field(None, alias="CircuitShortName")
    session_type: str | None = Field(None, alias="SessionType")
    session_name: str | None = Field(None, alias="SessionName")
    meeting_official_name: str | None = Field(None, alias="MeetingOfficialName")
    meeting_name: str | None = Field(None, alias="MeetingName")
    meeting_country_name: str | None = Field(None, alias="MeetingCountryName")
    meeting_country_code: str | None = Field(None, alias="MeetingCountryCode")
    year: int | None = Field(None, alias="Year")
    archive_status: dict[str, Any] | None = Field(None, alias="ArchiveStatus")
    gmt_offset: str | None = Field(None, alias="GmtOffset")
    path: str | None = Field(None, alias="Path")
    start_date: str | None = Field(None, alias="StartDate")
    end_date: str | None = Field(None, alias="EndDate")
    season: int | None = Field(None, alias="Season")


# ---------------------------------------------------------------------------
# SessionStatus
# ---------------------------------------------------------------------------

class SessionStatus(BaseModel):
    """Full SessionStatus channel payload.

    Status values: "Started", "Stopped", "Finished", "Finalised", "Aborted".
    """
    status: str | None = Field(None, alias="Status")
    session_part: int | None = Field(None, alias="SessionPart")


# ---------------------------------------------------------------------------
# TeamRadio
# ---------------------------------------------------------------------------

class TeamRadioEntry(BaseModel):
    """A captured team radio audio clip."""
    utc: str | None = Field(None, alias="Utc")
    racing_number: str | None = Field(None, alias="RacingNumber")
    audio_url: str | None = Field(None, alias="Path")


class TeamRadio(BaseModel):
    """Full TeamRadio channel payload."""
    captures: dict[str, TeamRadioEntry] = Field(default_factory=dict, alias="Captures")


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class Heartbeat(BaseModel):
    """Stream keepalive heartbeat."""
    utc: str | None = Field(None, alias="Utc")