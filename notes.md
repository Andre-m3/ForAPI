# Project Context: GPHub Backend (F1 API)

## UVICORN
 To start the API: `uvicorn api.main:app --reload`
## INGESTION PIPELINE
 To run the ingestion pipeline: `python -m ingestor.signalr_client`

## Latest task result message:
The F1 Data Lake architecture has been fully implemented and tested. All 8 comprehensive tests passed. Here's a summary of everything that was built:

## 1. Exhaustive Pydantic Models (`/models/`)

**`models/timing.py`** — Timing channels:
- `TimingData` (lap times, sectors, gaps, positions, speed traps, segment statuses)
- `TimingStats` (best sectors, best speeds, fastest lap)
- `TimingAppData` (stints, pit stops, tyre data)
- `TopThree` (top 3 leaderboard)
- `LapCount`, `ExtrapolatedClock`

**`models/telemetry.py`** — High-frequency telemetry:
- `CarData` (RPM, Speed, Gear, Throttle, Brake, DRS per driver)
- `PositionData` (X, Y, Z coordinates per driver per timestamp)

**`models/session.py`** — Session/metadata:
- `WeatherData` (air temp, track temp, humidity, pressure, wind, rainfall)
- `RaceControlMessages` (flags, penalties, investigations, incidents)
- `DriverList` (driver info: name, team, number, TLA, country)
- `SessionInfo` (session type, circuit, country, meeting details)
- `SessionStatus`, `TeamRadio`, `Heartbeat`

All models mirror the raw F1 data structure with `Field(alias=...)` for exact field name mapping.

## 2. Max-Capacity Database (`/db/database.py`)

**14 SQLite tables** with WAL mode for concurrent read/write:
- **Structured tables**: `lap_times`, `drivers`, `session_info`, `session_status`, `race_control_messages`, `weather_data`, `team_radio`, `top_three`, `lap_count`, `extrapolated_clock`
- **JSON-blob tables**: `car_data`, `position_data`, `timing_stats`, `timing_app_data` (with indexed columns for RPM/Speed/Gear/Throttle/Brake/DRS + full raw JSON)

**Async BatchWriter** — Accumulates high-frequency telemetry in memory and flushes to SQLite in bulk every 2 seconds (configurable), with max batch size of 500 rows. Uses `executemany()` for efficient bulk inserts. This prevents SQLite lock contention while preserving every single data point.

## 3. Complete FastAPI Endpoints (`/api/`)

**32 routes** across 3 routers:

**Telemetry router** (`/telemetry/`):
- `GET /live/all` — Full telemetry dump of ALL channels
- `GET /live/car-data` — All drivers' CarData
- `GET /live/car-data/{driver_number}` — Per-driver CarData
- `GET /live/position` — All drivers' Position
- `GET /live/position/{driver_number}` — Per-driver Position
- `GET /history/car-data/{driver_number}` — Historical car telemetry from DB
- `GET /history/position/{driver_number}` — Historical position from DB

**Timing router** (`/timing/`):
- `GET /live/timing-data` — Complete timing leaderboard
- `GET /live/timing-data/{driver_number}` — Per-driver timing
- `GET /live/timing-stats`, `/live/timing-app-data`, `/live/top-three`, `/live/lap-count`, `/live/clock`
- `GET /history/lap-times/{driver_id}` — Historical lap times from DB

**Session router** (`/session/`):
- `GET /live/drivers`, `/live/drivers/{driver_number}`, `/live/session-info`, `/live/session-status`, `/live/weather`, `/live/race-control`, `/live/team-radio`, `/live/heartbeat`
- `GET /history/drivers`, `/history/session-info`, `/history/race-control`, `/history/weather`

## 4. SignalR Client (`/ingestor/signalr_client.py`)

Updated to persist ALL data to the Data Lake:
- **CarData/Position** → enqueued to BatchWriter (non-blocking, sync enqueue)
- **TimingData** → extracts lap times/sectors and upserts per driver/lap
- **TimingStats/TimingAppData** → stores raw JSON per driver
- **DriverList** → upserts each driver
- **SessionInfo/SessionStatus/WeatherData/RaceControlMessages/TeamRadio/TopThree/LapCount/ExtrapolatedClock** → direct async DB writes
- **Heartbeat** → in-memory only (keepalive)

## Test Results (All 8 Passed)
1. ✓ Pydantic models import and validate correctly
2. ✓ Database initialization creates all 14 tables
3. ✓ BatchWriter flushes car/position data in bulk
4. ✓ Low-frequency DB inserts (drivers, session, weather, RCM, etc.)
5. ✓ FastAPI app loads with 32 routes across 3 routers
6. ✓ StateManager deep-merge preserves existing fields
7. ✓ Base64 + zlib decoder works correctly
8. ✓ SignalRClient instantiates with 15 channels