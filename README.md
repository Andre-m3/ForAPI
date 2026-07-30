# F1 Real-Time Data Lake API

**Version:** 0.3.0  
**Backend for:** GPHub Android Mobile Client  
**Protocol:** REST (JSON over HTTP)  
**Data Source:** Official Formula 1 Live Timing SignalR Stream

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Running the Server](#2-running-the-server)
3. [API Endpoints](#3-api-endpoints)
4. [Integration Directives for Android](#4-integration-directives-for-android)

---

## 1. Architecture Overview

This backend implements a **Hybrid Data Lake** architecture designed for high-throughput real-time F1 data ingestion combined with low-latency REST serving for mobile clients. The system is split into two complementary storage tiers:

### 1.1 In-Memory State Manager (Live Data)

| Aspect | Detail |
|---|---|
| **Technology** | Python `dict` wrapped in an `asyncio.Lock` |
| **Purpose** | Serve the *latest* state of every F1 channel instantly (sub-millisecond reads) |
| **Data** | Current speed, gear, throttle, position, timing leaderboard, weather, flags, etc. |
| **Update Model** | Deep-merge of partial deltas — the F1 stream sends only what changed, and the `StateManager` recursively merges these into the existing state |
| **Lifecycle** | Volatile — cleared on server restart |

The `StateManager` (`utils/state_manager.py`) exposes synchronous read methods (`get()`, `get_all()`) so FastAPI endpoints can return data **without awaiting any I/O**. Writes are async and lock-protected to prevent race conditions during delta merges.

### 1.2 SQLite Database (Historical Records)

| Aspect | Detail |
|---|---|
| **Technology** | SQLite via `aiosqlite` (fully async, non-blocking) |
| **Mode** | WAL (Write-Ahead Logging) for concurrent read/write |
| **Purpose** | Persist *completed* events: finished laps, final classification, session metadata, race control messages, weather history |
| **Schema** | 14 tables — 10 structured (relational) + 4 JSON-blob (high-frequency telemetry) |
| **Batch Writer** | High-frequency `CarData` and `Position` channels are accumulated in memory and flushed in bulk every 2 seconds (max 500 rows per batch) using `executemany()` to prevent lock contention |

### 1.3 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    F1 SignalR Stream (WSS)                       │
│         livetiming.formula1.com/signalr  (15 channels)           │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Base64 + zlib compressed JSON
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              SignalRClient (ingestor/signalr_client.py)          │
│  negotiate → ws_connect → handshake → listen loop → decode       │
│  Auto-reconnect (5 retries, 5s delay) on connection drops       │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
           ▼                              ▼
┌─────────────────────┐      ┌──────────────────────────────────┐
│  StateManager       │      │  SQLite Data Lake (aiosqlite)    │
│  (in-memory dict)   │      │                                  │
│  Live state for     │      │  Structured tables:              │
│  instant FastAPI    │      │   - lap_times, drivers,           │
│  reads              │      │     session_info, weather,       │
│                     │      │     race_control, team_radio...   │
└─────────┬───────────┘      │                                  │
          │                  │  JSON-blob tables (batched):     │
          │                  │   - car_data, position_data,      │
          │                  │     timing_stats, timing_app_data │
          │                  └──────────────┬───────────────────┘
          │                                 │
          ▼                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              FastAPI REST API (api/main.py)                      │
│  /telemetry/*  /timing/*  /session/*  (32 routes total)         │
│  Live endpoints read from StateManager (RAM)                    │
│  History endpoints read from SQLite (async)                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ JSON
                           ▼
                   GPHub Android Client
```

### 1.4 Application Lifecycle

The FastAPI `lifespan` context manager orchestrates startup and shutdown:

| Phase | Actions |
|---|---|
| **Startup** | 1. `init_db()` — create tables, enable WAL mode · 2. `batch_writer.start()` — launch background flush task · 3. `SignalRClient.run_with_reconnect()` — launch ingestion as a non-blocking `asyncio.Task` |
| **Shutdown** | 1. Cancel ingestion task & await settlement · 2. `client.disconnect()` — close WebSocket & aiohttp session · 3. `batch_writer.stop()` — flush remaining queued rows & close DB connections |

---

## 2. Running the Server

### 2.1 Prerequisites

- **Python 3.11+** (uses `X | Y` union syntax and `asyncio.TaskGroup`)
- **pip** (or `uv` / `poetry` if preferred)
- Internet access to `livetiming.formula1.com` (the SignalR stream is public, no auth required)

### 2.2 Installation

```bash
# Clone the repository
git clone https://github.com/Andre-m3/ForAPI.git
cd ForAPI

# Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate
# Linux/macOS
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

**Dependencies (`requirements.txt`):**

| Package | Purpose |
|---|---|
| `fastapi` | REST API framework |
| `uvicorn[standard]` | ASGI server with `httptools` + `websockets` |
| `aiohttp` | Async HTTP client + WebSocket for SignalR ingestion |
| `aiosqlite` | Async SQLite driver (non-blocking DB access) |
| `pydantic` | Data validation for SignalR payloads & API schemas |

### 2.3 Starting the API Server

```bash
# Development mode (auto-reload on file changes)
uvicorn api.main:app --reload

# Production mode (single worker, bound to all interfaces)
uvicorn api.main:app --host 0.0.0.0 --port 8000

# Production with multiple workers (if scaling is needed)
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

The server starts at `http://localhost:8000`. The SignalR ingestion client launches automatically in the background — no separate process is needed.

> **Database location:** The SQLite database is stored in a `data/` directory
> at the project root by default. This can be overridden with the `DATA_DIR`
> environment variable (used by the Docker setup below).

### 2.4 Docker Deployment (Recommended for Production)

The project ships with a production-ready `Dockerfile` and `docker-compose.yml`
for containerized deployment.

#### 2.4.1 Using Docker Compose (easiest)

```bash
# Build and start the container in detached mode
docker compose up -d --build

# View logs
docker compose logs -f

# Stop the container
docker compose down
```

The API will be available at `http://localhost:8000`.

**Persistent data:** The `docker-compose.yml` maps `./data` on the host to
`/app/data` inside the container. The SQLite database (`f1_season.db`) is
written there, so historical F1 data persists across container restarts.

#### 2.4.2 Using Docker directly

```bash
# Build the image
docker build -t f1-datalake-api .

# Run the container with a persistent volume
docker run -d \
  --name f1-datalake-api \
  -p 8000:8000 \
  -v "$(pwd)/data:/app/data" \
  -e DATA_DIR=/app/data \
  f1-datalake-api
```

#### 2.4.3 Cloud Deployment (Render / Railway / VPS)

The included Dockerfile is compatible with any cloud provider that supports
Docker:

- **Render / Railway:** Create a new service from this repository. The
  detected Dockerfile will be used automatically. Set the `DATA_DIR`
  environment variable to a persistent disk path (e.g. `/var/data`) and attach
  a persistent disk to that path.
- **VPS:** Use the `docker run` command above, or deploy with
  `docker compose up -d`. Ensure the `./data` directory is backed up.

### 2.5 Running the Ingestion Pipeline Standalone

If you want to run *only* the data ingestion (without the REST API) for testing:

```bash
python -m ingestor.signalr_client
```

### 2.6 Interactive API Documentation

FastAPI auto-generates interactive docs:

- **Swagger UI:** `http://localhost:8000/docs`
- **ReDoc:** `http://localhost:8000/redoc`

---

## 3. API Endpoints

All endpoints return JSON. Live endpoints read from in-memory state (instant). History endpoints read from SQLite (async, indexed).

### 3.1 Root & Health

| Method | Path | Description | Response |
|---|---|---|---|
| `GET` | `/` | Health check | `{"status": "ok", "service": "f1-realtime-datalake-api", "version": "0.3.0"}` |
| `GET` | `/favicon.ico` | Silences browser 404s | `204 No Content` |

---

### 3.2 Telemetry Router (`/telemetry`)

#### Live (In-Memory)

| Method | Path | Description | Response Shape |
|---|---|---|---|
| `GET` | `/telemetry/live/all` | Full dump of ALL channels | `dict[channel_name, Any]` |
| `GET` | `/telemetry/live/car-data` | All drivers' current CarData | `CarData` model (see below) |
| `GET` | `/telemetry/live/car-data/{driver_number}` | Single driver's CarData | `CarDataEntry` |
| `GET` | `/telemetry/live/position` | All drivers' current Position | `PositionData` model |
| `GET` | `/telemetry/live/position/{driver_number}` | Single driver's position history | `dict[timestamp, PositionEntry]` |

**CarData JSON structure:**
```json
{
  "Entries": {
    "1": {
      "Channels": {
        "0": {"Value": 12000},
        "2": {"Value": 320.5},
        "3": {"Value": 7},
        "4": {"Value": 95.0},
        "5": {"Value": 0.0},
        "45": {"Value": 1}
      }
    },
    "44": { "Channels": { ... } }
  }
}
```
Channel key mapping: `0`=RPM, `2`=Speed (km/h), `3`=Gear, `4`=Throttle (0-100), `5`=Brake (0-100), `45`=DRS.

**Position JSON structure:**
```json
{
  "Entries": {
    "1690000000.123": {
      "1": {"Status": 0, "X": 120.5, "Y": -340.2, "Z": 0.0},
      "44": {"Status": 0, "X": 115.3, "Y": -335.1, "Z": 0.0}
    }
  }
}
```

#### Historical (SQLite)

| Method | Path | Params | Description | Response |
|---|---|---|---|---|
| `GET` | `/telemetry/history/car-data/{driver_number}` | `?limit=1000` (1-10000) | Historical car telemetry | `list[dict]` with `timestamp, driver_number, rpm, speed, gear, throttle, brake, drs, raw_json` |
| `GET` | `/telemetry/history/position/{driver_number}` | `?limit=1000` (1-10000) | Historical position data | `list[dict]` with `timestamp, driver_number, status, x, y, z, raw_json` |

---

### 3.3 Timing Router (`/timing`)

#### Live (In-Memory)

| Method | Path | Description | Response Shape |
|---|---|---|---|
| `GET` | `/timing/live/timing-data` | Complete timing leaderboard | `TimingData` model |
| `GET` | `/timing/live/timing-data/{driver_number}` | Per-driver timing | `TimingDataLine` |
| `GET` | `/timing/live/timing-stats` | Best sectors, speeds, fastest lap | `TimingStats` model |
| `GET` | `/timing/live/timing-app-data` | Stints, pit stops, tyre data | `TimingAppData` model |
| `GET` | `/timing/live/top-three` | Top 3 leaderboard | `TopThree` model |
| `GET` | `/timing/live/lap-count` | Current lap / total laps | `{"CurrentLap": 15, "TotalLaps": 58}` |
| `GET` | `/timing/live/clock` | Extrapolated session clock | `ExtrapolatedClock` model |

**TimingDataLine JSON structure (per driver):**
```json
{
  "NumberOfLaps": 15,
  "Position": 1,
  "GapToLeader": "+0.000",
  "GapToPositionAhead": "",
  "LastLapTime": {"Value": "90.123", "Position": 1, "OverallFastest": true},
  "BestLapTime": {"Value": "89.456", "Position": 1, "OverallFastest": true},
  "Sectors": {
    "0": {"Value": "30.123", "Segments": {"0": {"Status": 1}}},
    "1": {"Value": "30.456", "Segments": {}},
    "2": {"Value": "29.544", "Segments": {}}
  },
  "Speeds": {
    "I1": {"Value": "280", "Position": 1},
    "FL": {"Value": "320", "Position": 1},
    "ST": {"Value": "295", "Position": 2}
  },
  "Status": 0,
  "InPit": false,
  "PitOut": false,
  "Retired": false,
  "Stopped": false
}
```

#### Historical (SQLite)

| Method | Path | Description | Response |
|---|---|---|---|
| `GET` | `/timing/history/lap-times/{driver_id}` | All recorded lap times for a driver | `list[dict]` with `driver_id, lap_number, sector_1, sector_2, sector_3, total_time, position, created_at` |

---

### 3.4 Session Router (`/session`)

#### Live (In-Memory)

| Method | Path | Description | Response Shape |
|---|---|---|---|
| `GET` | `/session/live/drivers` | All drivers' info | `DriverList` model |
| `GET` | `/session/live/drivers/{driver_number}` | Single driver info | `DriverInfo` |
| `GET` | `/session/live/session-info` | Session metadata | `SessionInfo` model |
| `GET` | `/session/live/session-status` | Session status (Started/Finished/etc.) | `{"Status": "Started", "SessionPart": 0}` |
| `GET` | `/session/live/weather` | Current weather | `WeatherData` model |
| `GET` | `/session/live/race-control` | Race control messages (flags, penalties) | `RaceControlMessages` model |
| `GET` | `/session/live/team-radio` | Team radio capture URLs | `TeamRadio` model |
| `GET` | `/session/live/heartbeat` | Stream keepalive timestamp | `{"Utc": "2026-07-30T10:00:00Z"}` |

**DriverInfo JSON structure:**
```json
{
  "RacingNumber": "1",
  "BroadcastName": "V VERSTAPPEN",
  "FullName": "Max Verstappen",
  "Tla": "VER",
  "TeamName": "Red Bull Racing",
  "TeamColour": "#3671C6",
  "FirstName": "Max",
  "LastName": "Verstappen",
  "CountryA2": "NL",
  "CountryA3": "NED",
  "HeadshotUrl": "https://...",
  "TeamId": "red-bull-racing"
}
```

**WeatherData JSON structure:**
```json
{
  "AirTemp": 25.3,
  "TrackTemp": 42.1,
  "Humidity": 55.0,
  "Pressure": 1013.5,
  "WindSpeed": 3.2,
  "WindDirection": 180,
  "Rainfall": 0.0
}
```

#### Historical (SQLite)

| Method | Path | Params | Description | Response |
|---|---|---|---|---|
| `GET` | `/session/history/drivers` | — | All driver records from DB | `list[dict]` |
| `GET` | `/session/history/session-info` | — | Stored session info | `dict \| null` |
| `GET` | `/session/history/race-control` | `?limit=100` (1-1000) | Recent race control messages | `list[dict]` |
| `GET` | `/session/history/weather` | `?limit=100` (1-1000) | Recent weather snapshots | `list[dict]` |

---

## 4. Integration Directives for Android

### 4.1 Polling Strategy

The API is designed for **polling**, not push (WebSocket support is planned for the Premium tier). To avoid overloading the mobile device's battery and radio, follow these guidelines:

| Data Type | Recommended Poll Interval | Endpoint |
|---|---|---|
| **Timing leaderboard** | 2-5 seconds | `/timing/live/timing-data` |
| **Car telemetry (all drivers)** | 1-3 seconds | `/telemetry/live/car-data` |
| **Single driver telemetry** | 1-2 seconds | `/telemetry/live/car-data/{driver_number}` |
| **Position (map)** | 1-3 seconds | `/telemetry/live/position` |
| **Weather** | 10-30 seconds | `/session/live/weather` |
| **Race control messages** | 5-10 seconds | `/session/live/race-control` |
| **Session status** | 10-30 seconds | `/session/live/session-status` |
| **Lap count** | 10-30 seconds | `/timing/live/lap-count` |
| **Driver list** | Once on load, then 30s | `/session/live/drivers` |
| **Session info** | Once on load | `/session/live/session-info` |

### 4.2 Efficient Consumption Patterns

1. **Use the `all` endpoint sparingly.** `/telemetry/live/all` returns the entire state. Use it only for initial load or reconnection. For ongoing updates, poll specific endpoints.

2. **Prefer per-driver endpoints when displaying a single driver's data.** `/telemetry/live/car-data/44` is significantly smaller than `/telemetry/live/car-data`.

3. **Use the `limit` query parameter on history endpoints.** Default is 1000 rows; requesting more increases payload size and parse time on mobile.

   ```
   GET /telemetry/history/car-data/44?limit=500
   GET /session/history/weather?limit=50
   ```

4. **Cache driver list and session info locally.** These change rarely. Fetch once, store in `SharedPreferences` or `Room`, and refresh every 30-60 seconds.

5. **Handle empty responses gracefully.** Before a session starts, live endpoints return `{}`. The Android client should display a "Waiting for session" state rather than crashing.

6. **Use HTTP connection pooling.** Configure `OkHttpClient` with a shared connection pool and a 5-10 second timeout. The server keeps the connection alive (HTTP/1.1 keep-alive), reducing handshake overhead.

   ```kotlin
   val client = OkHttpClient.Builder()
       .connectTimeout(5, TimeUnit.SECONDS)
       .readTimeout(10, TimeUnit.SECONDS)
       .connectionPool(ConnectionPool(5, 5, TimeUnit.MINUTES))
       .build()
   ```

### 4.3 Error Handling

| HTTP Status | Meaning | Client Action |
|---|---|---|
| `200` | Success | Parse JSON |
| `204` | No content (favicon) | Ignore |
| `404` | Endpoint not found | Check URL / API version |
| `422` | Validation error (bad query param) | Fix the request |
| `500` | Server error | Retry with exponential backoff |

### 4.4 Data Freshness

- **Live endpoints** return data that is at most 1-2 seconds old (the SignalR stream pushes updates in near real-time, and the `StateManager` updates synchronously).
- **Historical endpoints** have a 2-second lag for high-frequency data (the `BatchWriter` flushes every 2 seconds). Low-frequency data (lap times, weather, race control) is written immediately.

### 4.5 Future Considerations (Freemium Model)

The architecture is designed to support a future freemium model:

- **Free tier:** Access to historical endpoints (`/history/*`) and low-frequency live endpoints (weather, session status, lap count). Rate-limited.
- **Premium tier:** Access to high-frequency live endpoints (`/telemetry/live/*`, `/timing/live/*`) and future WebSocket push notifications.

When implementing the Android client, structure your API service layer so that endpoint access can be gated by user role in the future.

---

## Project Structure

```
ForAPI/
├── api/
│   ├── main.py              # FastAPI app, lifespan, favicon, health check
│   └── routers/
│       ├── telemetry.py     # /telemetry/* (7 routes)
│       ├── timing.py         # /timing/* (8 routes)
│       └── session.py        # /session/* (12 routes)
├── db/
│   └── database.py          # SQLite init, 14 tables, BatchWriter, async CRUD
├── ingestor/
│   └── signalr_client.py    # SignalR WSS client, 15 channels, auto-reconnect
├── models/
│   ├── timing.py            # Pydantic: TimingData, TimingStats, TopThree...
│   ├── telemetry.py         # Pydantic: CarData, PositionData
│   └── session.py           # Pydantic: WeatherData, DriverList, SessionInfo...
├── utils/
│   ├── decoder.py           # Base64 + zlib decompression
│   └── state_manager.py     # In-memory async state with deep-merge
├── static/
│   └── favicon.ico          # Served at GET /favicon.ico
├── data/                    # SQLite database directory (persistent, git-ignored)
├── requirements.txt
├── Dockerfile               # Production container image definition
├── docker-compose.yml       # Container orchestration with persistent volume
├── .dockerignore            # Files excluded from the Docker build context
├── README.md                # This file
├── glmContext.md
└── notes.md