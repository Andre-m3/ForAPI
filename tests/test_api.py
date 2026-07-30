"""
Comprehensive pytest test suite for the F1 Real-Time Data Lake API.

Tests verify that all FastAPI endpoints return the correct HTTP status codes
and JSON structures, even when the in-memory state manager is empty (i.e.,
no live session is active).  The tests use FastAPI's TestClient, which runs
the full lifespan (DB init, batch writer, SignalR background task).

The SignalR background task will fail to connect during tests (no live F1
session), but the lifespan is designed to handle this gracefully — the
reconnection logic logs warnings and the API remains fully functional.

Run with::

    pytest tests/ -v
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from fastapi.testclient import TestClient

# Suppress noisy logs from the SignalR reconnection attempts during tests.
logging.getLogger("ingestor.signalr_client").setLevel(logging.ERROR)
logging.getLogger("db.database").setLevel(logging.WARNING)

from api.main import app  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client() -> TestClient:
    """Create a TestClient that runs the full application lifespan.

    The client is module-scoped so the DB is initialized once and the
    background SignalR task is started/stopped only once for the entire
    test module.
    """
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Root & Health
# ---------------------------------------------------------------------------

class TestRootAndHealth:
    """Tests for the root health check and favicon endpoint."""

    def test_health_check(self, client: TestClient) -> None:
        """GET / should return 200 with status, service, and version."""
        response = client.get("/")
        assert response.status_code == 200
        data: dict[str, Any] = response.json()
        assert data["status"] == "ok"
        assert data["service"] == "f1-realtime-datalake-api"
        assert "version" in data

    def test_favicon(self, client: TestClient) -> None:
        """GET /favicon.ico should return 200 with image content type."""
        response = client.get("/favicon.ico")
        # If the favicon file exists in static/, we get 200; otherwise 404.
        # Both are acceptable — the endpoint must not 500.
        assert response.status_code in (200, 404)
        if response.status_code == 200:
            assert response.headers.get("content-type", "").startswith("image/")


# ---------------------------------------------------------------------------
# Telemetry Router
# ---------------------------------------------------------------------------

class TestTelemetryRouter:
    """Tests for /telemetry/* endpoints."""

    def test_live_all(self, client: TestClient) -> None:
        """GET /telemetry/live/all should return 200 with a dict."""
        response = client.get("/telemetry/live/all")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_car_data(self, client: TestClient) -> None:
        """GET /telemetry/live/car-data should return 200 with a dict."""
        response = client.get("/telemetry/live/car-data")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_car_data_by_driver(self, client: TestClient) -> None:
        """GET /telemetry/live/car-data/1 should return 200 with a dict."""
        response = client.get("/telemetry/live/car-data/1")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_position(self, client: TestClient) -> None:
        """GET /telemetry/live/position should return 200 with a dict."""
        response = client.get("/telemetry/live/position")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_position_by_driver(self, client: TestClient) -> None:
        """GET /telemetry/live/position/1 should return 200 with a dict."""
        response = client.get("/telemetry/live/position/1")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_history_car_data(self, client: TestClient) -> None:
        """GET /telemetry/history/car-data/1 should return 200 with a list."""
        response = client.get("/telemetry/history/car-data/1")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_car_data_with_limit(self, client: TestClient) -> None:
        """GET /telemetry/history/car-data/1?limit=10 should return 200."""
        response = client.get("/telemetry/history/car-data/1?limit=10")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_car_data_invalid_limit(self, client: TestClient) -> None:
        """GET /telemetry/history/car-data/1?limit=0 should return 422."""
        response = client.get("/telemetry/history/car-data/1?limit=0")
        assert response.status_code == 422

    def test_history_car_data_limit_too_large(self, client: TestClient) -> None:
        """GET /telemetry/history/car-data/1?limit=99999 should return 422."""
        response = client.get("/telemetry/history/car-data/1?limit=99999")
        assert response.status_code == 422

    def test_history_position(self, client: TestClient) -> None:
        """GET /telemetry/history/position/1 should return 200 with a list."""
        response = client.get("/telemetry/history/position/1")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


# ---------------------------------------------------------------------------
# Timing Router
# ---------------------------------------------------------------------------

class TestTimingRouter:
    """Tests for /timing/* endpoints."""

    def test_live_timing_data(self, client: TestClient) -> None:
        """GET /timing/live/timing-data should return 200 with a dict."""
        response = client.get("/timing/live/timing-data")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_timing_data_by_driver(self, client: TestClient) -> None:
        """GET /timing/live/timing-data/1 should return 200 with a dict."""
        response = client.get("/timing/live/timing-data/1")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_timing_stats(self, client: TestClient) -> None:
        """GET /timing/live/timing-stats should return 200 with a dict."""
        response = client.get("/timing/live/timing-stats")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_timing_app_data(self, client: TestClient) -> None:
        """GET /timing/live/timing-app-data should return 200 with a dict."""
        response = client.get("/timing/live/timing-app-data")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_top_three(self, client: TestClient) -> None:
        """GET /timing/live/top-three should return 200 with a dict."""
        response = client.get("/timing/live/top-three")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_lap_count(self, client: TestClient) -> None:
        """GET /timing/live/lap-count should return 200 with a dict."""
        response = client.get("/timing/live/lap-count")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_clock(self, client: TestClient) -> None:
        """GET /timing/live/clock should return 200 with a dict."""
        response = client.get("/timing/live/clock")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_history_lap_times(self, client: TestClient) -> None:
        """GET /timing/history/lap-times/1 should return 200 with a list."""
        response = client.get("/timing/history/lap-times/1")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


# ---------------------------------------------------------------------------
# Session Router
# ---------------------------------------------------------------------------

class TestSessionRouter:
    """Tests for /session/* endpoints."""

    def test_live_drivers(self, client: TestClient) -> None:
        """GET /session/live/drivers should return 200 with a dict."""
        response = client.get("/session/live/drivers")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_driver_by_number(self, client: TestClient) -> None:
        """GET /session/live/drivers/1 should return 200 with a dict."""
        response = client.get("/session/live/drivers/1")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_session_info(self, client: TestClient) -> None:
        """GET /session/live/session-info should return 200 with a dict."""
        response = client.get("/session/live/session-info")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_session_status(self, client: TestClient) -> None:
        """GET /session/live/session-status should return 200 with a dict."""
        response = client.get("/session/live/session-status")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_weather(self, client: TestClient) -> None:
        """GET /session/live/weather should return 200 with a dict."""
        response = client.get("/session/live/weather")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_race_control(self, client: TestClient) -> None:
        """GET /session/live/race-control should return 200 with a dict."""
        response = client.get("/session/live/race-control")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_team_radio(self, client: TestClient) -> None:
        """GET /session/live/team-radio should return 200 with a dict."""
        response = client.get("/session/live/team-radio")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_live_heartbeat(self, client: TestClient) -> None:
        """GET /session/live/heartbeat should return 200 with a dict."""
        response = client.get("/session/live/heartbeat")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    def test_history_drivers(self, client: TestClient) -> None:
        """GET /session/history/drivers should return 200 with a list."""
        response = client.get("/session/history/drivers")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_session_info(self, client: TestClient) -> None:
        """GET /session/history/session-info should return 200."""
        response = client.get("/session/history/session-info")
        assert response.status_code == 200
        # Can be dict or null (if no session has been stored yet).
        data: Any = response.json()
        assert data is None or isinstance(data, dict)

    def test_history_race_control(self, client: TestClient) -> None:
        """GET /session/history/race-control should return 200 with a list."""
        response = client.get("/session/history/race-control")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_race_control_with_limit(self, client: TestClient) -> None:
        """GET /session/history/race-control?limit=50 should return 200."""
        response = client.get("/session/history/race-control?limit=50")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_race_control_invalid_limit(self, client: TestClient) -> None:
        """GET /session/history/race-control?limit=0 should return 422."""
        response = client.get("/session/history/race-control?limit=0")
        assert response.status_code == 422

    def test_history_weather(self, client: TestClient) -> None:
        """GET /session/history/weather should return 200 with a list."""
        response = client.get("/session/history/weather")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_history_weather_with_limit(self, client: TestClient) -> None:
        """GET /session/history/weather?limit=50 should return 200."""
        response = client.get("/session/history/weather?limit=50")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


# ---------------------------------------------------------------------------
# 404 / Not Found
# ---------------------------------------------------------------------------

class TestNotFound:
    """Tests for non-existent endpoints."""

    def test_unknown_endpoint(self, client: TestClient) -> None:
        """GET /nonexistent should return 404."""
        response = client.get("/nonexistent")
        assert response.status_code == 404

    def test_unknown_driver_live(self, client: TestClient) -> None:
        """GET /telemetry/live/car-data/999 should still return 200 (empty dict)."""
        response = client.get("/telemetry/live/car-data/999")
        assert response.status_code == 200
        assert isinstance(response.json(), dict)