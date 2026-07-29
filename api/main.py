"""
FastAPI application entry point for the F1 Real-Time Data Lake API.

Exposes clean, fast REST endpoints (JSON) for the GPHub mobile client to consume.
On startup, initializes the SQLite database (WAL mode, Data Lake schema), starts
the async batch writer for high-frequency telemetry, and launches the SignalR
ingestion client as a non-blocking background asyncio task. On shutdown, the
SignalR connection is closed gracefully, pending database writes are flushed,
and the batch writer is stopped to prevent data corruption.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Response

from db.database import init_db, batch_writer
from ingestor.signalr_client import SignalRClient
from api.routers import telemetry, timing, session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event("startup"))
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan orchestration.

    Startup:
        1. Initialize the SQLite database (WAL mode, Data Lake schema).
        2. Start the async BatchWriter for high-frequency telemetry.
        3. Launch the SignalR ingestion client as a non-blocking background task.

    Shutdown:
        1. Cancel the ingestion task and wait for it to settle.
        2. Gracefully disconnect the SignalR client (WebSocket + session).
        3. Flush pending batch writes and stop the BatchWriter.
    """
    # --- Startup ---
    await init_db()
    await batch_writer.start()

    client: SignalRClient = SignalRClient()
    app.state.signalr_client = client
    app.state.ingestion_task = asyncio.create_task(
        client.run_with_reconnect(),
        name="signalr-ingestion",
    )
    logger.info("SignalR ingestion task launched in background.")

    yield

    # --- Shutdown ---
    # 1. Cancel the ingestion task and wait for it to finish.
    ingestion_task: asyncio.Task[None] | None = getattr(
        app.state, "ingestion_task", None
    )
    if ingestion_task is not None and not ingestion_task.done():
        ingestion_task.cancel()
        try:
            await ingestion_task
        except asyncio.CancelledError:
            pass
        logger.info("SignalR ingestion task stopped.")

    # 2. Gracefully disconnect the SignalR client.
    signalr_client: SignalRClient | None = getattr(
        app.state, "signalr_client", None
    )
    if signalr_client is not None:
        await signalr_client.disconnect()

    # 3. Flush pending DB writes and stop the batch writer.
    await batch_writer.stop()
    logger.info("Shutdown complete — all resources released.")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

app: FastAPI = FastAPI(
    title="F1 Real-Time Data Lake API",
    description="Self-hosted backend for the GPHub mobile client. "
    "Ingests ALL official F1 telemetry/timing data and serves it via REST.",
    version="0.3.0",
    lifespan=lifespan,
)

# Register routers
app.include_router(telemetry.router)
app.include_router(timing.router)
app.include_router(session.router)


# ---------------------------------------------------------------------------
# Favicon (silence 404 log clutter from browsers)
# ---------------------------------------------------------------------------

@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    """Return 204 No Content to silence browser favicon 404 requests."""
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Root health check
# ---------------------------------------------------------------------------

@app.get("/")
async def health_check() -> dict[str, str]:
    """Health check endpoint for uptime monitoring and client connectivity tests."""
    return {"status": "ok", "service": "f1-realtime-datalake-api", "version": "0.3.0"}