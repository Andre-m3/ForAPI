"""
FastAPI application entry point for the F1 Real-Time Data Lake API.

Exposes clean, fast REST endpoints (JSON) for the GPHub mobile client to consume.
On startup, initializes the SQLite database (WAL mode, Data Lake schema) and
starts the async batch writer for high-frequency telemetry. On shutdown, the
batch writer is flushed and stopped gracefully.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from db.database import init_db, batch_writer
from api.routers import telemetry, timing, session


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event("startup"))
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialize DB and start batch writer on startup."""
    await init_db()
    await batch_writer.start()
    yield
    await batch_writer.stop()


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

app: FastAPI = FastAPI(
    title="F1 Real-Time Data Lake API",
    description="Self-hosted backend for the GPHub mobile client. "
    "Ingests ALL official F1 telemetry/timing data and serves it via REST.",
    version="0.2.0",
    lifespan=lifespan,
)

# Register routers
app.include_router(telemetry.router)
app.include_router(timing.router)
app.include_router(session.router)


# ---------------------------------------------------------------------------
# Root health check
# ---------------------------------------------------------------------------

@app.get("/")
async def health_check() -> dict[str, str]:
    """Health check endpoint for uptime monitoring and client connectivity tests."""
    return {"status": "ok", "service": "f1-realtime-datalake-api", "version": "0.2.0"}