"""
FastAPI application entry point for the F1 Real-Time API.

Exposes clean, fast REST endpoints (JSON) for the GPHub mobile client to consume.
On startup, initializes the SQLite database for persistent session storage.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from db.database import init_db


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event("startup"))
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialize the SQLite DB on startup."""
    await init_db()
    yield


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

app: FastAPI = FastAPI(
    title="F1 Real-Time API",
    description="Self-hosted backend for the GPHub mobile client. "
    "Ingests official F1 telemetry/timing data and serves it via REST.",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def health_check() -> dict[str, str]:
    """Health check endpoint for uptime monitoring and client connectivity tests."""
    return {"status": "ok", "service": "f1-realtime-api", "version": "0.1.0"}