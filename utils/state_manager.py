"""
In-memory state manager for real-time F1 data.

Stores high-frequency telemetry/timing updates in a thread-safe, async-friendly
dictionary so that FastAPI endpoints can read the latest state instantly without
hitting the database.  The F1 SignalR stream sends partial updates (deltas) for
many channels; this manager merges those deltas into the current state.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class StateManager:
    """Async-safe in-memory store for real-time F1 data.

    Each top-level key corresponds to a SignalR channel name (e.g.
    ``"TimingData"``, ``"CarData"``, ``"Position"``).  Values are nested
    dictionaries that are updated in-place as deltas arrive.

    Attributes:
        _state: The backing dictionary holding all channel data.
        _lock: An asyncio lock to serialise concurrent writes.
    """

    def __init__(self) -> None:
        self._state: dict[str, Any] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Read access
    # ------------------------------------------------------------------

    def get(self, channel: str) -> Any:
        """Return the current state for *channel* (or ``None``)."""
        return self._state.get(channel)

    def get_all(self) -> dict[str, Any]:
        """Return a shallow copy of the entire state."""
        return dict(self._state)

    # ------------------------------------------------------------------
    # Write access
    # ------------------------------------------------------------------

    async def set(self, channel: str, data: Any) -> None:
        """Replace the entire state for *channel*."""
        async with self._lock:
            self._state[channel] = data

    async def update(self, channel: str, data: dict[str, Any]) -> None:
        """Deep-merge *data* into the existing state for *channel*.

        F1 timing data arrives as partial updates.  For example, a
        ``TimingData`` delta might contain only a single driver's new lap
        time.  This method recursively merges such deltas so that no
        existing data is lost.
        """
        async with self._lock:
            if channel not in self._state or not isinstance(self._state[channel], dict):
                self._state[channel] = {}
            self._deep_merge(self._state[channel], data)

    async def clear(self) -> None:
        """Clear all stored state."""
        async with self._lock:
            self._state.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
        """Recursively merge *source* into *target* (in-place)."""
        for key, value in source.items():
            if (
                key in target
                and isinstance(target[key], dict)
                and isinstance(value, dict)
            ):
                StateManager._deep_merge(target[key], value)
            else:
                target[key] = value


# ---------------------------------------------------------------------------
# Module-level singleton for convenience
# ---------------------------------------------------------------------------

state: StateManager = StateManager()