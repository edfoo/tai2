"""Backtest progress publishing.

Backtest jobs run in forked worker processes (``ProcessPoolExecutor``), so
`app.state` mutations made inside a worker are invisible to the server.  This
module provides the only cross-process channel for progress: a tiny JSON
"heartbeat" record keyed by ``job_id``, written to Redis when available and
falling back to a plain file otherwise (matching the app-wide "Postgres/Redis
are optional" invariant).

The protocol is deliberately tiny — a single event with
``{job_id, phase, current, total, message, ts}``.  Results are NOT published
through this channel (they still go through ``persistence.save_result`` /
``result_to_dict``); only progress markers flow here, so each publish is
microseconds and never risks a large payload stalling the event loop.

Redis is accessed with a **synchronous** client.  The engine emits progress via
``progress_cb`` from ``asyncio.to_thread`` worker threads (the CPU-bound loop),
where a sync client is exactly right and needs no event loop; the async fetch
phase only blinks during these sub-millisecond writes.

Usage::

    from app.services.backtest.progress import ProgressSink, make_progress_cb

    sink = ProgressSink.from_settings()
    cb = make_progress_cb(sink, job_id)
    result = await engine.run(progress_cb=cb)          # engine emits progress
    # ... later, from the server (job manager):
    latest = sink.read(job_id)
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# Redis key namespace for per-job progress records.
REDIS_KEY_PREFIX = "tai2:backtest:progress"

# How long a progress record may outlive its job before Redis reclaims it.
# Generous enough to cover very long runs, short enough to avoid unbounded
# accumulation of stale keys.
_TTL_SECONDS = 24 * 3600

# File-fallback directory (used when REDIS_URL is unset).  Kept separate from
# the results tree so the UI's "Saved Runs" browser (which globs
# ``*_results.json``) never mistakes a progress file for a result.
_PROGRESS_DIR: Path = (
    Path(__file__).resolve().parent.parent.parent.parent / "backtest_cache" / "progress"
)

# The current phase values emitted by the engine / grid, plus the "error" and
# "done" terminators.  Kept as a straightforward passthrough rather than a
# strict enum so a new phase never hard-fails the sink.
KNOWN_PHASES = ("fetch", "backtest", "metrics", "grid", "done", "error")


def progress_to_dict(progress: Any) -> dict[str, Any]:
    """Normalise a ``BacktestProgress``/``GridProgress`` into the wire dict.

    Accepts any object exposing ``phase``/``current``/``total``/``message``
    (dataclasses or plain objects), and tolerates dicts and missing attributes
    so a malformed/partial emitter cannot raise inside the callback.
    """
    if isinstance(progress, dict):
        src = progress
        phase = src.get("phase", "")
        current = src.get("current", 0)
        total = src.get("total", 0)
        message = src.get("message", "")
    else:
        phase = getattr(progress, "phase", "")
        current = getattr(progress, "current", 0)
        total = getattr(progress, "total", 0)
        message = getattr(progress, "message", "")

    return {
        "phase": str(phase or ""),
        "current": int(current or 0),
        "total": int(total or 0),
        "message": str(message or ""),
    }


class ProgressSink:
    """Writes/reads per-job progress records to Redis (or a file fallback)."""

    def __init__(self, redis_url: Optional[str] = None) -> None:
        self._redis_url = redis_url
        # Lazy sync client — constructed on first use so importing this module
        # (or constructing a sink) never requires a live Redis connection, and
        # so each forked worker opens its own connection rather than inheriting
        # a parent's socket across fork.
        self._redis: Any = None
        self._redis_failed = False

    @classmethod
    def from_settings(cls) -> "ProgressSink":
        """Build a sink from ``Settings.redis_url`` (None → file fallback)."""
        from app.core.config import get_settings

        return cls(redis_url=get_settings().redis_url)

    # -- backend plumbing -------------------------------------------------

    def _get_redis(self) -> Any:
        if self._redis is not None:
            return self._redis
        if self._redis_failed or not self._redis_url:
            return None
        try:
            import redis  # type: ignore

            self._redis = redis.Redis.from_url(
                self._redis_url, encoding="utf-8", decode_responses=True
            )
        except Exception as exc:  # pragma: no cover - network/env dependent
            logger.warning("Backtest progress: Redis unavailable (%s); using file fallback", exc)
            self._redis_failed = True
            self._redis = None
        return self._redis

    def _file_path(self, job_id: str) -> Path:
        return _PROGRESS_DIR / f"{job_id}.progress.json"

    # -- public API --------------------------------------------------------

    def publish(self, job_id: str, progress: Any) -> None:
        """Record a progress event for ``job_id`` (best-effort, never raises)."""
        body = progress_to_dict(progress)
        payload = {
            "job_id": job_id,
            "ts": time.time(),
            **body,
        }
        try:
            client = self._get_redis()
            if client is not None:
                client.set(
                    f"{REDIS_KEY_PREFIX}:{job_id}",
                    json.dumps(payload, default=str),
                    ex=_TTL_SECONDS,
                )
                return
        except Exception as exc:  # pragma: no cover - network dependent
            logger.debug("Backtest progress: Redis publish failed (%s); file fallback", exc)
        # File fallback (also used when Redis is absent/failed).
        try:
            _PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
            self._file_path(job_id).write_text(json.dumps(payload, default=str))
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Backtest progress: file publish failed: %s", exc)

    def read(self, job_id: str) -> dict[str, Any] | None:
        """Return the latest progress dict for ``job_id``, or None if absent."""
        try:
            client = self._get_redis()
            if client is not None:
                raw = client.get(f"{REDIS_KEY_PREFIX}:{job_id}")
                if raw:
                    return json.loads(raw)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.debug("Backtest progress: Redis read failed (%s); file fallback", exc)
        try:
            path = self._file_path(job_id)
            if path.exists():
                return json.loads(path.read_text())
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Backtest progress: file read failed: %s", exc)
        return None

    def clear(self, job_id: str) -> None:
        """Delete a job's progress record (called when a job finishes)."""
        try:
            client = self._get_redis()
            if client is not None:
                client.delete(f"{REDIS_KEY_PREFIX}:{job_id}")
        except Exception as exc:  # pragma: no cover - network dependent
            logger.debug("Backtest progress: Redis clear failed: %s", exc)
        try:
            path = self._file_path(job_id)
            if path.exists():
                path.unlink()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Backtest progress: file clear failed: %s", exc)


def make_progress_cb(sink: ProgressSink, job_id: str) -> Callable[[Any], None]:
    """Return a ``progress_cb`` suitable for ``BacktestEngine.run``/``BacktestGrid.run``.

    The callback adapts the engine/grid's progress dataclass into the canonical
    wire dict and hands it to the sink.  It is safe to call from worker threads
    (the engine invokes it from ``asyncio.to_thread``) — ``publish`` is
    synchronous and, on failure, logs at debug and continues.
    """
    return lambda progress: sink.publish(job_id, progress)


__all__ = [
    "ProgressSink",
    "make_progress_cb",
    "progress_to_dict",
    "REDIS_KEY_PREFIX",
]