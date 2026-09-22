"""Tests for the backtest progress sink + callback adapter."""

from __future__ import annotations

import json

import pytest

from app.services.backtest import progress as P
from app.services.backtest.models import BacktestProgress, GridProgress


def _sample_run_progress() -> BacktestProgress:
    return BacktestProgress(phase="backtest", current=42, total=100, message="Processed 42/100 candles")


def _sample_grid_progress() -> GridProgress:
    return GridProgress(phase="grid", current=3, total=10, message="Run 3/10: rsi_oversold=25")


# ── progress_to_dict ──────────────────────────────────────────────────────


def test_progress_to_dict_from_backtest_progress() -> None:
    d = P.progress_to_dict(_sample_run_progress())
    assert d == {"phase": "backtest", "current": 42, "total": 100, "message": "Processed 42/100 candles"}


def test_progress_to_dict_from_grid_progress() -> None:
    d = P.progress_to_dict(_sample_grid_progress())
    assert d["phase"] == "grid"
    assert d["current"] == 3
    assert d["total"] == 10


def test_progress_to_dict_accepts_plain_dict() -> None:
    d = P.progress_to_dict({"phase": "done", "current": 1, "total": 1, "message": "x"})
    assert d["phase"] == "done"


def test_progress_to_dict_tolerates_missing_attributes() -> None:
    class Partial:
        phase = "fetch"

    d = P.progress_to_dict(Partial())
    assert d == {"phase": "fetch", "current": 0, "total": 0, "message": ""}


def test_progress_to_dict_coerces_types() -> None:
    d = P.progress_to_dict({"phase": None, "current": "7", "total": "9", "message": 123})
    assert d == {"phase": "", "current": 7, "total": 9, "message": "123"}


# ── Sink: file fallback (no Redis) ────────────────────────────────────────


def test_sink_file_fallback_roundtrip(tmp_path, monkeypatch) -> None:
    """With no Redis URL, publish/read round-trips through the file fallback."""
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url=None)

    sink.publish("job123", _sample_run_progress())
    read = sink.read("job123")

    assert read is not None
    assert read["job_id"] == "job123"
    assert read["phase"] == "backtest"
    assert read["current"] == 42
    assert read["total"] == 100
    assert "ts" in read


def test_sink_clear_removes_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url=None)
    sink.publish("job123", _sample_run_progress())
    assert sink.read("job123") is not None
    sink.clear("job123")
    assert sink.read("job123") is None


def test_sink_read_unknown_job_returns_none(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url=None)
    assert sink.read("missing") is None


def test_make_progress_cb_publishes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url=None)
    cb = P.make_progress_cb(sink, "job456")

    cb(_sample_grid_progress())

    read = sink.read("job456")
    assert read is not None
    assert read["phase"] == "grid"
    assert read["current"] == 3


# ── Sink: Redis backend (fake client) ─────────────────────────────────────


class _FakeRedis:
    """Minimal stand-in for the sync redis client (no network)."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.deleted: list[str] = []

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.store.pop(key, None)


def test_sink_uses_redis_when_available(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url="redis://fake:6379/0")
    fake = _FakeRedis()
    # Inject the fake client directly so we avoid importing the real redis lib.
    sink._redis = fake

    sink.publish("job789", _sample_run_progress())

    # Only one key written, under the expected namespace.
    assert len(fake.store) == 1
    key = next(iter(fake.store))
    assert key == f"{P.REDIS_KEY_PREFIX}:job789"
    payload = json.loads(fake.store[key])
    assert payload["phase"] == "backtest"
    assert payload["current"] == 42

    read = sink.read("job789")
    assert read is not None
    assert read["job_id"] == "job789"


def test_sink_clear_deletes_redis_key(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url="redis://fake:6379/0")
    fake = _FakeRedis()
    sink._redis = fake
    sink.publish("job789", _sample_run_progress())
    sink.clear("job789")
    assert f"{P.REDIS_KEY_PREFIX}:job789" not in fake.store


@pytest.mark.parametrize(
    "progress",
    [_sample_run_progress(), _sample_grid_progress()],
)
def test_make_progress_cb_handles_both_kinds(tmp_path, monkeypatch, progress) -> None:
    monkeypatch.setattr(P, "_PROGRESS_DIR", tmp_path)
    sink = P.ProgressSink(redis_url=None)
    cb = P.make_progress_cb(sink, "jobXYZ")
    cb(progress)
    read = sink.read("jobXYZ")
    assert read is not None
    assert read["phase"] == progress.phase
    assert read["current"] == progress.current