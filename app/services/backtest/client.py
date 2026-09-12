"""Shared REST client library for backtesting.

Used by the thin CLI clients (``scripts/backtest_client.py`` and the three
A/B sweep clients).  These helpers build ``POST /backtest/run`` requests, poll
the job to completion, and turn the serialised result back into the summary
rows / close-reason counts the old headless scripts produced locally.

The strategy-specific *catalog* logic (which gates / variants / phases to
sweep) stays in each client script; this module only provides the transport
and the config/summary plumbing.
"""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any, Callable

import httpx

from app.services.strategies.defaults import strategy_defaults


class BacktestClientError(RuntimeError):
    """Raised when the server rejects a request or a job fails."""


def build_single_strategy_launcher(
    *,
    strategy_name: str,
    capital: float,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a ``launcher_config`` for a single strategy with overrides.

    Mirrors ``build_single_strategy_config`` in ``runner.py`` (canonical
    defaults + overrides, ``notional_usd`` = ``capital``) but returns the
    plain ``launcher_config`` dict for submission over REST rather than a
    ``BacktestConfig``.
    """
    strat_cfg = dict(strategy_defaults(strategy_name))
    strat_cfg["enabled"] = True
    if overrides:
        strat_cfg.update(overrides)
    return {
        "mode": "launcher_only",
        "notional_usd": float(capital),
        "strategies": {strategy_name: strat_cfg},
    }


def submit_and_poll(
    *,
    base_url: str,
    payload: dict[str, Any],
    poll_interval: float = 1.5,
) -> dict[str, Any]:
    """POST a backtest, poll to completion, and return the serialised result dict.

    Returns the ``{job_id, status, run_id, result}`` envelope.  Raises
    :class:`BacktestClientError` on transport errors, job failure, or a
    not-found job.
    """
    base = base_url.rstrip("/")
    try:
        resp = httpx.post(f"{base}/backtest/run", json=payload, timeout=60.0)
    except httpx.ConnectError as exc:
        raise BacktestClientError(
            f"could not connect to {base} — is the tai2 server running there? "
            f"(use --base-url to point at the server's host:port)"
        ) from exc
    except httpx.HTTPError as exc:
        raise BacktestClientError(f"request failed: {exc}") from exc
    if resp.status_code not in (200, 202):
        raise BacktestClientError(
            f"/backtest/run returned {resp.status_code}: {resp.text}"
        )
    job_id = resp.json()["job_id"]

    while True:
        try:
            status_resp = httpx.get(f"{base}/backtest/status/{job_id}", timeout=60.0)
        except httpx.ConnectError as exc:
            raise BacktestClientError(
                f"lost connection to {base} while polling — is the server still running?"
            ) from exc
        except httpx.HTTPError as exc:
            raise BacktestClientError(f"status poll failed: {exc}") from exc
        if status_resp.status_code == 404:
            raise BacktestClientError(f"job {job_id} not found")
        status = status_resp.json()
        state = status.get("status")
        if state == "completed":
            break
        if state == "failed":
            raise BacktestClientError(f"job {job_id} failed: {status.get('error')}")
        time.sleep(poll_interval)

    try:
        result_resp = httpx.get(f"{base}/backtest/result/{job_id}", timeout=60.0)
    except httpx.HTTPError as exc:
        raise BacktestClientError(f"result fetch failed: {exc}") from exc
    if result_resp.status_code != 200:
        raise BacktestClientError(
            f"/backtest/result returned {result_resp.status_code}: {result_resp.text}"
        )
    return result_resp.json()


def submit_many_and_poll(
    *,
    base_url: str,
    payloads: list[dict[str, Any]],
    max_workers: int | None = None,
    poll_interval: float = 1.5,
) -> list[tuple[dict[str, Any], BaseException | None]]:
    """Submit many backtests concurrently and return ``(envelope, error)`` pairs.

    The server runs a pool of workers, so submitting N jobs at once overlaps
    their execution.  Results are returned in submission order.  Each element
    is ``(envelope_dict, None)`` on success or ``(None, exception)`` on
    failure (the exception may be a :class:`BacktestClientError` or a
    transport error), so a single failed variant never aborts the batch.
    """
    if not payloads:
        return []

    def _one(payload: dict[str, Any]) -> tuple[dict[str, Any], BaseException | None]:
        try:
            return submit_and_poll(base_url=base_url, payload=payload, poll_interval=poll_interval), None
        except BaseException as exc:  # noqa: BLE001 - collect per-variant failure
            return None, exc

    workers = max_workers or len(payloads)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        return list(ex.map(_one, payloads))


def submit_many_and_poll_timed(
    *,
    base_url: str,
    payloads: list[dict[str, Any]],
    label: str = "",
    max_workers: int | None = None,
    poll_interval: float = 1.5,
) -> list[tuple[dict[str, Any], BaseException | None]]:
    """Like :func:`submit_many_and_poll`, but prints wall-clock timing.

    Emits a line like ``▶ <label>: N job(s) in X.XXs (M workers)`` after the
    batch completes.  Used by the A/B clients so the user can measure speedup
    by re-running with a different ``--workers`` value and comparing the
    elapsed seconds.
    """
    if not payloads:
        return []
    started = time.monotonic()
    results = submit_many_and_poll(
        base_url=base_url,
        payloads=payloads,
        max_workers=max_workers,
        poll_interval=poll_interval,
    )
    elapsed = time.monotonic() - started
    workers = max_workers or len(payloads)
    prefix = f"{label}: " if label else ""
    print(f"▶ {prefix}{len(payloads)} job(s) in {elapsed:.2f}s ({max(1, workers)} workers)")
    return results


def summary_row(
    envelope: dict[str, Any],
    *,
    run_id: str,
    ltf: str,
    htf: str,
    symbols: str,
    strategies: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a flat ``m_*`` summary row from a serialised result envelope.

    Mirrors ``persistence.result_summary_row`` (which operates on a
    ``BacktestResult``) but works on the REST JSON shape.
    """
    result = envelope.get("result") or {}
    metrics = result.get("metrics") or {}
    row: dict[str, Any] = {
        "run_id": run_id,
        "ltf": ltf,
        "htf": htf,
        "symbols": symbols,
        "strategies": strategies,
        "error": result.get("error") or "",
        "duration_seconds": result.get("duration_seconds") or 0.0,
        "candles_processed": result.get("candles_processed") or 0,
    }
    for k, v in metrics.items():
        row[f"m_{k}"] = v
    if extra:
        row.update(extra)
    return row


def count_close_reasons(envelope: dict[str, Any], *needles: str) -> int:
    """Count closed trades whose ``close_reason`` contains any of ``needles``.

    Operates on the serialised REST result (trades are dicts, not
    ``SimPosition``).
    """
    result = envelope.get("result") or {}
    n = 0
    for t in result.get("trades") or []:
        reason = (t.get("close_reason") or "").lower()
        if any(ndl.lower() in reason for ndl in needles):
            n += 1
    return n


def count_stop_outs(envelope: dict[str, Any]) -> int:
    """Count trades closed by a stop-loss."""
    return count_close_reasons(envelope, "stop", "sl")


def count_timeouts(envelope: dict[str, Any]) -> int:
    """Count trades closed by timeout / end-of-data (TP never reached)."""
    return count_close_reasons(envelope, "timeout", "end_of_data")


def count_tp(envelope: dict[str, Any]) -> int:
    """Count trades closed at take-profit."""
    return count_close_reasons(envelope, "tp")
