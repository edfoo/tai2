"""Backtesting engine for tai2.

Reuses the live trading strategy evaluation pipeline (``_compute_indicators``,
``_compute_structure``, ``Strategy.evaluate``) against historical OHLCV data
fetched from OKX, with a simulated broker that replicates OKX algo-order
TP/SL behaviour.

Modules
-------
models          – Dataclasses for backtest config, trades, equity, results.
data_fetcher   – Paginated OKX historical OHLCV fetcher with file cache.
snapshot_builder– Builds synthetic snapshots from historical candles.
simulator       – Simulated broker (fills, TP/SL close, position tracking).
metrics         – Performance metrics (Sharpe, max DD, win rate, etc.).
engine          – Orchestrator that ties everything together.
"""

from __future__ import annotations
