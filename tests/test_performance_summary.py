from __future__ import annotations

from pathlib import Path

from scripts.performance_summary import build_summary, parse_logs, summary_to_dict


def test_performance_summary_parses_peak_excursion_for_stop_out(tmp_path: Path) -> None:
    log_file = tmp_path / "app.log"
    log_file.write_text(
        "\n".join(
            [
                "2026-08-02 10:00:00,000 UTC · DEBUG:app.services.market_service:Launcher signal: XYZ-USDT-SWAP BUY [trend_pullback] last=10.0 notional=100.0 tp=11.0 sl=9.5",
                "2026-08-02 10:05:00,000 UTC · DEBUG:app.services.market_service:Alternator: XYZ-USDT-SWAP trailing profit — peak_pct=4.2 current_pct=3.8 peak_usd=4.2 current_usd=3.8 pullback_needed=10% — waiting",
                "2026-08-02 10:10:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: -1.0000 USDT (fill 1, trade abc)",
            ]
        ),
        encoding="utf-8",
    )

    pnl_trades, signals, seeded, cleared, summary = parse_logs([log_file])
    summary = build_summary(pnl_trades, signals, seeded, cleared, summary)

    assert len(pnl_trades) == 1
    assert pnl_trades[0].mfe_peak_pct == 4.2
    assert summary.stopout_peak_trades == [
        {
            "ts": "2026-08-02 10:10:00",
            "symbol": "XYZ-USDT-SWAP",
            "strategy": "trend_pullback",
            "pnl_usdt": -1.0,
            "mfe_pct": 4.2,
            "mfe_usd": 4.2,
        }
    ]

    payload = summary_to_dict(summary)
    assert payload["stopout_peak_trades"][0]["mfe_pct"] == 4.2


def test_performance_summary_deduplicates_fills_by_fill_id(tmp_path: Path) -> None:
    """The same closing fill reconciled against multiple trades must count once.

    market_service fill reconciliation can log the same OKX fill id multiple
    times (against different unreconciled trades).  Counting each line as a
    distinct trade double-counts realized PnL and can turn a loss into a
    profit.
    """
    log_file = tmp_path / "app.log"
    log_file.write_text(
        "\n".join(
            [
                "2026-08-02 10:00:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: +0.5000 USDT (fill 42, trade aaaa)",
                "2026-08-02 10:01:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: +0.5000 USDT (fill 42, trade bbbb)",
                "2026-08-02 10:02:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: -1.0000 USDT (fill 43, trade cccc)",
            ]
        ),
        encoding="utf-8",
    )

    pnl_trades, signals, seeded, cleared, summary = parse_logs([log_file])
    summary = build_summary(pnl_trades, signals, seeded, cleared, summary)

    # fill 42 appears twice but must only count once.
    assert len(pnl_trades) == 2
    assert summary.total_trades == 2
    assert summary.total_pnl == -0.5
    assert summary.wins == 1
    assert summary.losses == 1


def test_performance_summary_parses_trademgmt_peak_excursion(tmp_path: Path) -> None:
    """TradeMgmt supervision emits peak_pct/peak_usd lines that must be parsed.

    The TradeMgmt loop tracks peak favorable excursion during a trade; the
    performance summary must pick these up so stop-outs that were once in
    profit are flagged.
    """
    log_file = tmp_path / "app.log"
    log_file.write_text(
        "\n".join(
            [
                "2026-08-02 10:00:00,000 UTC · DEBUG:app.services.market_service:Launcher signal: XYZ-USDT-SWAP BUY [trend_pullback] last=10.0 notional=100.0 tp=11.0 sl=9.5",
                "2026-08-02 10:05:00,000 UTC · DEBUG:app.services.market_service:TradeMgmt: XYZ-USDT-SWAP peak_pct=4.2 current_pct=3.8 peak_usd=4.2 current_usd=3.8",
                "2026-08-02 10:10:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: -1.0000 USDT (fill 1, trade abc)",
            ]
        ),
        encoding="utf-8",
    )

    pnl_trades, signals, seeded, cleared, summary = parse_logs([log_file])
    summary = build_summary(pnl_trades, signals, seeded, cleared, summary)

    assert len(pnl_trades) == 1
    assert pnl_trades[0].mfe_peak_pct == 4.2
    assert summary.stopout_peak_trades == [
        {
            "ts": "2026-08-02 10:10:00",
            "symbol": "XYZ-USDT-SWAP",
            "strategy": "trend_pullback",
            "pnl_usdt": -1.0,
            "mfe_pct": 4.2,
            "mfe_usd": 4.2,
        }
    ]


def test_performance_summary_parses_trademgmt_trough_excursion(tmp_path: Path) -> None:
    """TradeMgmt trough_pct/trough_usd must be parsed from the correct groups.

    Regression test for a capture-group index swap: the trough fields were
    previously read from groups 6/7 (current_usd / trough_pct) instead of
    7/8 (trough_pct / trough_usd), so the reported trough % was actually the
    dollar-denominated current PnL and the trough $ was actually the %.
    """
    log_file = tmp_path / "app.log"
    log_file.write_text(
        "\n".join(
            [
                "2026-08-02 10:00:00,000 UTC · DEBUG:app.services.market_service:Launcher signal: XYZ-USDT-SWAP BUY [trend_pullback] last=10.0 notional=100.0 tp=11.0 sl=9.5",
                # current_usd=-0.0936 must NOT be mistaken for trough_pct.
                "2026-08-02 10:05:00,000 UTC · DEBUG:app.services.market_service:TradeMgmt: XYZ-USDT-SWAP peak_pct=4.2 current_pct=-1.86 peak_usd=4.2 current_usd=-0.0936 trough_pct=-1.86 trough_usd=-0.0936",
                "2026-08-02 10:10:00,000 UTC · DEBUG:app.services.market_service:Reconciled PnL for XYZ-USDT-SWAP: +1.0000 USDT (fill 1, trade abc)",
            ]
        ),
        encoding="utf-8",
    )

    pnl_trades, signals, seeded, cleared, summary = parse_logs([log_file])
    summary = build_summary(pnl_trades, signals, seeded, cleared, summary)

    assert len(pnl_trades) == 1
    # The trough % must be -1.86 (not the -0.0936 current_usd dollar value).
    assert pnl_trades[0].mae_trough_pct == -1.86
    assert pnl_trades[0].mae_trough_usd == -0.0936
    assert summary.tp_trough_trades == [
        {
            "ts": "2026-08-02 10:10:00",
            "symbol": "XYZ-USDT-SWAP",
            "strategy": "trend_pullback",
            "pnl_usdt": 1.0,
            "mae_pct": -1.86,
            "mae_usd": -0.0936,
        }
    ]

    payload = summary_to_dict(summary)
    assert payload["tp_trough_trades"][0]["mae_pct"] == -1.86
