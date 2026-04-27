## Technical Specification: Liquidity-Aware Trading Bot Logic

This summary outlines the mathematical and technical indicators required for an LLM to design or code an automated trading system that minimizes "whipsaw" losses and manages recovery math.

---

### 1. Volatility-Adjusted Stop-Loss (ATR)
Instead of static percentages, the bot should use the **Average True Range (ATR)** to set "breathing room" based on current market volatility.
* **Indicator:** ATR (Standard period: 14).
* **Logic:** Set the Stop-Loss ($SL$) at a multiple ($n$) of the ATR away from the Entry Price ($EP$).
* **Formula:** $SL = EP - (n \times \text{ATR})$
* **Application:** Prevents the bot from being stopped out by "noise." If volatility spikes, the bot automatically widens the stop and reduces position size to keep the dollar risk constant.

### 2. Liquidity Sweep Detection (The "Stop-Run" Filter)
Program the bot to identify "Liquidity Pools" where retail stop-losses cluster (e.g., previous day's lows or equal bottoms).
* **Indicator:** Volume Profile & Swing Highs/Lows.
* **Logic:** **"Sweep Before Entry."** The bot should not enter a "Long" position just because the price reaches support. It should wait for the price to dip *below* support (triggering stops) and then look for a **Market Structure Shift (MSS)**—a candle closing back above the support level.
* **Benefit:** This allows the bot to enter *after* the "math of recovery" has already forced other traders out.



### 3. Order Flow & Heatmap Integration
If the bot has access to Level 2 Market Data (the Order Book), it can "see" where orders are resting.
* **Indicator:** Cumulative Volume Delta (CVD) or Order Book Heatmaps.
* **Logic:** Identify "Limit Order" clusters. If the bot sees a massive wall of sell orders just below the entry, it should adjust the stop-loss to sit *behind* that wall, using the large orders as a physical barrier.

### 4. Dynamic Position Sizing (The 1% Risk Model)
To ensure the **Recovery Percentage ($y$)** remains manageable, the bot must calculate size based on the distance to the stop-loss.
* **Logic:** Total Risk per trade is always a fixed 1% of account equity.
* **Formula:** `Position Size = (Equity * 0.01) / (Entry Price - Stop Price)`
* **Benefit:** If the ATR dictates a wide stop-loss, the bot automatically buys fewer shares. This keeps the potential loss at 1%, requiring only a **1.01%** gain to break even.

---

### 5. Summary Table for LLM Implementation

| Module | Input Indicator | Primary Function |
| :--- | :--- | :--- |
| **Risk Manager** | Equity Balance | Calculates max loss to keep $y$ recovery low. |
| **Volatility Engine** | ATR | Dynamically spaces $SL$ outside of market noise. |
| **Liquidity Scanner** | Volume Profile | Identifies "Trap Zones" and obvious retail levels. |
| **Execution Logic** | Candle Close / MSS | Wait for "Stop-Loss Hunt" to finish before entering. |
| **Time Filter** | Session Clock | Exits stagnant trades to avoid "Theta decay" or dead capital. |