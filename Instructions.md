# Tai2 Implementation Prompts: A Step-by-Step Development Guide

This document contains a sequence of structured prompts to be used with an LLM to build the **tai2** trading application. Copy and paste each section into your AI assistant one at a time, ensuring each part is verified and tested before proceeding.

---

## Phase 1: Project Initialization and Environment

> Act as a Senior Python Engineer. Please set up the initial project structure for a cryptocurrency trading application called `tai2`. 
> 
> **Requirements:**
> 1. Use `uv` for package management. Generate a `pyproject.toml` with the following dependencies: `fastapi`, `uvicorn`, `nicegui`, `python-okx`, `pandas-ta`, `redis`, `asyncpg`, `pydantic-settings`, `httpx`, `pandas`, and `pytest`.
> 2. Create a professional directory structure:
>    - `app/core/`: Config and security.
>    - `app/db/`: Database and Redis logic.
>    - `app/services/`: OKX API and LLM logic.
>    - `app/ui/`: NiceGUI pages and components.
>    - `app/models/`: Pydantic schemas.
>    - `tests/`: Pytest suite.
> 3. Create a `config.py` using `Pydantic Settings` that reads from environment variables: `OKX_API_KEY`, `OKX_SECRET_KEY`, `OKX_PASSPHRASE`, `OPENROUTER_API_KEY`, `DATABASE_URL`, `REDIS_URL`, and a `WS_UPDATE_INTERVAL` (default 180 seconds).
> 4. Create a basic `main.py` that initializes a FastAPI app and includes a "Hello World" NiceGUI landing page.
> 5. **Testing:** Provide a `pytest` script to verify that the environment variables are loaded correctly and the FastAPI server can start.

---

## Phase 2: Database Schema and Redis State Management

> Now, implement the data layer for `tai2`. We are using **PostgreSQL with TimescaleDB** for trade history and **Redis** for real-time market snapshots.
> 
> **Requirements:**
> 1. **PostgreSQL/TimescaleDB:** Using `asyncpg`, create a schema for `executed_trades`. Fields must include: `id` (UUID), `timestamp` (TIMESTAMPTZ), `symbol`, `side` (buy/sell), `price`, `amount`, `llm_reasoning` (TEXT), and `pnl`.
> 2. **Redis:** Implement a `StateService` to store and retrieve the "Latest Market Snapshot." This snapshot should include current positions, order book status, and the latest calculated indicators.
> 3. **Concurrency:** Ensure the database connections are handled via an async pool.
> 4. **Testing:** Create `tests/test_db.py` to verify that we can write a trade to Postgres and cache/retrieve a JSON market state from Redis.

---

## Phase 3: OKX SDK Integration and Market Data Engine

> Implement the `MarketService` using the official `okx-python-sdk`.
> 
> **Requirements:**
> 1. Initialize the OKX Rest and WebSocket clients using the credentials from `config.py`.
> 2. Implement a background task that fetches:
>    - Current open positions and account balance.
>    - Order book depth (Top 20 levels).
>    - Live ticker data and funding rates.
>    - Open Interest and recent liquidation data.
> 3. **WebSocket Logic:** Use the `WS_UPDATE_INTERVAL` from the config to control how often the data is processed or emitted to the frontend.
> 4. **Professional Metrics:** Use `pandas-ta` to process OHLCV data and calculate: Bollinger Bands, Stochastic RSI, VWAP, and Volume. 
> 5. **Custom Metrics:** Manually calculate **Cumulative Volume Delta (CVD)** and **Order Flow Imbalance** based on the trade stream and order book.
> 6. Every update must refresh the Redis "Latest Market Snapshot."
> 7. **Testing:** Create `tests/test_okx.py` using mocks to verify the data fetching logic and the correctness of the indicator calculations.

---

## Phase 4: LLM Integration (OpenRouter) and Reasoning Engine

> Build the `ReasoningEngine` that connects the market data to the LLM via the **OpenRouter API**.
> 
> **Requirements:**
> 1. Create a service that fetches the "Latest Market Snapshot" from Redis.
> 2. **Prompt Engineering:** Design a default system prompt that instructs the LLM: "You are a professional hedge fund trader. Analyze the following data (Positions, TA Indicators, Order Book, CVD, Open Interest) to decide if a trade should be executed. Optimize for maximum profit and risk management. Reply in JSON format with 'action' (BUY/SELL/HOLD), 'amount', and 'justification'."
> 3. The prompt must be dynamic, allowing the user to update the "System Strategy" via the frontend later.
> 4. Implement an async function to send this data to OpenRouter (selectable model like Claude 3.5 or GPT-4o).
> 5. **Trade Execution Trigger:** If the LLM returns a BUY or SELL, call the `MarketService` to execute the order on OKX and save the result (with the reasoning) to the Postgres DB.
> 6. **Testing:** Create `tests/test_engine.py` to mock an LLM response and verify that a "BUY" signal correctly triggers a (mocked) OKX order and a DB write.

---

## Phase 5: NiceGUI Frontend Development

> Develop the reactive frontend using **NiceGUI**. 
> 
> **Requirements:**
> 1. **Layout:** Create a persistent header with centered hyperlinks separated by pipes: `LIVE | TA | ENGINE | HISTORY | DEBUG | CFG`.
> 2. **LIVE Page:** Display real-time account balances, active positions, and a live-updating ticker using a card-based layout.
> 3. **TA Page:** Implement charts and tables showing `pandas-ta` indicators, Funding Rates, CVD, and Open Interest.
> 4. **ENGINE Page:** >    - Show the LLM's latest "Decision Log" (Reasoning + Action).
>    - Add an interactive chat interface where the user can ask the LLM: "Explain why you haven't traded in the last hour based on the current CVD."
> 5. **HISTORY Page:** A sortable table showing all executed trades from the Postgres database.
> 6. **DEBUG Page:** A dual-window log viewer showing backend logs (market data/orders) and frontend events.
> 7. **CFG Page:** A settings form to update:
>    - `WS_UPDATE_INTERVAL`.
>    - The LLM System Prompt.
>    - The OpenRouter Model ID.
> 8. **Reactivity:** Use NiceGUI's timers or event bus to ensure that when Redis updates, the `LIVE` and `TA` pages refresh without a page reload.

---

## Phase 6: Final Integration and Orchestration

> Tie all components together into the final `tai2` application.
> 
> **Requirements:**
> 1. Create a `startup` event in FastAPI that initializes the Redis connection, Database pool, and starts the OKX WebSocket background task.
> 2. Ensure that the `WS_UPDATE_INTERVAL` set in the `CFG` page immediately updates the background task frequency.
> 3. Add global exception handling to capture API errors and display them in the `DEBUG` page.
> 4. Finalize the `README.md` with instructions on how to run the app using `uv run uvicorn app.main:app`.
> 5. **Testing:** Create a full integration test in `tests/test_integration.py` that simulates a full loop: Data Ingest -> LLM Logic -> Trade Execution -> DB Persistence.