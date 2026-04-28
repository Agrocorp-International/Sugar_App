# Sugar App

Flask web dashboard for Agrocorp sugar trading. Pulls trade data from Salesforce, stores it in Azure PostgreSQL, and displays it as a fast local dashboard.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in the credentials (Azure Postgres, Salesforce, TradeStation).
3. Run the app:
   ```bash
   python app.py
   ```
4. Open http://localhost:5000

## Routes
- `/` — dashboard with Sync Now button
- `/positions` — trade positions table
- `/info` — reference tables (futures, options, holidays)
- `/options` — options payoff chart
- `POST /sync` — pull from Salesforce → save to DB

## Sugar vs Cotton Positions

The sugar and cotton positions pages intentionally share pagination, filtered empty states, request-scoped market-price caching, inline editing, brokerage-fee display, and PnL calculation patterns. The remaining differences below are intentional for now:

- Strategy shape: sugar uses `instrument-spread-contract_xl-book-BF=fee`; cotton uses `instrument-spread-contract_xl-book-region-BF=fee`.
- Filters: cotton has `region_filter`; sugar has `neon_untagged`.
- Strategy warnings: sugar has the Strategy Warnings panel; cotton does not yet have an equivalent and passes `invalid_strategy_count=0`.
- Bulk tagging: sugar has Tag All Filtered and `/positions/api/filtered-ids`; cotton does not.
- Salesforce push: sugar push is enabled directly; cotton push is gated behind `COTTON_SF_PUSH_ENABLED`.
- Book mapping: sugar maps `Alpha`, `Whites`, and `Raws`; cotton maps `Alpha`, `Physical`, and `Alt Physical`, with `Physical` depending on `contract_xl`.
- Filter controls: sugar auto-submits filters with a hidden Apply button; cotton still shows an Apply button.
- Footer note: sugar has the hardcoded Salesforce filter reminder; cotton does not.
- Commodity math: sugar uses sugar lot multipliers (`SB=1120`, `SW=50`); cotton uses cotton multiplier (`CT=500`).

## Stack
Flask, SQLAlchemy, simple-salesforce, Bootstrap 5 (CDN).
