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

## Stack
Flask, SQLAlchemy, simple-salesforce, Bootstrap 5 (CDN).
