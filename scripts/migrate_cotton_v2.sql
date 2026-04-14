-- Cotton v2 migration: add 5-part Strategy__c parsed columns to cotton_trade_positions.
-- The 5th column `region` is cotton-only (sugar Strategy__c is 4-part).
--
-- Run once against Azure Postgres before starting the app on v2:
--   psql "host=... dbname=... user=..." -f scripts/migrate_cotton_v2.sql
--
-- Safe to run multiple times (IF NOT EXISTS clauses).
-- The cotton_market_prices and cotton_watched_contracts tables are new
-- and will be created automatically by db.create_all() on first app start.

ALTER TABLE cotton_trade_positions
  ADD COLUMN IF NOT EXISTS instrument   VARCHAR(100),
  ADD COLUMN IF NOT EXISTS spread       VARCHAR(100),
  ADD COLUMN IF NOT EXISTS contract_xl  VARCHAR(100),
  ADD COLUMN IF NOT EXISTS book_parsed  VARCHAR(100),
  ADD COLUMN IF NOT EXISTS region       VARCHAR(100);
