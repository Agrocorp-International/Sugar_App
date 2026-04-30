-- Add date-order indexes for the positions pages.
--
-- These match the ORDER BY used by /sugar/positions and /cotton/positions:
--   (data->>'Trade_Date__c') DESC
--
-- We deliberately use the text expression rather than CAST(... AS DATE):
-- text->date is STABLE (depends on DateStyle) and cannot be used in
-- expression indexes, while ->> is IMMUTABLE. Trade_Date__c is always
-- stored as ISO 'YYYY-MM-DD' (Salesforce + openpyxl normalisation),
-- so text DESC sorts identically to date DESC.
--
-- Run once against Azure Postgres (database: agrocorpproddb):
--   psql "host=... dbname=agrocorpproddb user=..." -f scripts/add_trade_date_order_indexes.sql
-- or open in pgAdmin Query Tool with auto-commit ON.
--
-- Safe to run multiple times. CONCURRENTLY avoids blocking normal reads/writes
-- while the index is built; do not wrap this file in an explicit transaction.

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sugar_trade_positions_trade_date_desc
ON sugar_trade_positions ((data->>'Trade_Date__c') DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cotton_trade_positions_trade_date_desc
ON cotton_trade_positions ((data->>'Trade_Date__c') DESC);
