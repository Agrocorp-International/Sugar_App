-- Add date-order indexes for the positions pages.
--
-- These match the ORDER BY used by /sugar/positions and /cotton/positions:
--   CAST(data->>'Trade_Date__c' AS DATE) DESC
--
-- Run once against Azure Postgres:
--   psql "host=... dbname=... user=..." -f scripts/add_trade_date_order_indexes.sql
--
-- Safe to run multiple times. CONCURRENTLY avoids blocking normal reads/writes
-- while the index is built; do not wrap this file in an explicit transaction.

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sugar_trade_positions_trade_date_desc
ON sugar_trade_positions ((CAST(data->>'Trade_Date__c' AS DATE)) DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cotton_trade_positions_trade_date_desc
ON cotton_trade_positions ((CAST(data->>'Trade_Date__c' AS DATE)) DESC);
