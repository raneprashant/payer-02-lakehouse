-- Run in Databricks SQL Editor ONCE for the DEV catalog.
-- Creates DEV catalog if missing (requires sufficient Unity Catalog permissions).
-- No TEST/PROD data is touched.
-- This creates schemas and two managed Unity Catalog volumes.
CREATE CATALOG IF NOT EXISTS payer_02_dev;
CREATE SCHEMA IF NOT EXISTS payer_02_dev.landing;
CREATE SCHEMA IF NOT EXISTS payer_02_dev.bronze;
CREATE SCHEMA IF NOT EXISTS payer_02_dev.silver;
CREATE SCHEMA IF NOT EXISTS payer_02_dev.gold;
CREATE SCHEMA IF NOT EXISTS payer_02_dev.control;

CREATE VOLUME IF NOT EXISTS payer_02_dev.landing.raw_files;
CREATE VOLUME IF NOT EXISTS payer_02_dev.control.checkpoints;

-- Verify once:
SHOW VOLUMES IN payer_02_dev.landing;
SHOW VOLUMES IN payer_02_dev.control;
