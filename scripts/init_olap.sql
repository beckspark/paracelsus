-- Initialize OLAP database with schemas for Meltano/dbt pipeline
-- This script runs on OLAP database startup

-- Create schema for raw data from Meltano (Singer targets)
CREATE SCHEMA IF NOT EXISTS raw;

-- Create schemas for dbt models
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant permissions to warehouse user
GRANT ALL PRIVILEGES ON SCHEMA raw TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA staging TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA marts TO warehouse;

-- Set default search path
ALTER DATABASE analytics SET search_path TO public, raw, staging, intermediate, marts;
