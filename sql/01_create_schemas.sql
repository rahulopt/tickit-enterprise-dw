-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 01_create_schemas.sql
-- Purpose: Create 3-tier schema structure
-- Schemas: stage (landing), dim (dimensions), fact (facts)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
