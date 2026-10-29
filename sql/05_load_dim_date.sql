-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 05_load_dim_date.sql
-- Purpose: Populate dim.date (static calendar - load once)
-- Date range: 2008-01-01 to 2010-12-31 (1096 days)
--
-- dateid format: sequential TICKIT source key (2008-01-01 = 1)
--   - Matches the original TICKIT dataset date keys (1 through 1096)
--   - DO NOT substitute YYYYMMDD unless source IDs are also transformed
--
-- Approach: Bulk INSERT via cross-joined digit CTEs
--   - Row-by-row WHILE loops in plpgsql are extremely slow in Redshift
--     (each INSERT is a separate transaction round-trip)
--   - Recursive CTEs are not supported in Redshift
--   - generate_series() availability varies across Redshift versions
--   - Cross-joining single-column digit tables to produce a sequence
--     is the standard portable Redshift pattern for bulk calendar loads
--
-- Date IDs are TICKIT surrogate keys rather than formatted dates.
-- This script assumes the source uses 1 for 2008-01-01, etc.
-- Verify against bronze data before loading a different TICKIT variant.
-- ============================================================

-- Incremental/idempotent: preserve existing date rows if this script is rerun.
-- Do not TRUNCATE this shared dimension while fact queries may be running.
INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
WITH
-- Generate digits 0-9
digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
    UNION ALL SELECT 8 UNION ALL SELECT 9
),
-- Cross join four digits to generate 0–1095 (1096 days)
sequence AS (
    SELECT (d3.n * 1000 + d2.n * 100 + d1.n * 10 + d0.n) AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
    WHERE (d3.n * 1000 + d2.n * 100 + d1.n * 10 + d0.n) <= 1095  -- 1096 days: index 0 to 1095
),
-- Generate one date per row
dates AS (
    SELECT DATEADD(day, n, '2008-01-01'::DATE) AS d
    FROM sequence
)
SELECT
    DATEDIFF(day, '2008-01-01'::DATE, d) + 1        AS dateid,
    d                                                 AS caldate,
    TO_CHAR(d, 'Dy')                                 AS day,
    CAST(DATE_PART('week',    d) AS SMALLINT)        AS week,
    TO_CHAR(d, 'Mon')                                AS month,
    CAST(DATE_PART('quarter', d) AS CHAR(5))         AS qtr,
    CAST(DATE_PART('year',    d) AS SMALLINT)        AS year,
    FALSE                                             AS holiday
FROM dates
WHERE d <= '2010-12-31'::DATE
  AND NOT EXISTS (SELECT 1 FROM dim.date existing WHERE existing.caldate = d)
ORDER BY d;

-- Verify: expect 1096 rows, dateid range 1–1096
-- 2008 (leap, 366) + 2009 (365) + 2010 (365) = 1096
SELECT COUNT(*) AS total_dates,
       MIN(caldate) AS start_date, MAX(caldate) AS end_date,
       MIN(dateid)  AS min_dateid, MAX(dateid)  AS max_dateid
FROM dim.date;
