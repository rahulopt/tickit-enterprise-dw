-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 05_load_dim_date.sql
-- Purpose: Populate dim.date (static calendar - load once)
-- Date range: 2008-01-01 to 2010-12-31 (1096 days)
--
-- dateid format: INTEGER YYYYMMDD (e.g. 20080101)
--   - Matches dateid in stage.sales, stage.listing, stage.event
--   - Human-readable, easy to filter (WHERE dateid = 20080315)
--   - Fits comfortably in INTEGER (max ~20101231, well within range)
--
-- Approach: Bulk INSERT via cross-joined digit CTEs
--   - Row-by-row WHILE loops in plpgsql are extremely slow in Redshift
--     (each INSERT is a separate transaction round-trip)
--   - Recursive CTEs are not supported in Redshift
--   - generate_series() availability varies across Redshift versions
--   - Cross-joining single-column digit tables to produce a sequence
--     is the standard portable Redshift pattern for bulk calendar loads
--
-- Note: Julian day (TO_CHAR(d, 'J')) was previously used but
--   produces values ~2454833+ which overflow SMALLINT (max 32767).
--   YYYYMMDD is the correct dateid format.
-- ============================================================

-- Truncate first — idempotent, safe to re-run
TRUNCATE TABLE dim.date;

INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
WITH
-- Generate digits 0-9
digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
    UNION ALL SELECT 8 UNION ALL SELECT 9
),
-- Cross join to get 0–1999 (covers 1096 days needed)
sequence AS (
    SELECT (d2.n * 100 + d1.n * 10 + d0.n) AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    WHERE (d2.n * 100 + d1.n * 10 + d0.n) <= 1095  -- 1096 days: index 0 to 1095
),
-- Generate one date per row
dates AS (
    SELECT DATEADD(day, n, '2008-01-01'::DATE) AS d
    FROM sequence
)
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)         AS dateid,
    d                                                 AS caldate,
    TO_CHAR(d, 'Dy')                                 AS day,
    CAST(DATE_PART('week',    d) AS SMALLINT)        AS week,
    TO_CHAR(d, 'Mon')                                AS month,
    CAST(DATE_PART('quarter', d) AS CHAR(5))         AS qtr,
    CAST(DATE_PART('year',    d) AS SMALLINT)        AS year,
    FALSE                                             AS holiday
FROM dates
WHERE d <= '2010-12-31'::DATE
ORDER BY d;

-- Verify: expect 1096 rows, dateid range 20080101–20101231
-- 2008 (leap, 366) + 2009 (365) + 2010 (365) = 1096
SELECT COUNT(*) AS total_dates,
       MIN(caldate) AS start_date, MAX(caldate) AS end_date,
       MIN(dateid)  AS min_dateid, MAX(dateid)  AS max_dateid
FROM dim.date;
