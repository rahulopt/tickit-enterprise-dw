-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 05_load_dim_date.sql
-- Purpose: Populate dim.date (static calendar - load once)
-- Date range: 2008-01-01 to 2010-12-31 (covers TICKIT dataset)
--
-- dateid format: INTEGER YYYYMMDD (e.g. 20080101)
--   - Matches dateid in stage.sales, stage.listing, stage.event
--   - Human-readable, easy to filter (WHERE dateid = 20080315)
--   - Fits comfortably in INTEGER (max ~20101231, well within range)
--
-- Note: Julian day number (TO_CHAR(d, 'J')) was previously used
--   but produces values ~2454833+ which overflow SMALLINT (max 32767)
--   and are not human-readable. YYYYMMDD is the correct choice.
-- ============================================================

INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)                     AS dateid,
    d::DATE                                                       AS caldate,
    TO_CHAR(d, 'Dy')                                             AS day,
    CAST(DATE_PART('week', d) AS SMALLINT)                      AS week,
    TO_CHAR(d, 'Mon')                                            AS month,
    CAST(DATE_PART('quarter', d) AS CHAR(5))                    AS qtr,
    CAST(DATE_PART('year', d) AS SMALLINT)                      AS year,
    FALSE                                                         AS holiday
FROM (
    SELECT DATEADD(day, seq.n, '2008-01-01'::DATE) AS d
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS n
        FROM stl_load_errors LIMIT 1100
        UNION ALL
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 + 1000 AS n
        FROM stl_load_errors LIMIT 100
    ) seq
    WHERE DATEADD(day, seq.n, '2008-01-01'::DATE) <= '2010-12-31'::DATE
);

-- Verify load
SELECT COUNT(*) AS total_dates, MIN(caldate) AS start_date, MAX(caldate) AS end_date,
       MIN(dateid) AS min_dateid, MAX(dateid) AS max_dateid
FROM dim.date;
