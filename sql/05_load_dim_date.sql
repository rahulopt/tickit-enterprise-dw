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
--
-- Note: stl_load_errors was previously used as a row source but
--   is unreliable — row count varies per environment and may
--   produce fewer than 1096 rows. generate_series is the correct
--   Redshift-native approach for sequence generation.
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
    SELECT ('2008-01-01'::DATE + n) AS d
    FROM generate_series(0, 1095) AS n   -- 1096 days: 2008-01-01 to 2010-12-31
                                         -- 2008 (366 leap) + 2009 (365) + 2010 (365) = 1096
) dates
WHERE d <= '2010-12-31'::DATE;

-- Verify load: expect 1096 rows, dateid range 20080101–20101231
SELECT COUNT(*) AS total_dates, MIN(caldate) AS start_date, MAX(caldate) AS end_date,
       MIN(dateid) AS min_dateid, MAX(dateid) AS max_dateid
FROM dim.date;
