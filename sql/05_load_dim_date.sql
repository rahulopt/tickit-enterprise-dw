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
-- Approach: DATEADD inside a stored procedure loop
--   generate_series() is not available in all Redshift versions
--   and behaves differently in Redshift Serverless vs provisioned.
--   A plpgsql loop with DATEADD is explicit, portable, and reliable.
--
-- Note: Julian day (TO_CHAR(d, 'J')) was previously used but
--   produces values ~2454833+ which overflow SMALLINT (max 32767).
--   YYYYMMDD is the correct dateid format.
-- ============================================================

CREATE OR REPLACE PROCEDURE sp_load_dim_date()
AS $$
DECLARE
    v_date      DATE    := '2008-01-01';
    v_end_date  DATE    := '2010-12-31';
    v_count     INTEGER := 0;
BEGIN
    -- Truncate first so procedure is idempotent (safe to re-run)
    TRUNCATE TABLE dim.date;

    WHILE v_date <= v_end_date LOOP
        INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
        VALUES (
            CAST(TO_CHAR(v_date, 'YYYYMMDD') AS INTEGER),   -- e.g. 20080101
            v_date,
            TO_CHAR(v_date, 'Dy'),                           -- Mon, Tue, ...
            CAST(DATE_PART('week',    v_date) AS SMALLINT),
            TO_CHAR(v_date, 'Mon'),                          -- Jan, Feb, ...
            CAST(DATE_PART('quarter', v_date) AS CHAR(5)),
            CAST(DATE_PART('year',    v_date) AS SMALLINT),
            FALSE
        );

        v_date  := DATEADD(day, 1, v_date);
        v_count := v_count + 1;
    END LOOP;

    RAISE INFO 'sp_load_dim_date: Inserted % rows (% to %)',
        v_count, '2008-01-01', '2010-12-31';
    -- Expected: 1096 rows (2008 leap=366 + 2009=365 + 2010=365)
END;
$$ LANGUAGE plpgsql;

-- Execute the procedure to populate dim.date
CALL sp_load_dim_date();

-- Verify load: expect 1096 rows, dateid range 20080101–20101231
SELECT COUNT(*) AS total_dates, MIN(caldate) AS start_date, MAX(caldate) AS end_date,
       MIN(dateid) AS min_dateid, MAX(dateid) AS max_dateid
FROM dim.date;
