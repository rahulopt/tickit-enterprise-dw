-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 05_load_dim_date.sql
-- Purpose: Populate dim.date (static calendar - load once)
-- Date range: 2008-01-01 to 2010-12-31 (covers TICKIT dataset)
-- ============================================================

-- COPY from S3 using Redshift's native date generation
-- We use INSERT with generate_series equivalent in Redshift

INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
SELECT
    CAST(TO_CHAR(d, 'J') AS SMALLINT)                           AS dateid,
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

-- Simpler approach using Redshift's generate_series (available in newer versions)
-- If above fails, use this:
-- 
-- INSERT INTO dim.date
-- SELECT
--     CAST(TO_CHAR(d::DATE, 'J') AS SMALLINT),
--     d::DATE,
--     TO_CHAR(d::DATE, 'Dy'),
--     CAST(EXTRACT(WEEK FROM d::DATE) AS SMALLINT),
--     TO_CHAR(d::DATE, 'Mon'),
--     CAST(EXTRACT(QUARTER FROM d::DATE) AS CHAR(5)),
--     CAST(EXTRACT(YEAR FROM d::DATE) AS SMALLINT),
--     FALSE
-- FROM (SELECT ('2008-01-01'::DATE + (n * INTERVAL '1 day'))::DATE AS d
--       FROM generate_series(0, 1095) n) dates;

-- Verify load
SELECT COUNT(*) AS total_dates, MIN(caldate) AS start_date, MAX(caldate) AS end_date
FROM dim.date;
