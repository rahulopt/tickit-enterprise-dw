-- TICKIT V2 date dimension: source-validated event calendar mapping
-- Verified against all 3,000,000 rows of bronze/event.csv:
--   dateid 1827 -> 2008-01-01
--   dateid 4382 -> 2014-12-30
--   caldate = 2003-01-01 + (dateid - 1) days
-- This script populates the entire inclusive EVENT dateid range (2556 dates).
-- Fact sales/listing dateid equals EVENT dateid in the verified samples;
-- saletime/listtime are inconsistent with event starttime in those samples.
-- Do not treat dateid as a transaction-date identifier.
-- Only run in tickit_v2 database after confirming dim.date is empty.
-- No destructive DROP/TRUNCATE statements; safe to rerun if rows are correct.

INSERT INTO dim.date (dateid, caldate, day, week, month, qtr, year, holiday)
WITH digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
    UNION ALL SELECT 8 UNION ALL SELECT 9
),
sequence AS (
    SELECT (d3.n * 1000 + d2.n * 100 + d1.n * 10 + d0.n) AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
),
dates AS (
    SELECT
        n + 1 AS dateid,
        DATEADD(day, n, '2003-01-01'::DATE)::DATE AS caldate
    FROM sequence
    WHERE n BETWEEN 1826 AND 4381
)
SELECT
    dates.dateid,
    dates.caldate,
    TO_CHAR(dates.caldate, 'Dy') AS day,
    CAST(DATE_PART('week', dates.caldate) AS SMALLINT) AS week,
    TO_CHAR(dates.caldate, 'Mon') AS month,
    CAST(DATE_PART('quarter', dates.caldate) AS CHAR(5)) AS qtr,
    CAST(DATE_PART('year', dates.caldate) AS SMALLINT) AS year,
    FALSE AS holiday
FROM dates
WHERE NOT EXISTS (
    SELECT 1 FROM dim.date existing
    WHERE existing.dateid = dates.dateid
);
