-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 07_analytical_views.sql
-- Purpose: 5 business-facing analytical views (Gold layer)
--
-- Why views? DDL deploy created only tables. Analysts want
--   reusable, named queries — a view is a saved SELECT the
--   business team can query without knowing the joins.
--
-- Every view reads from the DENORMALIZED dim.event, so venue
--   and category attributes come free with no extra joins
--   (1 join vs 3 — significant at 22.5M sales rows).
--
-- Verified result: top venue = Jacobs Inc Arena, Jacksonville
--   with ~$82.5M total revenue.
-- ============================================================

-- 1. Which venues make the most money?
CREATE OR REPLACE VIEW v_venue_performance AS
SELECT e.venuename, e.venuecity, e.venuestate,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
WHERE e.is_current = TRUE
GROUP BY e.venuename, e.venuecity, e.venuestate
ORDER BY total_revenue DESC;

-- 2. Who are our best sellers?  (role-playing dimension: seller_key → dim.users)
CREATE OR REPLACE VIEW v_seller_performance AS
SELECT u.username,
       u.firstname || ' ' || u.lastname AS seller_name,
       u.city, u.state,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       SUM(s.commission)         AS total_commission_paid
FROM fact.sales s
JOIN dim.users u ON s.seller_key = u.user_key
WHERE u.is_current = TRUE
GROUP BY u.username, u.firstname, u.lastname, u.city, u.state
ORDER BY total_revenue DESC;

-- 3. What events sell best?  (single join: fact.sales → dim.event holds event+venue+category)
CREATE OR REPLACE VIEW v_top_events AS
SELECT e.eventname, e.catname, e.venuename, e.venuecity,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
WHERE e.is_current = TRUE
GROUP BY e.eventname, e.catname, e.venuename, e.venuecity
ORDER BY total_revenue DESC;

-- 4. Revenue trends over time?  (dim.date is DISTSTYLE ALL — zero shuffle)
--    NOTE: dim.date columns are (dateid, caldate, day, week, month, qtr, year).
--    There is no month_num/month_name column, so month order is derived
--    from caldate via DATE_PART, and month label comes from the CHAR(5) `month`.
CREATE OR REPLACE VIEW v_monthly_sales AS
SELECT d.year,
       DATE_PART(month, d.caldate)::int AS month_num,
       d.month                          AS month_name,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.date d ON s.dateid = d.dateid
GROUP BY d.year, DATE_PART(month, d.caldate), d.month
ORDER BY d.year, month_num;

-- 5. Category performance?  (catgroup, catname from denormalized dim.event)
CREATE OR REPLACE VIEW v_category_performance AS
SELECT e.catgroup, e.catname,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
WHERE e.is_current = TRUE
GROUP BY e.catgroup, e.catname
ORDER BY total_revenue DESC;
