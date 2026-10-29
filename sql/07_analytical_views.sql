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
-- SCD Type 2 join pattern:
--   fact.sales stores event_key and seller_key/buyer_key —
--   these are SURROGATE keys pointing to a SPECIFIC historical
--   version of dim.event / dim.users.
--
--   DO NOT add WHERE is_current = TRUE on dim.event joins.
--   Doing so would re-map historical sales to the current event
--   version, breaking point-in-time accuracy (e.g., a sale at
--   an old venue would appear under the venue's new name/city).
--
--   For dim.users in seller/buyer views: same rule applies for
--   historical accuracy. Current views report on current seller
--   profiles (is_current = TRUE) only where explicitly noted.
--
-- Verified result: top venue = Jacobs Inc Arena, Jacksonville
--   with ~$82.5M total revenue.
-- ============================================================

-- 1. Which venues make the most money?
--    JOIN on event_key (surrogate) — no is_current filter needed.
--    Each fact.sales row already points to the correct dim.event
--    version that was current at time of sale.
CREATE OR REPLACE VIEW v_venue_performance AS
SELECT e.venuename, e.venuecity, e.venuestate,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
GROUP BY e.venuename, e.venuecity, e.venuestate
ORDER BY total_revenue DESC;

-- 2. Who are our best sellers?  (role-playing dimension: seller_key → dim.users)
--    JOIN on user_key (surrogate) — no is_current filter needed.
--    seller_key already points to the dim.users version at time of sale.
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
GROUP BY u.username, u.firstname, u.lastname, u.city, u.state
ORDER BY total_revenue DESC;

-- 3. What events sell best?
--    JOIN on event_key (surrogate) — no is_current filter.
--    Historical accuracy: sale at "old venue name" stays with old venue name.
CREATE OR REPLACE VIEW v_top_events AS
SELECT e.eventname, e.catname, e.venuename, e.venuecity,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
GROUP BY e.eventname, e.catname, e.venuename, e.venuecity
ORDER BY total_revenue DESC;

-- 4. Revenue trends over time?  (dim.date is DISTSTYLE ALL — zero shuffle)
--    dateid join — dim.date has no SCD, plain lookup.
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

-- 5. Category performance?
--    JOIN on event_key (surrogate) — no is_current filter.
--    catgroup/catname come from the dim.event version at time of sale.
CREATE OR REPLACE VIEW v_category_performance AS
SELECT e.catgroup, e.catname,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
JOIN dim.event e ON s.event_key = e.event_key
GROUP BY e.catgroup, e.catname
ORDER BY total_revenue DESC;
