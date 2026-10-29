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
-- Surrogate-key joins preserve the dimension version assigned at load time.
-- Fact loading currently resolves current versions, not historical versions
-- effective at saletime. These views do not establish point-in-time accuracy.
-- Business results require independently executed and saved query evidence.
-- ============================================================

-- 1. Which venues make the most money?
--    JOIN on event_key (surrogate) — no is_current filter needed.
--    Each fact.sales row points to the dim.event version assigned at load time.
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
--    seller_key points to the dim.users version assigned at load time.
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
--    Reports the event attributes assigned when the fact was loaded.
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

-- 4. Revenue by transaction month, using saletime rather than event dateid.
--    NULL timestamps are excluded because they have no transaction month.
CREATE OR REPLACE VIEW v_monthly_sales AS
SELECT DATE_PART(year, s.saletime)::smallint AS year,
       DATE_PART(month, s.saletime)::int AS month_num,
       TO_CHAR(s.saletime, 'Mon')::char(5) AS month_name,
       COUNT(DISTINCT s.salesid) AS total_sales,
       SUM(s.qtysold)            AS total_tickets_sold,
       SUM(s.pricepaid)          AS total_revenue,
       AVG(s.pricepaid)          AS avg_sale_price
FROM fact.sales s
WHERE s.saletime IS NOT NULL
GROUP BY DATE_PART(year, s.saletime), DATE_PART(month, s.saletime),
         TO_CHAR(s.saletime, 'Mon')::char(5)
ORDER BY year, month_num;

-- 5. Category performance?
--    JOIN on event_key (surrogate) — no is_current filter.
--    catgroup/catname come from the dim.event version assigned at load time.
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
