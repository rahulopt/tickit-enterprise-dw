-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 04_fact_tables.sql
-- Purpose: Fact tables - append-only transactional history
--
-- PHYSICAL DESIGN DECISIONS (Data Engineer choices):
--
-- fact.sales (22.5M rows, grows nightly)
--   DISTKEY(eventid) → The most common analytical query is
--   "revenue by event/venue". This join between fact.sales and
--   dim.event is the hottest join path. Co-locating both tables
--   on eventid means this join NEVER crosses nodes.
--   Why not sellerid/buyerid? Users are a role-playing dimension
--   requiring 2 joins per query. Eventid is 1 join and is the
--   primary analytics axis (revenue by venue requires eventid→venueid).
--   
--   COMPOUND SORTKEY(dateid, eventid) → Analysts filter by date
--   range constantly ("last 30 days revenue"). dateid first gives
--   zone map benefit for time-range filters. eventid second gives
--   efficient co-sorted scans for event+date queries.
--
-- fact.listing (25M rows, grows nightly)
--   DISTKEY(eventid) → Same reasoning. Listings are "what's 
--   available for event X" — eventid is the natural join key.
--   COMPOUND SORTKEY(dateid, eventid) → Same time-range pattern.
-- ============================================================

-- -------------------------------------------------------------
-- fact.sales
-- Grain: one row per ticket sale transaction
-- Role-playing dimension: both seller_key and buyer_key point
-- to dim.users (same table, two roles)
-- salesid: natural key from source, used as idempotency check
-- (INSERT INTO fact.sales ... WHERE NOT EXISTS prevents dupes)
-- -------------------------------------------------------------
DROP TABLE IF EXISTS fact.sales;
CREATE TABLE fact.sales (
    -- degenerate dimension (natural key from source)
    salesid       INTEGER         NOT NULL,
    -- foreign keys to dimensions
    event_key     BIGINT          NOT NULL,   -- → dim.event.event_key
    seller_key    BIGINT          NOT NULL,   -- → dim.users.user_key (seller)
    buyer_key     BIGINT          NOT NULL,   -- → dim.users.user_key (buyer)
    dateid        SMALLINT        NOT NULL,   -- → dim.date.dateid
    -- degenerate dimensions (low cardinality, not worth separate dim)
    listid        INTEGER,
    qtysold       SMALLINT,
    -- measures
    pricepaid     DECIMAL(8,2),
    commission    DECIMAL(8,2),
    saletime      TIMESTAMP,
    -- audit column
    load_ts       TIMESTAMP       DEFAULT GETDATE()
)
DISTKEY(eventid)       -- co-locate with dim.event for revenue by event queries
COMPOUND SORTKEY(dateid, eventid);

-- Note: eventid used in DISTKEY but not stored as column — 
-- Redshift requires DISTKEY column to be in the table.
-- Corrected version:
DROP TABLE IF EXISTS fact.sales;
CREATE TABLE fact.sales (
    salesid       INTEGER         NOT NULL,
    event_key     BIGINT          NOT NULL,
    seller_key    BIGINT          NOT NULL,
    buyer_key     BIGINT          NOT NULL,
    dateid        SMALLINT        NOT NULL,
    eventid       INTEGER         NOT NULL,   -- kept for DISTKEY + joins
    listid        INTEGER,
    qtysold       SMALLINT,
    pricepaid     DECIMAL(8,2),
    commission    DECIMAL(8,2),
    saletime      TIMESTAMP,
    load_ts       TIMESTAMP       DEFAULT GETDATE()
)
DISTKEY(eventid)
COMPOUND SORTKEY(dateid, eventid);

-- -------------------------------------------------------------
-- fact.listing
-- Grain: one row per ticket listing posted on marketplace
-- listid: natural key, used as idempotency check
-- -------------------------------------------------------------
DROP TABLE IF EXISTS fact.listing;
CREATE TABLE fact.listing (
    listid          INTEGER         NOT NULL,
    event_key       BIGINT          NOT NULL,
    seller_key      BIGINT          NOT NULL,
    dateid          SMALLINT        NOT NULL,
    eventid         INTEGER         NOT NULL,   -- kept for DISTKEY
    numtickets      SMALLINT,
    priceperticket  DECIMAL(8,2),
    totalprice      DECIMAL(8,2),
    listtime        TIMESTAMP,
    load_ts         TIMESTAMP       DEFAULT GETDATE()
)
DISTKEY(eventid)
COMPOUND SORTKEY(dateid, eventid);
