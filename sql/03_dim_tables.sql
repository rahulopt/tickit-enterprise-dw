-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 03_dim_tables.sql
-- Purpose: Dimension tables - SCD Type 2 historical tracking
--
-- PHYSICAL DESIGN DECISIONS (Data Engineer choices):
--
-- dim.date
--   DISTSTYLE ALL → 365 rows, replicate to every node.
--   Every fact query JOINs on dateid. With ALL, every node
--   has a local copy — zero network shuffles for date lookups.
--
-- dim.users (SCD Type 2)
--   DISTKEY(userid) → fact.sales has sellerid/buyerid = userid.
--   Co-locating on userid means JOIN between fact.sales and
--   dim.users (both seller and buyer lookups) is node-local.
--   COMPOUND SORTKEY(userid, effective_date) → SCD Type 2
--   lookups need "WHERE userid = X AND effective_date <= sale_date
--   AND expiry_date > sale_date". Compound sort on userid first
--   means zone maps skip users not in range, effective_date second
--   gives efficient range scan within a user's versions.
--
-- dim.event (Denormalized SCD Type 2)
--   DISTKEY(eventid) → fact.sales has eventid. Co-locating
--   dim.event with fact.sales on eventid is the most impactful
--   optimization — every sales query joins to event details.
--   COMPOUND SORTKEY(eventid, effective_date) → same SCD lookup
--   pattern as users.
--   Denormalized: venue + category columns embedded here.
--   Rationale: Analysts query "events at venue X" and "events
--   in category Y" far more than venue/category standalone.
--   2 joins eliminated per query = significant at 22.5M rows.
-- ============================================================

-- -------------------------------------------------------------
-- dim.date
-- Static calendar table — populated once, never changes
-- DISTSTYLE ALL: tiny table (365-400 rows), replicate everywhere
-- -------------------------------------------------------------
DROP TABLE IF EXISTS dim.date;
CREATE TABLE dim.date (
    dateid      SMALLINT        NOT NULL,
    caldate     DATE            NOT NULL,
    day         CHAR(3)         NOT NULL,     -- Mon, Tue, ...
    week        SMALLINT        NOT NULL,
    month       CHAR(5)         NOT NULL,     -- Jan, Feb, ...
    qtr         CHAR(5)         NOT NULL,     -- 1, 2, 3, 4
    year        SMALLINT        NOT NULL,
    holiday     BOOLEAN         DEFAULT FALSE
)
DISTSTYLE ALL
SORTKEY(dateid);

-- -------------------------------------------------------------
-- dim.users (SCD Type 2)
-- Surrogate key: user_key (system generated, never reused)
-- Natural key:   userid
-- SCD tracking:  effective_date, expiry_date, is_current
-- -------------------------------------------------------------
DROP TABLE IF EXISTS dim.users;
CREATE TABLE dim.users (
    -- surrogate key - unique per SCD version
    user_key      BIGINT          IDENTITY(1,1)   NOT NULL,
    -- natural key - same userid can appear multiple times (one per version)
    userid        INTEGER         NOT NULL,
    username      CHAR(8),
    firstname     VARCHAR(30),
    lastname      VARCHAR(30),
    city          VARCHAR(30),
    state         CHAR(2),
    email         VARCHAR(100),
    phone         CHAR(14),
    likesports    BOOLEAN,
    liketheatre   BOOLEAN,
    likeconcerts  BOOLEAN,
    likejazz      BOOLEAN,
    likeclassical BOOLEAN,
    likeopera     BOOLEAN,
    likerock      BOOLEAN,
    likevegas     BOOLEAN,
    likebroadway  BOOLEAN,
    likemusicals  BOOLEAN,
    -- SCD Type 2 control columns
    effective_date  DATE          NOT NULL,
    expiry_date     DATE          NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTKEY(userid)
COMPOUND SORTKEY(userid, effective_date);

-- -------------------------------------------------------------
-- dim.event (Denormalized SCD Type 2)
-- Surrogate key: event_key
-- Natural key:   eventid
-- Denormalized:  venue + category columns embedded (no separate dims)
-- -------------------------------------------------------------
DROP TABLE IF EXISTS dim.event;
CREATE TABLE dim.event (
    -- surrogate key
    event_key     BIGINT          IDENTITY(1,1)   NOT NULL,
    -- natural keys (kept for reference and joins)
    eventid       INTEGER         NOT NULL,
    venueid       SMALLINT,
    catid         SMALLINT,
    dateid        SMALLINT,
    -- event attributes
    eventname     VARCHAR(200),
    starttime     TIMESTAMP,
    -- denormalized venue attributes
    venuename     VARCHAR(100),
    venuecity     VARCHAR(30),
    venuestate    CHAR(2),
    venueseats    INTEGER,
    -- denormalized category attributes
    catgroup      VARCHAR(10),
    catname       VARCHAR(25),
    catdesc       VARCHAR(50),
    -- SCD Type 2 control columns
    effective_date  DATE          NOT NULL,
    expiry_date     DATE          NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTKEY(eventid)
COMPOUND SORTKEY(eventid, effective_date);
