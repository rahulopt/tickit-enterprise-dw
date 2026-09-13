-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 02_stage_tables.sql
-- Purpose: Staging tables - temporary landing for nightly COPY
--
-- PHYSICAL DESIGN DECISIONS (Data Engineer choices):
--   - DISTSTYLE EVEN: Staging tables do full-table TRUNCATE+COPY
--     every night. We don't join staging to anything, so no
--     co-location benefit. EVEN gives best parallel COPY throughput.
--   - No SORTKEY on staging: Data gets truncated every night.
--     Sorting would waste time for a table that only lives hours.
--   - No compression on staging: Redshift AUTO compression works
--     best with data that persists. Staging is transient.
-- ============================================================

-- -------------------------------------------------------------
-- stage.category
-- Source: 11 rows | Lambda ETL | Bronze → Silver → stage
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.category;
CREATE TABLE stage.category (
    catid     SMALLINT,
    catgroup  VARCHAR(10),
    catname   VARCHAR(25),
    catdesc   VARCHAR(50)
)
DISTSTYLE EVEN;

-- -------------------------------------------------------------
-- stage.venue
-- Source: 202 rows | Lambda ETL | Bronze → Silver → stage
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.venue;
CREATE TABLE stage.venue (
    venueid    SMALLINT,
    venuename  VARCHAR(100),
    venuecity  VARCHAR(30),
    venuestate CHAR(2),
    venueseats INTEGER
)
DISTSTYLE EVEN;

-- -------------------------------------------------------------
-- stage.users
-- Source: 2M rows | Glue Python Shell | Bronze → Silver → stage
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.users;
CREATE TABLE stage.users (
    userid        INTEGER,
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
    likemusicals  BOOLEAN
)
DISTSTYLE EVEN;

-- -------------------------------------------------------------
-- stage.event
-- Source: 3M rows | Glue Python Shell | Bronze → Silver → stage
-- 
-- NOTE: Denormalized — venue + category columns merged in.
-- Data modelers decided to denormalize here so ETL joins once
-- and dim.event becomes a single flat lookup, eliminating 2 joins
-- per analytical query.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.event;
CREATE TABLE stage.event (
    eventid       INTEGER,
    venueid       SMALLINT,
    catid         SMALLINT,
    dateid        SMALLINT,
    eventname     VARCHAR(200),
    starttime     TIMESTAMP,
    -- denormalized from venue
    venuename     VARCHAR(100),
    venuecity     VARCHAR(30),
    venuestate    CHAR(2),
    venueseats    INTEGER,
    -- denormalized from category
    catgroup      VARCHAR(10),
    catname       VARCHAR(25),
    catdesc       VARCHAR(50)
)
DISTSTYLE EVEN;

-- -------------------------------------------------------------
-- stage.listing
-- Source: 25M rows | Glue Spark | Bronze → Silver → stage
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.listing;
CREATE TABLE stage.listing (
    listid        INTEGER,
    sellerid      INTEGER,
    eventid       INTEGER,
    dateid        SMALLINT,
    numtickets    SMALLINT,
    priceperticket DECIMAL(8,2),
    totalprice    DECIMAL(8,2),
    listtime      TIMESTAMP
)
DISTSTYLE EVEN;

-- -------------------------------------------------------------
-- stage.sales
-- Source: 22.5M rows | Glue Spark | Bronze → Silver → stage
-- -------------------------------------------------------------
DROP TABLE IF EXISTS stage.sales;
CREATE TABLE stage.sales (
    salesid       INTEGER,
    listid        INTEGER,
    sellerid      INTEGER,
    buyerid       INTEGER,
    eventid       INTEGER,
    dateid        SMALLINT,
    qtysold       SMALLINT,
    pricepaid     DECIMAL(8,2),
    commission    DECIMAL(8,2),
    saletime      TIMESTAMP
)
DISTSTYLE EVEN;
