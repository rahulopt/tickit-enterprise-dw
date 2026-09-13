-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 06_stored_procedures.sql
-- Purpose: 3 stored procedures for Gold layer loading
--
-- Procedure 1: sp_load_staging
--   TRUNCATE + COPY from Silver Parquet → stage.* tables
--   Called after all 6 ETL jobs complete
--
-- Procedure 2: sp_load_dimensions
--   SCD Type 2 logic for dim.users and dim.event
--   Detects changes, closes old versions, inserts new versions
--   Also handles simple INSERT for first load (no existing records)
--
-- Procedure 3: sp_load_facts
--   Appends new records to fact.sales and fact.listing
--   Performs surrogate key lookup (userid → user_key, eventid → event_key)
--   Idempotent: uses NOT EXISTS to skip already-loaded salesids/listids
-- ============================================================

-- ============================================================
-- PROCEDURE 1: sp_load_staging
-- ============================================================
-- Purpose: Load Silver Parquet files into Redshift staging tables.
-- Pattern: TRUNCATE (tonight's batch only) → COPY from Silver S3
-- Why TRUNCATE not DROP: DDL is expensive. Truncate preserves
--   structure and vacuums automatically.
-- IAM Role: RedshiftServerlessRole has S3 read access
-- ============================================================

CREATE OR REPLACE PROCEDURE sp_load_staging(
    p_s3_bucket     VARCHAR(200),
    p_iam_role_arn  VARCHAR(500)
)
AS $$
DECLARE
    v_s3_base       VARCHAR(300);
    v_copy_options  VARCHAR(200);
BEGIN
    v_s3_base      := 's3://' || p_s3_bucket || '/silver/';
    v_copy_options := 'FORMAT AS PARQUET IAM_ROLE ''' || p_iam_role_arn || '''';

    RAISE INFO 'sp_load_staging: Starting at %', GETDATE();

    -- ── category ──────────────────────────────────────────────
    TRUNCATE TABLE stage.category;
    EXECUTE 'COPY stage.category FROM ''' || v_s3_base || 'category/ '' ' || v_copy_options;
    RAISE INFO 'stage.category loaded: % rows', (SELECT COUNT(*) FROM stage.category);

    -- ── venue ─────────────────────────────────────────────────
    TRUNCATE TABLE stage.venue;
    EXECUTE 'COPY stage.venue FROM ''' || v_s3_base || 'venue/ '' ' || v_copy_options;
    RAISE INFO 'stage.venue loaded: % rows', (SELECT COUNT(*) FROM stage.venue);

    -- ── users ─────────────────────────────────────────────────
    TRUNCATE TABLE stage.users;
    EXECUTE 'COPY stage.users FROM ''' || v_s3_base || 'users/ '' ' || v_copy_options;
    RAISE INFO 'stage.users loaded: % rows', (SELECT COUNT(*) FROM stage.users);

    -- ── event (denormalized) ──────────────────────────────────
    TRUNCATE TABLE stage.event;
    EXECUTE 'COPY stage.event FROM ''' || v_s3_base || 'event/ '' ' || v_copy_options;
    RAISE INFO 'stage.event loaded: % rows', (SELECT COUNT(*) FROM stage.event);

    -- ── listing ───────────────────────────────────────────────
    TRUNCATE TABLE stage.listing;
    EXECUTE 'COPY stage.listing FROM ''' || v_s3_base || 'listing/ '' ' || v_copy_options;
    RAISE INFO 'stage.listing loaded: % rows', (SELECT COUNT(*) FROM stage.listing);

    -- ── sales ─────────────────────────────────────────────────
    TRUNCATE TABLE stage.sales;
    EXECUTE 'COPY stage.sales FROM ''' || v_s3_base || 'sales/ '' ' || v_copy_options;
    RAISE INFO 'stage.sales loaded: % rows', (SELECT COUNT(*) FROM stage.sales);

    RAISE INFO 'sp_load_staging: Completed at %', GETDATE();

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_load_staging FAILED: % %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- PROCEDURE 2: sp_load_dimensions
-- ============================================================
-- Purpose: SCD Type 2 upsert into dim.users and dim.event
--
-- SCD Type 2 Algorithm:
--   Step 1 — Find changed records:
--     EXISTS in dim (is_current=TRUE) AND any attribute differs
--   Step 2 — Close old version:
--     UPDATE dim SET expiry_date = today-1, is_current = FALSE
--     WHERE userid = changed_userid AND is_current = TRUE
--   Step 3 — Insert new version:
--     INSERT INTO dim SELECT ... effective_date=today, expiry_date='9999-12-31', is_current=TRUE
--   Step 4 — Handle new records (never seen before):
--     INSERT INTO dim WHERE NOT EXISTS in dim at all
--
-- Key interview point: We compare on ALL business attributes.
--   If even one column changes (city, email, phone preference),
--   we create a new version. Historical sales stay linked to the
--   OLD user version via user_key (surrogate key never changes).
-- ============================================================

CREATE OR REPLACE PROCEDURE sp_load_dimensions(
    p_load_date DATE DEFAULT CURRENT_DATE
)
AS $$
DECLARE
    v_new_users     INTEGER := 0;
    v_changed_users INTEGER := 0;
    v_new_events    INTEGER := 0;
    v_changed_events INTEGER := 0;
BEGIN
    RAISE INFO 'sp_load_dimensions: Starting SCD Type 2 load for date %', p_load_date;

    -- ══════════════════════════════════════════════════════════
    -- DIM.USERS SCD Type 2
    -- ══════════════════════════════════════════════════════════

    -- Step 1: Close old versions for changed users
    -- A user "changed" if: same userid, is_current=TRUE, but
    -- at least one attribute differs between stage and dim
    UPDATE dim.users
    SET
        expiry_date = p_load_date - INTERVAL '1 day',
        is_current  = FALSE
    FROM stage.users s
    WHERE dim.users.userid     = s.userid
      AND dim.users.is_current = TRUE
      AND (
           COALESCE(dim.users.city,      '')  <> COALESCE(s.city,      '')
        OR COALESCE(dim.users.state,     '')  <> COALESCE(s.state,     '')
        OR COALESCE(dim.users.email,     '')  <> COALESCE(s.email,     '')
        OR COALESCE(dim.users.phone,     '')  <> COALESCE(s.phone,     '')
        OR COALESCE(dim.users.firstname, '')  <> COALESCE(s.firstname, '')
        OR COALESCE(dim.users.lastname,  '')  <> COALESCE(s.lastname,  '')
        -- preference changes also tracked (user opts into/out of categories)
        OR COALESCE(dim.users.likesports::text,    'f') <> COALESCE(s.likesports::text,    'f')
        OR COALESCE(dim.users.likeconcerts::text,  'f') <> COALESCE(s.likeconcerts::text,  'f')
        OR COALESCE(dim.users.likerock::text,      'f') <> COALESCE(s.likerock::text,      'f')
        OR COALESCE(dim.users.likejazz::text,      'f') <> COALESCE(s.likejazz::text,      'f')
        OR COALESCE(dim.users.likeclassical::text, 'f') <> COALESCE(s.likeclassical::text, 'f')
        OR COALESCE(dim.users.likeopera::text,     'f') <> COALESCE(s.likeopera::text,     'f')
        OR COALESCE(dim.users.liketheatre::text,   'f') <> COALESCE(s.liketheatre::text,   'f')
        OR COALESCE(dim.users.likevegas::text,     'f') <> COALESCE(s.likevegas::text,     'f')
        OR COALESCE(dim.users.likebroadway::text,  'f') <> COALESCE(s.likebroadway::text,  'f')
        OR COALESCE(dim.users.likemusicals::text,  'f') <> COALESCE(s.likemusicals::text,  'f')
      );

    GET DIAGNOSTICS v_changed_users = ROW_COUNT;
    RAISE INFO 'dim.users: % existing records closed (changed)', v_changed_users;

    -- Step 2: Insert new versions for changed users + brand new users
    -- "New version" = user exists in stage but has no is_current=TRUE in dim
    -- This covers both: changed (we just closed the old) + truly new
    INSERT INTO dim.users (
        userid, username, firstname, lastname, city, state, email, phone,
        likesports, liketheatre, likeconcerts, likejazz, likeclassical,
        likeopera, likerock, likevegas, likebroadway, likemusicals,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.userid, s.username, s.firstname, s.lastname, s.city, s.state,
        s.email, s.phone,
        s.likesports, s.liketheatre, s.likeconcerts, s.likejazz, s.likeclassical,
        s.likeopera, s.likerock, s.likevegas, s.likebroadway, s.likemusicals,
        p_load_date        AS effective_date,
        '9999-12-31'::DATE AS expiry_date,
        TRUE               AS is_current
    FROM stage.users s
    WHERE NOT EXISTS (
        -- No current record = either changed (we just closed it) or brand new
        SELECT 1 FROM dim.users d
        WHERE d.userid = s.userid AND d.is_current = TRUE
    );

    GET DIAGNOSTICS v_new_users = ROW_COUNT;
    RAISE INFO 'dim.users: % new/updated versions inserted', v_new_users;

    -- ══════════════════════════════════════════════════════════
    -- DIM.EVENT SCD Type 2 (denormalized)
    -- ══════════════════════════════════════════════════════════

    -- Step 1: Close old event versions for changed events
    -- Events change when venue info or event details change
    UPDATE dim.event
    SET
        expiry_date = p_load_date - INTERVAL '1 day',
        is_current  = FALSE
    FROM stage.event s
    WHERE dim.event.eventid    = s.eventid
      AND dim.event.is_current = TRUE
      AND (
           COALESCE(dim.event.eventname,  '') <> COALESCE(s.eventname,  '')
        OR COALESCE(dim.event.venuename,  '') <> COALESCE(s.venuename,  '')
        OR COALESCE(dim.event.venuecity,  '') <> COALESCE(s.venuecity,  '')
        OR COALESCE(dim.event.venuestate, '') <> COALESCE(s.venuestate, '')
        OR COALESCE(dim.event.catname,    '') <> COALESCE(s.catname,    '')
        OR COALESCE(dim.event.catgroup,   '') <> COALESCE(s.catgroup,   '')
        OR dim.event.starttime             IS DISTINCT FROM s.starttime
        OR dim.event.venueseats            IS DISTINCT FROM s.venueseats::INTEGER
      );

    GET DIAGNOSTICS v_changed_events = ROW_COUNT;
    RAISE INFO 'dim.event: % existing records closed (changed)', v_changed_events;

    -- Step 2: Insert new event versions
    INSERT INTO dim.event (
        eventid, venueid, catid, dateid, eventname, starttime,
        venuename, venuecity, venuestate, venueseats,
        catgroup, catname, catdesc,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.eventid, s.venueid, s.catid, s.dateid, s.eventname, s.starttime,
        s.venuename, s.venuecity, s.venuestate, s.venueseats::INTEGER,
        s.catgroup, s.catname, s.catdesc,
        p_load_date        AS effective_date,
        '9999-12-31'::DATE AS expiry_date,
        TRUE               AS is_current
    FROM stage.event s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.event d
        WHERE d.eventid = s.eventid AND d.is_current = TRUE
    );

    GET DIAGNOSTICS v_new_events = ROW_COUNT;
    RAISE INFO 'dim.event: % new/updated versions inserted', v_new_events;

    RAISE INFO 'sp_load_dimensions: Completed. Users changed=%, new=%  Events changed=%, new=%',
        v_changed_users, v_new_users, v_changed_events, v_new_events;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_load_dimensions FAILED: % %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- PROCEDURE 3: sp_load_facts
-- ============================================================
-- Purpose: Append new records to fact.sales and fact.listing
--
-- Surrogate Key Lookup Pattern:
--   Natural key (userid) → Surrogate key (user_key)
--   We look up the CURRENT version of the dimension record
--   at the TIME OF THE SALE (point-in-time lookup).
--
--   For users: JOIN on userid WHERE effective_date <= saletime
--              AND expiry_date > saletime (or is_current for latest)
--   For simplicity in this pattern (since we process nightly and
--   tonight's sale uses tonight's user version):
--   JOIN on userid AND is_current = TRUE
--   
--   NOTE: This works because: we process sales nightly, dim is
--   updated BEFORE facts, so the current user_key is the correct
--   one for tonight's sales.
--
-- Idempotency:
--   NOT EXISTS on salesid/listid prevents double-loading if
--   Step Functions retries due to transient failure.
-- ============================================================

CREATE OR REPLACE PROCEDURE sp_load_facts(
    p_load_date DATE DEFAULT CURRENT_DATE
)
AS $$
DECLARE
    v_sales_inserted    INTEGER := 0;
    v_listings_inserted INTEGER := 0;
BEGIN
    RAISE INFO 'sp_load_facts: Starting fact load for date %', p_load_date;

    -- ══════════════════════════════════════════════════════════
    -- FACT.SALES
    -- ══════════════════════════════════════════════════════════
    -- Surrogate key lookups:
    --   s.eventid  → dim.event.event_key   (is_current = TRUE)
    --   s.sellerid → dim.users.user_key    (seller, is_current = TRUE)
    --   s.buyerid  → dim.users.user_key    (buyer, is_current = TRUE)
    --
    -- Join to dim.users TWICE (role-playing dimension):
    --   du_seller = same table, aliased for seller role
    --   du_buyer  = same table, aliased for buyer role
    --
    -- NOT EXISTS: skip salesids already in fact.sales (idempotency)
    INSERT INTO fact.sales (
        salesid, event_key, seller_key, buyer_key, dateid,
        eventid, listid, qtysold, pricepaid, commission, saletime
    )
    SELECT
        s.salesid,
        de.event_key,
        du_seller.user_key  AS seller_key,
        du_buyer.user_key   AS buyer_key,
        s.dateid,
        s.eventid,
        s.listid,
        s.qtysold,
        s.pricepaid,
        s.commission,
        s.saletime
    FROM stage.sales s
    -- Join to current event version
    INNER JOIN dim.event de
        ON de.eventid = s.eventid AND de.is_current = TRUE
    -- Join to current seller version (role 1)
    INNER JOIN dim.users du_seller
        ON du_seller.userid = s.sellerid AND du_seller.is_current = TRUE
    -- Join to current buyer version (role 2)
    INNER JOIN dim.users du_buyer
        ON du_buyer.userid = s.buyerid AND du_buyer.is_current = TRUE
    -- Idempotency: skip already loaded sales
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.sales f WHERE f.salesid = s.salesid
    );

    GET DIAGNOSTICS v_sales_inserted = ROW_COUNT;
    RAISE INFO 'fact.sales: % new records inserted', v_sales_inserted;

    -- ══════════════════════════════════════════════════════════
    -- FACT.LISTING
    -- ══════════════════════════════════════════════════════════
    INSERT INTO fact.listing (
        listid, event_key, seller_key, dateid,
        eventid, numtickets, priceperticket, totalprice, listtime
    )
    SELECT
        l.listid,
        de.event_key,
        du_seller.user_key  AS seller_key,
        l.dateid,
        l.eventid,
        l.numtickets,
        l.priceperticket,
        l.totalprice,
        l.listtime
    FROM stage.listing l
    INNER JOIN dim.event de
        ON de.eventid = l.eventid AND de.is_current = TRUE
    INNER JOIN dim.users du_seller
        ON du_seller.userid = l.sellerid AND du_seller.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.listing f WHERE f.listid = l.listid
    );

    GET DIAGNOSTICS v_listings_inserted = ROW_COUNT;
    RAISE INFO 'fact.listing: % new records inserted', v_listings_inserted;

    RAISE INFO 'sp_load_facts: Completed. Sales inserted=%, Listings inserted=%',
        v_sales_inserted, v_listings_inserted;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_load_facts FAILED: % %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;
