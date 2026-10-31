-- ============================================================
-- TICKIT Enterprise Data Warehouse
-- Script: 06_stored_procedures.sql
-- Purpose: Atomic warehouse load, daily SCD2 dimensions and temporal facts
--
-- Procedure 1: sp_load_staging
--   DELETE + COPY from Silver Parquet → stage.* tables (single transaction)
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
-- Pattern: DELETE (current snapshot) → COPY from Silver S3
-- DELETE avoids Redshift TRUNCATE's implicit commit. If a COPY fails,
-- the entire staging transaction rolls back instead of leaving half a batch.
-- IAM Role: supplied by the caller and must have S3 read access
-- ============================================================

CREATE OR REPLACE PROCEDURE sp_load_staging(
    p_s3_bucket     VARCHAR(200),
    p_iam_role_arn  VARCHAR(500)
)
AS $$
DECLARE
    v_s3_base       VARCHAR(300);
    v_copy_options  VARCHAR(200);
    v_run_time      TIMESTAMP;
    v_row_count     BIGINT;
BEGIN
    LOCK TABLE stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales;
    v_s3_base      := 's3://' || p_s3_bucket || '/tickit-v2/silver/';
    v_copy_options := 'IAM_ROLE ' || QUOTE_LITERAL(p_iam_role_arn) || ' FORMAT AS PARQUET';

    v_run_time := GETDATE();
    RAISE INFO 'sp_load_staging: Starting at %', v_run_time;

    -- ── category ──────────────────────────────────────────────
    DELETE FROM stage.category;
    EXECUTE 'COPY stage.category FROM ' || QUOTE_LITERAL(v_s3_base || 'category/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.category;
    RAISE INFO 'stage.category loaded: % rows', v_row_count;

    -- ── venue ─────────────────────────────────────────────────
    DELETE FROM stage.venue;
    EXECUTE 'COPY stage.venue FROM ' || QUOTE_LITERAL(v_s3_base || 'venue/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.venue;
    RAISE INFO 'stage.venue loaded: % rows', v_row_count;

    -- ── users ─────────────────────────────────────────────────
    DELETE FROM stage.users;
    EXECUTE 'COPY stage.users FROM ' || QUOTE_LITERAL(v_s3_base || 'users/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.users;
    RAISE INFO 'stage.users loaded: % rows', v_row_count;

    -- ── event (denormalized) ──────────────────────────────────
    DELETE FROM stage.event;
    EXECUTE 'COPY stage.event FROM ' || QUOTE_LITERAL(v_s3_base || 'event/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.event;
    RAISE INFO 'stage.event loaded: % rows', v_row_count;

    -- ── listing ───────────────────────────────────────────────
    DELETE FROM stage.listing;
    EXECUTE 'COPY stage.listing FROM ' || QUOTE_LITERAL(v_s3_base || 'listing/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.listing;
    RAISE INFO 'stage.listing loaded: % rows', v_row_count;

    -- ── sales ─────────────────────────────────────────────────
    DELETE FROM stage.sales;
    EXECUTE 'COPY stage.sales FROM ' || QUOTE_LITERAL(v_s3_base || 'sales/') || ' ' || v_copy_options;
    SELECT COUNT(*) INTO v_row_count FROM stage.sales;
    RAISE INFO 'stage.sales loaded: % rows', v_row_count;

    v_run_time := GETDATE();
    RAISE INFO 'sp_load_staging: Completed at %', v_run_time;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_load_staging FAILED: % %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- PROCEDURE 2: sp_load_dimensions
-- ============================================================
-- Purpose: SCD Type 2 upsert into dim.users and dim.event
--
-- Daily snapshots use half-open intervals [effective_date, expiry_date).
-- Unchanged retries are no-ops. Changed same-day/backdated snapshots fail.
-- Missing source keys are retained: absence is not a deletion signal.
-- Default atomic procedures roll back expiry/insertion together on failure.
-- See docs/SCD2.md for bootstrap, migration and snapshot requirements.

-- Non-destructive metadata initialization; do not recreate dimension tables on upgrade.
CREATE TABLE IF NOT EXISTS dim.scd_load_state (
    load_date DATE NOT NULL
) DISTSTYLE ALL;

-- Validate temporal invariants before any fact joins or dimension mutations.
CREATE OR REPLACE PROCEDURE sp_validate_dimension_history()
AS $$
BEGIN
    IF EXISTS (
        SELECT userid FROM dim.users
        GROUP BY userid
        HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
    ) OR EXISTS (
        SELECT 1 FROM (
            SELECT effective_date, expiry_date, is_current,
                   LEAD(effective_date) OVER (
                       PARTITION BY userid ORDER BY effective_date
                   ) AS next_start
            FROM dim.users
        ) h
        WHERE effective_date >= expiry_date
           OR (next_start IS NOT NULL AND (expiry_date <> next_start OR is_current))
           OR (next_start IS NULL AND (NOT is_current OR expiry_date <> '9999-12-31'::DATE))
    ) THEN
        RAISE EXCEPTION 'Invalid dim.users history: gaps, overlaps, invalid ranges or current rows';
    END IF;
    IF EXISTS (
        SELECT eventid FROM dim.event
        GROUP BY eventid
        HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
    ) OR EXISTS (
        SELECT 1 FROM (
            SELECT effective_date, expiry_date, is_current,
                   LEAD(effective_date) OVER (
                       PARTITION BY eventid ORDER BY effective_date
                   ) AS next_start
            FROM dim.event
        ) h
        WHERE effective_date >= expiry_date
           OR (next_start IS NOT NULL AND (expiry_date <> next_start OR is_current))
           OR (next_start IS NULL AND (NOT is_current OR expiry_date <> '9999-12-31'::DATE))
    ) THEN
        RAISE EXCEPTION 'Invalid dim.event history: gaps, overlaps, invalid ranges or current rows';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sp_load_dimensions(
    p_load_date DATE
)
AS $$
DECLARE
    v_new_users     INTEGER := 0;
    v_changed_users INTEGER := 0;
    v_new_events    INTEGER := 0;
    v_changed_events INTEGER := 0;
BEGIN
    RAISE INFO 'sp_load_dimensions: Starting SCD Type 2 load for date %', p_load_date;
    -- Shared lock order across every loader; wrapper retains these until commit.
    LOCK TABLE stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales;
    LOCK TABLE dim.scd_load_state, dim.users, dim.event;
    IF p_load_date IS NULL OR p_load_date >= '9999-12-31'::DATE THEN
        RAISE EXCEPTION 'A non-null load date below 9999-12-31 is required';
    END IF;
    CALL sp_validate_dimension_history();
    IF EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date > p_load_date)
       OR EXISTS (SELECT 1 FROM dim.users WHERE effective_date > p_load_date)
       OR EXISTS (SELECT 1 FROM dim.event WHERE effective_date > p_load_date) THEN
        RAISE EXCEPTION 'Out-of-order dimension snapshot; replay snapshots chronologically';
    END IF;
    IF EXISTS (SELECT userid FROM stage.users GROUP BY userid HAVING COUNT(*) > 1)
       OR EXISTS (SELECT 1 FROM stage.users WHERE userid IS NULL) THEN
        RAISE EXCEPTION 'stage.users requires unique non-null userid';
    END IF;
    IF EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date)
       AND EXISTS (
           SELECT 1 FROM stage.users s WHERE NOT EXISTS (
               SELECT 1 FROM dim.users d WHERE d.userid = s.userid AND d.is_current = TRUE
           )
       ) THEN
        RAISE EXCEPTION 'Changed same-day users snapshot: new keys on an already processed date';
    END IF;
    IF EXISTS (
        SELECT 1 FROM dim.users JOIN stage.users s
          ON dim.users.userid = s.userid
        WHERE dim.users.is_current = TRUE
          AND (dim.users.effective_date >= p_load_date
               OR EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date))
          AND (
        DECODE(dim.users.username, s.username, 1, 0) = 0
        OR DECODE(dim.users.firstname, s.firstname, 1, 0) = 0
        OR DECODE(dim.users.lastname, s.lastname, 1, 0) = 0
        OR DECODE(dim.users.city, s.city, 1, 0) = 0
        OR DECODE(dim.users.state, s.state, 1, 0) = 0
        OR DECODE(dim.users.email, s.email, 1, 0) = 0
        OR DECODE(dim.users.phone, s.phone, 1, 0) = 0
        OR DECODE(dim.users.likesports, s.likesports, 1, 0) = 0
        OR DECODE(dim.users.liketheatre, s.liketheatre, 1, 0) = 0
        OR DECODE(dim.users.likeconcerts, s.likeconcerts, 1, 0) = 0
        OR DECODE(dim.users.likejazz, s.likejazz, 1, 0) = 0
        OR DECODE(dim.users.likeclassical, s.likeclassical, 1, 0) = 0
        OR DECODE(dim.users.likeopera, s.likeopera, 1, 0) = 0
        OR DECODE(dim.users.likerock, s.likerock, 1, 0) = 0
        OR DECODE(dim.users.likevegas, s.likevegas, 1, 0) = 0
        OR DECODE(dim.users.likebroadway, s.likebroadway, 1, 0) = 0
        OR DECODE(dim.users.likemusicals, s.likemusicals, 1, 0) = 0
          )
    ) THEN
        RAISE EXCEPTION 'Changed same-day users snapshot: daily history cannot represent intraday changes';
    END IF;
    IF EXISTS (SELECT eventid FROM stage.event GROUP BY eventid HAVING COUNT(*) > 1)
       OR EXISTS (SELECT 1 FROM stage.event WHERE eventid IS NULL) THEN
        RAISE EXCEPTION 'stage.event requires unique non-null eventid';
    END IF;
    IF EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date)
       AND EXISTS (
           SELECT 1 FROM stage.event s WHERE NOT EXISTS (
               SELECT 1 FROM dim.event d WHERE d.eventid = s.eventid AND d.is_current = TRUE
           )
       ) THEN
        RAISE EXCEPTION 'Changed same-day event snapshot: new keys on an already processed date';
    END IF;
    IF EXISTS (
        SELECT 1 FROM dim.event JOIN stage.event s
          ON dim.event.eventid = s.eventid
        WHERE dim.event.is_current = TRUE
          AND (dim.event.effective_date >= p_load_date
               OR EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date))
          AND (
        DECODE(dim.event.venueid, s.venueid, 1, 0) = 0
        OR DECODE(dim.event.catid, s.catid, 1, 0) = 0
        OR DECODE(dim.event.dateid, s.dateid, 1, 0) = 0
        OR DECODE(dim.event.eventname, s.eventname, 1, 0) = 0
        OR DECODE(dim.event.starttime, s.starttime, 1, 0) = 0
        OR DECODE(dim.event.venuename, s.venuename, 1, 0) = 0
        OR DECODE(dim.event.venuecity, s.venuecity, 1, 0) = 0
        OR DECODE(dim.event.venuestate, s.venuestate, 1, 0) = 0
        OR DECODE(dim.event.venueseats, s.venueseats, 1, 0) = 0
        OR DECODE(dim.event.catgroup, s.catgroup, 1, 0) = 0
        OR DECODE(dim.event.catname, s.catname, 1, 0) = 0
        OR DECODE(dim.event.catdesc, s.catdesc, 1, 0) = 0
          )
    ) THEN
        RAISE EXCEPTION 'Changed same-day event snapshot: daily history cannot represent intraday changes';
    END IF;


    -- ══════════════════════════════════════════════════════════
    -- DIM.USERS SCD Type 2
    -- ══════════════════════════════════════════════════════════

    -- Step 1: Close old versions for changed users
    -- A user "changed" if: same userid, is_current=TRUE, but
    -- at least one attribute differs between stage and dim
    UPDATE dim.users
    SET
        expiry_date = p_load_date,
        is_current  = FALSE
    FROM stage.users s
    WHERE dim.users.userid     = s.userid
      AND dim.users.is_current = TRUE
      AND (
        DECODE(dim.users.username, s.username, 1, 0) = 0
        OR DECODE(dim.users.firstname, s.firstname, 1, 0) = 0
        OR DECODE(dim.users.lastname, s.lastname, 1, 0) = 0
        OR DECODE(dim.users.city, s.city, 1, 0) = 0
        OR DECODE(dim.users.state, s.state, 1, 0) = 0
        OR DECODE(dim.users.email, s.email, 1, 0) = 0
        OR DECODE(dim.users.phone, s.phone, 1, 0) = 0
        OR DECODE(dim.users.likesports, s.likesports, 1, 0) = 0
        OR DECODE(dim.users.liketheatre, s.liketheatre, 1, 0) = 0
        OR DECODE(dim.users.likeconcerts, s.likeconcerts, 1, 0) = 0
        OR DECODE(dim.users.likejazz, s.likejazz, 1, 0) = 0
        OR DECODE(dim.users.likeclassical, s.likeclassical, 1, 0) = 0
        OR DECODE(dim.users.likeopera, s.likeopera, 1, 0) = 0
        OR DECODE(dim.users.likerock, s.likerock, 1, 0) = 0
        OR DECODE(dim.users.likevegas, s.likevegas, 1, 0) = 0
        OR DECODE(dim.users.likebroadway, s.likebroadway, 1, 0) = 0
        OR DECODE(dim.users.likemusicals, s.likemusicals, 1, 0) = 0
      );

    GET DIAGNOSTICS v_changed_users := ROW_COUNT;
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

    GET DIAGNOSTICS v_new_users := ROW_COUNT;
    RAISE INFO 'dim.users: % new/updated versions inserted', v_new_users;

    -- ══════════════════════════════════════════════════════════
    -- DIM.EVENT SCD Type 2 (denormalized)
    -- ══════════════════════════════════════════════════════════

    -- Step 1: Close old event versions for changed events
    -- Events change when venue info or event details change
    UPDATE dim.event
    SET
        expiry_date = p_load_date,
        is_current  = FALSE
    FROM stage.event s
    WHERE dim.event.eventid    = s.eventid
      AND dim.event.is_current = TRUE
      AND (
        DECODE(dim.event.venueid, s.venueid, 1, 0) = 0
        OR DECODE(dim.event.catid, s.catid, 1, 0) = 0
        OR DECODE(dim.event.dateid, s.dateid, 1, 0) = 0
        OR DECODE(dim.event.eventname, s.eventname, 1, 0) = 0
        OR DECODE(dim.event.starttime, s.starttime, 1, 0) = 0
        OR DECODE(dim.event.venuename, s.venuename, 1, 0) = 0
        OR DECODE(dim.event.venuecity, s.venuecity, 1, 0) = 0
        OR DECODE(dim.event.venuestate, s.venuestate, 1, 0) = 0
        OR DECODE(dim.event.venueseats, s.venueseats, 1, 0) = 0
        OR DECODE(dim.event.catgroup, s.catgroup, 1, 0) = 0
        OR DECODE(dim.event.catname, s.catname, 1, 0) = 0
        OR DECODE(dim.event.catdesc, s.catdesc, 1, 0) = 0
      );

    GET DIAGNOSTICS v_changed_events := ROW_COUNT;
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

    GET DIAGNOSTICS v_new_events := ROW_COUNT;
    RAISE INFO 'dim.event: % new/updated versions inserted', v_new_events;

    CALL sp_validate_dimension_history();
    INSERT INTO dim.scd_load_state (load_date)
    SELECT p_load_date
    WHERE NOT EXISTS (SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date);

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
-- Resolve versions at saletime/listtime, using the same half-open intervals.
-- Only new facts are validated/inserted. Existing facts keep their original keys.
-- Missing history fails the entire transaction; no current-version fallback.

CREATE OR REPLACE PROCEDURE sp_load_facts(
    p_load_date DATE
)
AS $$
DECLARE
    v_sales_inserted    INTEGER := 0;
    v_listings_inserted INTEGER := 0;
    v_missing_sales INTEGER := 0;
    v_missing_listings INTEGER := 0;
BEGIN
    RAISE INFO 'sp_load_facts: Starting fact load for date %', p_load_date;
    LOCK TABLE stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales;
    LOCK TABLE dim.scd_load_state, dim.users, dim.event;
    LOCK TABLE fact.sales, fact.listing;
    CALL sp_validate_dimension_history();
    IF p_load_date IS NULL OR NOT EXISTS (
        SELECT 1 FROM dim.scd_load_state WHERE load_date = p_load_date
    ) THEN
        RAISE EXCEPTION 'Load dimensions for this snapshot date before loading facts';
    END IF;
    IF EXISTS (SELECT salesid FROM stage.sales GROUP BY salesid HAVING COUNT(*) > 1)
       OR EXISTS (SELECT 1 FROM stage.sales WHERE salesid IS NULL) THEN
        RAISE EXCEPTION 'stage.sales requires unique non-null salesid';
    END IF;
    IF EXISTS (SELECT listid FROM stage.listing GROUP BY listid HAVING COUNT(*) > 1)
       OR EXISTS (SELECT 1 FROM stage.listing WHERE listid IS NULL) THEN
        RAISE EXCEPTION 'stage.listing requires unique non-null listid';
    END IF;


    -- Fail fast if stage rows lack matching dimension surrogate keys.
    -- INNER JOIN alone would silently discard these transactions.
    SELECT COUNT(*) INTO v_missing_sales
    FROM stage.sales s
    LEFT JOIN dim.event de ON de.eventid = s.eventid AND de.effective_date <= s.saletime::DATE AND s.saletime::DATE < de.expiry_date
    LEFT JOIN dim.users ds ON ds.userid = s.sellerid AND ds.effective_date <= s.saletime::DATE AND s.saletime::DATE < ds.expiry_date
    LEFT JOIN dim.users db ON db.userid = s.buyerid AND db.effective_date <= s.saletime::DATE AND s.saletime::DATE < db.expiry_date
    WHERE NOT EXISTS (SELECT 1 FROM fact.sales f WHERE f.salesid = s.salesid)
      AND (s.saletime IS NULL OR s.saletime::DATE > p_load_date OR de.event_key IS NULL OR ds.user_key IS NULL OR db.user_key IS NULL);

    SELECT COUNT(*) INTO v_missing_listings
    FROM stage.listing l
    LEFT JOIN dim.event de ON de.eventid = l.eventid AND de.effective_date <= l.listtime::DATE AND l.listtime::DATE < de.expiry_date
    LEFT JOIN dim.users du ON du.userid = l.sellerid AND du.effective_date <= l.listtime::DATE AND l.listtime::DATE < du.expiry_date
    WHERE NOT EXISTS (SELECT 1 FROM fact.listing f WHERE f.listid = l.listid)
      AND (l.listtime IS NULL OR l.listtime::DATE > p_load_date OR de.event_key IS NULL OR du.user_key IS NULL);

    IF v_missing_sales > 0 OR v_missing_listings > 0 THEN
        RAISE EXCEPTION 'Missing dimension references: sales=%, listings=%',
            v_missing_sales, v_missing_listings;
    END IF;

    -- ══════════════════════════════════════════════════════════
    -- FACT.SALES
    -- ══════════════════════════════════════════════════════════
    -- Temporal surrogate lookups preserve the version valid at transaction time.
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
    -- Join to event version at transaction time
    INNER JOIN dim.event de
        ON de.eventid = s.eventid AND de.effective_date <= s.saletime::DATE AND s.saletime::DATE < de.expiry_date
    -- Join to seller version at transaction time
    INNER JOIN dim.users du_seller
        ON du_seller.userid = s.sellerid AND du_seller.effective_date <= s.saletime::DATE AND s.saletime::DATE < du_seller.expiry_date
    -- Join to buyer version at transaction time
    INNER JOIN dim.users du_buyer
        ON du_buyer.userid = s.buyerid AND du_buyer.effective_date <= s.saletime::DATE AND s.saletime::DATE < du_buyer.expiry_date
    -- Idempotency: skip already loaded sales
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.sales f WHERE f.salesid = s.salesid
    );

    GET DIAGNOSTICS v_sales_inserted := ROW_COUNT;
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
        ON de.eventid = l.eventid AND de.effective_date <= l.listtime::DATE AND l.listtime::DATE < de.expiry_date
    INNER JOIN dim.users du_seller
        ON du_seller.userid = l.sellerid AND du_seller.effective_date <= l.listtime::DATE AND l.listtime::DATE < du_seller.expiry_date
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.listing f WHERE f.listid = l.listid
    );

    GET DIAGNOSTICS v_listings_inserted := ROW_COUNT;
    RAISE INFO 'fact.listing: % new records inserted', v_listings_inserted;

    RAISE INFO 'sp_load_facts: Completed. Sales inserted=%, Listings inserted=%',
        v_sales_inserted, v_listings_inserted;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_load_facts FAILED: % %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- One atomic Gold load prevents another execution from replacing staging between
-- dimension and fact processing. No COMMIT/TRUNCATE inside any nested procedure.
CREATE OR REPLACE PROCEDURE sp_load_warehouse(
    p_s3_bucket VARCHAR(200), p_iam_role_arn VARCHAR(500), p_load_date DATE
)
AS $$
BEGIN
    LOCK TABLE stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales;
    LOCK TABLE dim.scd_load_state, dim.users, dim.event;
    LOCK TABLE fact.sales, fact.listing;
    CALL sp_load_staging(p_s3_bucket, p_iam_role_arn);
    CALL sp_load_dimensions(p_load_date);
    CALL sp_load_facts(p_load_date);
END;
$$ LANGUAGE plpgsql;
