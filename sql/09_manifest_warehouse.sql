-- UNDEPLOYED candidate: apply only to an approved isolated database.
-- Requires existing stage/dim/fact objects and 06_stored_procedures.sql.
-- No shared-prefix staging loader is called. COPY URLs are exact manifests,
-- prepared from checksum-verified object versions by tickit_control.warehouse.
CREATE SCHEMA IF NOT EXISTS control;
CREATE TABLE IF NOT EXISTS control.batch_commits (
    batch_id VARCHAR(64) NOT NULL,
    input_sha VARCHAR(64) NOT NULL,
    output_sha VARCHAR(64) NOT NULL,
    attempt INTEGER NOT NULL,
    fence_epoch BIGINT NOT NULL,
    load_date DATE NOT NULL,
    committed_at TIMESTAMP DEFAULT GETDATE()
) DISTSTYLE ALL SORTKEY(batch_id);

CREATE OR REPLACE PROCEDURE sp_load_manifest_warehouse(
    p_batch_id VARCHAR(64), p_input_sha VARCHAR(64), p_output_sha VARCHAR(64),
    p_attempt INTEGER, p_epoch BIGINT, p_load_date DATE,
    p_bucket VARCHAR(200), p_copy_role VARCHAR(500), p_specs VARCHAR(65535)
)
AS $$
DECLARE
    v_existing VARCHAR(64);
    v_epoch BIGINT;
    v_dataset VARCHAR(20);
    v_url VARCHAR(1000);
    v_expected BIGINT;
    v_rows BIGINT;
    v_index INTEGER;
BEGIN
    -- Lock order agrees with the existing warehouse wrapper.
    LOCK TABLE stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales;
    LOCK TABLE dim.scd_load_state, dim.users, dim.event;
    LOCK TABLE fact.sales, fact.listing;
    LOCK TABLE control.batch_commits;
    IF p_batch_id IS NULL OR p_batch_id !~ '^[a-z0-9][a-z0-9-]{0,63}$'
       OR p_input_sha IS NULL OR p_input_sha !~ '^[a-f0-9]{64}$'
       OR p_output_sha IS NULL OR p_output_sha !~ '^[a-f0-9]{64}$'
       OR p_attempt IS NULL OR p_attempt < 1 OR p_epoch IS NULL OR p_epoch < 1
       OR p_bucket IS NULL OR p_bucket !~ '^[a-z0-9][a-z0-9.-]{2,62}$'
       OR p_load_date IS NULL OR p_specs IS NULL OR p_copy_role IS NULL THEN
        RAISE EXCEPTION 'Invalid manifest warehouse identity';
    END IF;
    SELECT MAX(input_sha) INTO v_existing FROM control.batch_commits WHERE batch_id = p_batch_id;
    IF v_existing IS NOT NULL THEN
        IF v_existing <> p_input_sha THEN
            RAISE EXCEPTION 'Committed batch ID reused with different inputs';
        END IF;
        -- Receipt survives SQS dedup expiry and a lost SQL response. An already
        -- committed batch never replaces staging or reallocates dimension keys.
        RETURN;
    END IF;
    SELECT COALESCE(MAX(fence_epoch), 0) INTO v_epoch FROM control.batch_commits;
    IF p_epoch < v_epoch THEN
        RAISE EXCEPTION 'Stale warehouse fencing epoch';
    END IF;
    FOR v_index IN 0..5 LOOP
        v_dataset := CASE v_index WHEN 0 THEN 'category' WHEN 1 THEN 'venue'
            WHEN 2 THEN 'users' WHEN 3 THEN 'event' WHEN 4 THEN 'listing' ELSE 'sales' END;
        v_url := JSON_EXTRACT_PATH_TEXT(p_specs, v_dataset, 'url');
        v_expected := JSON_EXTRACT_PATH_TEXT(p_specs, v_dataset, 'rows')::BIGINT;
        IF v_url IS NULL OR v_url <> 's3://' || p_bucket || '/tickit-v2/batches/' || p_batch_id
            || '/attempts/' || p_attempt || '/warehouse/' || v_dataset || '.copy.json'
           OR v_expected IS NULL OR v_expected < 0 THEN
            RAISE EXCEPTION 'Invalid dataset COPY manifest';
        END IF;
        EXECUTE 'DELETE FROM stage.' || QUOTE_IDENT(v_dataset);
        EXECUTE 'COPY stage.' || QUOTE_IDENT(v_dataset) || ' FROM ' || QUOTE_LITERAL(v_url)
            || ' IAM_ROLE ' || QUOTE_LITERAL(p_copy_role) || ' FORMAT AS PARQUET MANIFEST';
        EXECUTE 'SELECT COUNT(*) FROM stage.' || QUOTE_IDENT(v_dataset) INTO v_rows;
        IF v_rows <> v_expected THEN
            RAISE EXCEPTION 'COPY row count differs from committed output manifest';
        END IF;
    END LOOP;
    CALL sp_load_dimensions(p_load_date);
    CALL sp_load_facts(p_load_date);
    INSERT INTO control.batch_commits(batch_id,input_sha,output_sha,attempt,fence_epoch,load_date)
    VALUES(p_batch_id,p_input_sha,p_output_sha,p_attempt,p_epoch,p_load_date);
END;
$$ LANGUAGE plpgsql;
