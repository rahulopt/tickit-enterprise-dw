-- UNDEPLOYED isolated-only grants. Apply after 01..06 and 09 in tickit_it.
-- Render WORKFLOW_DB_USER as IAMR:<isolated ControllerRole name>.
-- Administrator creates that user with PASSWORD DISABLE before these grants.
-- Do not use 08: this loader does not need the shared-prefix staging wrapper.
ALTER USER "{{WORKFLOW_DB_USER}}" SET statement_timeout TO 120000;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE EXECUTE ON PROCEDURE public.sp_load_manifest_warehouse(
    VARCHAR,VARCHAR,VARCHAR,INTEGER,BIGINT,DATE,VARCHAR,VARCHAR,VARCHAR
) FROM PUBLIC;
REVOKE EXECUTE ON PROCEDURE public.sp_load_staging(VARCHAR,VARCHAR) FROM PUBLIC;
REVOKE EXECUTE ON PROCEDURE public.sp_load_warehouse(VARCHAR,VARCHAR,DATE) FROM PUBLIC;
GRANT USAGE ON SCHEMA public, stage, dim, fact, control TO "{{WORKFLOW_DB_USER}}";
GRANT SELECT, INSERT, DELETE, UPDATE ON
    stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales
TO "{{WORKFLOW_DB_USER}}";
GRANT SELECT ON dim.date TO "{{WORKFLOW_DB_USER}}";
GRANT SELECT, INSERT, UPDATE ON dim.users, dim.event, dim.scd_load_state,
    fact.sales, fact.listing, control.batch_commits TO "{{WORKFLOW_DB_USER}}";
-- UPDATE is required for ACCESS EXCLUSIVE LOCK. No DELETE privilege on receipts.
GRANT EXECUTE ON PROCEDURE public.sp_validate_dimension_history() TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_dimensions(DATE) TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_facts(DATE) TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_manifest_warehouse(
    VARCHAR,VARCHAR,VARCHAR,INTEGER,BIGINT,DATE,VARCHAR,VARCHAR,VARCHAR
) TO "{{WORKFLOW_DB_USER}}";
