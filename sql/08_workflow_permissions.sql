-- Template: render with tools/tickit_workflow.py render-permissions before execution.
-- Apply the rendered SQL after 06_stored_procedures.sql in the target database.
-- Redshift IAM authentication maps the existing workflow role to this DB user.
-- WORKFLOW_DB_USER is supplied locally (IAMR:<your-workflow-role>).
-- Existing users/roles and IAM policies are not recreated or broadened here.
GRANT USAGE ON SCHEMA public, stage, dim, fact TO "{{WORKFLOW_DB_USER}}";
GRANT SELECT, INSERT, DELETE ON
    stage.category, stage.venue, stage.users, stage.event, stage.listing, stage.sales
TO "{{WORKFLOW_DB_USER}}";
GRANT SELECT, INSERT, UPDATE ON dim.users, dim.event TO "{{WORKFLOW_DB_USER}}";
-- Redshift ACCESS EXCLUSIVE LOCK also requires UPDATE (INSERT alone is insufficient).
-- Loader SQL still appends facts and watermark rows; it does not update them.
GRANT SELECT, INSERT, UPDATE ON dim.scd_load_state, fact.sales, fact.listing
TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_staging(VARCHAR, VARCHAR)
TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_validate_dimension_history()
TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_dimensions(DATE)
TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_facts(DATE)
TO "{{WORKFLOW_DB_USER}}";
GRANT EXECUTE ON PROCEDURE public.sp_load_warehouse(VARCHAR, VARCHAR, DATE)
TO "{{WORKFLOW_DB_USER}}";
