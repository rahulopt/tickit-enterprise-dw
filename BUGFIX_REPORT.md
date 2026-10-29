# TICKIT Data Warehouse — offline bug-fix report

Date: 2026-10-29

## Fixed in this copy

- Redshift staging: repaired S3 path quoting/construction for the six Parquet COPY commands and corrected Redshift COPY clause order to `FROM ... IAM_ROLE ... FORMAT AS PARQUET`.
- Redshift staging: replaced `TRUNCATE` with transaction-friendly `DELETE` in the staging procedure so a failed COPY does not prematurely commit prior staging removals.
- Redshift dimensions: SCD Type 2 change detection now compares the full set of modeled user/event business attributes with null-safe `DECODE` checks (including user preferences, event date/time and category/venue details).
- Redshift facts: added missing-dimension-reference checks before inserting transactions so missing dimension keys produce an error rather than silent loss of records.
- Calendar: fixed the missing fourth digit in the date generator, generating 1,096 days; aligned keys with the documented sequential-ID assumption; made calendar reruns incremental rather than destructive.
- Event ETL: deduplicated venue/category lookup keys before denormalizing to prevent one event expanding into multiple output rows.
- Users ETL: added required source-column validation.
- Lambda/Python Shell ETL: fixed nullable/bad numeric field handling, safe upload-then-stale-cleanup logic, S3 multi-object delete batching, and UTC-aware duration timestamps. Note: the upload/cleanup steps are not a full transactional snapshot swap.
- S3 trigger: validates exact `bronze/.done` marker and target bucket, handles S3 notification encoding, uses a deterministic daily Standard execution name, and catches duplicate execution starts. An overlapping run is skipped via a best-effort status check, not queued.
- Step Functions: explicitly creates error payloads for Redshift `FAILED`/`ABORTED` statuses, checks task exceptions, and adds an overall workflow timeout.
- Deployment: regenerated all three Lambda ZIPs so deployed artifacts match the corrected source files. README deployment SQL order now includes date loading and reporting views.
- Added offline regression tests under `tests/`.

## Validation performed

- `python -m pytest -q`: **14 passed**.
- `python -m compileall -q glue lambda tests`: passed.
- `step-functions/tickit_pipeline.json`: parsed as valid JSON and references checked in tests.
- Lambda ZIP source files checked to match `.py` files.

## Important limitations before production deployment

- No AWS credentials / deployed resources were available for integration tests. Spark, Redshift SQL execution, IAM permissions, S3 event notifications, Glue job configuration, Lambda Layers and Step Functions execution **have not** been verified on live AWS resources.
- The date loader assumes source `dateid` is sequential from 1 at 2008-01-01 and covers 2008 through 2010. Verify that against the actual Bronze `date`, `event`, `sales`, and `listing` files. If wrong, use the source mapping, not a fabricated date key.
- The S3 trigger intentionally suppresses overlapping workflows, so a `.done` marker received while another daily batch runs is **not automatically replayed**. An SQS/event-ledger queue is required for guaranteed every-batch processing.
- Reprocessing the same day requires an explicit rerun policy because Standard Step Functions execution names cannot be reused for 90 days.
- The six Silver folders are not committed with a single atomic transaction. Use versioned delivery prefixes and a commit manifest for stronger production guarantees.
- Historical SCD point-in-time correctness and same-day multiple attribute changes need a timestamp-level design; the current dimension loader tracks daily changes and the fact loader maps to currently active dimension versions.
- The DDL files `02`, `03`, and `04` contain `DROP TABLE IF EXISTS`; do not rerun these on production tables with important data.
- Verify the prepared SQL, resource names and configuration in a dev AWS account before applying any changes to the currently deployed project.
