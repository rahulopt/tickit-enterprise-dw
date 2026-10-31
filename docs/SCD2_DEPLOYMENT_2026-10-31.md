# SCD2 deployment and verification — 31 October 2026

Daily SCD2 is deployed in Redshift Serverless database `tickit_v2` in the configured workgroup, region `us-east-1`. This report distinguishes a warehouse load from the complete six-branch Bronze-to-Gold workflow.

## Deployment

- Applied `sql/06_stored_procedures.sql` and `sql/08_workflow_permissions.sql` together in one Data API batch transaction. No dimension/fact tables were recreated.
- Read back all five procedures with `SHOW PROCEDURE`; their bodies match the repository, ignoring comments and whitespace.
- Updated the configured V2 workflow with the rendered template. AWS validation returned `OK` with no diagnostics. Resource configuration matches the previous deployed workflow.
- Fixed native Redshift logging syntax by evaluating functions/subqueries into variables before `RAISE INFO`. The actual Step Functions check also exposed an unsupported question-mark date pattern, which was removed.
- Added database grants for the existing workflow database identity. UPDATE is needed on watermark/fact tables for explicit exclusive locks; the loaders still append to those tables. No IAM policies were broadened.

## Native integration results

An isolated database, `tickit_v2_scd2_check_20261031`, and synthetic Parquet fixtures under the S3 prefix `scd2-verification/20261031/` were used. Tests exercised real Redshift PL/pgSQL, COPY, surrogate allocation and transaction rollback.

| Check | Result |
|---|---|
| Initial dimension/fact load through COPY | PASS |
| Identical retry retains surrogate keys and counts | PASS |
| NULL transition and denormalized venue change create versions | PASS |
| Old expiry equals new effective date | PASS |
| Midnight boundary selects the new version | PASS |
| Late sales/listings select the old version | PASS |
| Seller, buyer and event roles resolve independently | PASS |
| Changed same-day and out-of-order snapshots fail | PASS |
| Missing history rolls back staging, versions, watermark and facts | PASS |
| Workflow role can execute the wrapper with scoped grants | PASS |

The successful isolated workflow execution was `scd2-check-1793437943`. Its ARN is recorded in the local evidence bundle.

## Live warehouse verification

The already-existing production Silver files were reloaded through `sp_load_warehouse` using the real workflow role. A dedicated warehouse-only verification state machine bypassed ETL and SNS. Its default definition was restored to the isolated test database afterward.

- Execution: `live-scd2-check-1793438072`
- Execution status: `SUCCEEDED`
- Database identity: the configured workflow role mapped through IAM authentication
- Redshift statement ID: `2785b9ce-927c-4325-9618-b8278a04554d`
- Redshift statement status: `FINISHED`
- Observed statement duration: **39.064 seconds** (includes staging COPY, dimensions and facts; excludes Bronze-to-Silver ETL)

| Table | Before | After |
|---|---:|---:|
| dim.users | 2,000,000 | 2,000,000 |
| dim.event | 3,000,000 | 3,000,000 |
| fact.sales | 22,500,000 | 22,500,000 |
| fact.listing | 25,000,000 | 25,000,000 |

Dimension effective-date ranges and current-row counts, plus fact timestamp ranges, also remained unchanged. The dimension-history validator passed. Fact key/timestamp NULL checks found zero invalid rows. The accepted snapshot watermark is `2026-10-31`.

## Scope and remaining history limitation

The complete six-branch ETL workflow, SNS delivery, and S3 trigger/schedule integration were not exercised by this verification. The main workflow definition was updated, but the successful executions above are dedicated warehouse-only checks.

Existing dimensions start on `2026-10-29`; the existing facts occurred during 2023–2025. Those old facts and their existing surrogate assignments were preserved. This deployment does **not** reconstruct historical source attributes or establish point-in-time correctness of those existing 47.5 million facts. Authoritative historical dimension snapshots or an explicitly approved baseline are required for that backfill. New historical facts without dimension coverage fail rather than use today's attributes.

Daily snapshots do not capture intraday changes. Shared Bronze/Silver prefixes still require serialized publication and execution; batch manifests/versioned prefixes remain separate orchestration work.

## Evidence and rollback

Local backups and statement/execution evidence are under `deployment-backups/scd2-20261031/` (Git-ignored). The bundle contains before/after procedure definitions, the original and deployed workflow definitions, source hashes, Data API statement IDs, native fixture scripts, and execution histories. It contains no AWS credentials.

For a code rollback, restore the original three procedure definitions from the `procedure-before-*.json` files and restore `workflow-definition-before.json` with `update-state-machine`. The new helper procedures and metadata table may remain unused; do not drop or rebuild dimension/fact tables. The verification database and tiny synthetic S3 fixtures were retained for inspection.

Offline regression tests: **43 passed**. These complement the native checks above.


## Publication configuration cleanup

The public repository subsequently replaced infrastructure defaults with environment/job configuration, templated the workflow database grants, passed the bucket into Glue jobs, and aligned the marker trigger with the top-level `load_date` input. These publication changes are covered by offline tests and have not been redeployed as part of the GitHub update. The native deployment results above describe the verified SCD2 SQL and warehouse execution. Actual resource identifiers, source hashes and raw execution evidence remain in the Git-ignored local evidence bundle.
