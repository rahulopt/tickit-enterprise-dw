# Daily SCD Type 2 loading

The source delivers one user/event snapshot per day, with no business change timestamps. `p_load_date` is the date that snapshot becomes effective (UTC in the marker trigger). History represents observed daily state; intraday changes and pre-existing source history cannot be recovered from a latest-state CSV.

A version is valid when `effective_date <= transaction_date AND transaction_date < expiry_date`. Current versions end at `9999-12-31`. Changes, including NULL transitions and denormalized venue/category changes, close the previous version exactly at the next version's start and allocate a new surrogate. Unchanged rows retain their keys. Missing source keys are retained; use an explicit deletion attribute if deletions must be tracked.

Identical retries are no-ops. Changed snapshots on an already processed date are rejected, as are snapshots older than the latest processed date (including an unchanged processed day). To track intraday changes, the source contract and control columns must first be upgraded to business-effective timestamps. Do not overwrite the existing version or invent timestamps to accept conflicting snapshots.

## Running a batch

Deploy `sql/06_stored_procedures.sql` against existing schemas/tables, then render the `sql/08_workflow_permissions.sql` template for the configured workflow identity and apply its generated output. It creates the watermark table without dropping data. For a new warehouse, initialize the other SQL objects first. Do not rerun destructive dimension/fact DDL against a populated warehouse.

Start the rendered workflow with `{"load_date":"2026-10-31"}`. The S3 marker trigger already supplies this field. Manual and scheduled invocations must also supply it. The Data API binds this as a parameter, rather than interpolating it into SQL. The workflow calls:

```sql
CALL sp_load_warehouse('your-bucket', 'your-copy-role-arn', '2026-10-31'::DATE);
```

This wrapper locks staging, dimensions, and facts in a fixed order, then loads them in one transaction. A failure rolls back staging replacement, dimension expiration/insertion, the watermark, and both fact inserts. Do not use NONATOMIC or add COMMIT/TRUNCATE to these procedures. Redshift transaction behavior is documented in [Managing transactions](https://docs.aws.amazon.com/redshift/latest/dg/stored-procedure-transaction-management.html).

Individual loaders remain available for controlled maintenance. Use the wrapper for normal runs. SQL locks protect warehouse tables; they do not freeze S3. Serialize source publication and ETL executions, keep Bronze/Silver files immutable throughout a run, and replay skipped markers explicitly. Shared mutable S3 prefixes are still an orchestration limitation; concurrent source deliveries require versioned batch paths/manifests.

Render the grants locally; the output contains your configured identity and is Git-ignored:

```bash
python3 tools/tickit_workflow.py render-permissions --db-user "$TICKIT_WORKFLOW_DB_USER"
```

## Historical bootstrap and upgrades

Load authoritative dimension snapshots chronologically, starting at or before the earliest transaction date. Then load historical facts. New facts use `saletime`/`listtime`, not the source event `dateid`, to select versions. NULL times, future transactions beyond the snapshot day, missing history, duplicate stage keys, and invalid dimension intervals fail the batch. Existing fact IDs are skipped and their original surrogate assignments remain unchanged.

For a first snapshot dated today, historical transactions from earlier years will intentionally fail: that snapshot does not establish earlier history. If the business explicitly approves a baseline assumption, load that baseline with its approved effective date before later snapshots. Never silently extend today's attributes into the past.

Before upgrading a populated warehouse:

1. Back up tables and inspect existing history with `CALL sp_validate_dimension_history()`. Existing inclusive expiry dates will fail contiguous interval validation. Convert only reviewed inclusive closed intervals to exclusive boundaries; do not blindly add a day to already-exclusive dates.
2. Reconstruct missing history from authoritative snapshots where available. Do not reset current effective dates or regenerate surrogate keys. The watermark table starts empty; the procedure also guards against dates older than existing dimension versions.
3. Reconcile existing facts separately if previous current-version joins assigned incorrect keys. The new append-only loader does not rewrite 47.5 million historical rows automatically.
4. Deploy the procedures and render the new workflow. Validate the rendered ASL with AWS before deployment and run a small isolated Redshift integration batch before production use.

## Verification

`tests/test_scd2.py` executes the repository's dimension/fact SQL and guard predicates through a small SQLite compatibility harness. It covers changes, NULLs, unchanged retries, boundaries, late facts, rejected same-day/backdated snapshots, invalid histories, duplicate input, and rollback. It does not execute Redshift PL/pgSQL, COPY, locks, or the AWS workflow. The separate native deployment checks are recorded in the deployment report linked below.


Native Redshift and warehouse-only Step Functions verification passed on 31 October 2026. See the [deployment report](SCD2_DEPLOYMENT_2026-10-31.md) for statement IDs, checked behavior, preserved production counts, and the existing historical-data limitation.
