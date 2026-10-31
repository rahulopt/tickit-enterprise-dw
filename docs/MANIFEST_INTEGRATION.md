# Manifest-to-warehouse integration: local readiness

Update: isolated workflow/resource definitions, native submission adapters and packaged dependencies now exist. See [controlled AWS integration preparation](AWS_INTEGRATION_RUNBOOK.md) for current status; the original native blockers below describe the preceding milestone.

The manifest milestone passed 153 local tests; the latest isolated preparation suite passes 171 with socket connections blocked. No AWS resource was changed or invoked. This is a local candidate, not an AWS integration certification.

## One input contract

`tickit_control.batch.Batch.parse` is the authoritative v1 validator. Required fields:

| Field | Contract |
|---|---|
| `schema_version` | Integer `1` |
| `datasets` | Ordered array: category, venue, users, event, listing, sales |
| `batch_id` | 1–64 lowercase letters/digits/hyphens, first character alphanumeric |
| `bucket` | One isolated, versioned S3 bucket |
| `load_date` | Strict `YYYY-MM-DD`, interpreted at midnight UTC; never processing time |
| `business_timezone` | `UTC` |
| `files` | Exactly those six datasets, each with `key`, actual non-null `version_id`, lowercase hex `sha256`, integer `rows` |

Each source key is exactly `tickit-v2/batches/<batch_id>/bronze/<dataset>.csv`. Upload `input-manifest.json` last; ingress requires its S3 event version ID. The fingerprint is SHA-256 of the canonical JSON (sorted keys, compact separators, ASCII). Reusing a batch ID with different identity is rejected. Fixture generation writes `fixture-inventory.json`, which is explicitly not a runnable manifest; source version IDs cannot be invented before upload.

Admission adds `attempt`, `fence_epoch`, `manifest_sha256`, `output_root`. Each branch receives these fields plus a new `task_run_id` for each task execution. Lambda receives the object payload; Glue receives serialized JSON in `--BATCH_REQUEST`. Neither transport is wired into the existing deployed state machine.

## Complete local path

1. Queue ingress/admission validates the manifest and atomically admits one batch. Other batches remain durable; admission is independent of the FIFO deduplication interval.
2. Category, venue, users and event wrap the existing pandas transformations. Logical legacy selectors are intercepted: actual S3 reads use the manifest's VersionId and SHA-256. Event enrichment uses the same batch's category and venue CSVs. Shared cleanup cannot list/delete real S3 objects in manifest mode.
3. Listing and sales have a separate native Spark candidate: download/checksum the exact version, conditionally materialize it into a task-specific immutable input key, read that key, write unique scratch, then conditionally publish final parts. Local tests invoke the actual branch dispatch with an explicit small-data transformation reference. PySpark/Glue are unavailable locally; the native implementation has not run.
4. Each branch publishes versioned Parquet and a checksum-pinned `result.json` sidecar. `gather_outputs` validates six sidecars, dataset identity and batch/attempt/task lineage. Only then is one write-once output manifest committed.
5. The output manifest contains v1, datasets, batch ID, bucket, UTC load date, attempt, input fingerprint and six outputs. Each output has task ID, exact prefix, rows and explicit parts (`key`, `version_id`, `sha256`, `bytes`). `prepare_copy` checks every referenced version and materializes exact bytes under immutable warehouse keys; six COPY manifests enumerate these keys with mandatory/content-length metadata. COPY is not assumed to support VersionId URL parameters.
6. Undeployed `sql/09_manifest_warehouse.sql` locks stage/dimension/fact tables and the receipt table in a consistent order. It checks the durable receipt before touching staging, validates exact COPY-manifest locations/counts, calls existing SCD2 dimension/fact procedures and inserts the receipt in one transaction. It uses DELETE, not TRUNCATE, and introduces no internal COMMIT. Existing `06_stored_procedures.sql` is reused without modification.
7. Matching committed batch/input is a no-op, including after queue dedup expiry, admission-ledger loss or SQL response loss. Conflicting reused identity fails. Receipt retention must cover the replay horizon. Database locking serializes receipt checks because Redshift uniqueness constraints alone do not enforce this guarantee.
8. Work reservations precede side effects. Unknown SQL completion blocks release; no receipt is not proof of rollback. A receipt can reconcile a lost commit response. Failures remain BLOCKED until explicit reconciliation/resolution; later batches stay queued.

Attempt fencing is distinct from coordinator ownership: takeover of an expired coordinator lease preserves the same attempt/epoch. New attempts invalidate old publishers; stale owners cannot release leases. SQL checks a committed epoch high watermark as a second defense. It does not atomically consult DynamoDB: trusted submission/terminal reconciliation and proof of child quiescence remain essential. Do not reset coordination epochs while uncommitted work can exist. Never use TTL as lock release.

## Local evidence and limits

Tests traverse actual six entrypoints, real CSV and Parquet bytes, pinned descriptors, COPY manifests, admission/work registry and completion. They poison latest source versions, test initial/change/midnight historical joins, identical replay beyond five minutes, admission-ledger loss, same-day conflict, late transactional failure, missing history, wrong counts/checksums/lineage, interrupted SQL response, queued followers and stale attempt/owner fencing. SDK submission tests use Stubber. Schema tests compare physical contract names/types to the actual staging DDL.

SQLite executes extracted existing SCD2 guards/DML; the new manifest wrapper's transaction/COPY behavior is modeled locally. Structural SQL checks do not compile PL/pgSQL or exercise native Redshift locks, COPY or concurrent transactions. The local orchestrator is synchronous and is not a substitute for native asynchronous Glue/Data API reconciliation. Single-object streaming transport rejects objects above 5 GiB; native Spark scratch requires sufficient local disk and output parts below that limit. Empty/malformed native Spark inputs and physical Parquet compatibility need native verification. Local Python 3.9 emits SDK support deprecation warnings.

## Blockers before one isolated AWS test

- Render and review a separate-account state machine that passes the manifest context to all six branches, sets unique task-run IDs, discovers pinned Glue sidecars, commits outputs and calls the new SQL wrapper. The old `stepfunctions/tickit_pipeline_v2.template.json` and generated date-validation `workflow.template.json` still use legacy orchestration and are **not suitable** for this test.
- Wire trusted pre-submission reservations, service IDs, Glue/SQL terminal polling, response-loss reconciliation and registry sealing. No unresolved work may permit lease release or message loss.
- Package `tickit_control` and required pandas/PyArrow dependencies for Lambda/Glue; configure test-only bucket, ledger and pipeline identity. Verify Glue runtime/Spark transform and Parquet compatibility.
- Approve isolated deployment of SQL 09 plus its permissions template 10, compile it natively and verify rollback/COPY receipt behavior. Existing SCD2 procedures need no source change, but isolated prerequisites must exist. Restrict direct procedure/staging access to the trusted test loader.
- Verify actual account/resource/IAM isolation, bucket versioning, conditional-write/no-delete/no-overwrite policies for final/materialized objects and COPY manifests. Versioning alone does not prevent latest-key overwrite. Spark scratch needs separate scoped permissions. Resolve test-only job/workflow/SNS identifiers.
- Establish bounded Glue/Redshift runtimes, native concurrency/retry controls, cleanup and an approved regional cost ceiling before execution. Current milestone cost is $0 AWS. No complete native-test dollar ceiling is claimed without resolved regional capacity/runtime configuration.

Production activation remains blocked. SNS delivery and full AWS end-to-end behavior remain unverified and are outside the local implementation evidence.
