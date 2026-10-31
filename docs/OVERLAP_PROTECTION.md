# Queued admission and immutable batches — local implementation only

This milestone changes local candidate code only. Nothing has been deployed, invoked in AWS, committed or pushed. The existing deployed legacy trigger and production workflow configuration are unchanged. Do not activate these candidates against the existing shared S3 paths.

## Contract and architecture

```text
Versioned six-file input manifest in an immutable batch directory
    -> queue-only S3 ingress
    -> conditional DynamoDB QUEUED ledger / durable outbox intent
    -> SQS FIFO, one MessageGroupId per warehouse
    -> scheduled bounded dispatcher
    -> atomic lease + ACTIVE ledger transaction
    -> deterministic Standard execution name and input
    -> terminal SFN + sealed registry + terminal child jobs/SQL
    -> atomic completion + safe lease release
```

`lambda/s3_trigger.py` is now a queue-only candidate. It has no direct Step Functions startup or list-then-start skip path. It rejects legacy `.done` notifications loudly before creating clients. The new marker is `tickit-v2/batches/<batch_id>/input-manifest.json`, uploaded after all six immutable source files exist. Its S3 event must include a version ID. This is an intentional contract migration, not an automatic reinterpretation of legacy `bronze/.done` or `bronze/tickit.done` objects.

`tickit_control.ingress` loads the exact manifest object version and validates its batch/bucket. The batch contains UTC `load_date`, all six source keys, their version IDs, SHA-256 values and row counts. Do not derive the business date from retry processing time. Keep the isolated early date validator before ETL.

`Dispatcher.enqueue` first conditionally registers the manifest in the DynamoDB ledger, then publishes to FIFO. It returns acknowledgment only after SQS acknowledges. A crash between these actions leaves a discoverable QUEUED outbox intent. The scheduled reconciler republishes QUEUED intents; the ledger, not the FIFO five-minute deduplication window, provides durable duplicate protection. A reused batch ID with changed inputs is an error.

A second delivery remains in FIFO and/or the QUEUED ledger. A BUSY result never deletes its receipt. The dispatcher checks active work before receiving a new message, avoiding repeated receives of a healthy waiting batch. It is a bounded polling handler, **not** an auto-acknowledging SQS Lambda event-source handler.

## Atomic lease, ownership and recovery

DynamoDB uses a permanent `LOCK#<pipeline_id>` record and `BATCH#<pipeline_id>#<batch_id>` records. The warehouse identity / pipeline ID must be consistent for every producer and dispatcher targeting that warehouse. Initial admission changes the lease and batch ledger together with `TransactWriteItems`. Every write compares the previous revision; lease writes also condition on the observed owner and batch ID. Lock reads use `ConsistentRead=True`. The pending-batch GSI is used only for outbox reconciliation, never as evidence a lock is free.

Lease fields are owner, batch ID and ordinary numeric `expires_at`. Expiry authorizes recovery of the **same batch and attempt**. It does not authorize starting another batch. Heartbeats extend a held lease; another worker must wait for expiry before conditional takeover. Stale owners cannot finalize or release the recovered lease. Empty lock records retain their revision to prevent ABA races.

**Do not configure DynamoDB TTL on lease records or unfinished/deduplication ledger records.** TTL deletion is not lock release. Retain/archive deduplication tombstones for the allowed source-replay horizon. Deny ordinary workers `DeleteItem`; use explicit audited resolution paths rather than deleting locks.

Each attempt uses a stable Standard execution name and canonical input across worker ownership changes. This handles a crash before StartExecution and a successful start whose response was lost. Express workflows are rejected. A persisted ten-minute initial-start deadline prevents a missing old execution from being recreated indefinitely, including after the service's execution-name retention horizon. Existing observed executions can still be monitored after this deadline. A missing execution after the deadline is quarantined for manual reconciliation, not treated as evidence it never started.

A watchdog must run even when FIFO is empty: an acknowledged admitted batch is retained in DynamoDB, not dependent on its original SQS receipt. Bounded dispatcher invocations and SDK timeouts are required; never let a stale process remain paused beyond the service's execution-name retention window.

## Terminal execution is not sufficient proof of quiescence

`WorkTracker` records each external submission intent **before** the side effect, and records the service ID / observed completion after the response. Glue, SQL and synchronous Lambda operations are tracked. Lost submission responses remain unresolved until reconciled. A response timeout must not be assumed to mean the job never started.

Only a trusted terminal-state reconciler may seal the submission registry after observing the exact Standard execution terminal. A sealed registry with any unresolved submission, active Glue run, active SQL statement or unobserved Lambda completion blocks lease release. A successful execution must account for category, venue, users, event, listing, sales and warehouse operations. Missing instrumentation fails closed. A contradictory successful SFN status with a failed child service is treated as a blocked failure.

Isolated workflow/submission-wrapper candidates now exist; see [controlled test preparation](AWS_INTEGRATION_RUNBOOK.md). They are **not wired into the current production definition**. Native async response-loss reconciliation and IAM enforcement remain unverified. The adapter deliberately refuses safe completion without the registry rather than guessing from SFN terminal status.

On success, the owner atomically records SUCCEEDED and clears the active lease. On failure, the ledger records FAILED and the lease remains BLOCKED, preserving later queued batches and SCD2 date ordering. Recovery does not automatically retry failures. `resolve_failed` requires current ownership, fresh quiescence evidence and an audit reason. An approved retry gets a new attempt/output namespace; an approved abandonment retains an ABANDONED tombstone and deliberately permits later batches. Abandonment of a missing date requires a business decision.

FIFO preserves queue submission order, not an authoritative historical calendar. Producers must supply ordered committed batch dates/sequences. This implementation does not infer an unseen earlier delivery; the warehouse's existing backdate checks remain necessary. Outbox recovery is transport recovery, not historical reconstruction.

## Immutable S3 namespaces

```text
 tickit-v2/batches/<batch_id>/bronze/<dataset>.csv
 tickit-v2/batches/<batch_id>/input-manifest.json
 tickit-v2/batches/<batch_id>/attempts/<attempt>/silver/<dataset>/<task_run_id>/part-....parquet
 tickit-v2/batches/<batch_id>/attempts/<attempt>/output-manifest.json
```

- Input version IDs are pinned and bytes are checksum-verified. Mutable shared Bronze prefixes are rejected.
- Execution retries get new attempt directories. Individual ETL task retries require new task-run directories, never overwrite or delete an earlier task's output.
- `ImmutableObjects.put` uses `IfNoneMatch='*'` and a checksum. An identical retry is accepted after byte comparison; different existing content fails. Published output manifests are write-once and reference only their own batch/attempt/task paths.
- Bucket permissions must deny deletion/replacement of committed objects by normal writers. Conditional writes protect the current object version; a delete marker must not be used to evade immutability.
- Spark multipart/commit behavior and conditional-write-compatible publishing need isolated native validation. The small-object adapter is not a complete Spark committer.
- Redshift must consume only the committed output manifest. Its deployed shared-prefix staging loader is unchanged. A local manifest-aware wrapper now exists in `sql/09_manifest_warehouse.sql`; it is undeployed and must pass isolated native testing before use.

## Undeployed topology and environment contract

| Component | Proposed requirements |
|---|---|
| S3 | Separate test account/bucket, versioning, batch manifest events only; conditional-write and no-delete policies |
| Queue | Dedicated FIFO, one warehouse message group, 14-day retention; monitored DLQ and oldest-message age |
| DynamoDB | String partition key `pk`; revision/data fields; GSI `queued-batches` on `queue_partition` + `queue_sort`; on-demand capacity; PITR; no TTL on coordination records |
| Ingress | `TICKIT_BUCKET`, `ADMISSION_TABLE`, `BATCH_QUEUE_URL`, `TICKIT_PIPELINE_ID`; no StartExecution permission |
| Dispatcher/watchdog | `ADMISSION_TABLE`, `BATCH_QUEUE_URL`, `TICKIT_PIPELINE_ID`, `STATE_MACHINE_ARN`; constrained test-only start/describe and child-work inspection permissions |
| Work registry | Trusted pre-submission wrappers and terminal reconciler; attempt-aware conditional updates |
| Schedule | Bounded polling/watchdog independent of queue arrivals; e.g. every minute with a 120-second lease, monitored for missed heartbeats |
| Alarm/recovery | QUEUED outbox age, BLOCKED/DRAINING/unknown-start states, oldest FIFO message, DLQ, unresolved submissions and failed watchdog |

All clients are injected in library adapters; no resources are created at import. Handler factories create clients only when a future handler is invoked. Include `tickit_control/` in both Lambda deployment packages. No deployment artifacts or schedules are installed by this milestone.

## Local validation and remaining gate

Local tests use disk-backed SQLite queue/ledger substitutes, parallel callers, controlled crash injection, and botocore Stubber for actual request shapes. They cover simultaneous distinct/duplicate arrivals, queue/ledger restart, send/transaction/start response loss, duplicate delivery after FIFO dedup expires, expired lease recovery, stale owner rejection, failed/aborted work, unresolved children, operator retry/abandonment and immutable-object conflicts. Network sockets are blocked during the full test run.

Passing these tests establishes the local protocol and SDK contracts. It does not establish deployed IAM policies, DynamoDB/SQS native behavior, watchdog availability, Spark publishing, manifest-aware Redshift loading, SNS delivery or end-to-end reliability. Those require explicit isolated-environment approval. Production activation remains blocked.

References:
- https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
- https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html

## Latest local result

The complete local suite passed **171 tests**, with `socket.socket.connect` blocked and AWS credentials replaced by test-only placeholders for the run. SDK adapters were exercised through Stubber/fakes; no AWS service requests were made. Publication scanning and whitespace checks passed. AWS compute cost for this milestone was $0. No commit or push was performed.

## Manifest integration milestone

The v1 input/output contract, six branch entrypoints, pinned result sidecars, COPY preparation, atomic warehouse receipt candidate and full local path are documented in [MANIFEST_INTEGRATION.md](MANIFEST_INTEGRATION.md). This supersedes earlier descriptions of ETL/COPY as wholly absent: local candidates now exist, while native workflow wiring and AWS validation remain pending. The existing SCD2 procedure source is unchanged.
