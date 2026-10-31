# Isolated acceptance readiness — local preparation only

No AWS resources are created or invoked by this preparation package. No deployment, Glue execution, Step Functions execution, SNS publication, production data mutation or Git push is authorized. Existing native test evidence is separate from this new acceptance suite.

Run the local fixture generator with `python3 tools/tickit_isolated.py`. It creates five synthetic batches, each with six Bronze CSV files, under `tests/isolated/generated/`. Generated artifacts are Git-ignored. Fixtures contain invented identities and `example.invalid` email addresses. Manifests record file sizes, row counts and SHA-256 hashes.

## Schemas and timezone

Headers come from `sql/02_stage_tables.sql`; Bronze `event.csv` contains its first six source columns, before venue/category enrichment. The other five source files match their corresponding stage columns. UTC-naive timestamps use six fractional-second digits, matching the existing ETL parser contract.

The acceptance business timezone is **UTC**. The initial snapshot starts at `2026-10-29 00:00:00 UTC`; the changed snapshot starts at `2026-10-30 00:00:00 UTC`. A fact at `2026-10-29 23:59:59.999999 UTC` must use the old version, and a fact at `2026-10-30 00:00:00.000000 UTC` must use the new version. Host timezone is irrelevant. This defines the test contract; production business timezone still requires business approval.

The isolated validator rejects missing, non-string, incorrectly formatted, impossible and future load dates, and a timezone other than UTC. The generated workflow places it before the original date Choice and all ETL branches. Invalid input terminates on a Fail state. Valid historical dates remain permitted for controlled tests. This is local implementation; AWS execution semantics and deployment remain pending. The production workflow template and production Lambda scripts have not been changed.

Historical SCD scenarios will use explicit workflow `load_date` inputs. Do not fabricate S3 `eventTime` values. A real-marker smoke test must use fixtures dated to the actual UTC delivery day and a fresh isolated database; regenerating that dated batch and scheduling it within the approved execution budget remain PENDING. The earlier date-validation preparation used legacy `bronze/.done`. The new local queued-ingress candidate requires a version-pinned batch input manifest instead; neither legacy `.done` key activates it. Production notifications are unchanged. See `docs/OVERLAP_PROTECTION.md` before any isolated wiring.

## Isolation gate

`config.example.json` deliberately contains unresolved placeholders. `tools/tickit_isolated.py::validate_isolation` rejects unresolved references, reuse of production identifiers, incorrect service/region references, and use of the production AWS account. No real test configuration has been supplied.

The proposed test environment requires a separate AWS account, its own S3 bucket, Redshift namespace/workgroup/database, four Glue jobs, trigger and validator Lambdas, two ETL Lambdas, workflow, SNS topic and SQS subscriber. Do not reuse the previous test database: it shares a production workgroup.

Before AWS testing is approved, inspect actual IAM roles, policies, trusts, bucket/resource policies, script contents, and the fully rendered workflow. Permissions must be scoped to test resources, with no production role assumption or cross-account invocation. Use IAM policy simulation for negative production-access checks; do not attempt real production invocations to test denial. Local reference validation alone cannot prove these properties, so actual resource/IAM isolation remains PENDING.

## Six approved scenarios

| Scenario | Input | Expected outcome and verification | Pass criteria | Estimated incremental cost |
|---|---|---|---|---|
| 1. Initial load + identical replay | `initial/bronze/*.csv`, load date 2026-10-29; repeat unchanged | All six branches succeed; query isolated stage/dim/fact tables and compare complete rows and surrogate mappings before/after replay | Stage counts category=1, venue=1, users=2, event=1, listing=1, sales=1; dim users=2, event=1; facts=1 each; replay unchanged | Two full executions, modeled compute allowance up to $1.38 |
| 2. Changed attributes + historical joins + replay | `changed/bronze/*.csv`, load date 2026-10-30; repeat | User city becomes NULL, venue/category change; sale 101 is at midnight and 102 is late for the previous day; verify seller/buyer/event and listing versions by transaction date | Dim users=3, event=2; facts=3 each; no overlapping intervals; 101 gets new seller/event, 102 gets old seller/event; buyer stays unchanged; replay preserves rows/keys | Two full executions, up to $1.38 |
| 3. Same-day conflict | After scenario 2, submit `same_day_conflict`, date 2026-10-30 | SQL rejects changed same-day attributes; compare complete stage/dim/watermark/fact snapshots around wrapper failure; check failure notification | Rejected; every pre-load row/key preserved; no success notification | One execution, up to $0.69 |
| 4. Transaction rollback | After scenario 2, load `missing_history` and then `rollback_after_sales`, date 2026-10-31 | First fails temporal coverage. Second has a valid new sale plus a listing with NULL dateid, causing a listing INSERT constraint failure after sales insertion. Verify all warehouse state restores; use isolated native procedure trace/query evidence to prove failure ordering | Both fail; staging, dimensions, watermark and facts match pre-load state; sales 103 absent afterward. Local test records an actual sale INSERT callback before rollback in the second case | Two executions, up to $1.38 |
| 5. Overlapping executions | Concurrent distinct batch IDs/dates with deliberately overlapping ETL time; immutable input/output paths | Inspect lease ownership, queue state, execution history and output manifests/checksums | Exactly one warehouse writer; second batch queued without loss; no cross-batch overwrites; terminal jobs confirmed before lease release | Two executions, up to $1.38; local protocol implemented; native wiring/verification still PENDING |
| 6. SNS delivery and publication failure | Consume successful/failed notifications from previous scenarios through test SNS→SQS; one extra successful warehouse execution with intentionally denied test-topic publication | Verify SNS subscription and SQS message attributes/body, batch ID, outcome and statement ID. Compare committed warehouse before/after notification failure | Success and failure delivery proven; no false success; SNS denial does not imply DB rollback; duplicate messages handled by event ID | Delivery piggybacks other tests; one extra execution up to $0.69, plus small messaging cost |

The per-execution allocation assumes 10 minutes of total billed activity for each of two 1-DPU Python Shell jobs and two 2-DPU Spark jobs ($0.44 combined), plus five minutes at 8 RPUs ($0.25). These are planning assumptions, not measured runtimes or guaranteed maxima. Native initial-load, midnight, rollback and delivery assertions require future approved AWS tests even when local equivalents pass.

## Local checks and limitations

`tests/test_isolated_readiness.py` verifies headers/timestamps, actual pandas ETL compatibility with mocked AWS/Parquet output, initial/changed/late-fact SQL, identical retries, same-day rejection, missing-history failure, and rollback after sales insertion using the repository's SQLite SQL harness. It compares complete warehouse/staging snapshots. The harness does not prove Redshift lock/COPY behavior, Glue Spark execution, S3 atomicity, SNS delivery or Step Functions service integration behavior.

A local SQS/DynamoDB admission protocol, durable outbox, recovery/watchdog logic and immutable batch namespace contract now exist in `tickit_control/`. The candidate trigger no longer performs check-then-start skipping. Native isolated queue/lease behavior, workflow instrumentation, Spark paths and manifest-aware COPY remain PENDING; local tests do not prove AWS integration. SNS delivery remains PENDING.

## Complete-suite cost proposal

- Maximum 10 full isolated workflow executions across the matrix; no automatic full-workflow retries.
- Glue: 2 × 1 DPU Python Shell + 2 × 2 DPU Spark, with proposed 10-minute job timeouts: up to 10 × 6 × 10/60 × $0.44 = **$4.40** under that billing assumption.
- Redshift: dedicated workgroup, proposed base/max 8 RPUs, with 60 billed minutes allocated for the entire suite including setup and validation: 8 × 1 × $0.375 = **$3.00**. Confirm the limits and regional support before creation.
- Reserve **$2.60** for Lambda, S3, SNS/SQS, state transitions, logs, storage and runtime variation. Keep only tiny fixtures and short-lived artifacts; no production-sized copies.
- Proposed total approval envelope: **$10**, excluding taxes and unrelated account usage. This is a conservative estimate, not a hard billing cap: timeouts and cancellation are not instantaneous. Budget alerts alone do not stop spending. Track job/statement completion and usage before any further execution. Cloud resources do not exist yet, so these limits are not enforced.
- Reference example rates: https://aws.amazon.com/glue/pricing/ and https://aws.amazon.com/redshift/pricing/. Reconfirm current regional rates before AWS execution. Local preparation/testing incurred **$0 AWS compute cost**.

## Readiness gate

PASS: synthetic fixtures, real-schema headers, local early-date validator, validator-first isolated workflow, UTC boundary contract, local initial/changed/historical/retry/conflict/rollback assertions. **88 local tests passed with socket connections blocked**, including the original 61 tests.

PASS (local only): overlap protocol, queued delivery preservation, conditional admission, ownership/recovery and write-once namespace checks. The deployed overlap mechanism remains unsafe; native acceptance is still PENDING.

PENDING: resolved separate-account configuration, actual resource/IAM isolation, native AWS validation of the new validator, Glue/SFN/SNS execution, production business-timezone acceptance, current regional pricing confirmation, and explicit approval for AWS setup/execution.

Manifest integration now has a local end-to-end harness; see [readiness and native blockers](../../docs/MANIFEST_INTEGRATION.md). Generated `fixture-inventory.json` files are metadata for local CSVs, not executable input manifests. The generated date-validation workflow remains a legacy test artifact and must not be deployed for manifest integration.

Current controlled AWS preparation, packaged artifacts, deployment order, native gates and cleanup are in [AWS_INTEGRATION_RUNBOOK.md](../../docs/AWS_INTEGRATION_RUNBOOK.md). The earlier 88-test result above is historical, not the latest suite count.
