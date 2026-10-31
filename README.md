# TICKIT Enterprise Data Warehouse V2 🎫

**AWS data engineering portfolio project | 52,500,213 records validated in Amazon Redshift staging | 47,500,000 fact records**

A ticket-resale analytics warehouse built with S3 Bronze → Parquet Silver → Amazon Redshift. **Daily SCD2 is deployed and natively tested**, with the latest warehouse verification completed on **31 October 2026**. The six ETL outputs were validated individually; the complete six-branch workflow remains unverified.

> **Verification policy:** The numbers below come from recorded AWS CLI, PyArrow and Redshift SQL checks. Projected savings, untested end-to-end runtimes and unverified business-query results are intentionally excluded.

**31 October 2026 update:** Daily SCD2 procedures and the workflow definition are deployed. Native SCD2 fixtures and a warehouse-only Step Functions rerun against the existing Silver files passed; all 47.5M facts were preserved. The observed warehouse statement took **39.064 seconds**, excluding Bronze-to-Silver ETL. See the [deployment evidence and historical-data limitation](docs/SCD2_DEPLOYMENT_2026-10-31.md).

## Architecture

```text
Source CSVs (S3 Bronze)
           │
           ▼
  ETL (two Lambda + four AWS Glue jobs)
  ├─ category / venue              → Lambda + Parquet
  ├─ users / events                 → Glue Python Shell + Parquet
  └─ listing / sales                → Glue Spark + Snappy Parquet
           │
           ▼
 S3 Silver: tickit-v2/silver/{dataset}/
           │
           ▼
 Amazon Redshift Serverless: database tickit_v2
  ├─ stage: category, venue, users, event, listing, sales
  ├─ dim: date, users, event
  └─ fact: listing, sales
```

The deployed **Step Functions V2 definition** orchestrates six parallel ETL tasks, waits for Glue completion (`glue:startJobRun.sync`), then submits and polls one atomic Redshift warehouse call (`staging → dimensions → facts`), and publishes SNS success/failure notifications. **Its configuration has been inspected; a successful full execution has not been verified.** Do not describe this as an end-to-end benchmark.

## Verified dataset and warehouse counts

| Dataset | Silver Parquet files | Redshift `stage` rows | Additional verification |
|---|---:|---:|---|
| Category | 1 | 11 | Redshift Category COPY smoke test passed |
| Venue | 1 | 202 | Source schema and row count checked |
| Users | 1 | 2,000,000 | Unique, non-null `userid` |
| Events | 1 | 3,000,000 | Unique, non-null `eventid`; enriched 13-column Parquet |
| Listings | 10 | 25,000,000 | **0 NULL `listtime`** in Silver and Redshift staging |
| Sales | 10 | 22,500,000 | **0 NULL `saletime`** in Silver and Redshift staging |
| **Total** | **24** | **52,500,213** | **All six staging counts reconciled** |

### Dimensional warehouse

| Redshift table | Verified rows | Validation |
|---|---:|---|
| `dim.date` | 2,556 | Preloaded date dimension; date-key mapping previously verified |
| `dim.users` | 2,000,000 | 2M current rows; 2M unique current `userid`; 0 NULL `user_key` |
| `dim.event` | 3,000,000 | 3M current rows; 3M unique current `eventid`; 0 NULL `event_key` |
| `fact.listing` | 25,000,000 | 0 rows with missing event/seller key or NULL `listtime` |
| `fact.sales` | 22,500,000 | 0 rows with missing event/seller/buyer key or NULL `saletime` |
| **Facts combined** | **47,500,000** | **Both fact counts match staging** |

**Note:** Stage, dimension and fact counts represent different layers of the same data; do not add them together as distinct marketplace records.

## Measured processing results

| Operation | Observed AWS result | Scope |
|---|---:|---|
| Events Glue Python Shell | **39 seconds**, **1 DPU** | 29 October: individual Events job (3M rows) |
| Listings Glue Spark | **132 seconds**, **10 × G.1X workers** | Individual successful Listings V2 rerun (25M rows) |
| Redshift staging stored procedure | **19.39 seconds** | `DescribeStatement.Duration`, all 6 staging loads |
| Redshift facts stored procedure | **14.58 seconds** | 29 October: Sales + Listings facts |
| Atomic warehouse load with daily SCD2 | **39.064 seconds** | 31 October: staging COPY, dimensions and facts; existing Silver files |

These are **individual execution measurements**, not a sum or an end-to-end pipeline runtime. AWS Redshift Data API reports `Duration` in nanoseconds, converted here to seconds. No reproducible cost-savings percentage, complete Bronze-to-Gold wall-clock runtime, or full storage-reduction percentage has been established for V2.

## Data-quality issue found and fixed

Both Sales and Listings CSVs contain timestamps with six fractional-second digits, such as `2025-07-16 13:58:49.437077`. The original Glue Spark scripts specified `timestampFormat="yyyy-MM-dd HH:mm:ss"`, causing timestamp values to parse as NULL. V2 scripts were updated to:

```python
timestampFormat="yyyy-MM-dd HH:mm:ss.SSSSSS"
```

Both scripts now fail when parsed transaction timestamps are NULL. The corrected Silver output and Redshift staging were verified to contain **0 NULL transaction timestamps** across all **47.5 million** Sales + Listings rows. The repository scripts include this parsing fix and the NULL checks. Full deployed-versus-repository parity has not been verified (see [verification notes](docs/VERIFIED_METRICS.md)).

## Data model and semantics

- `dim.users` and `dim.event` use Redshift `IDENTITY` surrogate keys (`user_key`, `event_key`) and have SCD2-supporting columns: `effective_date`, `expiry_date`, `is_current`.
- Repository loaders implement daily SCD2 with half-open validity intervals, null-safe change detection, temporal integrity checks, and transaction-date fact lookups. Changed same-day and out-of-order snapshots are rejected; identical retries preserve surrogate keys.
- The workflow binds the input `load_date` and invokes `sp_load_warehouse` to load staging, dimensions, and facts in one atomic transaction. Existing facts retain their assigned keys. Missing historical coverage fails the load.
- The daily SCD2 procedures were deployed and verified on 31 October 2026; read-back procedure bodies match the repository. Existing pre-baseline historical facts retain their original assignments and still need authoritative history for reconciliation. See [SCD2 loading instructions](docs/SCD2.md) and the [deployment evidence](docs/SCD2_DEPLOYMENT_2026-10-31.md).
- `dateid` is the source **event-date key**, not the actual transaction date. `saletime` and `listtime` carry transaction timestamps (observed source values in 2023–2025); do not infer transaction dates from `dateid`.

## Business questions enabled (queries, not claimed results)

The warehouse supports analyses such as revenue by venue and event category, high-value buyers, tickets listed versus sold, and transaction trends. **No specific top venue, top buyer, category revenue or cost-saving outcome is claimed here without independently run and saved query evidence.**

## AWS implementation

- **Region:** `us-east-1` (N. Virginia)
- **Storage:** Amazon S3 (Bronze CSV, Silver Parquet/Snappy)
- **ETL:** AWS Lambda, Glue Python Shell, Glue Spark
- **Orchestration:** configurable AWS Step Functions workflow and SNS notifications
- **Warehouse:** configurable Redshift Serverless workgroup and database
- **Design:** staging layer, denormalized event/user dimensions, role-playing user dimension for buyer and seller, fact tables with surrogate key lookups
- **Isolation:** V2 writes Silver outputs under `tickit-v2/silver/` and uses the `tickit_v2` database; source CSVs remain under `bronze/`. The original `dev` database is not the V2 target.

## Repository layout

```text
README.md
docs/
  VERIFIED_METRICS.md          # measured evidence and limitations
  architecture.md             # current design and implementation limits
lambda/
glue/
  users_etl.py
  events_etl.py
  listings_etl.py
  sales_etl.py
sql/
stepfunctions/
  tickit_pipeline_v2.template.json # single workflow source; render before deployment
  config.example.json             # example deployment configuration
tools/
  tickit_workflow.py               # renders the deployable workflow definition
tests/
```

## Workflow configuration

The single workflow source is `stepfunctions/tickit_pipeline_v2.template.json`. It uses `{{CONFIG_KEY}}` placeholders. Create `stepfunctions/config.local.json` from the example configuration and replace its `REPLACE_WITH_...` values, or extract settings from an existing deployed definition. Then render it:

```bash
python3 tools/tickit_workflow.py render
```

This writes `stepfunctions/tickit_pipeline_v2.rendered.json`; local configuration and rendered output are ignored by Git. Rendering does not deploy or start the workflow. See the [configuration and deployment instructions](docs/STEP_FUNCTIONS_PARAMETERIZATION.md) for extraction, AWS validation, and deployment steps.

Infrastructure identifiers are supplied through local configuration and runtime settings. The repository includes placeholder examples; account IDs, bucket names, deployment ARNs and local backups stay out of the published files.

Copy `.env.example` to `.env`, fill in your values, and export them for local commands:

```bash
cp .env.example .env
# Edit .env before loading it.
set -a
. ./.env
set +a
```

| Runtime | Required configuration |
|---|---|
| Category/Venue Lambda | `TICKIT_BUCKET` environment variable |
| S3 marker Lambda | `TICKIT_BUCKET` and `STATE_MACHINE_ARN` environment variables |
| Users/Events Glue Python Shell | `--TICKIT_BUCKET` job argument; local runs may use `TICKIT_BUCKET` |
| Listings/Sales Glue Spark | `--TICKIT_BUCKET` job argument |
| Workflow | Resource settings in Git-ignored `stepfunctions/config.local.json`; runtime `load_date` input |

The workflow template supplies the bucket argument to all four Glue jobs. AWS SDKs use the configured region and normal credential chain. The Bronze/Silver paths and warehouse schema names are the documented dataset contract.

Render the workflow database grants for your execution identity:

```bash
python3 tools/tickit_workflow.py render-permissions --db-user "$TICKIT_WORKFLOW_DB_USER"
```

Apply the generated SQL after the stored procedures. Rendered SQL, rendered workflows, `.env`, local configuration and deployment backups are Git-ignored. Upload the updated Lambda/Glue scripts and configure their runtime settings when deploying this configuration cleanup; publishing the source does not redeploy AWS resources.

## Local checks

Offline tests were checked with Python 3.11 and require `pytest`, `boto3`, and `pandas`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pytest boto3 pandas
python -m pytest -q
python tools/check_public_config.py
```

Tests stub Parquet output and AWS clients; they do not need AWS credentials or PyArrow. Running the pandas ETL scripts requires PyArrow for Parquet output; Spark jobs require the Glue/Spark runtime. These tests do not establish live AWS integration or Redshift SQL execution.

See the [architecture details](docs/architecture.md) for loading behavior and implementation limits.

## Validation SQL

```sql
-- Redshift Serverless, database tickit_v2
SELECT 'sales' AS dataset, COUNT(*) AS rows,
       COUNT(*) - COUNT(saletime) AS null_times
FROM stage.sales
UNION ALL
SELECT 'listing', COUNT(*), COUNT(*) - COUNT(listtime)
FROM stage.listing;

SELECT COUNT(*) AS sales_rows,
       SUM(CASE WHEN event_key IS NULL OR seller_key IS NULL OR buyer_key IS NULL
                     OR saletime IS NULL THEN 1 ELSE 0 END) AS invalid_rows
FROM fact.sales;

SELECT COUNT(*) AS listing_rows,
       SUM(CASE WHEN event_key IS NULL OR seller_key IS NULL
                     OR listtime IS NULL THEN 1 ELSE 0 END) AS invalid_rows
FROM fact.listing;
```

## Verification status and next engineering work

**Verified:** individual ETL results; Silver row counts; corrected transaction timestamps; initial dimension/fact counts and missing-key/timestamp checks; native daily SCD2 changes, temporal joins, retry/rejection behavior and atomic rollback; deployed procedure-body parity; warehouse-only Step Functions execution with the real workflow role against the existing production Silver files.

**Not yet verified / incomplete:** the complete six-branch V2 ETL execution, schedule/trigger integration, historical dimension backfill for the pre-existing facts, measured cost savings, and deployed-versus-repository parity for artifacts outside the checked SCD2 procedures/workflow. A syntactically valid Step Functions definition is not proof of a successful execution.

See [verified metrics and evidence](docs/VERIFIED_METRICS.md) for the underlying measurements and honest limitations.

## Redshift V2 SQL Initialization

The date dimension is initialized separately before analytical
queries that depend on source date IDs.

1. `sql/05_load_dim_date.sql` populates `dim.date` using the source
   date-ID mapping: IDs 1827–4382, covering 2008-01-01 to 2014-12-30.
   The verified deployment contains 2,556 date records.

2. `sql/06_stored_procedures.sql` contains the daily SCD2 procedures
   deployed on 31 October 2026. Render `sql/08_workflow_permissions.sql`
   for the configured workflow database identity and apply the generated SQL. See the
   [upgrade and bootstrap instructions](docs/SCD2.md).

The Step Functions workflow does not currently initialize `dim.date`.
The V2 warehouse was initialized manually before loading dimensions
and facts. Daily SCD2 loading is deployed and natively verified; the complete six-branch ETL execution remains unverified.

### Analytical Views

3. `sql/07_analytical_views.sql` contains the repository's analytical
   views definitions. Run it after initializing the date dimension
   and deploying the required stored procedures.

   Verify compatibility with the deployed V2 schema before execution.
