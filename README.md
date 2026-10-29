# TICKIT Enterprise Data Warehouse V2 🎫

**AWS data engineering portfolio project | 52,500,213 records validated in Amazon Redshift staging | 47,500,000 fact records**

Built a ticket-resale analytics warehouse using an S3 Bronze → Parquet Silver → Redshift dimensional-model architecture. The **core V2 warehouse load has been executed and validated** in `us-east-1` as of **29 October 2026**. The six Silver ETL outputs were validated individually; a complete Step Functions execution has **not** yet been demonstrated.

> **Verification policy:** The numbers below come from recorded AWS CLI, PyArrow and Redshift SQL checks. Projected savings, untested end-to-end runtimes and unverified business-query results are intentionally excluded.

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

The deployed **Step Functions V2 definition** orchestrates six parallel ETL tasks, waits for Glue completion (`glue:startJobRun.sync`), then submits and polls Redshift stored-procedure statements (`staging → dimensions → facts`), and publishes SNS success/failure notifications. **Its configuration has been inspected; a successful full execution has not been verified.** Do not describe this as an end-to-end benchmark.

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

## Measured processing results (29 October 2026)

| Operation | Observed AWS result | Scope |
|---|---:|---|
| Events Glue Python Shell | **39 seconds**, **1 DPU** | Individual `tickit-events-etl-v2` job (3M rows) |
| Listings Glue Spark | **132 seconds**, **10 × G.1X workers** | Individual successful Listings V2 rerun (25M rows) |
| Redshift staging stored procedure | **19.39 seconds** | `DescribeStatement.Duration`, all 6 staging loads |
| Redshift facts stored procedure | **14.58 seconds** | `DescribeStatement.Duration`, Sales + Listings facts |

These are **individual execution measurements**, not a sum or an end-to-end pipeline runtime. AWS Redshift Data API reports `Duration` in nanoseconds, converted here to seconds. No reproducible cost-savings percentage, complete Bronze-to-Gold wall-clock runtime, or full storage-reduction percentage has been established for V2.

## Data-quality issue found and fixed

Both Sales and Listings CSVs contain timestamps with six fractional-second digits, such as `2025-07-16 13:58:49.437077`. The original Glue Spark scripts specified `timestampFormat="yyyy-MM-dd HH:mm:ss"`, causing timestamp values to parse as NULL. V2 scripts were updated to:

```python
timestampFormat="yyyy-MM-dd HH:mm:ss.SSSSSS"
```

Both scripts now fail when parsed transaction timestamps are NULL. The corrected Silver output and Redshift staging were verified to contain **0 NULL transaction timestamps** across all **47.5 million** Sales + Listings rows. The code in this repository must be synchronized with those deployed V2 versions (see [deployment sync notes](docs/VERIFIED_METRICS.md)).

## Data model and semantics

- `dim.users` and `dim.event` use Redshift `IDENTITY` surrogate keys (`user_key`, `event_key`) and have SCD2-supporting columns: `effective_date`, `expiry_date`, `is_current`.
- The deployed `sp_load_dimensions(DATE)` currently implements **initial/idempotent current-version insertion** (`NOT EXISTS`); **automatic SCD Type 2 attribute-change detection, old-version expiry and new-version creation have not been implemented or verified**.
- The deployed `sp_load_facts(DATE)` resolves the **current** event, seller and buyer surrogate keys and skips existing `salesid` / `listid`. Its date argument currently labels the run, **not** a date filter. It does **not** perform historical point-in-time SCD2 lookups.
- `dateid` is the source **event-date key**, not the actual transaction date. `saletime` and `listtime` carry transaction timestamps (observed source values in 2023–2025); do not infer transaction dates from `dateid`.

## Business questions enabled (queries, not claimed results)

The warehouse supports analyses such as revenue by venue and event category, high-value buyers, tickets listed versus sold, and transaction trends. **No specific top venue, top buyer, category revenue or cost-saving outcome is claimed here without independently run and saved query evidence.**

## AWS implementation

- **Region:** `us-east-1` (N. Virginia)
- **Storage:** Amazon S3 (Bronze CSV, Silver Parquet/Snappy)
- **ETL:** AWS Lambda, Glue Python Shell, Glue Spark
- **Orchestration definition:** AWS Step Functions V2 (`tickit-nightly-pipeline-v2`), SNS
- **Warehouse:** Redshift Serverless workgroup `ent-dw-1`, separate database `tickit_v2`
- **Design:** staging layer, denormalized event/user dimensions, role-playing user dimension for buyer and seller, fact tables with surrogate key lookups
- **Isolation:** V2 uses dedicated S3 `tickit-v2/` prefixes and `tickit_v2` database; the original `dev` database is not the V2 target.

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

For deployment, use placeholders for AWS account-specific ARNs, S3 bucket IDs and IAM roles. Never commit secrets or raw credentials. The supplied sync script exports a sanitized Step Functions template with `ACCOUNT_ID` placeholders; replace those placeholders before deployment. Compare Git-tracked Glue and SQL definitions with deployed `tickit-v2` resources before asserting reproducibility.

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

**Verified:** individual ETL results; Silver row counts; corrected transaction timestamps; Redshift staging procedure execution and row counts; initial dimension load and current-key uniqueness; fact procedure execution, row counts and missing-key/timestamp checks.

**Not yet verified / incomplete:** a full V2 Step Functions execution, schedule/trigger integration, automatic SCD2 updates, point-in-time dimensional joins, measured cost savings, and deployed-versus-repository parity for every SQL artifact. A syntactically valid Step Functions definition is not proof of a successful execution.

See [verified metrics and evidence](docs/VERIFIED_METRICS.md) for the underlying measurements and honest limitations.

## Redshift V2 SQL Initialization

The date dimension is initialized separately before analytical
queries that depend on source date IDs.

1. `sql/05_load_dim_date.sql` populates `dim.date` using the source
   date-ID mapping: IDs 1827–4382, covering 2008-01-01 to 2014-12-30.
   The verified deployment contains 2,556 date records.

2. `sql/06_stored_procedures.sql` is a repository SQL reference.
   Before using it for redeployment, reconcile its definitions with
   the currently deployed V2 stored procedures.

The Step Functions workflow does not currently initialize `dim.date`.
The V2 warehouse was initialized manually before loading dimensions
and facts. Full SCD Type 2 change tracking remains future work.

### Analytical Views

3. `sql/07_analytical_views.sql` contains the repository's analytical
   views definitions. Run it after initializing the date dimension
   and deploying the required stored procedures.

   Verify compatibility with the deployed V2 schema before execution.
