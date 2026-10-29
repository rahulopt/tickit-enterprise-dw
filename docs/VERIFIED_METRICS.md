# TICKIT V2 — Verified results and evidence

**As of:** 2026-10-29  
**AWS region:** `us-east-1`  
**Warehouse:** Redshift Serverless, `tickit_v2`

This report records results checked through the AWS CLI, Redshift Data API, and PyArrow during manual V2 validation. These are results from one deployment, not repeatable SLA guarantees or estimates of future performance.

## Source/Silver/Stage reconciliation

| Dataset | Parquet objects | Silver records | Redshift staging records |
|---|---:|---:|---:|
| category | 1 | 11 | 11 |
| venue | 1 | 202 | 202 |
| users | 1 | 2,000,000 | 2,000,000 |
| event | 1 | 3,000,000 | 3,000,000 |
| listing | 10 | 25,000,000 | 25,000,000 |
| sales | 10 | 22,500,000 | 22,500,000 |
| **TOTAL** | **24** | **52,500,213** | **52,500,213** |

The Silver counts came from Parquet metadata (`pyarrow.parquet.ParquetFile(...).metadata.num_rows`); staging counts from Redshift `COUNT(*)`. Silver data and staged data were independently verified after correcting the timestamp parsing bug.

## Dimension and fact checks

| Table | Rows | Other observed checks |
|---|---:|---|
| dim.date | 2,556 | Date dimension populated |
| dim.users | 2,000,000 | 2M current, 2M distinct current user IDs, 0 NULL surrogate keys |
| dim.event | 3,000,000 | 3M current, 3M distinct current event IDs, 0 NULL surrogate keys |
| fact.sales | 22,500,000 | 0 invalid rows according to event/seller/buyer key and timestamp NULL check |
| fact.listing | 25,000,000 | 0 invalid rows according to event/seller key and timestamp NULL check |

`COUNT(DISTINCT)` confirmed that source `userid` and `eventid` were unique and non-NULL before the initial dimension load. The final fact integrity check does not claim every possible referential, financial, or temporal invariant has been tested.

## Measured AWS statement/job durations

| Task | Measured duration | Evidence type |
|---|---:|---|
| Events V2 Glue, 1 DPU | 39 seconds | `get-job-runs` result: `SUCCEEDED`, `ExecutionTime=39` |
| Listings V2 Glue, 10 G.1X workers | 132 seconds | `get-job-run` result: `SUCCEEDED`, `ExecutionTime=132` |
| Staging stored procedure | 19.393193193 seconds | Redshift `DescribeStatement.Duration=19393193193` ns, status `FINISHED` |
| Facts stored procedure | 14.582355614 seconds | Redshift `DescribeStatement.Duration=14582355614` ns, status `FINISHED` |

Do not use these as proof of a two-minute *end-to-end* pipeline, or of cost reductions. Users/Category/Venue job runtimes and the corrected Sales rerun runtime were not conclusively recorded in the supplied evidence.

## Corrected timestamp parsing

**Original Bronze Sales example:** `2025-07-16 13:58:49.437077`  
**Original Bronze Listings example:** `2025-03-06 00:10:28.279758`

**Before:** `timestampFormat="yyyy-MM-dd HH:mm:ss"`  
**After:** `timestampFormat="yyyy-MM-dd HH:mm:ss.SSSSSS"`

- Sales Silver: **22,500,000** valid `saletime`, **0** NULL; ten Parquet files.
- Listings Silver: **25,000,000** valid `listtime`, **0** NULL; ten Parquet files.
- Redshift `stage.sales`: **0** NULL `saletime`.
- Redshift `stage.listing`: **0** NULL `listtime`.

Both corrected deployed scripts include fail-fast NULL timestamp checks; the GitHub copies should be synchronized with the deployed scripts before claiming code equivalence.

## Status of orchestration and SCD2

The V2 Step Functions **definition** was inspected. It contains parallel Lambda/Glue tasks, Redshift `executeStatement`/`describeStatement` polling, and SNS notifications. The `list-executions` call returned no executions at the time checked. The outcome of a separate definition validation command was not supplied. Thus an end-to-end orchestration execution is **not verified**.

The Redshift `dim.users` and `dim.event` tables contain SCD2-ready columns. The deployed `sp_load_dimensions(DATE)` procedure **only inserts business keys lacking a current row**; it does not detect attribute changes, expire old versions or insert changed versions. The deployed `sp_load_facts(DATE)` links to **current** dimension versions, not versions as of event/transaction timestamps. These are known implementation limitations rather than completed features.

## Claims intentionally excluded from README

- **80% Parquet storage reduction:** not verified on corrected V2 assets with a comparable before/after measurement.
- **60–90% overall cost reduction**, **99% Lambda savings**, **60% Python Shell savings:** no observed side-by-side cost baseline.
- **~2-minute full ETL stage** or **~2-minute end-to-end:** no successful V2 Step Functions run with recorded elapsed time.
- **Full SCD Type 2 change history / point-in-time reporting:** current dimensions and fact loading do not implement it.
- **Specific revenue leaders or customer rankings:** old README figures have not been re-validated by saved V2 analytical queries.

## Repeatable verification query

```sql
-- Run against tickit_v2, not dev
SELECT 'category' AS dataset, COUNT(*) AS rows, 0 AS null_times FROM stage.category
UNION ALL SELECT 'venue', COUNT(*), 0 FROM stage.venue
UNION ALL SELECT 'users', COUNT(*), 0 FROM stage.users
UNION ALL SELECT 'event', COUNT(*), 0 FROM stage.event
UNION ALL SELECT 'listing', COUNT(*), COUNT(*) - COUNT(listtime) FROM stage.listing
UNION ALL SELECT 'sales', COUNT(*), COUNT(*) - COUNT(saletime) FROM stage.sales;

SELECT 'users' AS dimension, COUNT(*) AS total_rows,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
       COUNT(DISTINCT CASE WHEN is_current THEN userid END) AS unique_current_ids,
       COUNT(*) - COUNT(user_key) AS null_surrogate_keys
FROM dim.users
UNION ALL
SELECT 'event', COUNT(*),
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
       COUNT(DISTINCT CASE WHEN is_current THEN eventid END),
       COUNT(*) - COUNT(event_key)
FROM dim.event;
```

**Reproducibility note:** Version the actual fixed Glue scripts and the deployed Step Functions definition separately from this report; keep AWS account identifiers and credentials out of documentation when they are unnecessary.
